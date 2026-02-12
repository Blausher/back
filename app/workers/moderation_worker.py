import asyncio
from datetime import datetime, timezone
import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.clients.model import ModelClient
from app.clients.postgres import get_pg_connection


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AdvertisementRow:
    item_id: int
    seller_id: int
    is_verified_seller: bool
    description: str
    category: int
    images_qty: int


class ModerationWorker:
    """Kafka consumer, который обрабатывает задачи модерации объявлений."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str = "moderation",
        group_id: str | None = None,
        dlq_topic: str = "moderation_dlq",
        model_path: str = "model.pkl",
    ) -> None:
        """Инициализирует consumer и загружает ML-модель."""
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )
        self.topic = topic
        self.group_id = group_id or os.getenv("KAFKA_MODERATION_GROUP_ID", "moderation-worker")
        self.dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", dlq_topic)
        self.model_client = ModelClient(model_path=model_path)
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

    async def run(self) -> None:
        """Запускает бесконечный цикл чтения сообщений из Kafka."""
        producer_started = False
        consumer_started = False
        try:
            await self.producer.start()
            producer_started = True
            await self.consumer.start()
            consumer_started = True
            logger.info(
                "Moderation worker started topic=%s dlq_topic=%s bootstrap_servers=%s group_id=%s",
                self.topic,
                self.dlq_topic,
                self.bootstrap_servers,
                self.group_id,
            )
            async for message in self.consumer:
                await self._handle_message(message.value)
        finally:
            if consumer_started:
                await self.consumer.stop()
            if producer_started:
                await self.producer.stop()
            logger.info("Moderation worker stopped")

    async def _handle_message(self, payload: bytes) -> None:
        """Обрабатывает одно сообщение и обновляет статус задачи в БД."""
        item_id = self._extract_item_id(payload)
        if item_id is None:
            logger.warning("Skipping invalid message payload=%s", payload)
            await self._send_to_dlq(
                error_message="Invalid message payload",
                payload=payload,
            )
            return

        logger.info("Processing moderation request item_id=%s", item_id)

        try:
            advertisement = await self._load_advertisement(item_id)
        except Exception as exc:
            logger.exception("Failed to read advertisement item_id=%s", item_id)
            await self._handle_processing_error(
                item_id=item_id,
                error_message=self._compose_error_message("Database read failed", exc),
                payload=payload,
            )
            return

        if advertisement is None:
            await self._handle_processing_error(
                item_id=item_id,
                error_message="Advertisement not found",
                payload=payload,
            )
            return

        try:
            is_violation, probability = self._predict(advertisement)
        except Exception as exc:
            logger.exception("Prediction failed item_id=%s", item_id)
            await self._handle_processing_error(
                item_id=item_id,
                error_message=self._compose_error_message("Prediction failed", exc),
                payload=payload,
            )
            return

        try:
            task_id = await self._mark_completed(item_id, is_violation, probability)
        except Exception as exc:
            logger.exception("Failed to update moderation result item_id=%s", item_id)
            await self._handle_processing_error(
                item_id=item_id,
                error_message=self._compose_error_message("Failed to update moderation result", exc),
                payload=payload,
            )
            return

        if task_id is None:
            logger.warning("No pending moderation task for item_id=%s", item_id)
            return

        logger.info(
            "Moderation completed task_id=%s item_id=%s is_violation=%s probability=%s",
            task_id,
            item_id,
            is_violation,
            probability,
        )

    async def _handle_processing_error(
        self,
        item_id: int,
        error_message: str,
        payload: bytes,
    ) -> None:
        await self._mark_failed(item_id, error_message)
        await self._send_to_dlq(error_message, payload)

    @staticmethod
    def _compose_error_message(base_message: str, exc: Exception | None) -> str:
        if exc is None:
            return base_message
        details = str(exc).strip()
        if not details:
            return base_message
        return f"{base_message}: {details}"

    @staticmethod
    def _extract_item_id(payload: Any) -> int | None:
        """Достает и валидирует item_id из JSON payload Kafka-сообщения."""
        if not isinstance(payload, (bytes, bytearray)):
            return None
        try:
            decoded = payload.decode("utf-8")
            body = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(body, dict):
            return None
        item_id = body.get("item_id")
        if not isinstance(item_id, int) or item_id < 0:
            return None
        return item_id

    async def _load_advertisement(self, item_id: int) -> AdvertisementRow | None:
        """Читает объявление и данные продавца из БД."""
        query = """
            SELECT
                a.item_id,
                a.seller_id,
                u.is_verified_seller,
                a.description,
                a.category,
                a.images_qty
            FROM advertisements AS a
            JOIN users AS u ON u.id = a.seller_id
            WHERE a.item_id = $1
        """
        async with get_pg_connection() as connection:
            row = await connection.fetchrow(query, item_id)
        if row is None:
            return None
        return AdvertisementRow(
            item_id=row["item_id"],
            seller_id=row["seller_id"],
            is_verified_seller=row["is_verified_seller"],
            description=row["description"],
            category=row["category"],
            images_qty=row["images_qty"],
        )

    def _predict(self, advertisement: AdvertisementRow) -> tuple[bool, float]:
        """Считает вероятность нарушения и бинарный итог по порогу 0.5."""
        probability = self.model_client.predict_probability(advertisement)
        is_violation = probability >= 0.5
        return is_violation, probability

    async def _mark_completed(self, item_id: int, is_violation: bool, probability: float) -> int | None:
        """Переводит старейшую pending-задачу в completed и пишет результат."""
        query = """
            WITH pending_task AS (
                SELECT id
                FROM moderation_results
                WHERE item_id = $1 AND status = 'pending'
                ORDER BY id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE moderation_results AS mr
            SET
                status = 'completed',
                is_violation = $2,
                probability = $3,
                error_message = NULL,
                processed_at = NOW()
            FROM pending_task
            WHERE mr.id = pending_task.id
            RETURNING mr.id
        """
        async with get_pg_connection() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(query, item_id, is_violation, probability)
        if row is None:
            return None
        return int(row["id"])

    async def _mark_failed(self, item_id: int, error_message: str) -> int | None:
        """Переводит pending-задачу в failed и пишет текст ошибки."""
        query = """
            WITH pending_task AS (
                SELECT id
                FROM moderation_results
                WHERE item_id = $1 AND status = 'pending'
                ORDER BY id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE moderation_results AS mr
            SET
                status = 'failed',
                is_violation = NULL,
                probability = NULL,
                error_message = $2,
                processed_at = NOW()
            FROM pending_task
            WHERE mr.id = pending_task.id
            RETURNING mr.id
        """
        try:
            async with get_pg_connection() as connection:
                async with connection.transaction():
                    row = await connection.fetchrow(query, item_id, error_message[:1000])
        except Exception:
            logger.exception("Failed to persist failed status item_id=%s", item_id)
            return None
        if row is None:
            return None
        task_id = int(row["id"])
        logger.info(
            "Moderation failed task_id=%s item_id=%s error=%s",
            task_id,
            item_id,
            error_message,
        )
        return task_id

    async def _send_to_dlq(
        self,
        error_message: str,
        payload: bytes | bytearray | None,
    ) -> None:
        """Отправляет сообщение об ошибке в DLQ топик."""
        original_message: dict[str, Any]
        retry_count = 1
        payload_text = ""
        if isinstance(payload, (bytes, bytearray)):
            payload_text = payload.decode("utf-8", errors="replace")
        elif payload is not None:
            payload_text = str(payload)

        try:
            parsed_payload = json.loads(payload_text) if payload_text else {}
        except json.JSONDecodeError:
            parsed_payload = {"raw_payload": payload_text}

        if isinstance(parsed_payload, dict):
            original_message = parsed_payload
            raw_retry_count = parsed_payload.get("retry_count")
            if isinstance(raw_retry_count, int) and raw_retry_count >= 0:
                retry_count = raw_retry_count + 1
        else:
            original_message = {"raw_payload": payload_text}

        timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        message = {
            "original_message": original_message,
            "error": error_message,
            "timestamp": timestamp,
            "retry_count": retry_count,
        }

        try:
            await self.producer.send_and_wait(self.dlq_topic, message)
        except Exception:
            logger.exception(
                "Failed to publish message to DLQ topic=%s",
                self.dlq_topic,
            )


async def main() -> None:
    """Точка входа: создает и запускает moderation worker."""
    worker = ModerationWorker()
    await worker.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
