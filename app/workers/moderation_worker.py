import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Any

import numpy as np
from aiokafka import AIOKafkaConsumer
from asyncpg import exceptions as pg_exc

from app.clients.postgres import get_pg_connection
from app.services.model import load_or_train_model


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
        model_path: str = "model.pkl",
    ) -> None:
        """Инициализирует consumer и загружает ML-модель."""
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )
        self.topic = topic
        self.group_id = group_id or os.getenv("KAFKA_MODERATION_GROUP_ID", "moderation-worker")
        self.model = load_or_train_model(model_path)
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
        )

    async def run(self) -> None:
        """Запускает бесконечный цикл чтения сообщений из Kafka."""
        await self.consumer.start()
        logger.info(
            "Moderation worker started topic=%s bootstrap_servers=%s group_id=%s",
            self.topic,
            self.bootstrap_servers,
            self.group_id,
        )
        try:
            async for message in self.consumer:
                await self._handle_message(message.value)
        finally:
            await self.consumer.stop()
            logger.info("Moderation worker stopped")

    async def _handle_message(self, payload: bytes) -> None:
        """Обрабатывает одно сообщение и обновляет статус задачи в БД."""
        item_id = self._extract_item_id(payload)
        if item_id is None:
            logger.warning("Skipping invalid message payload=%s", payload)
            return

        logger.info("Processing moderation request item_id=%s", item_id)

        try:
            advertisement = await self._load_advertisement(item_id)
        except Exception:
            logger.exception("Failed to read advertisement item_id=%s", item_id)
            await self._mark_failed(item_id, "Database read failed")
            return

        if advertisement is None:
            await self._mark_failed(item_id, "Advertisement not found")
            return

        try:
            is_violation, probability = self._predict(advertisement)
        except Exception:
            logger.exception("Prediction failed item_id=%s", item_id)
            await self._mark_failed(item_id, "Prediction failed")
            return

        try:
            task_id = await self._mark_completed(item_id, is_violation, probability)
        except Exception:
            logger.exception("Failed to update moderation result item_id=%s", item_id)
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
        features = np.array(
            [[
                1.0 if advertisement.is_verified_seller else 0.0,
                min(advertisement.images_qty, 10) / 10.0,
                len(advertisement.description) / 1000.0,
                advertisement.category / 100.0,
            ]],
            dtype=float,
        )
        probability = float(self.model.predict_proba(features)[0][1])
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
        except pg_exc.PostgresError:
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


async def main() -> None:
    """Точка входа: создает и запускает moderation worker."""
    worker = ModerationWorker()
    await worker.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
