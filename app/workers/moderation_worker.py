import asyncio
from datetime import datetime, timezone
import json
import logging
import os
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.clients.model import ModelClient, ModelNotLoadedError
from app.models.advertisement import Advertisement
from app.observability.metrics import PREDICTIONS_TOTAL
from app.repositories.advertisements import AdvertisementRepository
from app.repositories.moderation_results import ModerationResultRepository
from app.repositories.processed_events import ProcessedEventRepository


logger = logging.getLogger(__name__)
_UNSET = object()


class ModerationWorker:
    """Kafka consumer, который обрабатывает задачи модерации объявлений."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str = "moderation",
        group_id: str | None = None,
        dlq_topic: str = "moderation_dlq",
        model_path: str = "model.pkl",
        max_attempts: int = 3,
        retry_delay_seconds: float = 5.0,
        advertisement_repo: AdvertisementRepository | None = None,
        moderation_result_repo: ModerationResultRepository | None = None,
        processed_event_repo: ProcessedEventRepository | None = None,
    ) -> None:
        """Инициализирует consumer и загружает ML-модель."""
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )
        self.topic = topic
        self.group_id = group_id or os.getenv("KAFKA_MODERATION_GROUP_ID", "moderation-worker")
        self.dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", dlq_topic)
        self.max_attempts = self._parse_max_attempts(
            os.getenv("MODERATION_MAX_ATTEMPTS"),
            default=max_attempts,
        )
        self.retry_delay_seconds = self._parse_retry_delay_seconds(
            os.getenv("MODERATION_RETRY_DELAY_SECONDS"),
            default=retry_delay_seconds,
        )
        self.model_client = ModelClient(model_path=model_path)
        self.advertisement_repo = advertisement_repo or AdvertisementRepository()
        self.moderation_result_repo = moderation_result_repo or ModerationResultRepository()
        self.processed_event_repo = processed_event_repo or ProcessedEventRepository()
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
        initial_retry_count = self._extract_retry_count(payload)
        if initial_retry_count >= self.max_attempts:
            await self._handle_processing_error(
                item_id=item_id,
                error_message="Retry limit exceeded before processing",
                payload=payload,
                retry_count=initial_retry_count,
            )
            return
        pending_task_id: int | None | object = _UNSET
        first_time: bool | None = None

        for attempt in range(initial_retry_count + 1, self.max_attempts + 1):
            if pending_task_id is _UNSET:
                try:
                    pending_task_id = await self.moderation_result_repo.get_pending_task_id(item_id)
                except Exception as exc:
                    logger.exception("Failed to read pending moderation task item_id=%s", item_id)
                    should_retry = await self._retry_or_fail(
                        item_id=item_id,
                        payload=payload,
                        attempt=attempt,
                        error_message=self._compose_error_message("Pending task lookup failed", exc),
                        temporary=True,
                    )
                    if should_retry:
                        continue
                    return

            if pending_task_id is None:
                logger.warning("No pending moderation task for item_id=%s", item_id)
                return

            if first_time is None:
                event_id = f"moderation:{item_id}:{pending_task_id}"
                try:
                    first_time = await self.processed_event_repo.register_processing(
                        event_id=event_id,
                        item_id=item_id,
                        moderation_result_id=pending_task_id,
                    )
                except Exception as exc:
                    logger.exception(
                        "Failed to persist idempotency key item_id=%s event_id=%s",
                        item_id,
                        event_id,
                    )
                    should_retry = await self._retry_or_fail(
                        item_id=item_id,
                        payload=payload,
                        attempt=attempt,
                        error_message=self._compose_error_message("Idempotency persistence failed", exc),
                        temporary=True,
                    )
                    if should_retry:
                        continue
                    return

            if not first_time:
                logger.info(
                    "Duplicate moderation event skipped item_id=%s task_id=%s event_id=%s",
                    item_id,
                    pending_task_id,
                    event_id,
                )
                return

            try:
                advertisement = await self.advertisement_repo.select_advert(item_id)
            except Exception as exc:
                logger.exception("Failed to read advertisement item_id=%s", item_id)
                should_retry = await self._retry_or_fail(
                    item_id=item_id,
                    payload=payload,
                    attempt=attempt,
                    error_message=self._compose_error_message("Database read failed", exc),
                    temporary=True,
                )
                if should_retry:
                    continue
                return

            if advertisement is None:
                await self._handle_processing_error(
                    item_id=item_id,
                    error_message="Advertisement not found",
                    payload=payload,
                    retry_count=attempt,
                )
                return

            try:
                is_violation, probability = self._predict(advertisement)
            except Exception as exc:
                logger.exception("Prediction failed item_id=%s", item_id)
                should_retry = await self._retry_or_fail(
                    item_id=item_id,
                    payload=payload,
                    attempt=attempt,
                    error_message=self._compose_error_message("Prediction failed", exc),
                    temporary=self._is_temporary_prediction_error(exc),
                )
                if should_retry:
                    continue
                return

            try:
                task_id = await self.moderation_result_repo.mark_completed(item_id, is_violation, probability)
            except Exception as exc:
                logger.exception("Failed to update moderation result item_id=%s", item_id)
                should_retry = await self._retry_or_fail(
                    item_id=item_id,
                    payload=payload,
                    attempt=attempt,
                    error_message=self._compose_error_message("Failed to update moderation result", exc),
                    temporary=True,
                )
                if should_retry:
                    continue
                return

            if task_id is None:
                logger.warning("No pending moderation task for item_id=%s", item_id)
                return

            logger.info(
                "Moderation completed task_id=%s item_id=%s is_violation=%s probability=%s attempts=%s",
                task_id,
                item_id,
                is_violation,
                probability,
                attempt,
            )
            return

    async def _handle_processing_error(
        self,
        item_id: int,
        error_message: str,
        payload: bytes,
        retry_count: int | None = None,
    ) -> None:
        try:
            task_id = await self.moderation_result_repo.mark_failed(item_id, error_message)
        except Exception:
            logger.exception("Failed to persist failed status item_id=%s", item_id)
        else:
            if task_id is not None:
                logger.info(
                    "Moderation failed task_id=%s item_id=%s error=%s",
                    task_id,
                    item_id,
                    error_message,
                )
        await self._send_to_dlq(error_message, payload, retry_count=retry_count)

    async def _retry_or_fail(
        self,
        item_id: int,
        payload: bytes,
        attempt: int,
        error_message: str,
        temporary: bool,
    ) -> bool:
        if temporary and attempt < self.max_attempts:
            logger.warning(
                "Temporary moderation error item_id=%s attempt=%s/%s retry_in=%ss error=%s",
                item_id,
                attempt,
                self.max_attempts,
                self.retry_delay_seconds,
                error_message,
            )
            await asyncio.sleep(self.retry_delay_seconds)
            return True

        await self._handle_processing_error(
            item_id=item_id,
            error_message=error_message,
            payload=payload,
            retry_count=attempt,
        )
        return False

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

    def _predict(self, advertisement: Advertisement) -> tuple[bool, float]:
        """Считает вероятность нарушения и бинарный итог по порогу 0.5."""
        probability = self.model_client.predict_probability(advertisement)
        is_violation = probability >= 0.5
        PREDICTIONS_TOTAL.labels(result="violation" if is_violation else "no_violation").inc()
        return is_violation, probability

    @staticmethod
    def _is_temporary_prediction_error(exc: Exception) -> bool:
        return isinstance(exc, ModelNotLoadedError)

    async def _send_to_dlq(
        self,
        error_message: str,
        payload: bytes | bytearray | None,
        retry_count: int | None = None,
    ) -> None:
        """Отправляет сообщение об ошибке в DLQ топик."""
        original_message: dict[str, Any]
        resolved_retry_count = retry_count if retry_count is not None else 1
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
            if retry_count is None and isinstance(raw_retry_count, int) and raw_retry_count >= 0:
                resolved_retry_count = raw_retry_count + 1
        else:
            original_message = {"raw_payload": payload_text}

        timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        message = {
            "original_message": original_message,
            "error": error_message,
            "timestamp": timestamp,
            "retry_count": resolved_retry_count,
        }

        try:
            await self.producer.send_and_wait(self.dlq_topic, message)
        except Exception:
            logger.exception(
                "Failed to publish message to DLQ topic=%s",
                self.dlq_topic,
            )

    @staticmethod
    def _extract_retry_count(payload: Any) -> int:
        if not isinstance(payload, (bytes, bytearray)):
            return 0
        try:
            decoded = payload.decode("utf-8")
            body = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return 0
        if not isinstance(body, dict):
            return 0
        retry_count = body.get("retry_count")
        if not isinstance(retry_count, int) or retry_count < 0:
            return 0
        return retry_count

    @staticmethod
    def _parse_max_attempts(raw_value: str | None, default: int) -> int:
        try:
            parsed = int(raw_value) if raw_value is not None else default
        except (TypeError, ValueError):
            return default
        return max(1, parsed)

    @staticmethod
    def _parse_retry_delay_seconds(raw_value: str | None, default: float) -> float:
        try:
            parsed = float(raw_value) if raw_value is not None else default
        except (TypeError, ValueError):
            return default
        return max(0.0, parsed)


async def main() -> None:
    """Точка входа: создает и запускает moderation worker."""
    worker = ModerationWorker()
    await worker.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
