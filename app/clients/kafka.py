import asyncio
import json
import os
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer


class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str | None = None, topic: str = "moderation") -> None:
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = topic
        self._producer: AIOKafkaProducer | None = None
        self._lifecycle_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._producer is not None:
            return

        async with self._lifecycle_lock:
            if self._producer is not None:
                return

            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            await producer.start()
            self._producer = producer

    async def stop(self) -> None:
        if self._producer is None:
            return

        async with self._lifecycle_lock:
            producer = self._producer
            if producer is None:
                return

            try:
                await producer.stop()
            finally:
                self._producer = None

    async def send_moderation_request(self, item_id: int) -> None:
        await self.start()

        message = {
            "item_id": item_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if self._producer is None:
            raise RuntimeError("Kafka producer is not initialized")

        await self._producer.send_and_wait(self.topic, message)


kafka_client = KafkaProducerClient()
