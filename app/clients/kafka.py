import json
import os
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer


class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str | None = None, topic: str = "moderation") -> None:
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = topic

    async def send_moderation_request(self, item_id: int) -> None:
        message = {
            "item_id": item_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        await producer.start()
        try:
            await producer.send_and_wait(self.topic, message)
        finally:
            await producer.stop()
