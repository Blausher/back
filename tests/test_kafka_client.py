import pytest

from app.clients import kafka


class DummyProducer:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.sent_messages = []

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def send_and_wait(self, topic, message):
        self.sent_messages.append((topic, message))


@pytest.mark.asyncio
async def test_kafka_client_reuses_started_producer(monkeypatch):
    created_producers = []

    def producer_factory(**kwargs):
        producer = DummyProducer(**kwargs)
        created_producers.append(producer)
        return producer

    client = kafka.KafkaProducerClient(bootstrap_servers="kafka:9092", topic="moderation")
    monkeypatch.setattr(kafka, "AIOKafkaProducer", producer_factory)

    await client.start()
    await client.send_moderation_request(42)
    await client.send_moderation_request(43)

    assert len(created_producers) == 1
    assert created_producers[0].started is True
    assert created_producers[0].sent_messages[0][0] == "moderation"
    assert created_producers[0].sent_messages[0][1]["item_id"] == 42
    assert created_producers[0].sent_messages[1][1]["item_id"] == 43


@pytest.mark.asyncio
async def test_kafka_client_stop_resets_producer(monkeypatch):
    producer = DummyProducer()

    def producer_factory(**kwargs):
        return producer

    client = kafka.KafkaProducerClient()
    monkeypatch.setattr(kafka, "AIOKafkaProducer", producer_factory)

    await client.start()
    await client.stop()

    assert producer.started is True
    assert producer.stopped is True
    assert client._producer is None
