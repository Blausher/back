import asyncio

from app.workers import moderation_worker as mw


class DummyConsumer:
    """Минимальный мок Kafka consumer для unit-тестов воркера."""

    def __init__(self, *args, **kwargs):
        self.started = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.started = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class DummyProducer:
    """Минимальный мок Kafka producer, сохраняет отправленные сообщения."""

    def __init__(self, *args, **kwargs):
        self.started = False
        self.sent = []

    async def start(self):
        self.started = True

    async def stop(self):
        self.started = False

    async def send_and_wait(self, topic, message):
        self.sent.append((topic, message))


def _build_worker(monkeypatch):
    """Создает воркер с подмененными Kafka-клиентами и моделью."""

    monkeypatch.setattr(mw, "load_or_train_model", lambda _path="model.pkl": object())
    monkeypatch.setattr(mw, "AIOKafkaConsumer", DummyConsumer)
    monkeypatch.setattr(mw, "AIOKafkaProducer", DummyProducer)
    return mw.ModerationWorker()


def test_handle_message_marks_failed_and_sends_dlq_when_advert_not_found(monkeypatch):
    """Проверяет, что отсутствие объявления приводит к failed и отправке в DLQ."""

    async def run():
        worker = _build_worker(monkeypatch)
        failed_updates = []
        dlq_events = []

        async def fake_load_advertisement(_item_id):
            return None

        async def fake_mark_failed(item_id, error_message):
            failed_updates.append((item_id, error_message))
            return 1

        async def fake_send_to_dlq(error_message, payload):
            dlq_events.append((error_message, payload))

        worker._load_advertisement = fake_load_advertisement
        worker._mark_failed = fake_mark_failed
        worker._send_to_dlq = fake_send_to_dlq

        payload = b'{"item_id": 42}'
        await worker._handle_message(payload)

        assert failed_updates == [(42, "Advertisement not found")]
        assert dlq_events == [("Advertisement not found", payload)]

    asyncio.run(run())


def test_handle_message_marks_failed_and_sends_dlq_when_predict_fails(monkeypatch):
    """Проверяет, что ошибка предсказания помечает задачу как failed и пишет в DLQ."""

    async def run():
        worker = _build_worker(monkeypatch)
        failed_updates = []
        dlq_events = []

        async def fake_load_advertisement(_item_id):
            return mw.AdvertisementRow(
                item_id=42,
                seller_id=7,
                is_verified_seller=False,
                description="text",
                category=1,
                images_qty=1,
            )

        def fake_predict(_advertisement):
            raise RuntimeError("Model is not loaded")

        async def fake_mark_failed(item_id, error_message):
            failed_updates.append((item_id, error_message))
            return 1

        async def fake_send_to_dlq(error_message, payload):
            dlq_events.append((error_message, payload))

        worker._load_advertisement = fake_load_advertisement
        worker._predict = fake_predict
        worker._mark_failed = fake_mark_failed
        worker._send_to_dlq = fake_send_to_dlq

        payload = b'{"item_id": 42}'
        await worker._handle_message(payload)

        assert failed_updates == [(42, "Prediction failed: Model is not loaded")]
        assert dlq_events == [
            ("Prediction failed: Model is not loaded", payload),
        ]

    asyncio.run(run())


def test_handle_message_success_marks_completed_without_dlq(monkeypatch):
    """Проверяет happy path: completed без failed-обновления и без DLQ."""

    async def run():
        worker = _build_worker(monkeypatch)
        completed_updates = []
        failed_updates = []
        dlq_events = []

        async def fake_load_advertisement(_item_id):
            return mw.AdvertisementRow(
                item_id=42,
                seller_id=7,
                is_verified_seller=True,
                description="text",
                category=1,
                images_qty=2,
            )

        def fake_predict(_advertisement):
            return True, 0.91

        async def fake_mark_completed(item_id, is_violation, probability):
            completed_updates.append((item_id, is_violation, probability))
            return 55

        async def fake_mark_failed(item_id, error_message):
            failed_updates.append((item_id, error_message))
            return 1

        async def fake_send_to_dlq(error_message, payload):
            dlq_events.append((error_message, payload))

        worker._load_advertisement = fake_load_advertisement
        worker._predict = fake_predict
        worker._mark_completed = fake_mark_completed
        worker._mark_failed = fake_mark_failed
        worker._send_to_dlq = fake_send_to_dlq

        payload = b'{"item_id": 42}'
        await worker._handle_message(payload)

        assert completed_updates == [(42, True, 0.91)]
        assert failed_updates == []
        assert dlq_events == []

    asyncio.run(run())


def test_send_to_dlq_publishes_message(monkeypatch):
    """Проверяет контракт сообщения, публикуемого в moderation_dlq."""

    async def run():
        worker = _build_worker(monkeypatch)

        await worker._send_to_dlq(
            error_message="Prediction failed",
            payload=b'{"item_id": 100}',
        )

        assert len(worker.producer.sent) == 1
        topic, message = worker.producer.sent[0]
        assert topic == "moderation_dlq"
        assert message["original_message"] == {"item_id": 100}
        assert message["error"] == "Prediction failed"
        assert message["retry_count"] == 1
        assert "timestamp" in message
        assert message["timestamp"].endswith("Z")

    asyncio.run(run())


def test_send_to_dlq_increments_retry_count(monkeypatch):
    """Проверяет инкремент retry_count при повторной отправке в DLQ."""

    async def run():
        worker = _build_worker(monkeypatch)

        await worker._send_to_dlq(
            error_message="Prediction failed",
            payload=b'{"item_id": 100, "retry_count": 2}',
        )

        _, message = worker.producer.sent[0]
        assert message["retry_count"] == 3

    asyncio.run(run())
