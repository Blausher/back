import pytest

from app.models.advertisement import Advertisement
from app.workers import moderation_worker as mw
from tests.id_factory import new_id


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


class DummyAdvertisementRepo:
    def __init__(self, advertisement=None, exc: Exception | None = None):
        self.advertisement = advertisement
        self.exc = exc
        self.calls = []

    async def select_advert(self, item_id):
        self.calls.append(item_id)
        if self.exc is not None:
            raise self.exc
        return self.advertisement


class DummyModerationResultRepo:
    def __init__(
        self,
        pending_task_id=None,
        get_pending_exc: Exception | None = None,
        mark_completed_result=None,
        mark_completed_exc: Exception | None = None,
        mark_failed_result=None,
        mark_failed_exc: Exception | None = None,
    ):
        self.pending_task_id = pending_task_id if pending_task_id is not None else new_id()
        self.get_pending_exc = get_pending_exc
        self.mark_completed_result = (
            mark_completed_result if mark_completed_result is not None else new_id()
        )
        self.mark_completed_exc = mark_completed_exc
        self.mark_failed_result = mark_failed_result if mark_failed_result is not None else new_id()
        self.mark_failed_exc = mark_failed_exc
        self.pending_calls = []
        self.completed_calls = []
        self.failed_calls = []

    async def get_pending_task_id(self, item_id):
        self.pending_calls.append(item_id)
        if self.get_pending_exc is not None:
            raise self.get_pending_exc
        return self.pending_task_id

    async def mark_completed(self, item_id, is_violation, probability):
        self.completed_calls.append((item_id, is_violation, probability))
        if self.mark_completed_exc is not None:
            raise self.mark_completed_exc
        return self.mark_completed_result

    async def mark_failed(self, item_id, error_message):
        self.failed_calls.append((item_id, error_message))
        if self.mark_failed_exc is not None:
            raise self.mark_failed_exc
        return self.mark_failed_result


class DummyProcessedEventRepo:
    def __init__(self, first_time=True, exc: Exception | None = None):
        self.first_time = first_time
        self.exc = exc
        self.calls = []

    async def register_processing(self, event_id, item_id, moderation_result_id):
        self.calls.append((event_id, item_id, moderation_result_id))
        if self.exc is not None:
            raise self.exc
        return self.first_time


def _advertisement(item_id=None, seller_id=None, is_verified_seller=False, images_qty=1):
    item_id = item_id if item_id is not None else new_id()
    seller_id = seller_id if seller_id is not None else new_id()
    return Advertisement.model_validate(
        {
            "item_id": item_id,
            "seller_id": seller_id,
            "is_verified_seller": is_verified_seller,
            "name": "Desk",
            "description": "text",
            "category": 1,
            "images_qty": images_qty,
        }
    )


def _build_worker(
    monkeypatch,
    advertisement_repo=None,
    moderation_result_repo=None,
    processed_event_repo=None,
    **kwargs,
):
    """Создает воркер с подмененными Kafka-клиентами, моделью и репозиториями."""

    class DummyModelClient:
        def __init__(self, *args, **kwargs):
            pass

        def predict_probability(self, _advertisement):
            return 0.5

    monkeypatch.setattr(mw, "ModelClient", DummyModelClient)
    monkeypatch.setattr(mw, "AIOKafkaConsumer", DummyConsumer)
    monkeypatch.setattr(mw, "AIOKafkaProducer", DummyProducer)
    return mw.ModerationWorker(
        advertisement_repo=advertisement_repo or DummyAdvertisementRepo(),
        moderation_result_repo=moderation_result_repo or DummyModerationResultRepo(),
        processed_event_repo=processed_event_repo or DummyProcessedEventRepo(),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_handle_message_marks_failed_and_sends_dlq_when_advert_not_found(monkeypatch):
    """Проверяет, что отсутствие объявления приводит к failed и отправке в DLQ."""
    item_id = new_id()
    ad_repo = DummyAdvertisementRepo(advertisement=None)
    moderation_repo = DummyModerationResultRepo()
    processed_event_repo = DummyProcessedEventRepo(first_time=True)
    worker = _build_worker(
        monkeypatch,
        advertisement_repo=ad_repo,
        moderation_result_repo=moderation_repo,
        processed_event_repo=processed_event_repo,
    )
    dlq_events = []
    sleep_calls = []

    async def fake_send_to_dlq(error_message, payload, retry_count=None):
        dlq_events.append((error_message, payload, retry_count))

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    worker._send_to_dlq = fake_send_to_dlq
    monkeypatch.setattr(mw.asyncio, "sleep", fake_sleep)

    payload = f'{{"item_id": {item_id}}}'.encode("utf-8")
    await worker._handle_message(payload)

    assert ad_repo.calls == [item_id]
    assert moderation_repo.failed_calls == [(item_id, "Advertisement not found")]
    assert dlq_events == [("Advertisement not found", payload, 1)]
    assert sleep_calls == []


@pytest.mark.asyncio
async def test_handle_message_retries_temporary_prediction_error_then_sends_dlq(monkeypatch):
    """Проверяет 3 попытки для временной ошибки модели и отправку в DLQ после исчерпания."""
    item_id = new_id()
    pending_task_id = new_id()
    ad_repo = DummyAdvertisementRepo(advertisement=_advertisement(item_id=item_id))
    moderation_repo = DummyModerationResultRepo()
    moderation_repo.pending_task_id = pending_task_id
    processed_event_repo = DummyProcessedEventRepo(first_time=True)
    worker = _build_worker(
        monkeypatch,
        advertisement_repo=ad_repo,
        moderation_result_repo=moderation_repo,
        processed_event_repo=processed_event_repo,
        retry_delay_seconds=7,
    )
    dlq_events = []
    sleep_calls = []
    predict_attempts = []

    def fake_predict(_advertisement):
        predict_attempts.append("predict")
        raise mw.ModelNotLoadedError("Model is not loaded")

    async def fake_send_to_dlq(error_message, payload, retry_count=None):
        dlq_events.append((error_message, payload, retry_count))

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    worker._predict = fake_predict
    worker._send_to_dlq = fake_send_to_dlq
    monkeypatch.setattr(mw.asyncio, "sleep", fake_sleep)

    payload = f'{{"item_id": {item_id}}}'.encode("utf-8")
    await worker._handle_message(payload)

    assert moderation_repo.pending_calls == [item_id]
    assert processed_event_repo.calls == [(f"moderation:{item_id}:{pending_task_id}", item_id, pending_task_id)]
    assert predict_attempts == ["predict", "predict", "predict"]
    assert moderation_repo.completed_calls == []
    assert moderation_repo.failed_calls == [(item_id, "Prediction failed: Model is not loaded")]
    assert dlq_events == [("Prediction failed: Model is not loaded", payload, 3)]
    assert sleep_calls == [7, 7]


@pytest.mark.asyncio
async def test_handle_message_retries_temporary_prediction_error_until_success(monkeypatch):
    """Проверяет, что временная ошибка модели может восстановиться до DLQ."""
    item_id = new_id()
    ad_repo = DummyAdvertisementRepo(advertisement=_advertisement(item_id=item_id))
    moderation_repo = DummyModerationResultRepo(mark_completed_result=new_id())
    processed_event_repo = DummyProcessedEventRepo(first_time=True)
    worker = _build_worker(
        monkeypatch,
        advertisement_repo=ad_repo,
        moderation_result_repo=moderation_repo,
        processed_event_repo=processed_event_repo,
        retry_delay_seconds=3,
    )
    dlq_events = []
    sleep_calls = []
    predict_attempts = []

    def fake_predict(_advertisement):
        predict_attempts.append("predict")
        if len(predict_attempts) < 3:
            raise mw.ModelNotLoadedError("Model is not loaded")
        return True, 0.88

    async def fake_send_to_dlq(error_message, payload, retry_count=None):
        dlq_events.append((error_message, payload, retry_count))

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    worker._predict = fake_predict
    worker._send_to_dlq = fake_send_to_dlq
    monkeypatch.setattr(mw.asyncio, "sleep", fake_sleep)

    payload = f'{{"item_id": {item_id}}}'.encode("utf-8")
    await worker._handle_message(payload)

    assert predict_attempts == ["predict", "predict", "predict"]
    assert moderation_repo.completed_calls == [(item_id, True, 0.88)]
    assert moderation_repo.failed_calls == []
    assert dlq_events == []
    assert sleep_calls == [3, 3]


@pytest.mark.asyncio
async def test_handle_message_success_marks_completed_without_dlq(monkeypatch):
    """Проверяет happy path: completed без failed-обновления и без DLQ."""
    item_id = new_id()
    ad_repo = DummyAdvertisementRepo(
        advertisement=_advertisement(item_id=item_id, is_verified_seller=True, images_qty=2)
    )
    moderation_repo = DummyModerationResultRepo(mark_completed_result=new_id())
    processed_event_repo = DummyProcessedEventRepo(first_time=True)
    worker = _build_worker(
        monkeypatch,
        advertisement_repo=ad_repo,
        moderation_result_repo=moderation_repo,
        processed_event_repo=processed_event_repo,
    )
    dlq_events = []

    def fake_predict(_advertisement):
        return True, 0.91

    async def fake_send_to_dlq(error_message, payload, retry_count=None):
        dlq_events.append((error_message, payload, retry_count))

    worker._predict = fake_predict
    worker._send_to_dlq = fake_send_to_dlq

    payload = f'{{"item_id": {item_id}}}'.encode("utf-8")
    await worker._handle_message(payload)

    assert moderation_repo.completed_calls == [(item_id, True, 0.91)]
    assert moderation_repo.failed_calls == []
    assert dlq_events == []


@pytest.mark.asyncio
async def test_send_to_dlq_publishes_message(monkeypatch):
    """Проверяет контракт сообщения, публикуемого в moderation_dlq."""
    worker = _build_worker(monkeypatch)
    item_id = new_id()

    await worker._send_to_dlq(
        error_message="Prediction failed",
        payload=f'{{"item_id": {item_id}}}'.encode("utf-8"),
    )

    assert len(worker.producer.sent) == 1
    topic, message = worker.producer.sent[0]
    assert topic == "moderation_dlq"
    assert message["original_message"] == {"item_id": item_id}
    assert message["error"] == "Prediction failed"
    assert message["retry_count"] == 1
    assert "timestamp" in message
    assert message["timestamp"].endswith("Z")


@pytest.mark.asyncio
async def test_send_to_dlq_increments_retry_count(monkeypatch):
    """Проверяет инкремент retry_count при повторной отправке в DLQ."""
    worker = _build_worker(monkeypatch)
    item_id = new_id()

    await worker._send_to_dlq(
        error_message="Prediction failed",
        payload=f'{{"item_id": {item_id}, "retry_count": 2}}'.encode("utf-8"),
    )

    _, message = worker.producer.sent[0]
    assert message["retry_count"] == 3


@pytest.mark.asyncio
async def test_send_to_dlq_uses_explicit_retry_count(monkeypatch):
    """Проверяет, что явный retry_count не перезаписывается значением из payload."""
    worker = _build_worker(monkeypatch)
    item_id = new_id()

    await worker._send_to_dlq(
        error_message="Prediction failed",
        payload=f'{{"item_id": {item_id}, "retry_count": 1}}'.encode("utf-8"),
        retry_count=3,
    )

    _, message = worker.producer.sent[0]
    assert message["retry_count"] == 3


@pytest.mark.asyncio
async def test_handle_message_skips_duplicate_event(monkeypatch):
    """Проверяет, что дубль события не обрабатывается повторно."""
    item_id = new_id()
    ad_repo = DummyAdvertisementRepo(advertisement=_advertisement(item_id=item_id))
    moderation_repo = DummyModerationResultRepo()
    processed_event_repo = DummyProcessedEventRepo(first_time=False)
    worker = _build_worker(
        monkeypatch,
        advertisement_repo=ad_repo,
        moderation_result_repo=moderation_repo,
        processed_event_repo=processed_event_repo,
    )
    dlq_events = []

    async def fake_send_to_dlq(error_message, payload, retry_count=None):
        dlq_events.append((error_message, payload, retry_count))

    worker._send_to_dlq = fake_send_to_dlq

    payload = f'{{"item_id": {item_id}}}'.encode("utf-8")
    await worker._handle_message(payload)

    assert moderation_repo.completed_calls == []
    assert moderation_repo.failed_calls == []
    assert ad_repo.calls == []
    assert dlq_events == []
