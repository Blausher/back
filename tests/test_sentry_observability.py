from app.observability import sentry as sentry_observability


class DummyScope:
    def __init__(self) -> None:
        self.tags: dict[str, str] = {}
        self.extras: dict[str, object] = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def set_tag(self, key: str, value: str) -> None:
        self.tags[key] = value

    def set_extra(self, key: str, value: object) -> None:
        self.extras[key] = value


class DummySentrySDK:
    def __init__(self) -> None:
        self.init_calls: list[dict[str, object]] = []
        self.captured: list[Exception] = []
        self.scopes: list[DummyScope] = []

    def init(self, **kwargs) -> None:
        self.init_calls.append(kwargs)

    def push_scope(self) -> DummyScope:
        scope = DummyScope()
        self.scopes.append(scope)
        return scope

    def capture_exception(self, exc: Exception) -> None:
        self.captured.append(exc)


def test_init_sentry_from_env_skips_when_dsn_missing(monkeypatch):
    dummy_sdk = DummySentrySDK()

    monkeypatch.delenv("SENTRY_DSN", raising=False)
    monkeypatch.setattr(sentry_observability, "sentry_sdk", dummy_sdk)
    monkeypatch.setattr(sentry_observability, "_initialized_dsn", None)

    initialized = sentry_observability.init_sentry_from_env()

    assert initialized is False
    assert dummy_sdk.init_calls == []


def test_init_sentry_from_env_calls_sdk_with_expected_settings(monkeypatch):
    dummy_sdk = DummySentrySDK()

    monkeypatch.setenv("SENTRY_DSN", "http://public@example.invalid/1")
    monkeypatch.setenv("SENTRY_ENV", "test")
    monkeypatch.setenv("SENTRY_RELEASE", "build-123")
    monkeypatch.setattr(sentry_observability, "sentry_sdk", dummy_sdk)
    monkeypatch.setattr(sentry_observability, "FastApiIntegration", lambda: "fastapi")
    monkeypatch.setattr(sentry_observability, "StarletteIntegration", lambda: "starlette")
    monkeypatch.setattr(sentry_observability, "_initialized_dsn", None)

    initialized = sentry_observability.init_sentry_from_env()

    assert initialized is True
    assert dummy_sdk.init_calls == [
        {
            "dsn": "http://public@example.invalid/1",
            "environment": "test",
            "release": "build-123",
            "integrations": ["starlette", "fastapi"],
        }
    ]


def test_capture_exception_is_noop_when_sdk_unavailable(monkeypatch):
    monkeypatch.setattr(sentry_observability, "sentry_sdk", None)
    monkeypatch.setattr(sentry_observability, "_initialized_dsn", "http://public@example.invalid/1")

    sentry_observability.capture_exception(RuntimeError("boom"))


def test_capture_exception_attaches_tags_and_extras(monkeypatch):
    dummy_sdk = DummySentrySDK()
    error = RuntimeError("boom")

    monkeypatch.setattr(sentry_observability, "sentry_sdk", dummy_sdk)
    monkeypatch.setattr(sentry_observability, "_initialized_dsn", "http://public@example.invalid/1")

    sentry_observability.capture_exception(
        error,
        tags={"component": "api"},
        extras={"item_id": 42},
    )

    assert dummy_sdk.captured == [error]
    assert len(dummy_sdk.scopes) == 1
    assert dummy_sdk.scopes[0].tags == {"component": "api"}
    assert dummy_sdk.scopes[0].extras == {"item_id": 42}
