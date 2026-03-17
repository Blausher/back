from __future__ import annotations

import os
from typing import Any, Mapping

try:
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration
    from sentry_sdk.integrations.starlette import StarletteIntegration
except Exception:  # pragma: no cover
    sentry_sdk = None
    FastApiIntegration = None
    StarletteIntegration = None


_initialized_dsn: str | None = None


def init_sentry_from_env() -> bool:
    """Initializes Sentry when a DSN is configured."""
    global _initialized_dsn

    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn or sentry_sdk is None:
        return False

    if _initialized_dsn == dsn:
        return True

    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv("SENTRY_ENV", "local"),
        release=os.getenv("SENTRY_RELEASE") or None,
        integrations=[
            StarletteIntegration(),
            FastApiIntegration(),
        ],
    )
    _initialized_dsn = dsn
    return True


def capture_exception(
    exc: Exception,
    *,
    tags: Mapping[str, Any] | None = None,
    extras: Mapping[str, Any] | None = None,
) -> None:
    """Sends an exception to Sentry when the SDK is available and configured."""
    if sentry_sdk is None or _initialized_dsn is None:
        return

    with sentry_sdk.push_scope() as scope:
        for key, value in (tags or {}).items():
            scope.set_tag(key, str(value))
        for key, value in (extras or {}).items():
            scope.set_extra(key, value)
        sentry_sdk.capture_exception(exc)
