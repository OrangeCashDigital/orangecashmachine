# -*- coding: utf-8 -*-
"""
tests/api/test_rate_limit_middleware.py
========================================

Regresión de H6 (AUDIT-apps-2026-08-03): las probes de observabilidad
(/health, /ready, /metrics) deben quedar EXCLUIDAS del rate limit —
comparten el SSOT SILENT_PATHS con el logging middleware.

Hermético: Redis se simula con un fake que falla si se le llama en paths
silenciosos, y registra llamadas en paths normales. Sin servidor real
(httpx/uvicorn) — se invoca dispatch() directamente.

Cubre:
  · path en SILENT_PATHS  → no toca Redis, pasa call_next.
  · path normal por debajo del límite → usa Redis, responde 200.
  · path normal sobre el límite      → 429 con Retry-After.
"""

from __future__ import annotations

from api.middleware import SILENT_PATHS
from starlette.requests import Request
from starlette.responses import Response


def _make_request(path: str) -> Request:
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "scheme": "http",
        "client": ("203.0.113.7", 4242),
        "server": ("testserver", 80),
        "headers": [],
    }
    return Request(scope)


class _FakePipe:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def zremrangebyscore(self, *args):
        self.calls.append("zremrangebyscore")
        return self

    def zcard(self, *args):
        self.calls.append("zcard")
        return self

    def zadd(self, *args):
        self.calls.append("zadd")
        return self

    def expire(self, *args):
        self.calls.append("expire")
        return self

    async def execute(self):
        # count = zcard del pipe (position 1); el caller lo interpreta como
        # la cantidad de requests en la ventana actual.
        return [None, self._zcard_value, None, True]


class _FakeRedis:
    """Pipeline que registra llamadas; execute() devuelve zcard configurable."""

    def __init__(self, zcard_value: int = 0) -> None:
        self.pipe = _FakePipe()
        self.pipe._zcard_value = zcard_value

    def pipeline(self):
        return self.pipe


async def _passthrough(request):
    return Response(status_code=200)


async def test_silent_paths_never_touch_redis() -> None:
    """H6: /health y /metrics no registran en Redis — pasan directo."""
    from api.middleware.rate_limit import RateLimitMiddleware

    redis = _FakeRedis()
    middleware = RateLimitMiddleware(app=object(), redis_client=redis, rpm=60)

    for path in SILENT_PATHS:
        response = await middleware.dispatch(_make_request(path), _passthrough)
        assert response.status_code == 200, path
        assert redis.pipe.calls == [], f"{path} tocó Redis: {redis.pipe.calls}"


async def test_normal_path_under_limit_uses_redis_and_passes() -> None:
    from api.middleware.rate_limit import RateLimitMiddleware

    redis = _FakeRedis(zcard_value=5)  # 5 < 60 rpm
    middleware = RateLimitMiddleware(app=object(), redis_client=redis, rpm=60)

    response = await middleware.dispatch(_make_request("/api/trades"), _passthrough)

    assert response.status_code == 200
    assert redis.pipe.calls == ["zremrangebyscore", "zcard", "zadd", "expire"]


async def test_normal_path_over_limit_returns_429() -> None:
    from api.middleware.rate_limit import RateLimitMiddleware

    redis = _FakeRedis(zcard_value=60)  # 60 >= 60 rpm
    middleware = RateLimitMiddleware(app=object(), redis_client=redis, rpm=60)

    response = await middleware.dispatch(_make_request("/api/trades"), _passthrough)

    assert response.status_code == 429
    assert response.headers["Retry-After"] == "60"
