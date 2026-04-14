# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import base64
import os
import time
from itertools import islice
from typing import Iterator, Optional, Protocol, runtime_checkable

import redis
from redis import ConnectionPool
from loguru import logger

try:
    from prometheus_client import Counter, Gauge, Histogram
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False

_DEFAULT_HOST:      str   = "localhost"
_DEFAULT_PORT:      int   = 6379
_DEFAULT_DB:        int   = 1  # db=1 — canónico para dev. db=0 = legacy (cursores expirados).
                               # Ref: .env REDIS_DB=1
_DEFAULT_ENV:       str   = "development"
_DEFAULT_TTL_DAYS:  int   = 30
_SOCKET_TIMEOUT:    int   = 3
_CONNECT_TIMEOUT:   int   = 3
_POOL_MAX_CONN:     int   = 10
_SCAN_COUNT:        int   = 100
_DELETE_BATCH_SIZE: int   = 500
_RETRY_ATTEMPTS:    int   = 3
_RETRY_BASE_MS:     float = 50.0

@runtime_checkable
class CursorStore(Protocol):
    """
    Contrato mínimo que cualquier cursor store debe cumplir.

    get() y update() son async — siempre usar await.
    delete(), is_healthy(), get_raw(), set_raw() son síncronos.
    """

    async def get(self, exchange: str, symbol: str, timeframe: str) -> Optional[int]: ...
    async def update(self, exchange: str, symbol: str, timeframe: str, timestamp_ms: int) -> bool: ...
    def delete(self, exchange: str, symbol: str, timeframe: str) -> bool: ...
    def is_healthy(self) -> bool: ...
    def get_raw(self, key: str) -> Optional[str]: ...
    def set_raw(self, key: str, value: str, ttl_seconds: int) -> None: ...

_CAS_SCRIPT = """
local current = redis.call("GET", KEYS[1])
if not current then
    redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
    return 1
end
local curr_num = tonumber(current)
local new_num  = tonumber(ARGV[1])
if (not curr_num) or (new_num > curr_num) then
    redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
    return 1
end
return 0
"""

def _make_metrics():
    if not _PROMETHEUS_AVAILABLE:
        return None, None, None, None, None, None
    try:
        return (
            Histogram("cursor_get_latency_seconds", "Latencia de cursor.get()", ["exchange"]),
            Counter("cursor_update_total", "Total de updates exitosos", ["exchange"]),
            Counter("cursor_miss_total", "Total de cursor misses", ["exchange"]),
            Counter("cursor_error_total", "Total de errores", ["operation"]),
            Gauge("cursor_active_total", "Cursores activos por exchange", ["exchange"]),
            Gauge("cursor_lag_seconds", "Retraso medio del cursor por exchange", ["exchange"]),
        )
    except Exception:
        return None, None, None, None, None, None

_get_latency, _update_total, _miss_total, _error_total, _active_cursors, _cursor_lag = _make_metrics()


async def _retry_async(fn, attempts: int = _RETRY_ATTEMPTS, base_ms: float = _RETRY_BASE_MS):
    """
    Versión async de _retry: usa asyncio.sleep en lugar de time.sleep.
    Evita bloquear el event loop cuando Redis está caído o lento.
    Solo reintenta en ConnectionError y TimeoutError — el resto propaga inmediato.
    """
    last_exc: Exception = RuntimeError("no attempts")
    for attempt in range(attempts):
        try:
            return fn()
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
            last_exc = exc
            if attempt < attempts - 1:
                wait = (base_ms * (2 ** attempt)) / 1000.0
                logger.warning(
                    "Redis retry {}/{} in {:.0f}ms | error={}",
                    attempt + 1, attempts, base_ms * (2 ** attempt), exc,
                )
                await asyncio.sleep(wait)
    raise last_exc


def _retry(fn, attempts: int = _RETRY_ATTEMPTS, base_ms: float = _RETRY_BASE_MS):
    """
    Versión síncrona — solo para contextos no-async (healthcheck, tests).
    En pipelines async usar _retry_async.
    """
    last_exc: Exception = RuntimeError("no attempts")
    for attempt in range(attempts):
        try:
            return fn()
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
            last_exc = exc
            if attempt < attempts - 1:
                wait = (base_ms * (2 ** attempt)) / 1000.0
                logger.warning(
                    "Redis retry {}/{} in {:.0f}ms | error={}",
                    attempt + 1, attempts, base_ms * (2 ** attempt), exc,
                )
                time.sleep(wait)
    raise last_exc


class RedisCursorStore:
    """
    Cursor store backed by Redis. Production-grade v5 (congelado).

    Keys individuales por cursor para TTL real por campo.
    list_cursors: pipeline batching (SCAN + 1 roundtrip, no N+1).
    delete_exchange: batches de 500 (no carga todo en memoria).
    cursor_lag: solo label exchange (evita explosion de cardinalidad Prometheus).

    Async I/O
    ---------
    get() y update() usan _retry_async — no bloquean el event loop.
    is_healthy() usa _retry síncrono — solo se llama fuera de pipelines.
    """

    def __init__(
        self,
        host:     str = _DEFAULT_HOST,
        port:     int = _DEFAULT_PORT,
        db:       int = _DEFAULT_DB,
        env:      str = _DEFAULT_ENV,
        ttl_days: int = _DEFAULT_TTL_DAYS,
        password: str | None = None,
    ) -> None:
        self._env_raw = env.lower()
        self._env     = _encode(self._env_raw)
        self._ttl     = ttl_days * 86_400
        pool = ConnectionPool(
            host=host, port=port, db=db, password=password or None,
            max_connections=_POOL_MAX_CONN,
            socket_timeout=_SOCKET_TIMEOUT,
            socket_connect_timeout=_CONNECT_TIMEOUT,
            retry_on_timeout=True,
            decode_responses=True,
        )
        self._client     = redis.Redis(connection_pool=pool)
        self._cas_script = self._client.register_script(_CAS_SCRIPT)
        logger.debug(
            "CursorStore v5 ready | host={}:{} db={} env={} ttl_days={}",
            host, port, db, env, ttl_days,
        )

    def _key(self, exchange: str, symbol: str, timeframe: str) -> str:
        return f"{self._env}:cursor:{_encode(exchange)}:{_encode(symbol)}:{_encode(timeframe)}"

    def _exchange_prefix(self, exchange: str) -> str:
        return f"{self._env}:cursor:{_encode(exchange)}:"

    def _record_lag(self, exchange: str, ts_ms: int) -> None:
        if _cursor_lag:
            try:
                lag = (time.time() * 1000 - ts_ms) / 1000.0
                _cursor_lag.labels(exchange=exchange).set(lag)
            except Exception:
                pass

    def is_healthy(self) -> bool:
        try:
            return self._client.ping()
        except Exception:
            return False

    async def get(self, exchange: str, symbol: str, timeframe: str) -> Optional[int]:
        key = self._key(exchange, symbol, timeframe)
        t0  = time.monotonic()
        try:
            raw = await _retry_async(lambda: self._client.get(key))
            if _get_latency:
                _get_latency.labels(exchange=exchange).observe(time.monotonic() - t0)
            if raw is None:
                if _miss_total:
                    _miss_total.labels(exchange=exchange).inc()
                logger.debug("Cursor miss | key={}", key)
                return None
            ts_ms = int(raw)
            self._record_lag(exchange, ts_ms)
            logger.debug("Cursor hit | key={} ts_ms={}", key, ts_ms)
            return ts_ms
        except Exception as exc:
            if _error_total:
                _error_total.labels(operation="get").inc()
            logger.warning(
                "CursorStore.get failed (fallback to parquet) | key={} error={}", key, exc
            )
            return None

    async def update(self, exchange: str, symbol: str, timeframe: str, timestamp_ms: int) -> bool:
        key = self._key(exchange, symbol, timeframe)
        try:
            result = await _retry_async(
                lambda: self._cas_script(keys=[key], args=[str(timestamp_ms), str(self._ttl)])
            )
            if result == 0:
                # CAS skip es señal de salud: el cursor ya está adelante o igual.
                # INFO (no DEBUG) para que sea visible en producción sin grep extra.
                logger.info(
                    "Cursor already current — CAS skip | key={} attempted={} (no update needed)",
                    key, timestamp_ms,
                )
                return False
            if _update_total:
                _update_total.labels(exchange=exchange).inc()
            self._record_lag(exchange, timestamp_ms)
            logger.debug("Cursor updated | key={} ts_ms={}", key, timestamp_ms)
            return True
        except Exception as exc:
            if _error_total:
                _error_total.labels(operation="update").inc()
            logger.warning(
                "CursorStore.update failed (non-critical) | key={} error={}", key, exc
            )
            return False

    def delete(self, exchange: str, symbol: str, timeframe: str) -> bool:
        key = self._key(exchange, symbol, timeframe)
        try:
            deleted = bool(self._client.delete(key))
            logger.info("Cursor deleted | key={}", key)
            return deleted
        except Exception as exc:
            if _error_total:
                _error_total.labels(operation="delete").inc()
            logger.warning("CursorStore.delete failed | key={} error={}", key, exc)
            return False

    def delete_exchange(self, exchange: str) -> int:
        pattern = self._exchange_prefix(exchange) + "*"
        deleted = 0
        try:
            for batch in _batched(self._scan_iter(pattern), _DELETE_BATCH_SIZE):
                if batch:
                    deleted += int(self._client.delete(*batch))
            if _active_cursors:
                _active_cursors.labels(exchange=exchange).set(0)
            logger.info("All cursors deleted | exchange={} count={}", exchange, deleted)
            return deleted
        except Exception as exc:
            if _error_total:
                _error_total.labels(operation="delete_exchange").inc()
            logger.warning(
                "CursorStore.delete_exchange failed | exchange={} error={}", exchange, exc
            )
            return 0

    def list_cursors(self, exchange: str) -> dict[str, int]:
        pattern = self._exchange_prefix(exchange) + "*"
        result: dict[str, int] = {}
        try:
            keys = list(self._scan_iter(pattern))
            if not keys:
                return result
            pipe   = self._client.pipeline()
            for key in keys:
                pipe.get(key)
            values = _retry(lambda: pipe.execute())
            for key, raw in zip(keys, values):
                if raw:
                    result[key] = int(raw)
            if _active_cursors:
                _active_cursors.labels(exchange=exchange).set(len(result))
            logger.debug("Cursor list | exchange={} found={}", exchange, len(result))
            return result
        except Exception as exc:
            if _error_total:
                _error_total.labels(operation="list").inc()
            logger.warning(
                "CursorStore.list_cursors failed | exchange={} error={}", exchange, exc
            )
            return {}

    def scan_exchanges(self) -> list[str]:
        prefix    = f"{self._env}:cursor:"
        exchanges: set[str] = set()
        try:
            for key in self._scan_iter(f"{prefix}*"):
                if key.startswith(prefix):
                    rest         = key[len(prefix):]
                    exchange_enc = rest.split(":")[0]
                    try:
                        exchanges.add(_decode(exchange_enc))
                    except Exception:
                        pass
            result = list(exchanges)
            logger.debug("Scan exchanges | env={} found={}", self._env_raw, len(result))
            return result
        except Exception as exc:
            if _error_total:
                _error_total.labels(operation="scan").inc()
            logger.warning("CursorStore.scan_exchanges failed | error={}", exc)
            return []

    def get_raw(self, key: str) -> Optional[str]:
        try:
            return _retry(lambda: self._client.get(key))
        except Exception as exc:
            logger.warning("CursorStore.get_raw failed | key={} error={}", key, exc)
            return None

    def set_raw(self, key: str, value: str, ttl_seconds: int) -> None:
        try:
            _retry(lambda: self._client.set(key, value, ex=ttl_seconds))
        except Exception as exc:
            logger.warning("CursorStore.set_raw failed | key={} error={}", key, exc)

    def _scan_iter(self, pattern: str) -> Iterator[str]:
        cursor = 0
        while True:
            result = self._client.scan(cursor=cursor, match=pattern, count=_SCAN_COUNT)
            cursor, keys = result[0], result[1]
            for k in keys:
                yield k
            if cursor == 0:
                break


class InMemoryCursorStore:
    """CursorStore en memoria. Uso: tests unitarios y entornos sin Redis."""

    def __init__(self) -> None:
        self._store: dict[str, int] = {}
        self._store_raw: dict[str, str] = {}

    def is_healthy(self) -> bool:
        return True

    async def get(self, exchange: str, symbol: str, timeframe: str) -> Optional[int]:
        return self._store.get(f"{exchange}:{symbol}:{timeframe}")

    async def update(self, exchange: str, symbol: str, timeframe: str, timestamp_ms: int) -> bool:
        key     = f"{exchange}:{symbol}:{timeframe}"
        current = self._store.get(key)
        if current is not None and current > timestamp_ms:
            return False
        self._store[key] = timestamp_ms
        return True

    def delete(self, exchange: str, symbol: str, timeframe: str) -> bool:
        return bool(self._store.pop(f"{exchange}:{symbol}:{timeframe}", None))

    def get_raw(self, key: str) -> Optional[str]:
        return self._store_raw.get(key)

    def set_raw(self, key: str, value: str, ttl_seconds: int) -> None:
        self._store_raw[key] = value


def encode_cursor_key(value: str) -> str:
    """Codifica un segmento de clave Redis en base64 urlsafe (sin padding)."""
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


_encode = encode_cursor_key  # alias interno — no eliminar: usado por RedisCursorStore

def _decode(value: str) -> str:
    padding = 4 - len(value) % 4
    if padding != 4:
        value += "=" * padding
    return base64.urlsafe_b64decode(value).decode()

def _batched(iterable: Iterator, size: int) -> Iterator[list]:
    it = iter(iterable)
    while True:
        batch = list(islice(it, size))
        if not batch:
            break
        yield batch


def build_cursor_store_from_config(config=None) -> RedisCursorStore:
    """
    Factory principal: construye RedisCursorStore desde AppConfig.
    Centraliza configuracion validada, cacheada y auditable (DRY).
    config debe pasarse explícito — no se carga automáticamente.
    """
    if config is None:
        raise RuntimeError(
            "build_cursor_store_from_config() requires an AppConfig. "
            "Load config via load_appconfig_standalone() and pass it explicitly."
        )
    redis_cfg = config.integrations.redis
    return RedisCursorStore(
        host=redis_cfg.host,
        port=redis_cfg.port,
        db=redis_cfg.db,
        password=getattr(redis_cfg, "password", None) or None,
        env=config.environment.name if hasattr(config, "environment") else "development",
        ttl_days=getattr(redis_cfg, "ttl_days", _DEFAULT_TTL_DAYS),
    )


def build_cursor_store_from_env(env: Optional[str] = None) -> RedisCursorStore:
    """
    Construye un RedisCursorStore desde variables de entorno.

    Parameters
    ----------
    env : str | None
        Entorno explícito (ej: run_cfg.env). Si no se pasa,
        se resuelve via resolve_env() respetando OCM_ENV → settings.yaml → 'development'.
    """
    from core.config.loader.env_resolver import resolve_env
    return RedisCursorStore(
        host=os.getenv("REDIS_HOST", _DEFAULT_HOST),
        port=int(os.getenv("REDIS_PORT", str(_DEFAULT_PORT))),
        db=int(os.getenv("REDIS_DB", str(_DEFAULT_DB))),
        password=os.getenv("REDIS_PASSWORD") or None,
        env=env or resolve_env(),
        ttl_days=int(os.getenv("CURSOR_TTL_DAYS", str(_DEFAULT_TTL_DAYS))),
    )
