# -*- coding: utf-8 -*-
"""
infra/state/factories.py
=========================

Factories centralizadas para todos los stores de infra/state/.

Responsabilidad
---------------
Construir instancias de RedisCursorStore, GapRegistry y
LatenessCalibrationStore desde variables de entorno, con
degradación controlada cuando Redis no está disponible.

Por qué este módulo existe
--------------------------
Antes, cada módulo de dominio (gap_registry.py, lateness_calibration.py)
tenía su propia factory con imports condicionales dentro de la función
para evitar circulares. Esto:

  1. Hacía los módulos difíciles de testear — no se podía patchear
     el import sin hackear sys.modules.
  2. Ocultaba el grafo de dependencias real del lector.
  3. Violaba SRP — un registro de gaps no debería saber cómo
     conectarse a Redis.

Solución
--------
Las factories viven aquí. Los módulos de dominio solo reciben
un RedisCursorStore ya construido en su constructor.

Uso
---
    from platform.infra.state.factories import (
        build_cursor_store,
        build_gap_registry,
        build_lateness_calibration_store,
    )

    store    = build_cursor_store()
    registry = build_gap_registry()
    cal      = build_lateness_calibration_store()

SafeOps
-------
build_gap_registry y build_lateness_calibration_store retornan None
si Redis no está disponible. El caller decide si degradar o fallar.
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from loguru import logger

from platform.infra.state.cursor_store import RedisCursorStore, build_cursor_store_from_env

if TYPE_CHECKING:
    from platform.infra.state.gap_registry import GapRegistry
    from platform.infra.state.lateness_calibration import LatenessCalibrationStore


def build_cursor_store(env: Optional[str] = None) -> RedisCursorStore:
    """
    Construye un RedisCursorStore desde variables de entorno.

    Thin wrapper sobre build_cursor_store_from_env que permite a los
    callers importar solo desde infra.state.factories sin conocer
    cursor_store.py directamente.

    Parameters
    ----------
    env : str, optional
        Entorno explícito (e.g. "production"). Si None, resuelve
        via resolve_env() (OCM_ENV → settings.yaml → "development").
    """
    return build_cursor_store_from_env(env=env)


def build_gap_registry(
    env: Optional[str] = None,
) -> Optional["GapRegistry"]:
    """
    Construye un GapRegistry desde variables de entorno.

    Returns None si Redis no está disponible — el caller decide
    si operar en modo degradado o fallar explícitamente.

    Parameters
    ----------
    env : str, optional
        Entorno explícito. Si None, usa resolve_env().
    """
    from platform.infra.state.gap_registry import GapRegistry

    try:
        store = build_cursor_store_from_env(env=env)
        if not store.is_healthy():
            logger.warning(
                "build_gap_registry: Redis no disponible — registry deshabilitado"
            )
            return None
        return GapRegistry(store)
    except Exception as exc:
        logger.warning("build_gap_registry: no se pudo inicializar | error={}", exc)
        return None


def build_lateness_calibration_store(
    env: Optional[str] = None,
) -> Optional["LatenessCalibrationStore"]:
    """
    Construye un LatenessCalibrationStore desde variables de entorno.

    Returns None si Redis no está disponible.

    Parameters
    ----------
    env : str, optional
        Entorno explícito. Si None, usa resolve_env().
    """
    from platform.infra.state.lateness_calibration import LatenessCalibrationStore

    try:
        store = build_cursor_store_from_env(env=env)
        if not store.is_healthy():
            logger.warning(
                "build_lateness_calibration_store: "
                "Redis no disponible — calibración deshabilitada"
            )
            return None
        return LatenessCalibrationStore(store)
    except Exception as exc:
        logger.warning(
            "build_lateness_calibration_store: no se pudo inicializar | error={}", exc
        )
        return None


def build_stream_publisher(
    stream_name: str = "ohlcv",
    env:         Optional[str] = None,
) -> Optional["StreamPublisher"]:  # noqa: F821
    """
    Construye un StreamPublisher desde variables de entorno.

    Ensambla: redis.Redis → RedisStreamPublisher → StreamPublisher.

    Returns None si Redis no está disponible.

    Parameters
    ----------
    stream_name : nombre lógico del stream (sin prefijo).
    env         : entorno explícito. Si None, usa resolve_env().
    """
    from platform.infra.state.redis_stream import RedisStreamPublisher
    from market_data.streaming.publisher import StreamPublisher

    try:
        store = build_cursor_store_from_env(env=env)
        if not store.is_healthy():
            logger.warning(
                "build_stream_publisher: Redis no disponible — publisher deshabilitado"
            )
            return None
        infra = RedisStreamPublisher(
            client      = store._pool and store._client or _get_redis_client(store),
            stream_name = stream_name,
        )
        return StreamPublisher(publisher=infra)
    except Exception as exc:
        logger.warning("build_stream_publisher: no se pudo inicializar | error={}", exc)
        return None


def build_stream_source(
    router:        "EventRouter",  # noqa: F821
    stream_name:   str = "ohlcv",
    consumer_name: str = "worker-1",
    env:           Optional[str] = None,
) -> Optional["StreamSource"]:  # noqa: F821
    """
    Construye un StreamSource desde variables de entorno.

    Ensambla: redis.Redis → RedisStreamConsumer → StreamSource.

    Llama a ensure_group() automáticamente — idempotente.
    Returns None si Redis no está disponible.

    Parameters
    ----------
    router        : EventRouter ya configurado (inyectado).
    stream_name   : nombre lógico del stream.
    consumer_name : nombre de este worker dentro del consumer group.
    env           : entorno explícito. Si None, usa resolve_env().
    """
    from platform.infra.state.redis_stream import RedisStreamConsumer
    from market_data.streaming.source import StreamSource

    try:
        store = build_cursor_store_from_env(env=env)
        if not store.is_healthy():
            logger.warning(
                "build_stream_source: Redis no disponible — source deshabilitado"
            )
            return None
        consumer = RedisStreamConsumer(
            client        = _get_redis_client(store),
            stream_name   = stream_name,
            consumer_name = consumer_name,
        )
        consumer.ensure_group()
        return StreamSource(consumer=consumer, router=router)
    except Exception as exc:
        logger.warning("build_stream_source: no se pudo inicializar | error={}", exc)
        return None


def _get_redis_client(store: "RedisCursorStore"):
    """
    Extrae el cliente Redis del RedisCursorStore.

    RedisCursorStore expone _client internamente. Si la interfaz
    cambia, este helper es el único punto a actualizar.
    """
    return store._client


# ---------------------------------------------------------------------------
# Alias de compatibilidad
# ---------------------------------------------------------------------------
# Históricamente algunos módulos generados importaban get_cursor_store.
# Este alias mantiene compatibilidad sin duplicar lógica (DRY).
# ---------------------------------------------------------------------------
get_cursor_store = build_cursor_store
