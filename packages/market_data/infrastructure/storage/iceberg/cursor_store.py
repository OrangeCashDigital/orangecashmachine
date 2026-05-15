# -*- coding: utf-8 -*-
"""
infrastructure/storage/iceberg/cursor_store.py

Responsabilidad única: gestionar cursores de ingestión (último timestamp
procesado por símbolo+timeframe) sobre Redis con fallback en memoria.

Bounded context: Infrastructure / Storage / Iceberg.
Depende de: ports.outbound.cache (abstracto), nunca de IcebergStorage.
"""
from __future__ import annotations

import time
from typing import Optional

from loguru import logger


class CursorStore:
    """
    SSOT para cursores de ingestión.

    Estrategia de caché en dos niveles:
      L1 — dict en memoria (proceso-local, O(1))
      L2 — Redis (compartido entre workers, TTL configurable)

    El caller (IcebergStorage o PipelineOrchestrator) inyecta el
    redis_client; CursorStore nunca crea conexiones propias (DIP).
    """

    _DEFAULT_TTL_S: int = 3600  # 1 hora

    def __init__(
        self,
        redis_client,           # redis.Redis | FakeRedis | None
        exchange:    str,
        market_type: str,
        ttl_s:       int = _DEFAULT_TTL_S,
    ) -> None:
        self._redis      = redis_client
        self._exchange   = exchange
        self._market_type = market_type
        self._ttl_s      = ttl_s
        self._l1: dict[str, Optional[int]] = {}

    # ── key helper ────────────────────────────────────────────────────────

    def _key(self, symbol: str, timeframe: str) -> str:
        return f"ocm:cursor:{self._exchange}:{self._market_type}:{symbol}:{timeframe}"

    # ── lectura ───────────────────────────────────────────────────────────

    def get(self, symbol: str, timeframe: str) -> Optional[int]:
        """Retorna el último timestamp ms procesado, o None si no existe."""
        cache_key = (symbol, timeframe)

        # L1: memoria
        if cache_key in self._l1:
            return self._l1[cache_key]

        # L2: Redis
        if self._redis is not None:
            try:
                raw = self._redis.get(self._key(symbol, timeframe))
                if raw is not None:
                    val = int(raw)
                    self._l1[cache_key] = val
                    return val
            except Exception:
                logger.opt(exception=True).warning(
                    "CursorStore.get Redis error | {}/{}", symbol, timeframe,
                )

        return None

    # ── escritura ─────────────────────────────────────────────────────────

    def set(
        self,
        symbol:    str,
        timeframe: str,
        timestamp_ms: int,
    ) -> None:
        """Persiste cursor en L1 y L2 (Redis) si está disponible."""
        cache_key = (symbol, timeframe)
        self._l1[cache_key] = timestamp_ms

        if self._redis is not None:
            try:
                self._redis.setex(
                    self._key(symbol, timeframe),
                    self._ttl_s,
                    str(timestamp_ms),
                )
            except Exception:
                logger.opt(exception=True).warning(
                    "CursorStore.set Redis error | {}/{}", symbol, timeframe,
                )

    # ── invalidación ──────────────────────────────────────────────────────

    def invalidate(self, symbol: str, timeframe: str) -> None:
        """Elimina cursor de L1 y L2 (útil en tests y resets manuales)."""
        cache_key = (symbol, timeframe)
        self._l1.pop(cache_key, None)

        if self._redis is not None:
            try:
                self._redis.delete(self._key(symbol, timeframe))
            except Exception:
                logger.opt(exception=True).debug(
                    "CursorStore.invalidate Redis error | {}/{}", symbol, timeframe,
                )
