# -*- coding: utf-8 -*-
"""
market_data/infrastructure/storage/iceberg/timestamp_cache.py
==============================================================

TimestampCacheService — cache jerárquico L1/L2 de timestamps OHLCV.

Responsabilidad
---------------
Gestionar exclusivamente el cache de last/oldest timestamps:
  L1 — dict en memoria (in-process, por instancia)
  L2 — Redis distribuido (cross-process, si inyectado)

Por qué separado de IcebergStorage
-----------------------------------
IcebergStorage es responsable de persistencia (save, load, scan).
El cache es una responsabilidad ortogonal: puede cambiar (TTL, LRU,
Redis Cluster) sin tocar la lógica de persistencia. SRP.

Uso
---
  cache = TimestampCacheService(cursor_store=redis_cursor)
  ts = cache.get(symbol="BTC/USDT", timeframe="1m")  # L1 o L2
  cache.set(symbol="BTC/USDT", timeframe="1m", ts=pd.Timestamp(...))
  cache.invalidate(symbol="BTC/USDT", timeframe="1m")
  cache.invalidate_all()

Principios: SRP · SSOT · DIP · KISS
"""
from __future__ import annotations

from typing import Optional

import pandas as pd
from loguru import logger

from market_data.ports.outbound.state import CursorStorePort as CursorStore


class TimestampCacheService:
    """
    Cache L1/L2 de last timestamps para IcebergStorage.

    L1: dict in-process, invalidado explícitamente tras cada write.
    L2: Redis cross-process via CursorStore, opcional (inyectado).

    Thread-safety: GIL protege el dict L1 para operaciones simples.
    Para uso con múltiples threads, considerar threading.Lock en L1.
    """

    def __init__(
        self,
        cursor_store: Optional[CursorStore] = None,
    ) -> None:
        self._cursor = cursor_store
        # L1: keyed por (symbol, timeframe)
        self._l1: dict[tuple[str, str], Optional[pd.Timestamp]] = {}

    # =========================================================================
    # Public API
    # =========================================================================

    def get(
        self,
        symbol:    str,
        timeframe: str,
        exchange:  str = "unknown",
        market_type: str = "unknown",
    ) -> Optional[pd.Timestamp]:
        """
        Busca en L1 → L2. Retorna None si miss en ambos.

        No hace scan Iceberg (L3) — eso es responsabilidad de IcebergStorage.
        """
        key = (symbol, timeframe)

        # L1
        if key in self._l1:
            logger.debug("TimestampCache L1 hit | {}/{}", symbol, timeframe)
            return self._l1[key]

        # L2
        if self._cursor is not None:
            ts = self._l2_get(symbol, timeframe, exchange, market_type)
            if ts is not None:
                # No propagar L2 a L1 — el cursor puede estar adelantado
                # respecto al snapshot Iceberg visible en este proceso.
                return ts

        return None

    def set(
        self,
        symbol:    str,
        timeframe: str,
        ts:        Optional[pd.Timestamp],
    ) -> None:
        """Actualiza L1 con timestamp conocido (post-scan L3)."""
        self._l1[(symbol, timeframe)] = ts

    def invalidate(self, symbol: str, timeframe: str) -> None:
        """Invalida L1 para (symbol, timeframe). Llamar tras save_ohlcv."""
        self._l1.pop((symbol, timeframe), None)

    def invalidate_all(self) -> None:
        """Invalida todo el cache L1. Útil en tests y reinicio."""
        self._l1.clear()

    # =========================================================================
    # L2 helpers
    # =========================================================================

    def _l2_get(
        self,
        symbol:      str,
        timeframe:   str,
        exchange:    str,
        market_type: str,
    ) -> Optional[pd.Timestamp]:
        """
        Lee timestamp desde Redis L2.
        Fail-Soft: retorna None si Redis falla.
        """
        try:
            if self._cursor is None:
                return None
            raw = self._cursor.get_raw(
                f"{exchange.lower()}:{symbol}:{market_type.lower()}:{timeframe}"
            )
            if raw is None:
                logger.debug("TimestampCache L2 miss | {}/{}", symbol, timeframe)
                return None
            ts = pd.Timestamp(int(raw), unit="ms", tz="UTC")
            logger.debug(
                "TimestampCache L2 hit | {}/{} ts={}", symbol, timeframe, ts
            )
            return ts
        except Exception:
            logger.opt(exception=True).debug(
                "TimestampCache L2 error | {}/{}", symbol, timeframe,
            )
            return None
