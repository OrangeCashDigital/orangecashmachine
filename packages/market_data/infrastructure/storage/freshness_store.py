# -*- coding: utf-8 -*-
"""
market_data/infrastructure/storage/freshness_store.py
=====================================================

FreshnessStore — almacenamiento compartido en Redis para estados de freshness.

Responsabilidad
---------------
Persistir snapshots de freshness producidos por GapAwareStream para que
el consumer en trading (en otro proceso) pueda consultarlos.

Diseño: Redis L1/L2 (mismo patrón que CursorStore).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Optional

from loguru import logger

from shared.contracts.boundaries import FreshnessSnapshot
from market_data.ports.outbound.freshness_store_port import FreshnessStorePort

if TYPE_CHECKING:
    import redis

class FreshnessStore:
    def __init__(self, redis_client: Optional[redis.Redis], ttl_s: int = 60) -> None:
        self._redis = redis_client
        self._ttl_s = ttl_s
        self._log = logger.bind(component="FreshnessStore")

    def _key(self, stream_id: str) -> str:
        return f"ocm:freshness:{stream_id}"

    def set(self, stream_id: str, snapshot: FreshnessSnapshot) -> None:
        if self._redis is None:
            return
        try:
            data = {
                "stream_id": snapshot.stream_id,
                "stream_type": snapshot.stream_type,
                "last_seen_ms": snapshot.last_seen_ms,
                "lag_ms": snapshot.lag_ms,
                "is_stale": snapshot.is_stale,
                "staleness_reason": snapshot.staleness_reason,
                "threshold_ms": snapshot.threshold_ms,
                "read_at_ms": snapshot.read_at_ms,
                "source": snapshot.source,
            }
            self._redis.setex(self._key(stream_id), self._ttl_s, json.dumps(data))
        except Exception as exc:
            self._log.warning("FreshnessStore.set Redis error | {} | {}", stream_id, exc)

    def get(self, stream_id: str) -> Optional[FreshnessSnapshot]:
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(self._key(stream_id))
            if raw is None:
                return None
            data = json.loads(raw)
            return FreshnessSnapshot(**data)
        except Exception as exc:
            self._log.warning("FreshnessStore.get Redis error | {} | {}", stream_id, exc)
            return None
