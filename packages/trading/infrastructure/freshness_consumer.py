# -*- coding: utf-8 -*-
"""
trading/infrastructure/freshness_consumer.py
============================================

Adaptador de Redis para FreshnessConsumerPort.

Permite a trading consumir el estado de frescura publicado por market_data.
"""

import json
from typing import TYPE_CHECKING, cast

from trading.ports.inbound.freshness_consumer import FreshnessConsumerPort

from shared.contracts.boundaries import FreshnessSnapshot

if TYPE_CHECKING:
    import redis

class RedisFreshnessConsumer(FreshnessConsumerPort):
    def __init__(self, redis_client: "redis.Redis") -> None:
        self._redis = redis_client

    def get_freshness(self, stream_id: str) -> FreshnessSnapshot:
        key = f"ocm:freshness:{stream_id}"
        raw = self._redis.get(key)
        if raw is None:
            # Fail-closed: si no hay datos en Redis, se considera un fallo de frescura.
            raise RuntimeError(f"No freshness snapshot in Redis for stream_id: {stream_id}")
            
        data = json.loads(cast("str | bytes", raw))
        return FreshnessSnapshot(**data)
