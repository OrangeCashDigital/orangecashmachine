# -*- coding: utf-8 -*-
"""
tests/portfolio/test_position_store_unicity.py
================================================

No-regresión de B-16 / H-08 — unicidad del order_id en el PositionStore.

Hallazgo (docs/audits/2026-08-auditoria-integral.md H-08): Order.order_id
usaba str(uuid.uuid4())[:8] (32 bits de entropía). El order_id es la clave
de posición en el PositionStore (InMemory o Redis): un order_id truncado
eleva la probabilidad de colisión a volumen alto → overwrite silencioso de
una posición abierta por otra (riesgo de portfolio).

Fix: order_id = UUID4 completo (36 chars) + los stores elevan
PositionIdCollisionError si dos posiciones DISTINTAS comparten order_id en
lugar de sobrescribir en silencio.

Principios de test: Aislamiento (stores reales en memoria, sin Redis),
Fail-Fast (una responsabilidad), Nomenclatura test_<método>_<condición>.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from portfolio.infra.memory_store import InMemoryPositionStore
from portfolio.models.position import PositionIdCollisionError, PositionSnapshot


def _pos(order_id: str, symbol: str = "BTC/USDT", side: str = "long") -> PositionSnapshot:
    return PositionSnapshot(
        symbol=symbol,
        exchange="bybit",
        side=side,
        quantity=1.0,
        avg_entry=50_000.0,
        size_pct=0.10,
        entry_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        order_id=order_id,
    )


def test_order_id_generated_is_full_uuid_not_truncated() -> None:
    """B-16: Order.order_id deja de generarse truncado a 8 chars."""
    from trading.execution.order import Order, OrderSide
    from trading.strategies.base import Signal

    signal = Signal(
        symbol="BTC/USDT",
        timeframe="1m",
        direction="long",
        price=50_000.0,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        confidence=1.0,
    )
    order = Order(symbol="BTC/USDT", side=OrderSide.BUY, size_pct=0.10, signal=signal)
    # UUID4 completo = 36 chars con guiones (8-4-4-4-12) o 32 hex.
    assert len(order.order_id) >= 32
    parsed = str(uuid.UUID(order.order_id))  # lanza si no es formato UUID
    assert len(parsed) == 36


def test_memory_store_rejects_distinct_position_same_id() -> None:
    """Posiciones distintas con el mismo order_id → colisión, sin overwrite."""
    store = InMemoryPositionStore()
    store.save(_pos("id-1", symbol="BTC/USDT"))
    with pytest.raises(PositionIdCollisionError, match="id-1"):
        store.save(_pos("id-1", symbol="ETH/USDT"))  # distinto symbol, mismo id


def test_memory_store_allows_same_identity_resave() -> None:
    """Re-guardar la MISMA posición (misma identidad) es idempotente, sin error."""
    store = InMemoryPositionStore()
    p = _pos("id-1")
    store.save(p)
    store.save(p)  # mismo id y misma identidad → OK
    assert len(store.all()) == 1


def test_memory_store_distinct_ids_do_not_collide() -> None:
    """order_ids distintos (estado post-fix) cohabitan sin colisión."""
    store = InMemoryPositionStore()
    store.save(_pos("a" * 36, symbol="BTC/USDT"))
    store.save(_pos("b" * 36, symbol="ETH/USDT"))
    assert len(store.all()) == 2


def test_redis_store_rejects_distinct_position_same_id() -> None:
    """RedisPositionStore también eleva colisión en vez de sobrescribir."""
    from portfolio.infra.redis_store import RedisPositionStore

    client = MagicMock(name="redis_client")
    # Sin posición previa → get devuelve None
    client.get.return_value = None
    store = RedisPositionStore(redis_client=client, exchange="bybit")

    store.save(_pos("id-1", symbol="BTC/USDT"))

    # Segunda save con MISMO order_id pero distinto symbol → Redis ya tiene valor
    client.get.return_value = (
        b'{"order_id":"id-1","symbol":"BTC/USDT","exchange":"bybit","side":"long",'
        b'"quantity":1.0,"avg_entry":50000.0,"size_pct":0.1,'
        b'"entry_at":"2024-01-01T00:00:00+00:00"}'
    )
    with pytest.raises(PositionIdCollisionError, match="id-1"):
        store.save(_pos("id-1", symbol="ETH/USDT"))


def test_redis_store_safeops_when_collision_check_redis_down() -> None:
    """Redis caído durante el control de colisión → guard se omite, sin cruzar.

    SafeOps del RedisPositionStore: un fallo de conexión en el GET del control
    de colisión NO debe elevar (se loggea warning y se cae al try de save, que
    también es fail-soft). El orden-id completo ya hace la colisión práctica
    imposible; el guard es solo defensa en profundidad.
    """
    from portfolio.infra.redis_store import RedisPositionStore

    client = MagicMock(name="redis_client")
    client.get.side_effect = ConnectionError("broker caído")
    # get lanza → no hay interceptación de colisión; save() cae a su try
    # propio (también lanza, loggeado, nunca al caller).
    store = RedisPositionStore(redis_client=client, exchange="bybit")
    store.save(_pos("id-1", symbol="BTC/USDT"))  # no debe lanzar
