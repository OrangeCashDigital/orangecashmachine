# -*- coding: utf-8 -*-
"""
tests/trading/test_transport_mapping.py
==========================================

Mapeo de órdenes CCXT → OrderState (F3/ADR-0016) en composition_root.

Cubre el contrato de transporte: estados crudos del exchange (closed → FILLED,
open → SUBMITTED, canceled → CANCELLED, rejected → REJECTED) y los campos
filled/average a OrderState. Es el puente trading↔market_data (BC-50).
"""

from __future__ import annotations

from trading.bootstrap.composition_root import map_ccxt_order
from trading.execution.transport import OrderStatus


def test_closed_maps_to_filled() -> None:
    raw = {"id": "o1", "status": "closed", "filled": 0.02, "average": 50_000.0}
    state = map_ccxt_order(raw)
    assert state.status == OrderStatus.FILLED
    assert state.fill_price == 50_000.0
    assert state.confirmed_filled


def test_open_maps_to_submitted() -> None:
    state = map_ccxt_order({"id": "o2", "status": "open"})
    assert state.status == OrderStatus.SUBMITTED
    assert not state.confirmed_filled


def test_canceled_rejected_mapping() -> None:
    assert map_ccxt_order({"status": "canceled"}).status == OrderStatus.CANCELLED
    assert map_ccxt_order({"status": "cancelled"}).status == OrderStatus.CANCELLED
    assert map_ccxt_order({"status": "rejected"}).status == OrderStatus.REJECTED
    assert map_ccxt_order({"status": "expired"}).status == OrderStatus.REJECTED


def test_unknown_status_defaults_submitted() -> None:
    assert map_ccxt_order({"status": "weird"}).status == OrderStatus.SUBMITTED


def test_order_id_and_missing_optional_fields() -> None:
    state = map_ccxt_order({"id": "o9", "status": "closed"})
    assert state.order_id == "o9"
    assert state.filled_qty is None
    assert state.fill_price is None
