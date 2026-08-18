# -*- coding: utf-8 -*-
"""
tests/trading/test_transport_mapping.py
==========================================

Mapeo de órdenes CCXT → OrderState (F3/ADR-0016) en composition_root.

Cubre el contrato de transporte: estados crudos del exchange (closed → FILLED,
open → SUBMITTED, canceled → CANCELLED, rejected → REJECTED) y los campos
filled/average a OrderState. Es el puente trading↔market_data (BC-50).

También cubre la cancelación (F3/ADR-0029, B-MD-008 paso 3): el port
OrderTransport.cancel, PaperTransport.cancel y _BybitTransport.cancel.
"""

from __future__ import annotations

from trading.bootstrap.composition_root import _BybitTransport, map_ccxt_order
from trading.execution.transport import OrderStatus, PaperTransport


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


# ----------------------------------------------------------------------
# Cancelación (ADR-0029, B-MD-008 paso 3)
# ----------------------------------------------------------------------


def test_paper_transport_cancel_returns_cancelled() -> None:
    """PaperTransport confirma CANCELLED sin I/O (ADR-0029 decisión 2)."""
    t = PaperTransport()
    state = t.cancel("BTC/USDT", "e1")
    assert state.status == OrderStatus.CANCELLED
    assert state.order_id == "e1"
    assert state.error is None


def test_paper_transport_submit_still_filled() -> None:
    """PaperTransport sigue confirmando FILLED en submit (sin regresión)."""
    t = PaperTransport()
    state = t.submit("BTC/USDT", "buy", 0.02, client_order_id="c1")
    assert state.status == OrderStatus.FILLED


def test_bybit_transport_cancel_maps_confirmed_cancelled(monkeypatch) -> None:
    """_BybitTransport.cancel delega en CCXTAdapter.cancel_order y mapea el raw."""
    from trading.bootstrap import composition_root as cr

    captured: dict[str, object] = {}

    def _fake_run(factory, op):
        # run_ccxt_async real ejecuta op(adapter) con await (asyncio.run).
        class _FakeAdapter:
            async def cancel_order(self, order_id: str, symbol: str, **kwargs):
                captured["order_id"] = order_id
                captured["symbol"] = symbol
                return {"id": order_id, "status": "canceled"}

            async def connect(self) -> None:
                return None

            async def close(self) -> None:
                return None

        import asyncio

        return asyncio.run(op(_FakeAdapter()))

    monkeypatch.setattr(cr, "run_ccxt_async", _fake_run)

    transport = _BybitTransport.__new__(_BybitTransport)
    transport._factory = lambda: None
    transport._exchange = "bybit"

    state = transport.cancel("BTC/USDT", "e1")

    assert captured == {"order_id": "e1", "symbol": "BTC/USDT"}
    assert state.status == OrderStatus.CANCELLED
    assert state.order_id == "e1"


def test_bybit_transport_cancel_fail_closed_on_error(monkeypatch) -> None:
    """Errores del transporte → OrderState.ERROR (SafeOps, fail-closed)."""
    from trading.bootstrap import composition_root as cr

    def _fake_run(factory, op):
        raise RuntimeError("network down")

    monkeypatch.setattr(cr, "run_ccxt_async", _fake_run)

    transport = _BybitTransport.__new__(_BybitTransport)
    transport._factory = lambda: None
    transport._exchange = "bybit"

    state = transport.cancel("BTC/USDT", "e1")

    assert state.status == OrderStatus.ERROR
    assert "network down" in (state.error or "")
    assert state.order_id == "e1"
