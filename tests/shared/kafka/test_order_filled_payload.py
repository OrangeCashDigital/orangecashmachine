from shared.kafka.schemas.orders import OrderFilledPayload


def _payload(**kw):
    base = dict(
        event_id="evt-1",
        occurred_at="2026-01-01T00:00:00Z",
        order_id="ord-1",
        exchange="bybit",
        symbol="BTC/USDT",
        side="sell",
        fill_price=51000.0,
        size_pct=0.5,
    )
    base.update(kw)
    return OrderFilledPayload(**base)


def test_campos_economicos_round_trip():
    p = _payload(
        filled_qty=1.5,
        fee_amount_usd=2.5,
        fee_currency="USDT",
        fee_status="known",
        timeframe="1m",
        closed_qty=1.5,
        avg_entry_price=50000.0,
        remaining_qty=0.5,
    )
    assert OrderFilledPayload.from_dict(p.to_dict()) == p


def test_fee_unknown_se_conserva_como_none():
    p = _payload(fee_amount_usd=None)
    assert OrderFilledPayload.from_dict(p.to_dict()).fee_amount_usd is None
