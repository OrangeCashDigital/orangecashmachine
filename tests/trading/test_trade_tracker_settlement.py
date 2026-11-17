from datetime import datetime, timedelta, timezone

from trading.analytics.trade_tracker import TradeTracker
from trading.domain.models.settlement import FeeStatus, Settlement


def _settlement():
    return Settlement.compute(
        order_id="ord-1",
        symbol="BTC/USDT",
        closed_qty=1.5,
        avg_entry_price=50000.0,
        exit_price=51000.0,
        fee_amount_usd=2.0,
        fee_currency="USDT",
        fee_status=FeeStatus.KNOWN,
    )


def test_record_settlement_registra_trade():
    tracker = TradeTracker()
    entry = datetime.now(timezone.utc) - timedelta(minutes=5)
    trade = tracker.record_settlement(
        _settlement(),
        exchange="bybit",
        timeframe="1m",
        size_pct=0.5,
        entry_at=entry,
    )
    assert trade is not None
    assert trade.trade_id == "ord-1"
    assert trade.closed_qty == 1.5
    assert len(tracker._closed) == 1


def test_record_settlement_devuelve_none_si_falla():
    tracker = TradeTracker()
    trade = tracker.record_settlement(
        _settlement(),
        exchange="bybit",
        timeframe="1m",
        size_pct=0.5,
        entry_at="no-es-fecha",  # type: ignore[arg-type]
    )
    assert trade is None
    assert len(tracker._closed) == 0
