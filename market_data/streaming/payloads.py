from __future__ import annotations

"""Event payload definitions for streaming ingestion."""

from dataclasses import dataclass, asdict
from typing import List, Dict, Any


@dataclass(frozen=True)
class OHLCVBar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ts": self.ts,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OHLCVBar":
        return cls(
            ts=int(data["ts"]),
            open=float(data["open"]),
            high=float(data["high"]),
            low=float(data["low"]),
            close=float(data["close"]),
            volume=float(data["volume"]),
        )


@dataclass(frozen=True)
class EventPayload:
    event_id: str
    exchange: str
    symbol: str
    timeframe: str
    batch_start_ts: int
    bars: List[OHLCVBar]
    meta: Dict[str, Any] | None = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "batch_start_ts": self.batch_start_ts,
            "bars": [b.to_dict() for b in self.bars],
            "meta": self.meta,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EventPayload":
        bars = [OHLCVBar.from_dict(b) for b in data["bars"]]
        return cls(
            event_id=str(data["event_id"]),
            exchange=str(data["exchange"]),
            symbol=str(data["symbol"]),
            timeframe=str(data["timeframe"]),
            batch_start_ts=int(data["batch_start_ts"]),
            bars=bars,
            meta=data.get("meta"),
        )
