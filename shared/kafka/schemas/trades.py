# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/trades.py
================================

Wire payloads para microestructura de mercado (trades individuales).

Topología
---------
  RESTTradesPoller / WebSocketTradesStream
      → [trades.raw]    → TradePayload (tick individual)
      → [trades.agg]    → TradeSeriesPayload (ventana agregada)

  Consumers downstream:
      TradeToOHLCVAggregator  → [ohlcv.stream]
      OrderBookBuilder        → estado L2 interno
      MicropriceEngine        → [microprice.rt]
      FeaturePipeline         → [features.rt]

Kappa source field
------------------
Igual que OHLCV: "live" | "backfill" | "replay".
El pipeline downstream no distingue por transporte, solo por source.

Routing key
-----------
  make_symbol_key(exchange, symbol) → b"bybit:BTC/USDT"
  Orden por símbolo — mismo símbolo → misma partición → FIFO.

Schema version history
----------------------
  v1 — campos base: tick individual y serie agregada.

Principios: SSOT · DDD · Kappa · Fail-Fast · KISS
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional

from shared.kafka.schemas._base import (
    _VALID_SOURCES,
    DATASOURCE_BACKFILL,
    DATASOURCE_LIVE,
    DATASOURCE_REPLAY,
    BasePayload,
    DataSource,
    KappaSourceMixin,
    SchemaVersionError,
)

# ---------------------------------------------------------------------------
# Constantes de schema
# ---------------------------------------------------------------------------

TradeSideWire = Literal["buy", "sell", "unknown"]

_VALID_SIDES = frozenset({"buy", "sell", "unknown"})

# Alias de compatibilidad — el canónico es SchemaVersionError (_base.py).
TradeSchemaVersionError = SchemaVersionError


# ---------------------------------------------------------------------------
# TradePayload — tick individual → trades.raw
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TradePayload(BasePayload, KappaSourceMixin):
    """
    Tick individual de microestructura en formato wire.

    Un RawTrade del dominio serializado para cruzar la frontera BC.
    Publicado a: trades.raw
    Consumidores: TradeToOHLCVAggregator, OrderBookBuilder (indirecto),
                  FeaturePipeline.

    Campos
    ------
    exchange     : exchange de origen
    market_type  : "spot" | "linear" | "inverse"
    symbol       : par normalizado
    trade_id     : ID nativo del exchange (dedup)
    timestamp_ms : Unix epoch ms UTC
    price        : precio de ejecución (str para preservar precisión Decimal)
    amount       : cantidad base ejecutada (str)
    side         : "buy" | "sell" | "unknown"
    source       : Kappa discriminator — KappaSourceMixin
    run_id       : correlación con el run

    Nota sobre price/amount como str
    ---------------------------------
    RawTrade usa Decimal internamente. JSON no tiene tipo Decimal.
    Transportamos como str para preservar precisión exacta del exchange
    sin pérdida de float. El consumer convierte str → Decimal en ACL.
    """

    exchange: str = ""
    market_type: str = ""
    symbol: str = ""
    trade_id: str = ""
    timestamp_ms: int = 0
    price: str = "0"  # Decimal serializado como str
    amount: str = "0"  # Decimal serializado como str
    side: TradeSideWire = "unknown"
    run_id: str = ""
    meta: Optional[Dict[str, Any]] = None

    # Kappa helpers is_live/is_backfill/is_replay heredados de KappaSourceMixin.

    # ------------------------------------------------------------------
    # Serialización
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "exchange": self.exchange,
                "market_type": self.market_type,
                "symbol": self.symbol,
                "trade_id": self.trade_id,
                "timestamp_ms": self.timestamp_ms,
                "price": self.price,
                "amount": self.amount,
                "side": self.side,
                "source": self.source,
                "run_id": self.run_id,
                "meta": self.meta,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TradePayload":
        version = int(data.get("event_version", 1))
        if version != cls.SCHEMA_VERSION:
            raise SchemaVersionError(f"TradePayload schema v{version} incompatible con v{cls.SCHEMA_VERSION} esperada.")
        source = data.get("source", DATASOURCE_LIVE)
        if source not in _VALID_SOURCES:
            raise SchemaVersionError(f"TradePayload.source desconocido: {source!r}. Válidos: {sorted(_VALID_SOURCES)}.")
        side = data.get("side", "unknown")
        if side not in _VALID_SIDES:
            side = "unknown"  # fail-soft: lado desconocido no es error fatal
        return cls(
            event_id=str(data["event_id"]),
            occurred_at=str(data.get("occurred_at", "")),
            exchange=str(data["exchange"]),
            market_type=str(data["market_type"]),
            symbol=str(data["symbol"]),
            trade_id=str(data["trade_id"]),
            timestamp_ms=int(data["timestamp_ms"]),
            price=str(data["price"]),
            amount=str(data["amount"]),
            side=side,
            source=source,
            run_id=str(data.get("run_id", "")),
            meta=data.get("meta"),
        )


# ---------------------------------------------------------------------------
# TradeSeriesPayload — ventana agregada → trades.agg
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TradeSeriesPayload(BasePayload, KappaSourceMixin):
    """
    Ventana temporal agregada de trades en formato wire.

    Una TradeSeries del dominio serializada para cruzar la frontera BC.
    Publicado a: trades.agg
    Consumidores: FeaturePipeline (VWAP, OFI, momentum).

    Campos de microestructura
    -------------------------
    vwap            : VWAP de la ventana (str, Decimal serializado)
    total_volume    : volumen total base (str)
    total_cost      : coste total cotizado (str)
    buy_volume      : volumen comprador (str)
    sell_volume     : volumen vendedor (str)
    buy_sell_imbalance : OFI ∈ [-1, +1] (float — pérdida de precisión
                      aceptable para feature downstream; no para ejecución)
    open_price      : precio del primer trade de la ventana (str)
    close_price     : precio del último trade (str)
    high_price      : máximo de la ventana (str)
    low_price       : mínimo de la ventana (str)
    trade_count     : número de trades en la ventana
    window_start_ms : timestamp inicio de ventana (ms UTC)
    window_end_ms   : timestamp fin de ventana (ms UTC)
    """

    exchange: str = ""
    market_type: str = ""
    symbol: str = ""
    window_start_ms: int = 0
    window_end_ms: int = 0
    trade_count: int = 0
    vwap: str = "0"
    total_volume: str = "0"
    total_cost: str = "0"
    buy_volume: str = "0"
    sell_volume: str = "0"
    buy_sell_imbalance: float = 0.0
    open_price: str = "0"
    close_price: str = "0"
    high_price: str = "0"
    low_price: str = "0"
    run_id: str = ""
    meta: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "exchange": self.exchange,
                "market_type": self.market_type,
                "symbol": self.symbol,
                "window_start_ms": self.window_start_ms,
                "window_end_ms": self.window_end_ms,
                "trade_count": self.trade_count,
                "vwap": self.vwap,
                "total_volume": self.total_volume,
                "total_cost": self.total_cost,
                "buy_volume": self.buy_volume,
                "sell_volume": self.sell_volume,
                "buy_sell_imbalance": self.buy_sell_imbalance,
                "open_price": self.open_price,
                "close_price": self.close_price,
                "high_price": self.high_price,
                "low_price": self.low_price,
                "source": self.source,
                "run_id": self.run_id,
                "meta": self.meta,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TradeSeriesPayload":
        version = int(data.get("event_version", 1))
        if version != cls.SCHEMA_VERSION:
            raise SchemaVersionError(
                f"TradeSeriesPayload schema v{version} incompatible con v{cls.SCHEMA_VERSION} esperada."
            )
        source = data.get("source", DATASOURCE_LIVE)
        if source not in _VALID_SOURCES:
            raise SchemaVersionError(f"TradeSeriesPayload.source desconocido: {source!r}.")
        return cls(
            event_id=str(data["event_id"]),
            occurred_at=str(data.get("occurred_at", "")),
            exchange=str(data["exchange"]),
            market_type=str(data["market_type"]),
            symbol=str(data["symbol"]),
            window_start_ms=int(data["window_start_ms"]),
            window_end_ms=int(data["window_end_ms"]),
            trade_count=int(data["trade_count"]),
            vwap=str(data["vwap"]),
            total_volume=str(data["total_volume"]),
            total_cost=str(data["total_cost"]),
            buy_volume=str(data["buy_volume"]),
            sell_volume=str(data["sell_volume"]),
            buy_sell_imbalance=float(data["buy_sell_imbalance"]),
            open_price=str(data["open_price"]),
            close_price=str(data["close_price"]),
            high_price=str(data["high_price"]),
            low_price=str(data["low_price"]),
            source=source,
            run_id=str(data.get("run_id", "")),
            meta=data.get("meta"),
        )


# Alias de compatibilidad — SSOT de versión: SCHEMA_VERSION de cada clase.
TRADE_SCHEMA_VERSION: int = TradePayload.SCHEMA_VERSION
TRADE_SERIES_SCHEMA_VERSION: int = TradeSeriesPayload.SCHEMA_VERSION


__all__ = [
    "TRADE_SCHEMA_VERSION",
    "TRADE_SERIES_SCHEMA_VERSION",
    "TradeSideWire",
    "DataSource",
    "DATASOURCE_LIVE",
    "DATASOURCE_BACKFILL",
    "DATASOURCE_REPLAY",
    "TradeSchemaVersionError",
    "TradePayload",
    "TradeSeriesPayload",
]
