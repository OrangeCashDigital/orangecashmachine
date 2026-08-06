# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/external.py
================================

Wire payload canónico para datos no-streaming de fuentes externas.

Topología
---------
  Fuente externa (CoinGlass / CoinMarketCap / Glassnode / FRED / ...)
      → [external.raw]  → ExternalMetricPayload

El external.raw es el topic único del modelo canónico de eventos de
ADR-0014: un mismo evento homogéneo (source, metric, symbol, ts, value)
para cualquier fuente no-streaming. Los processors downstream enrutan
por metric — no saben de qué transporte vino el dato.

Routing key
-----------
  make_external_key(source_id, metric, symbol) → b"coinglass:funding_rate:BTC/USDT"
  symbol=None (métrica global) → b"coinglass:btc_dominance:global"

Schema version history
----------------------
  v1 — campos base: source_id, metric, symbol, timestamp_ms, value,
       quality_flags.

Principios: SSOT · Kappa · Fail-Fast · KISS
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from shared.kafka.schemas._base import BasePayload, SchemaVersionError

# Alias de compatibilidad — el canónico es SchemaVersionError (_base.py).
ExternalSchemaVersionError = SchemaVersionError


@dataclass(frozen=True)
class ExternalMetricPayload(BasePayload):
    """Un dato canónico escalar de una fuente externa no-streaming.

    source_id      : identidad de la fuente ("coinglass", "coinmarketcap", ...).
    metric         : nombre canónico de la métrica ("funding_rate",
                     "open_interest", "btc_dominance", "altcoin_season_index", ...).
    symbol         : par normalizado (ej. "BTC/USDT"). None = métrica global.
    timestamp_ms   : Unix epoch ms UTC del dato de mercado.
    value          : valor canónico como str (Decimal-safe). Ej. "0.0001".
    quality_flags  : reserva data_quality (ADR-0014). Vacío si sin novedades.
    """

    source_id: str = ""
    metric: str = ""
    symbol: Optional[str] = None
    timestamp_ms: int = 0
    value: str = "0"
    quality_flags: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update(
            {
                "source_id": self.source_id,
                "metric": self.metric,
                "symbol": self.symbol,
                "timestamp_ms": self.timestamp_ms,
                "value": self.value,
                "quality_flags": list(self.quality_flags),
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExternalMetricPayload":
        version = int(data.get("event_version", 1))
        if version != cls.SCHEMA_VERSION:
            raise ExternalSchemaVersionError(
                f"ExternalMetricPayload schema v{version} incompatible con v{cls.SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id=str(data["event_id"]),
            occurred_at=str(data.get("occurred_at", "")),
            source_id=str(data["source_id"]),
            metric=str(data["metric"]),
            symbol=data.get("symbol"),
            timestamp_ms=int(data["timestamp_ms"]),
            value=str(data["value"]),
            quality_flags=tuple(data.get("quality_flags") or []),
        )


# Alias de compatibilidad — SSOT de versión: ExternalMetricPayload.SCHEMA_VERSION.
EXTERNAL_SCHEMA_VERSION: int = ExternalMetricPayload.SCHEMA_VERSION


__all__ = [
    "EXTERNAL_SCHEMA_VERSION",
    "ExternalMetricPayload",
    "ExternalSchemaVersionError",
]
