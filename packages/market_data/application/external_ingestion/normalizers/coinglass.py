"""
market_data/application/external_ingestion/normalizers/coinglass.py
===================================================================

Normalizador CoinGlass: raw → ExternalMetricEvent.

Métricas soportadas (v1)
------------------------
  funding_rate  — rows: {"symbol": "BTC-USDT-PERP", "fundingRate": 0.0001, ...}
  open_interest — rows: {"symbol": "BTC-USDT-PERP", "openInterest": 1234.5, ...}

Convenciones
------------
- Símbolo Coinglass "BTC-USDT-PERP" → "BTC/USDT" (par canónico OCM).
- fundingRate/OI → value como str.
- timestamp: el provider expone "updateTime"/"createdTime" en ms — el
  orquestador pasa la columna concreta via key; este normalizador usa
  "updateTime" por defecto. El normalizador es puro: deriva el
  timestamp del payload (no del reloj).

Fail-fast: una row sin symbol o sin el campo value lanza KeyError con
mensaje explícito — nunca produce eventos corruptos silenciosamente.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from market_data.application.external_ingestion.normalizers.generic import (
    build_external_event,
    coerce_timestamp_ms,
)
from market_data.domain.events.external_events import ExternalMetricEvent

__all__ = ["normalize_coinglass"]


def _canonical_symbol(raw: str) -> str:
    """Convierte "BTC-USDT-PERP" → "BTC/USDT" (primeros dos segmentos)."""
    parts = raw.split("-")
    if len(parts) >= 2:
        return f"{parts[0]}/{parts[1]}"
    return raw


def normalize_coinglass(
    metric: str,
    payload: Sequence[Mapping[str, object]],
    symbols: Sequence[str] | None = None,
    fetched_at_ms: int = 0,
) -> list[ExternalMetricEvent]:
    """Mapea rows crudas de CoinGlass a eventos canónicos.

    `fetched_at_ms` (inyectado por el orquestador) no se usa aquí:
    CoinGlass expone su propio timestamp de mercado por fila
    (updateTime/createdTime), que es el que se propaga al evento.
    """
    if metric == "funding_rate":
        return _normalize_funding(payload, symbols)
    if metric == "open_interest":
        return _normalize_open_interest(payload, symbols)
    raise ValueError(f"normalize_coinglass: métrica '{metric}' no soportada. Soportadas: funding_rate, open_interest.")


def _filter_symbols(
    rows: Sequence[Mapping[str, object]],
    symbols: Sequence[str] | None,
) -> list[Mapping[str, object]]:
    if not symbols:
        return list(rows)
    wanted = {_canonical_symbol(s) for s in symbols}
    return [r for r in rows if _canonical_symbol(str(r["symbol"])) in wanted]


def _normalize_funding(
    payload: Sequence[Mapping[str, object]],
    symbols: Sequence[str] | None,
) -> list[ExternalMetricEvent]:
    rows = _filter_symbols(payload, symbols)

    def _ts(row: Mapping[str, object]) -> int:
        value = row.get("updateTime")
        if value is None:
            value = row["createdTime"]
        return coerce_timestamp_ms(value)

    events: list[ExternalMetricEvent] = []
    for row in rows:
        rate: Any = row["fundingRate"]
        events.append(
            build_external_event(
                source_id="coinglass",
                metric="funding_rate",
                timestamp_ms=_ts(row),
                value=rate,
                symbol=_canonical_symbol(str(row["symbol"])),
            )
        )
    return events


def _normalize_open_interest(
    payload: Sequence[Mapping[str, object]],
    symbols: Sequence[str] | None,
) -> list[ExternalMetricEvent]:
    rows = _filter_symbols(payload, symbols)
    events: list[ExternalMetricEvent] = []
    for row in rows:
        ts = row.get("updateTime")
        if ts is None:
            ts = row["createdTime"]
        events.append(
            build_external_event(
                source_id="coinglass",
                metric="open_interest",
                timestamp_ms=coerce_timestamp_ms(ts),
                value=row["openInterest"],
                symbol=_canonical_symbol(str(row["symbol"])),
            )
        )
    return events
