"""
market_data/application/external_ingestion/normalizers/coinmarketcap.py
=======================================================================

Normalizador CoinMarketCap: raw → ExternalMetricEvent.

Métricas soportadas (v1)
------------------------
  market_metrics — rows: {"btc_dominance": 55.2, "eth_dominance": 9.1,
                          "total_market_cap_usd": ..., ...}

Cada clave cruda se convierte en un evento global (symbol=None) con
metric = nombre de clave canónico (btc_dominance → "btc_dominance").

timestamp: el snapshot global no expone un timestamp de mercado por
fila; el orquestador inyecta `fetched_at_ms` (momento de captura) de
forma explícita. Así el normalizador es puro y determinista: dado el
mismo payload y el mismo fetched_at_ms produce los mismos eventos
(sin datetime.now()).

Fail-fast: si el payload no es un mapping plano, lanza TypeError.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from market_data.application.external_ingestion.normalizers.generic import (
    build_external_event,
)
from market_data.domain.events.external_events import ExternalMetricEvent

__all__ = ["normalize_coinmarketcap"]


def normalize_coinmarketcap(
    metric: str,
    payload: Sequence[Mapping[str, object]],
    symbols: Sequence[str] | None = None,
    fetched_at_ms: int = 0,
) -> list[ExternalMetricEvent]:
    """Mapea el payload de métricas globales de CoinMarketCap a eventos.

    El payload (ya extraído de data[0]) se trata como un dict de
    métricas globales: una fila = un dict con N claves escalares.

    `fetched_at_ms` es el timestamp de captura (unic epoch ms UTC)
    inyectado por el orquestador — nunca se deriva del reloj aquí.
    """
    if metric != "market_metrics":
        raise ValueError(f"normalize_coinmarketcap: métrica '{metric}' no soportada. Soportada: market_metrics.")

    now_ms = fetched_at_ms
    events: list[ExternalMetricEvent] = []
    for row in payload:
        if not isinstance(row, Mapping):
            raise TypeError("normalize_coinmarketcap: cada row debe ser un Mapping de métricas globales.")
        for key, value in row.items():
            if isinstance(value, (dict, list)):
                # Métricas no escalares (p.ej. arrays de series) se omiten:
                # el modelo canónico es escalar por (metric, symbol, timestamp).
                continue
            events.append(
                build_external_event(
                    source_id="coinmarketcap",
                    metric=str(key),
                    timestamp_ms=now_ms,
                    value=value,
                )
            )
    return events
