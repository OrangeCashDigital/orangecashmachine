"""
market_data/application/external_ingestion/normalizers
======================================================

Normalizadores raw → evento canónico por provider.

El orquestador NO sabe de proveedores individuales: le pide a
normalize() el conjunto. Cada provider registra su normalizador puro
en _REGISTRY. Fallar a un provider desconocido = fail-fast (nunca
publicar eventos corruptos).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from market_data.application.external_ingestion.normalizers import (  # noqa: F401
    coinglass,
    coinmarketcap,
    generic,  # noqa: F401
)
from market_data.application.external_ingestion.normalizers.generic import (
    build_external_event,
    coerce_timestamp_ms,
)
from market_data.domain.events.external_events import ExternalMetricEvent

__all__ = [
    "Normalizer",
    "get_normalizer",
    "normalize",
    "build_external_event",
    "coerce_timestamp_ms",
]

Normalizer = Callable[
    [
        str,
        Sequence[Mapping[str, object]],
        Sequence[str] | None,
        int,
    ],
    list[ExternalMetricEvent],
]

_REGISTRY: dict[str, Normalizer] = {
    "coinglass": coinglass.normalize_coinglass,
    "coinmarketcap": coinmarketcap.normalize_coinmarketcap,
}


def get_normalizer(source_id: str) -> Normalizer:
    """Devuelve el normalizador para una fuente. Unknown → KeyError adjuntado."""
    try:
        return _REGISTRY[source_id]
    except KeyError:
        raise ValueError(
            f"get_normalizer: sin normalizador registrado para source_id='{source_id}'. Conocidos: {sorted(_REGISTRY)}"
        ) from None


def normalize(
    source_id: str,
    metric: str,
    payload: Sequence[Mapping[str, object]],
    symbols: Sequence[str] | None = None,
    *,
    fetched_at_ms: int,
) -> list[ExternalMetricEvent]:
    """Convierte raw de una fuente a eventos canónicos (punto de entrada).

    `fetched_at_ms` es el timestamp de captura (Unix epoch ms UTC) que
    el orquestador obtiene de PollingResult.fetched_at. Los normalizadores
    son puros: reciben el timestamp de forma explícita, nunca del reloj.
    """
    return get_normalizer(source_id)(metric, payload, symbols, fetched_at_ms)
