"""
market_data/application/external_ingestion/normalizers/generic.py
=================================================================

Helpers puros para construir ExternalMetricEvent (evento canónico).

Reglas
------
- Funciones puras, sin I/O, sin framework.
- El adapter NO normaliza: entrega raw. El normalizador mapea raw →
  evento canónico (separación adquisición/transformación, ADR-0014).
- value se serializa a str (Decimal-safe) — convención del repo.

timestamp_ms es el timestamp de MERCADO del dato, no el del fetch.
Los providers entregan timestamps en su propia escala; cada normalizador
es responsable de convertirlos a Unix epoch ms UTC.
"""

from __future__ import annotations

from typing import Any

from market_data.domain.events.external_events import ExternalMetricEvent

__all__ = ["build_external_event", "coerce_timestamp_ms"]


def build_external_event(
    *,
    source_id: str,
    metric: str,
    timestamp_ms: int,
    value: Any,
    symbol: str | None = None,
) -> ExternalMetricEvent:
    """Construye el evento canónico a partir de un escalar crudo.

    value: float/int/str — se serializa a str sin pérdida decimal.
    """
    return ExternalMetricEvent(
        source_id=source_id,
        metric=metric,
        timestamp_ms=timestamp_ms,
        value=str(value),
        symbol=symbol,
    )


def coerce_timestamp_ms(value: Any) -> int:
    """Coerce un timestamp crudo a Unix epoch ms.

    - int/float sin decimales → se asume ya en ms.
    - str numérico → int.
    - cualquier otra cosa → TypeError (fail-fast, no silencio).
    """
    if isinstance(value, bool):
        raise TypeError(f"coerce_timestamp_ms: bool no es timestamp ({value!r})")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError as err:
            raise TypeError(f"coerce_timestamp_ms: str no numérico ({value!r})") from err
    raise TypeError(f"coerce_timestamp_ms: tipo no soportado ({type(value).__name__})")
