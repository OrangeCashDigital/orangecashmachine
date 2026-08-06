"""
market_data/domain/events/external_events.py
=============================================

Evento canónico del dominio para datos no-streaming externos.

Es el LENGUAJE ÚNICO al que se convierten TODAS las fuentes externas
(CoinGlass, CoinMarketCap, Glassnode, FRED, ... — ver ADR-0014). El
consumidor downstream no sabe de qué transporte vino el dato.

Un solo evento escalar por (source_id, metric, symbol, timestamp):
la 'value' se serializa como str (Decimal-safe) por consistencia con la
convención de funding_rate del repo.

quality_flags es la reserva data_quality (ADR-0014): la validación
completa llega en fases posteriores; aquí se transportan los flags
crudos sin transformación.

Principios: SSOT · Kappa · DDD · KISS
"""

from __future__ import annotations

from dataclasses import dataclass, field

from market_data.domain.events._base import DomainEvent

__all__ = ["ExternalMetricEvent"]


@dataclass(frozen=True)
class ExternalMetricEvent(DomainEvent):
    """Un dato canónico de una fuente externa.

    source_id      : identidad de la fuente ("coinglass", "coinmarketcap", ...).
    metric         : nombre canónico de la métrica.
    timestamp_ms   : Unix epoch ms UTC del dato (mercado), NO del fetch.
    symbol         : None = métrica global (p.ej. BTC dominance).
    value          : valor canónico como str (Decimal-safe).
    quality_flags  : reserva data_quality; vacío si sin novedades.
    """

    source_id: str = ""
    metric: str = ""
    timestamp_ms: int = 0
    value: str = "0"
    symbol: str | None = None
    quality_flags: tuple[str, ...] = field(default_factory=tuple)
