"""
market_data/adapters/inbound/external
=====================================

Adapters de fuentes externas no-streaming (capacidad external_ingestion,
ADR-0014). Solo adquisición de datos crudos — ninguna lógica de features
ni señales. Cada adapter implementa PollingSourcePort.
"""

from __future__ import annotations

from market_data.adapters.inbound.external.coinglass import CoinglassPollingSource
from market_data.adapters.inbound.external.coinmarketcap import (
    CoinMarketCapPollingSource,
)

__all__ = [
    "CoinMarketCapPollingSource",
    "CoinglassPollingSource",
]
