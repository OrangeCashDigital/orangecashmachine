# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/external/coinmarketcap.py
======================================================

CoinMarketCap API — implementa PollingSourcePort.

Métrica
-------
  market_metrics → GET /v1/global-metrics/quotes/latest
                   (btc_dominance, eth_dominance, total_market_cap_usd, ...)

Solo adquisición: devuelve PollingResult crudo (JSON provider-native).
La normalización ocurre en application/external_ingestion/normalizers.

BC-52: no importa SDK de vendor a nivel módulo — usa aiohttp (HTTP genérico).
"""

from __future__ import annotations

import aiohttp

from market_data.adapters.inbound.external.base import HTTPDataSourceBase
from market_data.ports.inbound.external.errors import (
    ExternalRateLimitError,
    ExternalSourceUnavailable,
)
from market_data.ports.inbound.external.polling import (
    PollingRequest,
    PollingResult,
    PollingSourcePort,
)

__all__ = ["CoinMarketCapPollingSource"]

_SUPPORTED_METRICS = frozenset({"market_metrics"})


class CoinMarketCapPollingSource(HTTPDataSourceBase, PollingSourcePort):
    """Adapter de polling de métricas globales de CoinMarketCap."""

    source_id: str = "coinmarketcap"

    def __init__(self, api_key: str) -> None:
        super().__init__(
            base_url="https://pro-api.coinmarketcap.com/v1",
            headers={
                "X-CMC_PRO_API_KEY": api_key,
                "Accept": "application/json",
            },
        )
        self.api_key = api_key

    async def fetch(self, request: PollingRequest) -> PollingResult:
        if request.metric not in _SUPPORTED_METRICS:
            raise ValueError(
                f"CoinMarketCapPollingSource: métrica '{request.metric}' no soportada. "
                f"Soportadas: {sorted(_SUPPORTED_METRICS)}"
            )
        url = f"{self.base_url}/global/market/quotes/latest"
        try:
            async with self._get_session().get(url) as resp:
                if resp.status == 429:
                    raise ExternalRateLimitError("CoinMarketCap 429 (rate limit) en market_metrics")
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError as exc:
            raise ExternalSourceUnavailable(f"CoinMarketCap unreachable en /market_metrics: {exc}") from exc

        inner = (data or {}).get("data") if isinstance(data, dict) else {}
        rows = [dict(inner)] if isinstance(inner, dict) else []
        return PollingResult(source_id=self.source_id, metric=request.metric, payload=rows)
