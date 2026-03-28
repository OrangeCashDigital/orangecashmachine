from __future__ import annotations

"""
services/data_providers/coinmarketcap_adapter.py
================================================

Adapter para la API de CoinMarketCap (métricas globales de mercado).

Uso
---
    async with CoinMarketCapAdapter(api_key="...") as adapter:
        metrics = await adapter.fetch_global_metrics()
"""

from typing import Any, Dict, Optional

import aiohttp

from services.data_providers.base import DataProviderAdapter

# Timeouts por operación (segundos)
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30.0, connect=10.0)


class CoinMarketCapAdapter(DataProviderAdapter):
    """
    Adapter para CoinMarketCap Pro API.

    Gestiona una ClientSession compartida por instancia — no crea
    una sesión nueva por request (evita overhead de TCP y agotamiento
    de file descriptors en uso intensivo).

    Uso como context manager (recomendado):
        async with CoinMarketCapAdapter(api_key=...) as adapter:
            metrics = await adapter.fetch_global_metrics()
    """

    def __init__(self, api_key: str) -> None:
        self.api_key  = api_key
        self.base_url = "https://pro-api.coinmarketcap.com"
        self._session: Optional[aiohttp.ClientSession] = None

    def _get_session(self) -> aiohttp.ClientSession:
        """Devuelve la sesión compartida, creándola lazy si no existe."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=_REQUEST_TIMEOUT,
                headers={"X-CMC_PRO_API_KEY": self.api_key},
            )
        return self._session

    async def close(self) -> None:
        """Cierra la sesión HTTP. Idempotente, nunca lanza excepción."""
        if self._session is not None and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None

    async def fetch_global_metrics(self) -> Dict[str, Any]:
        url = f"{self.base_url}/v1/global-metrics/quotes/latest"
        async with self._get_session().get(url) as resp:
            resp.raise_for_status()
            return await resp.json()
