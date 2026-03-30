from __future__ import annotations

"""
services/data_providers/coinglass_adapter.py
=============================================

Adapter para la API de Coinglass (funding rates, open interest, liquidaciones).

Uso
---
    async with CoinglassAdapter(api_key="...") as adapter:
        data = await adapter.fetch_funding_rates()
"""

from typing import Any, Dict, Optional

import aiohttp

from market_data.adapters.data_providers.base import DataProviderAdapter

# Timeouts por operación (segundos)
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30.0, connect=10.0)


class CoinglassAdapter(DataProviderAdapter):
    """
    Adapter para Coinglass Open API.

    Gestiona una ClientSession compartida por instancia — no crea
    una sesión nueva por request (evita overhead de TCP y agotamiento
    de file descriptors en uso intensivo).

    Uso como context manager (recomendado):
        async with CoinglassAdapter(api_key=...) as adapter:
            rates = await adapter.fetch_funding_rates()

    Uso manual:
        adapter = CoinglassAdapter(api_key=...)
        try:
            rates = await adapter.fetch_funding_rates()
        finally:
            await adapter.close()
    """

    def __init__(self, api_key: str) -> None:
        self.api_key  = api_key
        self.base_url = "https://open-api.coinglass.com/api"
        self._session: Optional[aiohttp.ClientSession] = None

    def _get_session(self) -> aiohttp.ClientSession:
        """Devuelve la sesión compartida, creándola lazy si no existe."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=_REQUEST_TIMEOUT,
                headers={"CG-API-KEY": self.api_key},
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

    async def fetch_funding_rates(self) -> Dict[str, Any]:
        url = f"{self.base_url}/futures/funding_rates"
        async with self._get_session().get(url) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def fetch_open_interest(self) -> Dict[str, Any]:
        url = f"{self.base_url}/futures/open_interest"
        async with self._get_session().get(url) as resp:
            resp.raise_for_status()
            return await resp.json()
