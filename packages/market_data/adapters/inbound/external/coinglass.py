# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/external/coinglass.py
===================================================

Adapter CoinGlass Open API — implementa PollingSourcePort.

Métricas
--------
  funding_rate  → GET /futures/funding_rates
  open_interest → GET /futures/open_interest

Solo adquisición: devuelve PollingResult crudo (JSON provider-native).
La normalización ocurre en application/external_ingestion/normalizers.

BC-52: no importa SDK de vendor a nivel módulo — usa aiohttp (HTTP genérico).
La API key se pasa por constructor (nunca hardcodeada). La sesión se cierra
en shutdown (el orquestador llama a close()). Clasificación 4xx/5xx, Retry-After
y health() los gestiona HTTPDataSourceBase.

Principios: DIP · SRP · SafeOps · KISS
"""

from __future__ import annotations

import aiohttp

from market_data.adapters.inbound.external.base import HTTPDataSourceBase
from market_data.ports.inbound.external.errors import ExternalSourceUnavailable
from market_data.ports.inbound.external.polling import (
    PollingRequest,
    PollingResult,
    PollingSourcePort,
)

__all__ = ["CoinglassPollingSource"]

_ENDPOINTS: dict[str, str] = {
    "funding_rate": "/futures/funding_rates",
    "open_interest": "/futures/open_interest",
}


class CoinglassPollingSource(HTTPDataSourceBase, PollingSourcePort):
    """Adapter de polling de la API CoinGlass."""

    source_id: str = "coinglass"

    def __init__(self, api_key: str) -> None:
        super().__init__(
            base_url="https://open-api.coinglass.com/api",
            headers={"CG-API-KEY": api_key},
            health_path="/futures/funding_rates",
        )
        self.api_key = api_key

    async def fetch(self, request: PollingRequest) -> PollingResult:
        """Adquiere la fotografía cruda de una métrica de CoinGlass."""
        endpoint = _ENDPOINTS.get(request.metric)
        if endpoint is None:
            raise ValueError(
                f"CoinglassPollingSource: métrica '{request.metric}' no soportada. Soportadas: {sorted(_ENDPOINTS)}"
            )
        url = f"{self.base_url}{endpoint}"
        try:
            async with self._get_session().get(url) as resp:
                if resp.status >= 400:
                    self.classify_status(resp.status, resp.headers.get("Retry-After") if resp.headers else None)
                data = await resp.json()
        except aiohttp.ClientError as exc:
            raise ExternalSourceUnavailable(f"CoinGlass unreachable en {endpoint}: {exc}") from exc

        rows = data.get("data") if isinstance(data, dict) else []
        if not isinstance(rows, list):
            rows = []
        return PollingResult(source_id=self.source_id, metric=request.metric, payload=list(rows))
