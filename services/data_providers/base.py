from __future__ import annotations
from typing import Any, Dict


class DataProviderAdapter:
    """
    Interfaz base para proveedores de datos externos.
    (NO trading)
    """

    async def fetch_funding_rates(self) -> Dict[str, Any]:
        raise NotImplementedError

    async def fetch_open_interest(self) -> Dict[str, Any]:
        raise NotImplementedError

    async def fetch_global_metrics(self) -> Dict[str, Any]:
        raise NotImplementedError
