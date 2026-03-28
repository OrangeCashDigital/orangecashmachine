from __future__ import annotations

"""
services/data_providers/base.py
================================

Interfaz base para proveedores de datos externos de mercado.
(NO trading — solo datos de referencia: funding rates, OI, métricas globales)

Contrato
--------
- Las subclases implementan los métodos relevantes para su proveedor.
- Los métodos no implementados lanzan NotImplementedError con mensaje explícito.
- Cada instancia gestiona su propia ClientSession — no crear una por request.
- close() nunca debe lanzar excepción.

Lifecycle esperado
------------------
    async with CoinglassAdapter(api_key=...) as adapter:
        data = await adapter.fetch_funding_rates()
"""

from typing import Any, Dict


class DataProviderAdapter:
    """
    Interfaz base para proveedores de datos externos.

    Subclases deben:
    1. Inicializar self._session en __aenter__ o __init__
    2. Cerrar self._session en close() sin lanzar excepción
    3. Implementar los métodos relevantes para su proveedor

    Los métodos no implementados lanzan NotImplementedError en runtime —
    no se usa ABC aquí porque los proveedores tienen APIs heterogéneas
    y no todos implementan el mismo conjunto de métodos.
    """

    async def close(self) -> None:
        """
        Cierra la sesión HTTP. Idempotente, nunca lanza excepción.

        Las subclases deben sobreescribir este método para cerrar
        self._session u otros recursos, capturando todas las excepciones
        internamente — el contrato garantiza que close() nunca propaga.
        """
        # Base no-op: subclases con sesión deben sobreescribir

    async def __aenter__(self) -> "DataProviderAdapter":
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    # ----------------------------------------------------------
    # Métodos opcionales — implementar según proveedor
    # ----------------------------------------------------------

    async def fetch_funding_rates(self) -> Dict[str, Any]:
        raise NotImplementedError(
            f"{type(self).__name__} does not implement fetch_funding_rates()"
        )

    async def fetch_open_interest(self) -> Dict[str, Any]:
        raise NotImplementedError(
            f"{type(self).__name__} does not implement fetch_open_interest()"
        )

    async def fetch_global_metrics(self) -> Dict[str, Any]:
        raise NotImplementedError(
            f"{type(self).__name__} does not implement fetch_global_metrics()"
        )
