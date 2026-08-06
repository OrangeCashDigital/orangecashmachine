"""
market_data/ports/inbound/external/errors.py
============================================

Errores del boundary de adquisición no-streaming (external_ingestion).

Viven en el puerto (no en el dominio) porque describen fallos del
*contrato de adquisición* (transporte/API externa), no invariantes del
dominio de market data. El dominio no debe conocer estos errores.

Jerarquía
---------
ExternalSourceError (base)
├── ExternalAuthenticationError — 401/403: API key rechazada por el proveedor
├── ExternalRateLimitError     — 429: superado el rate limit (con Retry-After)
├── ExternalRequestError        — otros 4xx: petición rechazada (config/proveedor)
└── ExternalSourceUnavailable   — 5xx / timeout / DNS: fuente caída o no recuperable

Principios: SRP · DIP · KISS
"""

from __future__ import annotations

__all__ = [
    "ExternalSourceError",
    "ExternalSourceUnavailable",
    "ExternalRateLimitError",
    "ExternalAuthenticationError",
    "ExternalRequestError",
]


class ExternalSourceError(Exception):
    """Error base del boundary de adquisición externa."""


class ExternalSourceUnavailable(ExternalSourceError):
    """La fuente externa está caída o respondió de forma no recuperable.

    Cubre: 5xx, timeout, timeout de conexión, fallos DNS y timeouts de lectura.
    """


class ExternalRateLimitError(ExternalSourceError):
    """El proveedor devolvió 429 / excedimos el rate limit.

    retry_after_s: segundos sugeridos por el proveedor (header Retry-After), si presentes.
    """

    def __init__(self, message: str, retry_after_s: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


class ExternalAuthenticationError(ExternalSourceError):
    """401/403: la API key fue rechazada por el proveedor (credenciales inválidas)."""


class ExternalRequestError(ExternalSourceError):
    """Otro 4xx: la petición fue rechazada (métrica/símbolo erróneo o política del proveedor)."""
