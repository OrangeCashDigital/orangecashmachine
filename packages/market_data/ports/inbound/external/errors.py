"""
market_data/ports/inbound/external/errors.py
=============================================

Errores del boundary de adquisición no-streaming (external_ingestion).

Viven en el puerto (no en el dominio) porque describen fallos del
*contrato de adquisición* (transporte/API externa), no invariantes del
dominio de market data. El dominio no debe conocer estos errores.

Jerarquía
---------
ExternalSourceError (base)
├── ExternalSourceUnavailable — API caída / timeout / 5xx
└── ExternalRateLimitError     — 429 / superado el rate limit del proveedor

Principios: SRP · DIP · KISS
"""

from __future__ import annotations

__all__ = [
    "ExternalSourceError",
    "ExternalSourceUnavailable",
    "ExternalRateLimitError",
]


class ExternalSourceError(Exception):
    """Error base del boundary de adquisición externa."""


class ExternalSourceUnavailable(ExternalSourceError):
    """La fuente externa está caída o respondió de forma no recuperable."""


class ExternalRateLimitError(ExternalSourceError):
    """El proveedor devolvió 429 / excedimos el rate limit."""
