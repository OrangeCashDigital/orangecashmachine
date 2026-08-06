"""
market_data/ports/inbound/external
==================================

Contratos de adquisición no-streaming (external_ingestion).

Capacidad de ADR-0014: adquisición periódica (polling), batch e
histórico (replay) desde fuentes externas, con normalización a evento
canónico en application. Estos puertos son el detalle de adquisición —
no contienen lógica de negocio ni SDKs de vendor (BC-49).
"""

from __future__ import annotations

from market_data.ports.inbound.external.errors import (
    ExternalAuthenticationError,
    ExternalRateLimitError,
    ExternalRequestError,
    ExternalSourceError,
    ExternalSourceUnavailable,
)
from market_data.ports.inbound.external.polling import (
    HealthStatus,
    PollingRequest,
    PollingResult,
    PollingSourcePort,
)
from market_data.ports.inbound.external.replay import HistoricalRequest, ReplayPort

__all__ = [
    "ExternalAuthenticationError",
    "ExternalRateLimitError",
    "ExternalRequestError",
    "ExternalSourceError",
    "ExternalSourceUnavailable",
    "HealthStatus",
    "HistoricalRequest",
    "PollingRequest",
    "PollingResult",
    "PollingSourcePort",
    "ReplayPort",
]
