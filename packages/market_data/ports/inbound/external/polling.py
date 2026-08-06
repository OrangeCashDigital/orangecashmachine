"""
market_data/ports/inbound/external/polling.py
=============================================

Contrato de adquisición periódica no-streaming (REST / polling).

El adapter obtiene datos crudos — no calcula inteligencia ni señales
(separación adquisición/transformación, ver ADR-0014). La normalización
a evento canónico ocurre en application/external_ingestion/.

Structural subtyping via runtime_checkable Protocol: los adapters
conforman por conformidad estructural, no por herencia.

Framework-agnostic: PollingResult.payload es raw JSON/dict sin ningún
framework (provider-native). El transform de DataFrame para persistence
sigue siendo responsabilidad de ports/outbound/normalization.py.

Principios: DIP · ISP · SRP · Clean Architecture
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

__all__ = ["PollingRequest", "PollingResult", "PollingSourcePort", "HealthStatus"]


@dataclass(frozen=True, slots=True)
class HealthStatus:
    """Resultado de un health check de una fuente externa.

    ok:        True si la fuente responde y las credenciales son válidas.
    latency_ms: latencia del request de comprobación (monotónica).
    detail:     motivo del fallo (auth / rate-limit / config / unreachable).
    """

    ok: bool
    latency_ms: int = 0
    detail: str = ""


@dataclass(frozen=True, slots=True)
class PollingRequest:
    """Una petición de captura para una fuente externa.

    metric:
        Nombre canónico de la métrica ("funding_rate", "open_interest",
        "market_metrics", ...). El conjunto de nombres lo fija cada
        provider — no es un enum literal porque las fuentes exponen
        métricas heterogéneas.
    symbols:
        Subconjunto de símbolos. None = todos (p.ej. métrica global).
    """

    metric: str
    symbols: Sequence[str] | None = None


@dataclass(frozen=True, slots=True)
class PollingResult:
    """Respuesta cruda de un ciclo de fetch.

    La respuesta es provider-native (JSON/dict) SIN normalizar. La
    normalización a evento canónico ocurre en la capa application
    (normalizers), no en el adapter.

    fetched_at es la base de tiempo de captura (UTC). El adapter NO
    inventa timestamp de mercado: solo registra el momento en que obtuvo
    el dato bruto.
    """

    source_id: str
    metric: str
    payload: Sequence[Mapping[str, object]] = field(default_factory=tuple)
    fetched_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )


@runtime_checkable
class PollingSourcePort(Protocol):
    """Contrato de adquisición periódica desde una fuente externa.

    Implementado por: adapters/inbound/external/*
    Usado por       : ExternalIngestionOrchestrator (application/external_ingestion).

    Convenciones:
    - source_id es la identidad canónica de la fuente ("coinglass",
      "coinmarketcap", "glassnode", "fred", ...).
    - fetch() con payload vacío NO es un error: retorna un PollingResult
      vacío. Lanza ExternalSourceUnavailable / ExternalRateLimitError
      solo para fallos de transporte o API.
    - El ciclo de vida es el del orquestador: una instancia por source_id,
      reutilizada entre ciclos. Cerrar recursos (p.ej. ClientSession aiohttp)
      es responsabilidad del orquestador vía close() en shutdown.
    """

    source_id: str

    async def fetch(self, request: PollingRequest) -> PollingResult:
        """Adquiere la fotografía cruda de una métrica de la fuente."""
        ...

    async def health(self) -> HealthStatus:
        """Valida credenciales y disponibilidad antes del polling continuo.

        Nunca lanza: devuelve un HealthStatus con el diagnóstico. Lo usa el
        orquestador para gatear el arranque de una fuente.
        """
        ...

    async def close(self) -> None:
        """Libera los recursos del adapter (p.ej. sesión HTTP).

        Debe ser idempotente y nunca lanzar excepción (SafeOps). Lo invoca
        el gestionador de lifecycle — no se espera que los tests lo usen.
        """
        ...
