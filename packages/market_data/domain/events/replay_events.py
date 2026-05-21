# -*- coding: utf-8 -*-
"""
market_data/domain/events/replay_events.py
==========================================

Domain events para control del Event Replay Engine — Kappa Architecture.

Arquitectura Kappa y Replay
----------------------------
En Kappa Architecture, el log de eventos (Kafka) ES la fuente de verdad.
El "replay" es el mecanismo para reprocesar eventos históricos desde un
punto temporal arbitrario, sin fuentes de datos separadas.

Casos de uso
------------
- Recalcular features tras un cambio de lógica de transformación.
- Regenerar Silver/Gold después de un bug de escritura.
- Poblar un nuevo consumer sin esperar datos en tiempo real.
- Smoke test de un pipeline nuevo contra datos históricos reales.

Eventos
-------
ReplayRequested  — solicitud de replay de un stream desde un instante.
ReplayCompleted  — replay finalizado, incluyendo estadísticas de ejecución.

Responsabilidad de los eventos vs la implementación
-----------------------------------------------------
Estos eventos definen el CONTRATO de dominio del replay.
La implementación del engine (Kafka seek_to_beginning, Redis state reset,
Prefect flow triggers) vive en infrastructure — no aquí.

Streams soportados (source)
----------------------------
"ohlcv"      → stream de velas OHLCV (Nivel 1.5)
"trades"     → stream de trades (Nivel 1)
"orderbook"  → stream de order book snapshots + deltas (Nivel 0)
"all"        → todos los streams del par (para reconstrucción completa)

Principios
----------
DDD        — Domain Events: hechos / intenciones del dominio.
SSOT       — este módulo es la única definición de replay contracts.
Clean Arch — sin referencias a Kafka, Redis, Prefect ni Dagster.
KISS       — los eventos son datos, no comportamiento.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from market_data.domain.events._base import DomainEvent

# ---------------------------------------------------------------------------
# ReplayRequested
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReplayRequested(DomainEvent):
    """
    Evento/comando: solicita replay de un stream desde un instante dado.

    Publicado por: operadores (CLI, Dagster run, script de mantenimiento).
    Consumido por: ReplayEngine (infrastructure) que ejecuta el seek y
                   reprocesa los eventos contra los consumers activos.

    Campos
    ------
    exchange     : exchange a reprocesar.
    symbol       : par de trading a reprocesar.
    stream       : tipo de stream ("ohlcv" | "trades" | "orderbook" | "all").
    from_ms      : timestamp de inicio del replay (Unix ms UTC).
    to_ms        : timestamp de fin (Unix ms UTC). None → hasta el presente.
    run_id       : ID de ejecución para correlación con LineageTracker.
    requested_by : identificador del solicitante (usuario, job, sistema).

    Invariantes (fail-fast)
    -----------------------
    - exchange, symbol, stream: no-vacíos.
    - from_ms > 0.
    - Si to_ms no es None: to_ms > from_ms.
    """

    exchange: str = ""
    symbol: str = ""
    stream: str = "ohlcv"  # "ohlcv"|"trades"|"orderbook"|"all"
    from_ms: int = 0
    to_ms: Optional[int] = None  # None = hasta el presente
    run_id: str = ""
    requested_by: str = "system"

    # Streams válidos — SSOT local (no crear enum para un único uso — KISS)
    _VALID_STREAMS: frozenset = field(
        default=frozenset({"ohlcv", "trades", "orderbook", "all"}),
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not self.exchange:
            raise ValueError("ReplayRequested.exchange must be non-empty")
        if not self.symbol:
            raise ValueError("ReplayRequested.symbol must be non-empty")
        if not self.stream:
            raise ValueError("ReplayRequested.stream must be non-empty")
        if self.stream not in self._VALID_STREAMS:
            raise ValueError(
                f"ReplayRequested.stream={self.stream!r} is not valid. Must be one of: {sorted(self._VALID_STREAMS)}"
            )
        if self.from_ms <= 0:
            raise ValueError(f"ReplayRequested.from_ms must be > 0, got {self.from_ms}")
        if self.to_ms is not None and self.to_ms <= self.from_ms:
            raise ValueError(f"ReplayRequested.to_ms ({self.to_ms}) must be > from_ms ({self.from_ms})")

    @property
    def is_bounded(self) -> bool:
        """True si el replay tiene un límite temporal explícito."""
        return self.to_ms is not None

    @property
    def window_ms(self) -> Optional[int]:
        """Duración del rango de replay en ms. None si no acotado."""
        if self.to_ms is None:
            return None
        return self.to_ms - self.from_ms

    def __repr__(self) -> str:
        return (
            f"ReplayRequested("
            f"exchange={self.exchange!r}, symbol={self.symbol!r}, "
            f"stream={self.stream!r}, "
            f"from_ms={self.from_ms}, to_ms={self.to_ms}, "
            f"requested_by={self.requested_by!r}, "
            f"event_id={self.event_id!r}"
            f")"
        )


# ---------------------------------------------------------------------------
# ReplayCompleted
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReplayCompleted(DomainEvent):
    """
    Hecho inmutable: un replay ha finalizado.

    Publicado por: ReplayEngine (infrastructure) al completar el seek.
    Consumido por: LineageTracker, dashboards de observabilidad, alertas.

    Campos
    ------
    replay_request_id : event_id del ReplayRequested que originó este replay.
    exchange          : exchange reprocesado.
    symbol            : par reprocesado.
    stream            : tipo de stream reprocesado.
    events_replayed   : número total de eventos reprocesados.
    duration_ms       : duración total del replay en milisegundos reales.
    run_id            : ID de ejecución para correlación.
    success           : True si el replay completó sin errores fatales.
    error_message     : mensaje de error si success=False. Vacío si success=True.
    """

    replay_request_id: str = ""
    exchange: str = ""
    symbol: str = ""
    stream: str = ""
    events_replayed: int = 0
    duration_ms: int = 0
    run_id: str = ""
    success: bool = True
    error_message: str = ""

    def __post_init__(self) -> None:
        if not self.replay_request_id:
            raise ValueError(
                "ReplayCompleted.replay_request_id must be non-empty — "
                "must reference the originating ReplayRequested.event_id"
            )
        if not self.success and not self.error_message:
            raise ValueError("ReplayCompleted: error_message is required when success=False")

    def __repr__(self) -> str:
        status = "OK" if self.success else f"FAILED({self.error_message!r})"
        return (
            f"ReplayCompleted("
            f"exchange={self.exchange!r}, symbol={self.symbol!r}, "
            f"stream={self.stream!r}, "
            f"events={self.events_replayed}, duration_ms={self.duration_ms}, "
            f"status={status}, event_id={self.event_id!r}"
            f")"
        )


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------

__all__ = [
    "ReplayRequested",
    "ReplayCompleted",
]
