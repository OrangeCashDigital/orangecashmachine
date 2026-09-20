# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/book_builder.py
===========================================

Puerto OUTBOUND: contrato del BookBuilder de order book L2 (schema v2).

Responsabilidad
---------------
Desacoplar el use case de reconstrucción del libro (application) del stream
processor Kafka que lo alimenta/publica (infrastructure). BC-07 prohíbe que
infrastructure importe application (DIP); este port es la frontera que lo
permite: la aplicación IMPLEMENTA el contrato y el adaptador (infrastructure)
depende de la abstracción, no de la implementación concreta.

Este módulo define:
  OutcomeKind       — categoría del resultado de procesar una entrada.
  BookBuilderOutcome— value object inmutable resultado de procesar snapshot/delta.
  BookBuilderPort   — Protocol: on_snapshot/on_delta/check_stale.

El adaptador Kafka (book_builder_consumer) consume BookBuilderOutcome para
traducirlo a publicaciones (book.snapshot / book.delta), métricas o DLQ sin
acoplarse al use case concreto.

Principios: DIP · ISP · SRP · BC-07 · contrato fuerte de tipos (sin Any)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Protocol, Tuple, runtime_checkable


class OutcomeKind(str, Enum):
    """Categoría del resultado de procesar una entrada en el BookBuilder.

    SNAPSHOT_APPLIED        — snapshot base aplicado; publicar book.snapshot.
    DELTA_APPLIED           — delta atómico aplicado; publicar book.delta.
    DELTA_BEFORE_SNAPSHOT   — delta recibido sin snapshot base; desechar.
    GAP_DETECTED            — update_id discontinuo; invalidar y pedir snapshot.
    STRUCTURAL_INVALID      — deltas corruptos (qty<0); invalidar.
    STALE                   — libro sin actualización dentro de la ventana.
    """

    SNAPSHOT_APPLIED = "snapshot_applied"
    DELTA_APPLIED = "delta_applied"
    DELTA_BEFORE_SNAPSHOT = "delta_before_snapshot"
    GAP_DETECTED = "gap_detected"
    STRUCTURAL_INVALID = "structural_invalid"
    STALE = "stale"


@dataclass(frozen=True)
class BookBuilderOutcome:
    """Resultado inmutable de procesar una entrada en el BookBuilder.

    Campos
    ------
    kind        : OutcomeKind
    exchange    : exchange de origen
    symbol      : par normalizado
    update_id   : 'u' procesado (0 si N/A)
    bids/asks   : listas (price_str, size_str) a PUBLICAR (None si el outcome
                  no conlleva publicación, p.ej. gap/stale/descartado).
    timestamp_ms: timestamp del evento procesado
    detail      : mensaje legible (gap esperado/recibido, razón de invalidación)
    """

    kind: OutcomeKind
    exchange: str = ""
    symbol: str = ""
    update_id: int = 0
    bids: Optional[List[Tuple[str, str]]] = None
    asks: Optional[List[Tuple[str, str]]] = None
    timestamp_ms: int = 0
    detail: str = ""

    @property
    def publishes(self) -> bool:
        """True si este outcome debe traducirse en una publicación Kafka."""
        return self.kind in (OutcomeKind.SNAPSHOT_APPLIED, OutcomeKind.DELTA_APPLIED)


@runtime_checkable
class BookBuilderPort(Protocol):
    """
    Contrato del servidor de reconstrucción de order book L2 (schema v2).

    Implementado por: market_data.application.processing.book_builder.BookBuilder
    Usado por       : market_data.infrastructure.kafka.book_builder_consumer

    bid/ask levels se pasan como (price_str, size_str) de origen (str preserva
    precisión Decimal, D-7c). El continuidad del estado se rige por ``update_id``
    ('u' de Bybit, D-7b).

    Cada método devuelve BookBuilderOutcome con el resultado a traducir.
    """

    def on_snapshot(
        self,
        exchange: str,
        symbol: str,
        timestamp_ms: int,
        bids: List[Tuple[str, str]],
        asks: List[Tuple[str, str]],
        update_id: int = 0,
    ) -> BookBuilderOutcome: ...

    def on_delta(
        self,
        exchange: str,
        symbol: str,
        timestamp_ms: int,
        bids: List[Tuple[str, str]],
        asks: List[Tuple[str, str]],
        update_id: int = 0,
    ) -> BookBuilderOutcome: ...

    def check_stale(self, now_ms: int) -> List[BookBuilderOutcome]: ...


__all__ = [
    "OutcomeKind",
    "BookBuilderOutcome",
    "BookBuilderPort",
]
