# -*- coding: utf-8 -*-
"""
market_data/domain/events/orderbook_events.py
=============================================

Domain events de microestructura de order book — Nivel 0.

Jerarquía de eventos
---------------------
OrderBookSnapshotReceived — snapshot L2 completo ha cruzado la frontera
                            del dominio (REST o mensaje inicial de WS).
OrderBookDeltaReceived    — delta atómico L2 recibido desde stream WS.

Relación con ingestion.py
--------------------------
ingestion.py   → eventos de OHLCV (Nivel 1.5 / derivado).
este módulo    → eventos de order book (Nivel 0 / nativo del exchange).
Los niveles no se mezclan — cada uno es un stream independiente.

Patrón de herencia
------------------
Sigue el patrón de DomainEvent establecido en ingestion.py:
  - occurred_at: str (ISO-8601 UTC)
  - event_id: str (UUID4)
  - Todos los campos con defaults → compatibilidad con frozen dataclass.
  - __post_init__ valida campos obligatorios (fail-fast).

Consumidores esperados
----------------------
OrderBookSnapshotReceived:
  - OrderBookStateManager → construye/resetea el libro en memoria.
  - BronzeOrderBookWriter → persiste snapshot en Iceberg Bronze.
  - MicrostructureAnalyzer → calcula spread, depth imbalance.

OrderBookDeltaReceived:
  - OrderBookStateManager → aplica delta al estado en memoria.
  - BronzeOrderBookWriter → persiste delta en Iceberg Bronze (append-only).

Ningún consumer conoce a los demás — desacoplamiento por contrato de evento.

Principios
----------
DDD        — Domain Events: hechos inmutables del pasado.
Fail-Fast  — campos obligatorios validados en __post_init__.
Desacopl.  — consumidores reaccionan al evento sin conocerse.
SSOT       — un único lugar define qué es "recibir order book data".
Clean Arch — sin dependencias de infraestructura ni OHLCV.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from market_data.domain.events._base import DomainEvent
from market_data.domain.value_objects.order_book import (
    OrderBookDelta,
    OrderBookSnapshot,
)


# ---------------------------------------------------------------------------
# OrderBookSnapshotReceived
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderBookSnapshotReceived(DomainEvent):
    """
    Hecho inmutable: un snapshot L2 completo ha sido recibido.

    Emitido cuando:
    - El adapter REST llama a ``fetch_order_book`` (polling o bootstrap).
    - El adapter WebSocket (cryptofeed) recibe el mensaje inicial
      de sincronización del canal ``l2_book``.

    Campos
    ------
    snapshot    : el snapshot completo (requerido — validado en __post_init__).
    source      : "rest" (polling REST) | "live" (WebSocket stream inicial).
    run_id      : correlación con LineageTracker. Vacío si no aplica.

    Nota sobre source
    -----------------
    "rest"  → snapshot obtenido vía fetch_order_book (CCXT).
    "live"  → snapshot inicial de sincronización del stream WS (cryptofeed).
    Usar OHLCVSource.REST / OHLCVSource.LIVE para consistencia.
    """

    snapshot: Optional[OrderBookSnapshot] = field(default=None)
    source: str = "rest"
    run_id: str = ""

    def __post_init__(self) -> None:
        # Llamar al __post_init__ de DomainEvent no es necesario
        # (genera event_id y occurred_at via default_factory).
        # Solo validamos los campos propios de este evento.
        if self.snapshot is None:
            raise ValueError(
                "OrderBookSnapshotReceived.snapshot is required — "
                "use OrderBookSnapshotReceived(snapshot=...) explicitly."
            )

    @property
    def exchange(self) -> str:
        assert self.snapshot is not None
        return self.snapshot.exchange

    @property
    def symbol(self) -> str:
        assert self.snapshot is not None
        return self.snapshot.symbol

    @property
    def timestamp_ms(self) -> int:
        assert self.snapshot is not None
        return self.snapshot.timestamp_ms

    def __repr__(self) -> str:
        snap = self.snapshot
        if snap is None:
            return "OrderBookSnapshotReceived(snapshot=None)"
        return (
            f"OrderBookSnapshotReceived("
            f"exchange={snap.exchange!r}, symbol={snap.symbol!r}, "
            f"ts_ms={snap.timestamp_ms}, "
            f"bids={snap.bid_depth_levels}, asks={snap.ask_depth_levels}, "
            f"source={self.source!r}, event_id={self.event_id!r}"
            f")"
        )


# ---------------------------------------------------------------------------
# OrderBookDeltaReceived
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderBookDeltaReceived(DomainEvent):
    """
    Hecho inmutable: un delta atómico L2 ha sido recibido desde el stream.

    Emitido por el adapter WebSocket (cryptofeed) cada vez que el exchange
    notifica un cambio en un nivel del libro de órdenes.

    Campos
    ------
    delta   : el delta atómico (requerido — validado en __post_init__).
    source  : siempre "live" — los deltas solo existen en streams WS.
    run_id  : correlación con LineageTracker. Vacío si no aplica.

    Frecuencia
    ----------
    Los deltas son el tipo de mensaje más frecuente del sistema.
    Para un par activo en Bybit, pueden llegar a >1000 deltas/segundo.
    El event object debe ser lightweight — no calcular nada en __post_init__.
    """

    delta: Optional[OrderBookDelta] = field(default=None)
    source: str = "live"
    run_id: str = ""

    def __post_init__(self) -> None:
        if self.delta is None:
            raise ValueError(
                "OrderBookDeltaReceived.delta is required — use OrderBookDeltaReceived(delta=...) explicitly."
            )

    @property
    def exchange(self) -> str:
        assert self.delta is not None
        return self.delta.exchange

    @property
    def symbol(self) -> str:
        assert self.delta is not None
        return self.delta.symbol

    @property
    def timestamp_ms(self) -> int:
        assert self.delta is not None
        return self.delta.timestamp_ms

    def __repr__(self) -> str:
        d = self.delta
        if d is None:
            return "OrderBookDeltaReceived(delta=None)"
        return (
            f"OrderBookDeltaReceived("
            f"exchange={d.exchange!r}, symbol={d.symbol!r}, "
            f"ts_ms={d.timestamp_ms}, "
            f"side={d.side.value!r}, price={d.price}, qty={d.quantity}, "
            f"is_delete={d.is_delete}, event_id={self.event_id!r}"
            f")"
        )


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------

__all__ = [
    "OrderBookSnapshotReceived",
    "OrderBookDeltaReceived",
]
