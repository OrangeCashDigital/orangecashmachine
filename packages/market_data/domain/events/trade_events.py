# -*- coding: utf-8 -*-
"""
market_data/domain/events/trade_events.py
==========================================

TradeReceived — evento de dominio de ingesta de microestructura.

Representa el cruce de frontera: un lote de trades crudos ha entrado
al dominio desde un adaptador de exchange (REST o WebSocket).

Principios
----------
DDD        — Domain Event: hecho inmutable del pasado.
Fail-Fast  — lote vacío o inconsistente se rechaza en construcción.
Desacopl.  — consumidores reaccionan al evento sin conocerse entre sí.
SSOT       — un solo lugar define qué es "recibir trades".
Clean Arch — sin dependencias de infraestructura ni frameworks.

Consumidores esperados
----------------------
- Bronze writer       → persiste el lote raw en Iceberg.
- Quality pipeline    → valida schema y anomalías.
- Microestructura     → construye TradeSeries para análisis OFI/VWAP.
Ninguno conoce a los demás — desacoplamiento por contrato de evento.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Sequence

from market_data.domain.value_objects.raw_trade import RawTrade


# ---------------------------------------------------------------------------
# TradeReceived
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class TradeReceived:
    """
    Hecho inmutable: un lote de trades ha sido recibido desde el exchange.

    Campos
    ------
    trades      : lote de trades ordenado por timestamp_ms ascendente.
    exchange    : exchange de origen (redundante con trades, explícito por contrato).
    market_type : ``"spot"`` | ``"linear"`` | ``"inverse"``.
    symbol      : par de trading (redundante con trades, explícito por contrato).
    event_id    : UUID4 globalmente único — sin dependencia de BD ni secuencias.
    occurred_at : wall-clock UTC del momento de creación del evento.
                  Distinto del timestamp de los trades que contiene.

    Invariantes (fail-fast)
    -----------------------
    - trades no puede ser vacío (cero trades no es un evento).
    - exchange, market_type y symbol no pueden ser vacíos.
    - Cohesión de lote: todos los trades deben pertenecer al mismo
      exchange, market_type y symbol que declara el evento.
      Un lote mezclado es un bug del adaptador, no un caso borde.

    Uso canónico
    ------------
    # Desde adaptador REST o WebSocket:
    event = TradeReceived.from_trades(
        trades=[...],
        exchange="bybit",
        market_type="linear",
        symbol="BTC/USDT",
    )
    await event_bus.publish(event)
    """

    # -- payload ----------------------------------------------------------------
    trades:      tuple[RawTrade, ...]   # ordenado por timestamp_ms asc
    exchange:    str
    market_type: str
    symbol:      str

    # -- metadatos del evento ---------------------------------------------------
    event_id:    str      = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: datetime = field(
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )

    # -- validación en construcción (fail-fast) ---------------------------------

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if not self.trades:
            raise ValueError(
                "TradeReceived: trades must be non-empty — "
                "receiving zero trades is not a domain event"
            )
        if not self.exchange:
            raise ValueError("TradeReceived: exchange must be non-empty")
        if not self.market_type:
            raise ValueError("TradeReceived: market_type must be non-empty")
        if not self.symbol:
            raise ValueError("TradeReceived: symbol must be non-empty")

        # Cohesión de lote — falla en el primer trade inconsistente (fail-fast)
        for t in self.trades:
            if t.exchange != self.exchange:
                raise ValueError(
                    f"TradeReceived: trade.exchange={t.exchange!r} "
                    f"!= event.exchange={self.exchange!r}. "
                    f"A mixed-exchange batch is an adapter bug."
                )
            if t.market_type != self.market_type:
                raise ValueError(
                    f"TradeReceived: trade.market_type={t.market_type!r} "
                    f"!= event.market_type={self.market_type!r}."
                )
            if t.symbol != self.symbol:
                raise ValueError(
                    f"TradeReceived: trade.symbol={t.symbol!r} "
                    f"!= event.symbol={self.symbol!r}. "
                    f"A mixed-symbol batch is an adapter bug."
                )

    # -- factory canónico -------------------------------------------------------

    @classmethod
    def from_trades(
        cls,
        trades:      Sequence[RawTrade],
        exchange:    str,
        market_type: str,
        symbol:      str,
    ) -> TradeReceived:
        """
        Factory canónico: acepta cualquier Sequence, ordena y construye.

        Ordena por timestamp_ms ascendente para garantizar el contrato de
        orden del evento aunque el adaptador entregue trades desordenados
        (comportamiento de algunos endpoints REST de KuCoin).

        Parameters
        ----------
        trades      : lista o secuencia de RawTrade (puede estar desordenada).
        exchange    : exchange de origen.
        market_type : tipo de mercado.
        symbol      : par de trading.
        """
        sorted_trades = tuple(sorted(trades, key=lambda t: t.timestamp_ms))
        return cls(
            trades=sorted_trades,
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
        )

    # -- propiedades de conveniencia --------------------------------------------

    @property
    def count(self) -> int:
        """Número de trades en el lote."""
        return len(self.trades)

    @property
    def earliest_ms(self) -> int:
        """Timestamp del trade más antiguo del lote."""
        return self.trades[0].timestamp_ms

    @property
    def latest_ms(self) -> int:
        """Timestamp del trade más reciente del lote."""
        return self.trades[-1].timestamp_ms

    @property
    def window_ms(self) -> int:
        """Duración de la ventana temporal del lote en milisegundos."""
        return self.latest_ms - self.earliest_ms

    # -- representación ---------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"TradeReceived("
            f"exchange={self.exchange!r}, "
            f"market_type={self.market_type!r}, "
            f"symbol={self.symbol!r}, "
            f"count={self.count}, "
            f"window_ms={self.window_ms}, "
            f"event_id={self.event_id!r}"
            f")"
        )


__all__ = ["TradeReceived"]
