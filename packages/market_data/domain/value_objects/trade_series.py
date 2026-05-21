# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/trade_series.py
=================================================

TradeSeries — agregado inmutable de microestructura de mercado.

Representa una ventana temporal contigua de RawTrade sobre un único
exchange/market_type/symbol. Es la unidad de análisis para:

  - VWAP y construcción de barras OHLCV desde tick data.
  - Order Flow Imbalance (OFI): imbalance comprador/vendedor.
  - Detección de actividad anómala (burst, thin market).
  - Resampling a cualquier timeframe sin llamar al exchange.

Principios
----------
DDD        — Value Object / Aggregate root de microestructura.
Fail-Fast  — orden temporal y cohesión validados en construcción.
Inmutabilidad — las operaciones de filtrado devuelven nuevas instancias.
SRP        — estadísticas de microestructura viven aquí, no en servicios.
Clean Arch — sin dependencias de infraestructura.

Contrato de ordenación
----------------------
``trades`` está ordenado ascendentemente por ``timestamp_ms``.
El constructor directo asume orden garantizado (fail-fast si viola).
Usar ``TradeSeries.from_unsorted()`` o ``TradeSeries.from_event()``
cuando el orden no esté garantizado.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Iterator, Sequence

from market_data.domain.value_objects.raw_trade import RawTrade, TradeSide

if TYPE_CHECKING:
    # Importación solo para type hints — evita ciclo de imports en runtime.
    from market_data.domain.events.trade_events import TradeReceived


# ---------------------------------------------------------------------------
# TradeSeries
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TradeSeries:
    """
    Secuencia inmutable y ordenada de RawTrade para un exchange/market_type/symbol.

    Campos
    ------
    exchange    : exchange de origen.
    market_type : ``"spot"`` | ``"linear"`` | ``"inverse"``.
    symbol      : par de trading.
    trades      : tuple de RawTrade ordenado por timestamp_ms ascendente.

    Invariantes (fail-fast)
    -----------------------
    - exchange, market_type y symbol no pueden ser vacíos.
    - trades no puede ser vacío.
    - Cohesión: todos los trades pertenecen al mismo exchange/market_type/symbol.
    - Orden: trades[i].timestamp_ms <= trades[i+1].timestamp_ms.
      Violación indica bug en el adaptador — se rechaza explícitamente.

    Estadísticas de dominio expuestas como properties
    -------------------------------------------------
    total_volume, total_cost, vwap, open/close/high/low,
    buy_volume, sell_volume, buy_sell_imbalance.

    El conocimiento de estas métricas pertenece al dominio de microestructura,
    no a servicios de aplicación ni a adaptadores de features.
    """

    exchange: str
    market_type: str
    symbol: str
    trades: tuple[RawTrade, ...]  # ordenado asc por timestamp_ms

    # -- validación en construcción (fail-fast) ---------------------------------

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if not self.exchange:
            raise ValueError("TradeSeries.exchange must be non-empty")
        if not self.market_type:
            raise ValueError("TradeSeries.market_type must be non-empty")
        if not self.symbol:
            raise ValueError("TradeSeries.symbol must be non-empty")
        if not self.trades:
            raise ValueError("TradeSeries must contain at least one trade")

        # Cohesión de lote
        for t in self.trades:
            if t.exchange != self.exchange:
                raise ValueError(f"TradeSeries: trade.exchange={t.exchange!r} != series.exchange={self.exchange!r}")
            if t.market_type != self.market_type:
                raise ValueError(
                    f"TradeSeries: trade.market_type={t.market_type!r} != series.market_type={self.market_type!r}"
                )
            if t.symbol != self.symbol:
                raise ValueError(f"TradeSeries: trade.symbol={t.symbol!r} != series.symbol={self.symbol!r}")

        # Orden temporal estricto — un lote desordenado es bug del adaptador
        for i in range(len(self.trades) - 1):
            a, b = self.trades[i].timestamp_ms, self.trades[i + 1].timestamp_ms
            if a > b:
                raise ValueError(
                    f"TradeSeries: trades must be sorted ascending by timestamp_ms. "
                    f"Violation at index {i}: {a} > {b}. "
                    f"Use TradeSeries.from_unsorted() if order is not guaranteed."
                )

    # -- factories --------------------------------------------------------------

    @classmethod
    def from_unsorted(
        cls,
        trades: Sequence[RawTrade],
        exchange: str,
        market_type: str,
        symbol: str,
    ) -> TradeSeries:
        """
        Ordena por timestamp_ms ascendente y construye la serie.

        Usar cuando los trades provienen de un endpoint REST que no garantiza
        orden (e.g. KuCoin /api/v1/trades devuelve descendente por defecto).
        """
        sorted_trades = tuple(sorted(trades, key=lambda t: t.timestamp_ms))
        return cls(
            exchange=exchange,
            market_type=market_type,
            symbol=symbol,
            trades=sorted_trades,
        )

    @classmethod
    def from_event(cls, event: TradeReceived) -> TradeSeries:
        """
        Construye una TradeSeries directamente desde un TradeReceived.

        ``TradeReceived.from_trades`` garantiza orden — no re-ordenamos.
        Hace explícita la relación evento → agregado de análisis en el dominio.

        Parameters
        ----------
        event : TradeReceived ya validado y ordenado.
        """
        return cls(
            exchange=event.exchange,
            market_type=event.market_type,
            symbol=event.symbol,
            trades=event.trades,
        )

    # -- protocolo de contenedor ------------------------------------------------

    def __iter__(self) -> Iterator[RawTrade]:
        return iter(self.trades)

    def __len__(self) -> int:
        return len(self.trades)

    def __getitem__(self, index: int) -> RawTrade:
        return self.trades[index]

    # -- bounds temporales ------------------------------------------------------

    @property
    def start_ms(self) -> int:
        """Timestamp del primer trade (más antiguo)."""
        return self.trades[0].timestamp_ms

    @property
    def end_ms(self) -> int:
        """Timestamp del último trade (más reciente)."""
        return self.trades[-1].timestamp_ms

    @property
    def duration_ms(self) -> int:
        """Duración de la ventana temporal en milisegundos."""
        return self.end_ms - self.start_ms

    # -- volumen y coste --------------------------------------------------------

    @property
    def total_volume(self) -> Decimal:
        """Suma de amount (activo base) de todos los trades."""
        return sum((t.amount for t in self.trades), Decimal(0))

    @property
    def total_cost(self) -> Decimal:
        """Suma de cost (price × amount) de todos los trades."""
        return sum((t.cost for t in self.trades), Decimal(0))

    @property
    def vwap(self) -> Decimal:
        """
        Volume-Weighted Average Price.

            VWAP = Σ(price_i × amount_i) / Σ(amount_i)

        Retorna ``Decimal(0)`` si total_volume es cero (defensa
        contra amounts todos cero, teóricamente imposible por invariante
        de RawTrade pero guardado por corrección aritmética).
        """
        vol = self.total_volume
        if vol == Decimal(0):
            return Decimal(0)
        return self.total_cost / vol

    # -- precios OHLC -----------------------------------------------------------

    @property
    def open_price(self) -> Decimal:
        """Precio del primer trade (más antiguo)."""
        return self.trades[0].price

    @property
    def close_price(self) -> Decimal:
        """Precio del último trade (más reciente)."""
        return self.trades[-1].price

    @property
    def high_price(self) -> Decimal:
        """Precio máximo de ejecución en la serie."""
        return max(t.price for t in self.trades)

    @property
    def low_price(self) -> Decimal:
        """Precio mínimo de ejecución en la serie."""
        return min(t.price for t in self.trades)

    # -- microestructura --------------------------------------------------------

    @property
    def buy_volume(self) -> Decimal:
        """Volumen de activo base ejecutado en el lado comprador."""
        return sum(
            (t.amount for t in self.trades if t.side is TradeSide.BUY),
            Decimal(0),
        )

    @property
    def sell_volume(self) -> Decimal:
        """Volumen de activo base ejecutado en el lado vendedor."""
        return sum(
            (t.amount for t in self.trades if t.side is TradeSide.SELL),
            Decimal(0),
        )

    @property
    def buy_sell_imbalance(self) -> Decimal:
        """
        Order Flow Imbalance (OFI) en [-1.0, +1.0].

            imbalance = (buy_vol - sell_vol) / total_vol

            +1.0  → presión compradora pura (todo el volumen es BUY agresivo)
            -1.0  → presión vendedora pura  (todo el volumen es SELL agresivo)
             0.0  → balance perfecto o serie sin volumen clasificado

        Base del indicador de Cont et al. (2014) "The Price Impact of
        Order Book Events". Los trades UNKNOWN no se incluyen en numerador
        pero sí en denominador (total_volume) — conservador por diseño.
        """
        total = self.total_volume
        if total == Decimal(0):
            return Decimal(0)
        return (self.buy_volume - self.sell_volume) / total

    @property
    def trade_count(self) -> int:
        """Número de trades en la serie."""
        return len(self.trades)

    # -- operaciones de ventana -------------------------------------------------

    def slice_ms(self, start_ms: int, end_ms: int) -> TradeSeries:
        """
        Nueva TradeSeries con los trades en el rango [start_ms, end_ms].

        El rango es cerrado en ambos extremos para permitir ventanas
        adyacentes sin gaps ni solapamientos.

        Fail-fast si:
          - start_ms > end_ms (rango invertido es bug del caller).
          - El rango produce una serie vacía.

        Parameters
        ----------
        start_ms : timestamp inferior del rango (inclusive).
        end_ms   : timestamp superior del rango (inclusive).
        """
        if start_ms > end_ms:
            raise ValueError(f"TradeSeries.slice_ms: start_ms={start_ms} > end_ms={end_ms}")
        sliced = tuple(t for t in self.trades if start_ms <= t.timestamp_ms <= end_ms)
        if not sliced:
            raise ValueError(
                f"TradeSeries.slice_ms({start_ms}, {end_ms}) produced an empty series "
                f"— no trades in range for {self.exchange}/{self.symbol}"
            )
        return TradeSeries(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=self.symbol,
            trades=sliced,
        )

    def filter_side(self, side: TradeSide) -> TradeSeries:
        """
        Nueva TradeSeries con solo los trades del lado dado.

        Fail-fast si el filtro produce una serie vacía.
        UNKNOWN es un valor válido de ``side`` — no se trata especialmente.

        Parameters
        ----------
        side : TradeSide.BUY | TradeSide.SELL | TradeSide.UNKNOWN.
        """
        filtered = tuple(t for t in self.trades if t.side is side)
        if not filtered:
            raise ValueError(
                f"TradeSeries.filter_side({side.value!r}): "
                f"no trades with side={side.value!r} "
                f"in {self.exchange}/{self.symbol}"
            )
        return TradeSeries(
            exchange=self.exchange,
            market_type=self.market_type,
            symbol=self.symbol,
            trades=filtered,
        )

    # -- representación ---------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"TradeSeries("
            f"exchange={self.exchange!r}, "
            f"market_type={self.market_type!r}, "
            f"symbol={self.symbol!r}, "
            f"trades={len(self.trades)}, "
            f"window_ms={self.duration_ms}, "
            f"vwap={self.vwap:.8f}"
            f")"
        )


__all__ = ["TradeSeries"]
