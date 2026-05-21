# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/order_book.py
===============================================

Value Objects de microestructura de Nivel 0: Order Book.

Jerarquía de realidad del mercado
----------------------------------
Nivel 0 — fuentes primarias nativas del exchange:
    PriceLevel          → un nivel de precio con su cantidad
    OrderBookSide       → lado del libro (bid / ask)
    OrderBookSnapshot   → fotografía completa L2 en un instante
    OrderBookDelta      → actualización atómica de un nivel L2

Estos tipos NO dependen de Candle ni OHLCVChunk.
El OHLCV se deriva de Trades — nunca al revés (Clean Architecture Nivel 0).

Convenciones de ordenación (estándar de mercado)
-------------------------------------------------
bids : ordenados DESCENDENTE por precio (mejor bid primero).
asks : ordenados ASCENDENTE  por precio (mejor ask primero).
El constructor lo valida — fail-fast si el caller viola el contrato.

Protocolo L2 delta
------------------
OrderBookDelta.quantity == 0.0 → eliminar ese nivel del libro.
Es el protocolo estándar de Bybit, KuCoin y la mayoría de exchanges.
La propiedad ``is_delete`` lo expresa de forma legible (Clean Code).

Principios
----------
DDD        — VOs puros: inmutables, definidos por valor, sin identidad de negocio.
Fail-Fast  — invariantes validados en construcción, no en uso.
SSOT       — OrderBookSide vive aquí; importar desde aquí, nunca redefinir.
SRP        — este módulo solo define tipos de order book.
Clean Arch — sin dependencias de infraestructura ni de OHLCV.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# ---------------------------------------------------------------------------
# OrderBookSide
# ---------------------------------------------------------------------------


class OrderBookSide(str, Enum):
    """
    Lado del libro de órdenes.

    BID → órdenes de compra (demanda).
    ASK → órdenes de venta (oferta).

    Hereda ``str`` para serialización JSON directa sin encoder personalizado.
    """

    BID = "bid"
    ASK = "ask"


# ---------------------------------------------------------------------------
# PriceLevel
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PriceLevel:
    """
    Un nivel de precio en el libro de órdenes L2.

    Representa la cantidad acumulada disponible a un precio dado.
    Es la unidad atómica del order book — dos PriceLevel con el mismo
    price/quantity son el mismo dato (value semantics).

    Invariantes (fail-fast)
    -----------------------
    - price    > 0  (un precio negativo o cero no existe en mercados reales).
    - quantity >= 0 (0.0 es válido: representa un nivel a eliminar en deltas).
    """

    price: float
    quantity: float

    def __post_init__(self) -> None:
        if self.price <= 0.0:
            raise ValueError(f"PriceLevel.price must be > 0, got {self.price}")
        if self.quantity < 0.0:
            raise ValueError(f"PriceLevel.quantity must be >= 0, got {self.quantity}")

    @property
    def is_empty(self) -> bool:
        """True si la cantidad es cero (nivel vacío o marcado para eliminar)."""
        return self.quantity == 0.0

    def __repr__(self) -> str:
        return f"PriceLevel(price={self.price}, quantity={self.quantity})"


# ---------------------------------------------------------------------------
# OrderBookSnapshot
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderBookSnapshot:
    """
    Fotografía completa del libro de órdenes L2 en un instante dado.

    Representa el estado completo del order book tal como lo reporta el
    exchange — ya sea vía REST (fetch_order_book) o el mensaje inicial
    de un stream WebSocket.

    Campos
    ------
    exchange     : identificador del exchange (``"bybit"``, ``"kucoin"``…).
    symbol       : par en formato CCXT (``"BTC/USDT"``, ``"ETH/USDT:USDT"``…).
    timestamp_ms : Unix epoch en milisegundos UTC del snapshot.
    bids         : niveles bid, ordenados DESCENDENTE por precio.
    asks         : niveles ask, ordenados ASCENDENTE  por precio.
    depth        : número de niveles solicitados al exchange (0 = ilimitado).

    Propiedades derivadas
    ---------------------
    best_bid    → mejor precio de compra (bids[0].price).
    best_ask    → mejor precio de venta  (asks[0].price).
    spread      → diferencia absoluta best_ask - best_bid.
    spread_bps  → spread en puntos básicos (spread / mid_price * 10_000).
    mid_price   → (best_bid + best_ask) / 2.
    is_crossed  → True si best_bid >= best_ask (estado anómalo del mercado).

    Invariantes (fail-fast)
    -----------------------
    - exchange y symbol: no-vacíos.
    - timestamp_ms > 0.
    - bids y asks no pueden ser ambos vacíos simultáneamente.
    - bids ordenados DESCENDENTE: bids[i].price >= bids[i+1].price.
    - asks ordenados ASCENDENTE:  asks[i].price <= asks[i+1].price.
    """

    exchange: str
    symbol: str
    timestamp_ms: int
    bids: tuple[PriceLevel, ...]  # ordenado DESCENDENTE por precio
    asks: tuple[PriceLevel, ...]  # ordenado ASCENDENTE  por precio
    depth: int = 0  # 0 = ilimitado / desconocido

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if not self.exchange:
            raise ValueError("OrderBookSnapshot.exchange must be non-empty")
        if not self.symbol:
            raise ValueError("OrderBookSnapshot.symbol must be non-empty")
        if self.timestamp_ms <= 0:
            raise ValueError(f"OrderBookSnapshot.timestamp_ms must be > 0, got {self.timestamp_ms}")
        if not self.bids and not self.asks:
            raise ValueError("OrderBookSnapshot: bids and asks cannot both be empty")

        # Orden bids — DESCENDENTE
        for i in range(len(self.bids) - 1):
            if self.bids[i].price < self.bids[i + 1].price:
                raise ValueError(
                    f"OrderBookSnapshot.bids must be sorted DESCENDING by price. "
                    f"Violation at index {i}: "
                    f"{self.bids[i].price} < {self.bids[i + 1].price}. "
                    f"Use OrderBookSnapshot.from_raw() if order is not guaranteed."
                )

        # Orden asks — ASCENDENTE
        for i in range(len(self.asks) - 1):
            if self.asks[i].price > self.asks[i + 1].price:
                raise ValueError(
                    f"OrderBookSnapshot.asks must be sorted ASCENDING by price. "
                    f"Violation at index {i}: "
                    f"{self.asks[i].price} > {self.asks[i + 1].price}. "
                    f"Use OrderBookSnapshot.from_raw() if order is not guaranteed."
                )

    # -- factory ----------------------------------------------------------------

    @classmethod
    def from_raw(
        cls,
        exchange: str,
        symbol: str,
        timestamp_ms: int,
        bids: list[list[float]],  # [[price, qty], ...]
        asks: list[list[float]],  # [[price, qty], ...]
        depth: int = 0,
    ) -> OrderBookSnapshot:
        """
        Construye desde el formato crudo de CCXT / cryptofeed.

        Acepta listas [[price, qty], ...] desordenadas y aplica el orden
        canónico (bids DESC, asks ASC) antes de construir.

        Usado en el ACL de los adapters inbound — nunca en el dominio.

        Parameters
        ----------
        bids : lista de [price, qty] de órdenes de compra (puede estar desordenada).
        asks : lista de [price, qty] de órdenes de venta  (puede estar desordenada).
        """
        sorted_bids = tuple(
            PriceLevel(price=float(b[0]), quantity=float(b[1])) for b in sorted(bids, key=lambda x: x[0], reverse=True)
        )
        sorted_asks = tuple(
            PriceLevel(price=float(a[0]), quantity=float(a[1])) for a in sorted(asks, key=lambda x: x[0])
        )
        return cls(
            exchange=exchange,
            symbol=symbol,
            timestamp_ms=timestamp_ms,
            bids=sorted_bids,
            asks=sorted_asks,
            depth=depth,
        )

    # -- propiedades derivadas --------------------------------------------------

    @property
    def best_bid(self) -> float | None:
        """Mejor precio de compra. None si no hay bids."""
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> float | None:
        """Mejor precio de venta. None si no hay asks."""
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> float | None:
        """Precio medio (best_bid + best_ask) / 2. None si faltan bids o asks."""
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def spread(self) -> float | None:
        """Spread absoluto: best_ask - best_bid. None si faltan bids o asks."""
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def spread_bps(self) -> float | None:
        """
        Spread en puntos básicos: spread / mid_price * 10_000.

        None si mid_price es None o cero.
        """
        mid = self.mid_price
        if mid is None or mid == 0.0:
            return None
        sp = self.spread
        if sp is None:
            return None
        return (sp / mid) * 10_000.0

    @property
    def is_crossed(self) -> bool:
        """
        True si best_bid >= best_ask — estado anómalo del mercado.

        Un libro cruzado indica latencia extrema, bug del exchange
        o condición de mercado excepcional. Los consumers deben
        registrar y descartar snapshots cruzados.
        """
        if self.best_bid is None or self.best_ask is None:
            return False
        return self.best_bid >= self.best_ask

    @property
    def bid_depth_levels(self) -> int:
        """Número de niveles bid en el snapshot."""
        return len(self.bids)

    @property
    def ask_depth_levels(self) -> int:
        """Número de niveles ask en el snapshot."""
        return len(self.asks)

    def __repr__(self) -> str:
        return (
            f"OrderBookSnapshot("
            f"exchange={self.exchange!r}, symbol={self.symbol!r}, "
            f"ts_ms={self.timestamp_ms}, "
            f"bids={len(self.bids)}, asks={len(self.asks)}, "
            f"spread_bps={self.spread_bps:.2f}"
            f")"
        )


# ---------------------------------------------------------------------------
# OrderBookDelta
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrderBookDelta:
    """
    Actualización atómica de un único nivel L2 del order book.

    Representa un delta tal como lo emite el exchange en su stream
    WebSocket — la unidad mínima de cambio del estado del libro.

    Campos
    ------
    exchange     : identificador del exchange.
    symbol       : par de trading.
    timestamp_ms : Unix epoch en milisegundos UTC de la actualización.
    side         : bid (compra) o ask (venta).
    price        : precio del nivel actualizado.
    quantity     : nueva cantidad en ese nivel.
                   0.0 → eliminar el nivel (protocolo estándar L2).

    Protocolo de eliminación de nivel
    ----------------------------------
    quantity == 0.0 significa "eliminar este nivel del libro".
    Es el estándar de Bybit, KuCoin y la mayoría de exchanges.
    Usar la propiedad ``is_delete`` para claridad en el código del consumer.

    Invariantes (fail-fast)
    -----------------------
    - exchange y symbol: no-vacíos.
    - timestamp_ms > 0.
    - price > 0.
    - quantity >= 0 (0.0 es válido — eliminar nivel).
    """

    exchange: str
    symbol: str
    timestamp_ms: int
    side: OrderBookSide
    price: float
    quantity: float  # 0.0 = eliminar este nivel

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if not self.exchange:
            raise ValueError("OrderBookDelta.exchange must be non-empty")
        if not self.symbol:
            raise ValueError("OrderBookDelta.symbol must be non-empty")
        if self.timestamp_ms <= 0:
            raise ValueError(f"OrderBookDelta.timestamp_ms must be > 0, got {self.timestamp_ms}")
        if self.price <= 0.0:
            raise ValueError(f"OrderBookDelta.price must be > 0, got {self.price}")
        if self.quantity < 0.0:
            raise ValueError(f"OrderBookDelta.quantity must be >= 0, got {self.quantity}")

    # -- propiedades semánticas ------------------------------------------------

    @property
    def is_delete(self) -> bool:
        """
        True si este delta elimina el nivel del libro.

        Equivalente a ``quantity == 0.0`` — usar esta propiedad
        en lugar de comparar directamente con 0.0 (Clean Code).
        """
        return self.quantity == 0.0

    @property
    def is_bid(self) -> bool:
        return self.side is OrderBookSide.BID

    @property
    def is_ask(self) -> bool:
        return self.side is OrderBookSide.ASK

    def __repr__(self) -> str:
        action = "DELETE" if self.is_delete else "UPSERT"
        return (
            f"OrderBookDelta("
            f"exchange={self.exchange!r}, symbol={self.symbol!r}, "
            f"ts_ms={self.timestamp_ms}, "
            f"side={self.side.value!r}, price={self.price}, "
            f"quantity={self.quantity} [{action}]"
            f")"
        )


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------

__all__ = [
    "OrderBookSide",
    "PriceLevel",
    "OrderBookSnapshot",
    "OrderBookDelta",
]
