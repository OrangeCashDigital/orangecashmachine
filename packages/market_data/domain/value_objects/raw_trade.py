# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/raw_trade.py
=============================================

RawTrade — unidad atómica e inmutable de microestructura de mercado.

Representa un único trade ejecutado tal como lo reporta el exchange,
normalizado al vocabulario del dominio OCM.

Principios
----------
DDD        — Value Object puro: identidad por valor, no por referencia.
Fail-Fast  — invariantes validados en construcción, no en uso.
SSOT       — TradeSide vive aquí; es el único lugar de verdad para el enum.
SRP        — este módulo solo define RawTrade y TradeSide.
Clean Arch — sin dependencias de infraestructura ni frameworks.

Nota sobre Decimal
------------------
Se usa ``Decimal`` para preservar la precisión del exchange.
Los adaptadores (fetcher, storage) convierten float↔Decimal en la frontera.
El dominio no conoce float ni DoubleType — esa es responsabilidad del adaptador.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum


# ---------------------------------------------------------------------------
# TradeSide
# ---------------------------------------------------------------------------

class TradeSide(str, Enum):
    """
    Dirección agresora del trade desde la perspectiva del taker.

    BUY     → taker compró (lifted the ask): presión compradora.
    SELL    → taker vendió (hit the bid):    presión vendedora.
    UNKNOWN → el exchange no reporta lado (algunos pares spot legacy,
               string vacío de CCXT, o campo ausente).

    Hereda ``str`` para serialización JSON directa sin encoder personalizado.
    """

    BUY     = "buy"
    SELL    = "sell"
    UNKNOWN = "unknown"

    @classmethod
    def from_raw(cls, raw: str | None) -> TradeSide:
        """
        Parsea el valor crudo que devuelve CCXT.

        Fail-soft: string vacío, None o valor desconocido → UNKNOWN.
        No lanza excepción — un lado desconocido es información válida,
        no un error de ingesta.

        Parameters
        ----------
        raw : valor de CCXT (``"buy"``, ``"sell"``, ``""``, ``None``).
        """
        if not raw:
            return cls.UNKNOWN
        normalised = raw.strip().lower()
        try:
            return cls(normalised)
        except ValueError:
            return cls.UNKNOWN


# ---------------------------------------------------------------------------
# RawTrade
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class RawTrade:
    """
    Unidad atómica e inmutable de microestructura de mercado.

    Modela un único trade ejecutado tal como lo reporta el exchange,
    normalizado al vocabulario del dominio. Es un Value Object puro:
    la identidad estructural la da la combinación (exchange, trade_id).

    Campos
    ------
    exchange     : identificador del exchange (``"bybit"``, ``"kucoin"``…).
    market_type  : ``"spot"`` | ``"linear"`` | ``"inverse"``.
                   Afecta la semántica del trade (mark price, funding).
    symbol       : par en formato CCXT (``"BTC/USDT"``, ``"ETH/USDT:USDT"``…).
    trade_id     : ID nativo del exchange — único dentro del scope del exchange.
    timestamp_ms : Unix epoch en milisegundos UTC.
    price        : precio de ejecución en activo cotizado.
    amount       : cantidad de activo base ejecutada.
    side         : dirección agresora del taker.

    Propiedades derivadas
    ---------------------
    cost         : price × amount (activo cotizado gastado). No se almacena
                   como campo para evitar redundancia — el storage lo materializa.
    timestamp_utc: datetime UTC correspondiente a timestamp_ms.
    is_buy       : shortcut semántico.
    is_sell      : shortcut semántico.

    Invariantes (fail-fast en construcción)
    ----------------------------------------
    - exchange, market_type, symbol, trade_id: no-vacíos.
    - timestamp_ms > 0.
    - price > 0.
    - amount > 0.
    """

    # -- identidad --------------------------------------------------------------
    exchange:     str        # "bybit", "kucoin", "kucoinfutures"
    market_type:  str        # "spot" | "linear" | "inverse"
    symbol:       str        # "BTC/USDT", "ETH/USDT:USDT"
    trade_id:     str        # ID nativo del exchange

    # -- temporalidad -----------------------------------------------------------
    timestamp_ms: int        # Unix epoch milisegundos UTC

    # -- precio y volumen -------------------------------------------------------
    price:        Decimal    # precio de ejecución (activo cotizado)
    amount:       Decimal    # cantidad de activo base ejecutada

    # -- microestructura --------------------------------------------------------
    side:         TradeSide  # dirección agresora del taker

    # -- validación en construcción (fail-fast) ---------------------------------

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if not self.exchange:
            raise ValueError("RawTrade.exchange must be non-empty")
        if not self.market_type:
            raise ValueError("RawTrade.market_type must be non-empty")
        if not self.symbol:
            raise ValueError("RawTrade.symbol must be non-empty")
        if not self.trade_id:
            raise ValueError("RawTrade.trade_id must be non-empty")
        if self.timestamp_ms <= 0:
            raise ValueError(
                f"RawTrade.timestamp_ms must be > 0, got {self.timestamp_ms}"
            )
        if self.price <= Decimal(0):
            raise ValueError(
                f"RawTrade.price must be > 0, got {self.price}"
            )
        if self.amount <= Decimal(0):
            raise ValueError(
                f"RawTrade.amount must be > 0, got {self.amount}"
            )

    # -- propiedades derivadas --------------------------------------------------

    @property
    def cost(self) -> Decimal:
        """Valor en activo cotizado: price × amount."""
        return self.price * self.amount

    @property
    def timestamp_utc(self) -> datetime:
        """Datetime UTC correspondiente a timestamp_ms."""
        return datetime.fromtimestamp(self.timestamp_ms / 1_000.0, tz=timezone.utc)

    @property
    def is_buy(self) -> bool:
        return self.side is TradeSide.BUY

    @property
    def is_sell(self) -> bool:
        return self.side is TradeSide.SELL

    # -- representación ---------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"RawTrade("
            f"exchange={self.exchange!r}, "
            f"market_type={self.market_type!r}, "
            f"symbol={self.symbol!r}, "
            f"trade_id={self.trade_id!r}, "
            f"ts_ms={self.timestamp_ms}, "
            f"price={self.price}, "
            f"amount={self.amount}, "
            f"side={self.side.value!r}"
            f")"
        )


__all__ = ["RawTrade", "TradeSide"]
