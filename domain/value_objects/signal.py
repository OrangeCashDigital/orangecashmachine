# -*- coding: utf-8 -*-
"""
domain/value_objects/signal.py
===============================

Signal — value object de dominio.

Representa la intención de trading generada por una estrategia.
Inmutable en práctica (frozen=True no aplica por datetime mutable,
pero ningún campo debe modificarse tras construcción).

Reglas de dominio
-----------------
- confidence ∈ [0.0, 1.0]        — invariante validada en __post_init__
- is_actionable ≡ signal ∈ {buy, sell}  — derivada, nunca almacenar
- hold signals son válidas (carry information: "no actúes")

Ubicación: domain/ (sin imports externos a stdlib)

Principios: SOLID · DDD · SSOT · Fail-Fast
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

# stdlib únicamente — dominio puro
SignalType = Literal["buy", "sell", "hold"]


@dataclass
class Signal:
    """
    Value object que representa una señal de trading.

    Generado por: BaseStrategy.generate_signals()
    Consumido por: OMS.submit(), RiskManager.validate()

    Campos
    ------
    symbol      : par normalizado  (e.g. "BTC/USDT")
    timeframe   : marco temporal   (e.g. "1h", "4h")
    signal      : dirección        ("buy" | "sell" | "hold")
    price       : precio de cierre al momento de la señal
    timestamp   : timestamp UTC de la vela que generó la señal
    confidence  : confianza de la señal ∈ [0.0, 1.0]
    metadata    : datos adicionales de la estrategia (sin schema fijo)

    Inmutabilidad
    -------------
    No usar frozen=True porque datetime es mutable en Python < 3.11.
    Por convención, ningún campo se modifica tras construcción.
    """
    symbol:     str
    timeframe:  str
    signal:     SignalType
    price:      float
    timestamp:  datetime
    confidence: float = 1.0
    metadata:   dict  = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Fail-fast: invariantes de dominio validadas en construcción."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(
                f"Signal.confidence debe estar en [0, 1], recibido: {self.confidence}"
            )
        if self.price <= 0.0:
            raise ValueError(
                f"Signal.price debe ser positivo, recibido: {self.price}"
            )
        if not self.symbol:
            raise ValueError("Signal.symbol no puede estar vacío")
        if not self.timeframe:
            raise ValueError("Signal.timeframe no puede estar vacío")

    @property
    def is_actionable(self) -> bool:
        """True si la señal debe generar una orden (buy o sell)."""
        return self.signal in ("buy", "sell")

    @property
    def is_buy(self) -> bool:
        return self.signal == "buy"

    @property
    def is_sell(self) -> bool:
        return self.signal == "sell"

    def __str__(self) -> str:
        return (
            f"Signal({self.signal.upper()} {self.symbol} @ {self.price:.4f}"
            f" tf={self.timeframe} conf={self.confidence:.2f})"
        )

    def __repr__(self) -> str:
        return self.__str__()
