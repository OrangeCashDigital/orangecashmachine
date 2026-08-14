# -*- coding: utf-8 -*-
"""
shared/types/signal.py
===============================

Signal — value object de dominio.

Representa la intención de trading generada por una estrategia.
Frozen (dataclass frozen=True): ningún campo puede reasignarse tras la
construcción. Nota: `metadata` es un dict mutable — frozen impide
reasignar el atributo pero NO impide mutar el contenido del dict; por
convención, no se muta tras la construcción.

Reglas de dominio
-----------------
- confidence ∈ [0.0, 1.0]        — invariante validada en __post_init__
- is_actionable ≡ direction ∈ {buy, sell}  — derivada, nunca almacenar
- hold signals son válidas (carry information: "no actúes")

Ubicación: shared/types/ (kernel compartido — solo imports intra-shared y stdlib)

Principios: SOLID · DDD · SSOT · Fail-Fast
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

# SignalType re-exporta SignalDirection desde shared.enums (BC-45: el SSOT del
# vocabulario de dominio vive en la raíz del kernel; types solo re-expone).
from shared.enums import SignalDirection

SignalType = SignalDirection


@dataclass(frozen=True)
class Signal:
    """
    Value object que representa una señal de trading.

    Generado por: BaseStrategy.generate_signals()
    Consumido por: OMS.submit(), RiskManager.validate()

    Campos
    ------
    symbol      : par normalizado  (e.g. "BTC/USDT")
    timeframe   : marco temporal   (e.g. "1h", "4h")
    direction   : dirección        ("buy" | "sell" | "hold")
    price       : precio de cierre al momento de la señal
    timestamp   : timestamp UTC de la vela que generó la señal
    confidence  : confianza de la señal ∈ [0.0, 1.0]
    metadata    : datos adicionales de la estrategia (sin schema fijo).
                  Dict mutable — por convención no se muta tras construcción.

    quantity (F1, ADR-0025/0026/0027)
    ---------------------------------
    Cantidad TARGET pedida por el productor de la señal — SOLO un objetivo
    de tamaño, NUNCA la cantidad económica asentada. Para un SELL/cierre
    debe estar ya derivada de Position.quantity (p. ej. el stop-loss del
    TradingEngine la toma del snapshot del portfolio, la SSOT); el OMS la
    clampa contra su posición económica local (nunca se pide más de lo
    disponible). Para un BUY, un quantity explícito pide ese tamaño en
    unidades base en lugar del sizing por capital (size_pct).
    La cantidad ejecutada real es SIEMPRE Order.filled_qty (fill del
    exchange), nunca este campo.

    Inmutabilidad
    -------------
    frozen=True: la dataclass es inmutable — reasignar cualquier atributo
    tras la construcción lanza FrozenInstanceError.
    Nota: metadata es un dict mutable; frozen impide reasignar el atributo
    pero NO impide mutar el contenido del dict. Por convención, no se
    muta tras la construcción.
    """

    symbol: str
    timeframe: str
    direction: SignalType
    price: float
    timestamp: datetime
    confidence: float = 1.0
    metadata: dict = field(default_factory=dict)
    quantity: Optional[float] = None

    def __post_init__(self) -> None:
        """Fail-fast: invariantes de dominio validadas en construcción."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Signal.confidence debe estar en [0, 1], recibido: {self.confidence}")
        if self.price <= 0.0:
            raise ValueError(f"Signal.price debe ser positivo, recibido: {self.price}")
        if not self.symbol:
            raise ValueError("Signal.symbol no puede estar vacío")
        if not self.timeframe:
            raise ValueError("Signal.timeframe no puede estar vacío")
        if self.quantity is not None and self.quantity <= 0.0:
            raise ValueError(f"Signal.quantity debe ser > 0 cuando se especifica, recibido: {self.quantity}")

    @property
    def signal(self) -> "SignalType":
        """
        DEPRECATED — usar Signal.direction.
        Alias de compatibilidad durante la migración Signal.signal → Signal.direction.
        Eliminable cuando todos los consumidores usen .direction.
        """
        import warnings

        warnings.warn(
            "Signal.signal está deprecado — usar Signal.direction",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.direction

    @property
    def is_actionable(self) -> bool:
        """True si la señal debe generar una orden (buy o sell)."""
        return self.direction in ("buy", "sell")

    @property
    def is_buy(self) -> bool:
        return self.direction == "buy"

    @property
    def is_sell(self) -> bool:
        return self.direction == "sell"

    def __str__(self) -> str:
        return (
            f"Signal({self.direction.upper()} {self.symbol} @ {self.price:.4f}"
            f" tf={self.timeframe} conf={self.confidence:.2f})"
        )

    def __repr__(self) -> str:
        return self.__str__()
