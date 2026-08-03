# -*- coding: utf-8 -*-
"""
shared/types/signal.py
===============================

Signal — value object de dominio.

Representa la intención de trading generada por una estrategia.
No es frozen (auditoría pendiente, ver Fase 7 del plan de shared/):
`metadata` es un dict mutable — frozen=True impediría reasignar atributos
pero NO impediría mutar el contenido del dict. Por convención, ningún
campo se modifica ni se muta tras construcción.

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

# SignalType re-exporta SignalDirection desde _base (BC-33: el SSOT de los
# literales wire vive en shared.kafka.schemas._base; types solo re-expone).
from shared.kafka.schemas._base import SignalDirection

SignalType = SignalDirection


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
    direction   : dirección        ("buy" | "sell" | "hold")
    price       : precio de cierre al momento de la señal
    timestamp   : timestamp UTC de la vela que generó la señal
    confidence  : confianza de la señal ∈ [0.0, 1.0]
    metadata    : datos adicionales de la estrategia (sin schema fijo).
                  Dict mutable — por convención no se muta tras construcción.

    Inmutabilidad
    -------------
    No se usa frozen=True (auditoría pendiente — Fase 7 del plan shared/).
    Nota: metadata es un dict mutable; frozen=True impediría reasignar
    atributos pero NO mutar el contenido del dict. Por convención, ningún
    campo se modifica ni se muta tras construcción.
    """

    symbol: str
    timeframe: str
    direction: SignalType
    price: float
    timestamp: datetime
    confidence: float = 1.0
    metadata: dict = field(default_factory=dict)

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
