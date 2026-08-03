# -*- coding: utf-8 -*-
"""
shared/contracts/
=================

Protocolos y abstracciones que definen contratos inter-bounded-context.

Uso correcto (DIP):
    from shared.contracts.boundaries import FeatureSource, SignalProtocol

Los bounded contexts dependen de estas abstracciones, nunca entre sí.
Regla: ZERO imports de módulos internos del proyecto.
"""

from shared.contracts.boundaries import (
    FeatureSource,
    RiskGate,
    SignalProtocol,
)

__all__ = [
    "FeatureSource",
    "SignalProtocol",
    "RiskGate",
]
