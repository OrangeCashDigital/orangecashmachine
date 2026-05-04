# -*- coding: utf-8 -*-
"""
domain/value_objects/
=====================
Tipos inmutables sin identidad. Igualdad por valor, no por referencia.

Importar desde aquí (SSOT):
    from domain.value_objects import Signal, SignalType, Timeframe
"""
from domain.value_objects.signal import Signal, SignalType
from domain.value_objects.timeframe import Timeframe

__all__ = [
    "Signal",
    "SignalType",
    "Timeframe",
]
