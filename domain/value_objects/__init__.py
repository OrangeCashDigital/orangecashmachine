# -*- coding: utf-8 -*-
"""
domain/value_objects/
=====================
Tipos inmutables sin identidad. Igualdad por valor, no por referencia.
"""
from domain.value_objects.signal import Signal, SignalType

__all__ = ["Signal", "SignalType"]
