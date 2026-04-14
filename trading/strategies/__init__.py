# -*- coding: utf-8 -*-
"""
trading/strategies/
====================

Re-exporta los contratos públicos del subsistema de estrategias.

OCP: nuevas estrategias se registran via @register_strategy en su propio
módulo — este __init__ no necesita cambiar al añadir estrategias.
"""
from trading.strategies.base import BaseStrategy, Signal, SignalType
from trading.strategies.registry import (
    StrategyRegistry,
    StrategyNotFoundError,
    register_strategy,
)

# Trigger del auto-registro de builtins: importar registry ya ejecuta
# _register_builtins() en module-level de registry.py.

__all__ = [
    "BaseStrategy",
    "Signal",
    "SignalType",
    "StrategyRegistry",
    "StrategyNotFoundError",
    "register_strategy",
]
