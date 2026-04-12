# -*- coding: utf-8 -*-
"""
trading/strategies/base.py
==========================

Contrato base para todas las estrategias de trading.

Principios
----------
- generate_signals() es el único método obligatorio
- Las estrategias NO ejecutan órdenes — solo generan señales
- El bot de ejecución decide qué hacer con las señales
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

import pandas as pd


SignalType = Literal["buy", "sell", "hold"]


@dataclass
class Signal:
    """Señal de trading generada por una estrategia."""
    symbol:     str
    timeframe:  str
    signal:     SignalType
    price:      float
    timestamp:  datetime
    confidence: float = 1.0          # 0.0 – 1.0
    metadata:   dict  = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence must be in [0, 1], got {self.confidence}")

    @property
    def is_actionable(self) -> bool:
        return self.signal in ("buy", "sell")


class BaseStrategy(ABC):
    """
    Clase base para estrategias de trading.

    Subclases deben implementar generate_signals().
    No deben tener estado mutable entre llamadas — deben ser
    stateless respecto a posiciones o capital.
    """

    name: str = "base"

    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> list[Signal]:
        """
        Genera señales de trading a partir de un DataFrame OHLCV.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame con columnas: timestamp, open, high, low, close, volume.
            Debe estar ordenado por timestamp ascendente.

        Returns
        -------
        list[Signal]
            Lista de señales. Puede estar vacía si no hay señal clara.
        """
        ...

    def validate_df(self, df: pd.DataFrame) -> None:
        """Valida que el DataFrame tenga las columnas mínimas requeridas."""
        required = {"timestamp", "open", "high", "low", "close", "volume"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"DataFrame missing columns: {missing}")
        if df.empty:
            raise ValueError("DataFrame is empty")
