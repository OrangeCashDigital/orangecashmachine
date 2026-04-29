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

import pandas as pd

# Signal vive en domain/ — re-exportado aquí para backwards compatibility.
# Importar directamente desde domain.value_objects en código nuevo.
from domain.value_objects.signal import Signal, SignalType  # noqa: F401


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
