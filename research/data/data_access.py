"""
research/data/data_access.py
=============================

Research Data Access Layer – OrangeCashMachine

Responsabilidad única
---------------------
Proveer una API limpia y estable para acceder a datos OHLCV
desde el Data Lake dentro del entorno de research.

Desacopla la capa de research de la implementación del loader:

    Research Layer
          ↓
    data_access.py   ← esta capa de anticorrupción
          ↓
    Data Platform (MarketDataLoader)

Si mañana cambia el loader (nuevo backend, nueva estructura
de particiones), solo se toca este archivo — el código de
research no se modifica.

Principios aplicados
--------------------
• SOLID  – SRP: solo expone la API de research, no implementa carga
           OCP: el loader es inyectable para testing y extensión
• DRY    – _get_loader() centraliza la inicialización lazy
• KISS   – funciones simples con responsabilidad única
• SafeOps – loader inicializado de forma lazy (no en import time),
            tipo de retorno correcto para MultiSymbolResult,
            logging estructurado consistente con el resto del proyecto

Ejemplo de uso
--------------
from research.data.data_access import get_ohlcv, get_multiple_ohlcv

# Carga completa
df = get_ohlcv("BTC/USDT", "1h")

# Carga con rango de fechas
df = get_ohlcv("BTC/USDT", "1h", start="2023-01-01", end="2023-06-30")

# Multi-símbolo con resultado tipado
result = get_multiple_ohlcv(["BTC/USDT", "ETH/USDT"], "1h")
if not result.all_succeeded:
    print(result.errors)
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from data_platform.loaders.market_data_loader import (
    MarketDataLoader,
    MarketDataLoaderError,
    MultiSymbolResult,
)


# ==========================================================
# Constants
# ==========================================================

# Permite sobreescribir el path del Data Lake via variable de entorno
# sin modificar código — útil en distintos entornos (dev, staging, prod)
import os as _os
_DATA_LAKE_PATH: Optional[Path] = (
    Path(_os.environ["OCM_DATA_LAKE_PATH"])
    if "OCM_DATA_LAKE_PATH" in _os.environ
    else None
)


# ==========================================================
# Lazy Loader
# ==========================================================

_loader_instance: Optional[MarketDataLoader] = None


def _get_loader() -> MarketDataLoader:
    """
    Devuelve el loader singleton, inicializándolo de forma lazy.

    Lazy init en lugar de singleton en import time:
    - El import del módulo no falla si el Data Lake no existe todavía
    - El loader se puede resetear en tests sobreescribiendo _loader_instance
    - El path del Data Lake se resuelve cuando se necesita, no antes

    Returns
    -------
    MarketDataLoader
        Instancia compartida del loader.

    Raises
    ------
    MarketDataLoaderError
        Si el Data Lake no existe en el path configurado.
    """
    global _loader_instance

    if _loader_instance is None:
        _loader_instance = MarketDataLoader(data_lake_path=_DATA_LAKE_PATH)
        logger.debug(
            "MarketDataLoader initialized (lazy) | path={}",
            _DATA_LAKE_PATH or "default",
        )

    return _loader_instance


def _reset_loader() -> None:
    """
    Resetea el singleton del loader.

    Uso exclusivo en tests para inyectar un loader mock:

        data_access._loader_instance = MockLoader()
        # o bien:
        data_access._reset_loader()
    """
    global _loader_instance
    _loader_instance = None


# ==========================================================
# Public API
# ==========================================================

def get_ohlcv(
    symbol:    str,
    timeframe: str,
    start:     Optional[str] = None,
    end:       Optional[str] = None,
    columns:   Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Carga datos OHLCV desde el Data Lake.

    Parameters
    ----------
    symbol : str
        Par de trading, e.g. "BTC/USDT".
    timeframe : str
        Intervalo temporal, e.g. "1m", "5m", "1h", "1d".
    start : str, optional
        Fecha inicio ISO 8601, e.g. "2023-01-01".
        Si se omite junto con end, carga todos los datos disponibles.
    end : str, optional
        Fecha fin ISO 8601, e.g. "2023-06-30".
    columns : list[str], optional
        Subconjunto de columnas. Útil para optimizar memoria:
        e.g. ["timestamp", "close"] para cálculos de precio.

    Returns
    -------
    pd.DataFrame
        DataFrame OHLCV ordenado por timestamp.

    Raises
    ------
    MarketDataLoaderError
        Si no existen datos o no se pueden leer.
    ValueError
        Si start o end tienen formato inválido, o start > end.
    """
    loader = _get_loader()

    df = loader.load_ohlcv_range(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start,
        end_date=end,
        columns=columns,
    )

    logger.info(
        "Research OHLCV loaded | symbol={} timeframe={} start={} end={} rows={}",
        symbol, timeframe, start, end, len(df),
    )

    return df


def get_multiple_ohlcv(
    symbols:    List[str],
    timeframe:  str,
    start:      Optional[str] = None,
    end:        Optional[str] = None,
) -> MultiSymbolResult:
    """
    Carga múltiples símbolos para el mismo timeframe.

    Devuelve un MultiSymbolResult tipado — no un dict anónimo.
    El caller puede inspeccionar qué símbolos fallaron y por qué:

        result = get_multiple_ohlcv(["BTC/USDT", "ETH/USDT"], "1h")

        # Acceder a los datos
        btc_df = result.data["BTC/USDT"]

        # Verificar fallos parciales
        if not result.all_succeeded:
            for symbol, error in result.errors.items():
                print(f"{symbol} failed: {error}")

    Parameters
    ----------
    symbols : list[str]
        Lista de pares de trading.
    timeframe : str
        Intervalo temporal común para todos los símbolos.
    start : str, optional
        Fecha inicio ISO 8601.
    end : str, optional
        Fecha fin ISO 8601.

    Returns
    -------
    MultiSymbolResult
        Resultado tipado con .data (dict) y .errors (dict).
        Nunca lanza aunque algunos símbolos fallen — los fallos
        quedan registrados en .errors (SafeOps).
    """
    loader = _get_loader()

    result = loader.load_multiple_symbols(
        symbols=symbols,
        timeframe=timeframe,
        start_date=start,
        end_date=end,
    )

    logger.info(
        "Research multi-symbol load | timeframe={} loaded={} failed={}",
        timeframe, len(result.loaded), len(result.failed),
    )

    if result.failed:
        logger.warning(
            "Some symbols failed to load | failed={}",
            result.failed,
        )

    return result


def get_ohlcv_dict(
    symbols:   List[str],
    timeframe: str,
    start:     Optional[str] = None,
    end:       Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Carga múltiples símbolos y devuelve solo los exitosos como dict.

    Conveniencia para código de research que espera un dict simple
    y no necesita inspeccionar los fallos:

        dfs = get_ohlcv_dict(["BTC/USDT", "ETH/USDT"], "1h")
        for symbol, df in dfs.items():
            compute_features(df)

    Para manejo explícito de fallos, usar get_multiple_ohlcv()
    que devuelve el MultiSymbolResult completo.

    Returns
    -------
    Dict[str, pd.DataFrame]
        Solo los símbolos que cargaron correctamente.
        Dict vacío si todos fallaron.
    """
    result = get_multiple_ohlcv(
        symbols=symbols,
        timeframe=timeframe,
        start=start,
        end=end,
    )
    return result.data
