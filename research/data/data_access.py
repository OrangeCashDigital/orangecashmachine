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
    data_access.py   ← capa de anticorrupción
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
• SafeOps – loader inicializado lazy (no en import time),
            cache keyed por exchange — nunca devuelve el exchange
            incorrecto si se llama con exchanges distintos

Ejemplo de uso
--------------
from research.data.data_access import get_ohlcv, get_multiple_ohlcv

# Carga completa
df = get_ohlcv("BTC/USDT", "1h")

# Exchange explícito
df = get_ohlcv("BTC/USDT", "1h", exchange="binance")

# Multi-símbolo con resultado tipado
result = get_multiple_ohlcv(["BTC/USDT", "ETH/USDT"], "1h")
if not result.all_succeeded:
    print(result.errors)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from data_platform.loaders.market_data_loader import (
    MarketDataLoader,
    MarketDataLoaderError,
    MultiSymbolResult,
)
from data_platform.loaders.gold_loader import GoldLoader


# ==========================================================
# Constants
# ==========================================================

# Permite sobreescribir el path del Data Lake via variable de entorno
# sin modificar código — útil en distintos entornos (dev, staging, prod)
_DATA_LAKE_PATH: Optional[Path] = (
    Path(os.environ["OCM_DATA_LAKE_PATH"])
    if "OCM_DATA_LAKE_PATH" in os.environ
    else None
)

# Exchange por defecto para research
_DEFAULT_EXCHANGE: str = os.environ.get("OCM_EXCHANGE", "kucoin")


# ==========================================================
# Loader cache — keyed por exchange
# ==========================================================

# Dict[exchange_key, MarketDataLoader] — evita devolver el exchange
# incorrecto cuando se llama con exchanges distintos en la misma sesión.
# exchange_key = exchange.lower() o "_any_" si exchange=None.
_loader_cache: Dict[str, MarketDataLoader] = {}


def _get_loader(exchange: Optional[str] = None) -> MarketDataLoader:
    """
    Devuelve el loader para el exchange dado, creándolo de forma lazy.

    Cache keyed por exchange — cada exchange tiene su propia instancia.
    Esto evita el bug donde _get_loader("binance") devuelve silenciosamente
    el loader de "kucoin" si este fue inicializado primero.

    Parameters
    ----------
    exchange : str, optional
        Exchange explícito. Si None, usa _DEFAULT_EXCHANGE.
    """
    key = (exchange or _DEFAULT_EXCHANGE).lower()

    if key not in _loader_cache:
        _loader_cache[key] = MarketDataLoader(
            data_lake_path=_DATA_LAKE_PATH,
            exchange=key,
        )
        logger.debug(
            "MarketDataLoader initialized | path={} exchange={}",
            _DATA_LAKE_PATH or "default",
            key,
        )

    return _loader_cache[key]


def _reset_loader(exchange: Optional[str] = None) -> None:
    """
    Resetea el loader del exchange dado, o toda la cache si exchange=None.

    Uso exclusivo en tests para inyectar un loader mock:

        import research.data.data_access as da
        da._loader_cache["kucoin"] = MockLoader()
        # o bien limpiar todo:
        da._reset_loader()
    """
    if exchange is None:
        _loader_cache.clear()
    else:
        _loader_cache.pop(exchange.lower(), None)


# ==========================================================
# Public API
# ==========================================================

def get_ohlcv(
    symbol:    str,
    timeframe: str,
    start:     Optional[str] = None,
    end:       Optional[str] = None,
    columns:   Optional[List[str]] = None,
    version:   str = "latest",
    as_of:     Optional[str] = None,
    exchange:  Optional[str] = None,
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
    version : str
        "latest" (default) o versión específica como "v000003".
    as_of : str, optional
        ISO 8601 timestamp. Resuelve la versión vigente en ese momento.
        Permite reproducibilidad temporal: get_ohlcv("BTC/USDT", "1h",
        start="2026-01-01", as_of="2026-03-01T00:00:00Z")
    exchange : str, optional
        Exchange explícito. Si None, usa OCM_EXCHANGE o "kucoin".

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
    loader = _get_loader(exchange)

    df = loader.load_ohlcv_range(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start,
        end_date=end,
        columns=columns,
        version=version,
        as_of=as_of,
    )

    logger.info(
        "Research OHLCV loaded | symbol={} timeframe={} version={} as_of={} "
        "exchange={} start={} end={} rows={}",
        symbol, timeframe, version, as_of or "-",
        exchange or _DEFAULT_EXCHANGE,
        start, end, len(df),
    )

    return df


def get_multiple_ohlcv(
    symbols:   List[str],
    timeframe: str,
    start:     Optional[str] = None,
    end:       Optional[str] = None,
    version:   str = "latest",
    as_of:     Optional[str] = None,
    exchange:  Optional[str] = None,
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
    exchange : str, optional
        Exchange explícito. Si None, usa OCM_EXCHANGE o "kucoin".

    Returns
    -------
    MultiSymbolResult
        Resultado tipado con .data (dict) y .errors (dict).
        Nunca lanza aunque algunos símbolos fallen — los fallos
        quedan registrados en .errors (SafeOps).
    """
    loader = _get_loader(exchange)

    result = loader.load_multiple_symbols(
        symbols=symbols,
        timeframe=timeframe,
        start_date=start,
        end_date=end,
        version=version,
        as_of=as_of,
    )

    logger.info(
        "Research multi-symbol load | timeframe={} exchange={} loaded={} failed={}",
        timeframe, exchange or _DEFAULT_EXCHANGE,
        len(result.loaded), len(result.failed),
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
    version:   str = "latest",
    as_of:     Optional[str] = None,
    exchange:  Optional[str] = None,
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
    return get_multiple_ohlcv(
        symbols=symbols,
        timeframe=timeframe,
        start=start,
        end=end,
        version=version,
        as_of=as_of,
        exchange=exchange,
    ).data


# ==========================================================
# Gold / Feature Store cache — keyed por exchange
# ==========================================================

_gold_loader_cache: Dict[str, GoldLoader] = {}

_GOLD_PATH: Optional[Path] = (
    Path(os.environ["OCM_GOLD_PATH"])
    if "OCM_GOLD_PATH" in os.environ
    else None
)


def _get_gold_loader(exchange: Optional[str] = None) -> GoldLoader:
    """
    Devuelve el GoldLoader para el exchange dado, creándolo de forma lazy.

    Mismo patrón de cache keyed-by-exchange que _get_loader().
    """
    key = (exchange or _DEFAULT_EXCHANGE).lower()
    if key not in _gold_loader_cache:
        _gold_loader_cache[key] = GoldLoader(
            gold_path=_GOLD_PATH,
            exchange=key,
        )
        logger.debug(
            "GoldLoader initialized | path={} exchange={}",
            _GOLD_PATH or "default", key,
        )
    return _gold_loader_cache[key]


def _reset_gold_loader(exchange: Optional[str] = None) -> None:
    """Resetea el GoldLoader cache. Uso exclusivo en tests."""
    if exchange is None:
        _gold_loader_cache.clear()
    else:
        _gold_loader_cache.pop(exchange.lower(), None)


# ==========================================================
# Feature Store Public API
# ==========================================================

def get_features(
    symbol:      str,
    market_type: str,
    timeframe:   str,
    version:     str = "latest",
    as_of:       Optional[str] = None,
    columns:     Optional[List[str]] = None,
    exchange:    Optional[str] = None,
) -> pd.DataFrame:
    """
    Carga el dataset Gold de features (OHLCV + indicadores técnicos).

    Punto de entrada unificado para research que necesita features
    precalculados — evita recalcular en cada notebook o estrategia.

    Parameters
    ----------
    symbol      : Par de trading, e.g. "BTC/USDT".
    market_type : "spot" | "swap".
    timeframe   : Intervalo, e.g. "1h".
    version     : "latest" (default) o versión exacta "v000003".
    as_of       : ISO 8601 timestamp para reproducibilidad temporal.
    columns     : Subconjunto de columnas para reducir memoria.
    exchange    : Exchange explícito. Si None, usa OCM_EXCHANGE.

    Returns
    -------
    pd.DataFrame
        DataFrame con OHLCV + features (return_1, log_return,
        volatility_20, high_low_spread, vwap), ordenado por timestamp.

    Raises
    ------
    DataNotFoundError
        Si no hay datos Gold para el símbolo/timeframe dado.
    VersionNotFoundError
        Si la versión solicitada no existe.
    DataReadError
        Si el archivo Parquet no puede leerse.

    Examples
    --------
    # Carga completa de features
    df = get_features("BTC/USDT", "spot", "1h")

    # Reproducibilidad temporal en backtest
    df = get_features("BTC/USDT", "spot", "1h", as_of="2026-03-01T00:00:00Z")

    # Solo columnas de precio + volatilidad
    df = get_features("BTC/USDT", "spot", "1h", columns=["timestamp", "close", "volatility_20"])
    """
    loader = _get_gold_loader(exchange)
    df = loader.load_features(
        symbol=symbol,
        market_type=market_type,
        timeframe=timeframe,
        version=version,
        as_of=as_of,
        columns=columns,
        exchange=exchange,
    )

    logger.info(
        "Research features loaded | symbol={} market={} timeframe={} "
        "version={} as_of={} exchange={} rows={} cols={}",
        symbol, market_type, timeframe,
        version, as_of or "-",
        exchange or _DEFAULT_EXCHANGE,
        len(df), len(df.columns),
    )
    return df


def get_features_dict(
    symbols:     List[str],
    market_type: str,
    timeframe:   str,
    version:     str = "latest",
    as_of:       Optional[str] = None,
    exchange:    Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Carga features Gold para múltiples símbolos. Devuelve solo los exitosos.

    Los fallos se logean como warnings pero nunca propagan (SafeOps):
    el caller recibe un dict parcial si algún símbolo no tiene datos Gold.

    Returns
    -------
    Dict[str, pd.DataFrame]
        Mapa símbolo → DataFrame de features.
        Dict vacío si todos fallaron.

    Examples
    --------
    dfs = get_features_dict(["BTC/USDT", "ETH/USDT"], "spot", "1h")
    for symbol, df in dfs.items():
        run_strategy(df)
    """
    loader  = _get_gold_loader(exchange)
    result  = {}

    for symbol in symbols:
        try:
            result[symbol] = loader.load_features(
                symbol=symbol,
                market_type=market_type,
                timeframe=timeframe,
                version=version,
                as_of=as_of,
                exchange=exchange,
            )
        except Exception as exc:
            logger.warning(
                "get_features_dict: failed loading {} | err={}", symbol, exc
            )

    logger.info(
        "Research features dict | market={} timeframe={} exchange={} "
        "loaded={} failed={}",
        market_type, timeframe,
        exchange or _DEFAULT_EXCHANGE,
        len(result), len(symbols) - len(result),
    )
    return result
