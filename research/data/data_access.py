#!/usr/bin/env python3
"""
research/data/data_access.py
==============================

API de acceso a datos OHLCV y features para research y backtesting.

Fuente de datos
---------------
• OHLCV  → IcebergStorage (tabla silver.ohlcv)
• Features → GoldLoader (parquet gold/features/)

Uso
---
    from research.data.data_access import get_ohlcv, get_multiple_ohlcv
    from research.data.data_access import get_features

    df = get_ohlcv("BTC/USDT", "1h")
    df = get_ohlcv("BTC/USDT", "1h", start="2024-01-01", exchange="bybit")

Principios
----------
• KISS   — API simple: get_ohlcv(symbol, timeframe)
• SafeOps — errores explícitos, nunca silenciosos
• Cache  — IcebergStorage singleton por exchange (tabla compartida)
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from market_data.storage.iceberg.iceberg_storage import IcebergStorage
from data_platform.loaders.gold_loader import GoldLoader
from data_platform.ohlcv_utils import (
    DataNotFoundError,
    DataReadError,
    MarketDataLoaderError,
)

# Re-exportar para compatibilidad con código existente que importa desde aquí
__all__ = [
    "get_ohlcv",
    "get_multiple_ohlcv",
    "get_ohlcv_dict",
    "get_features",
    "get_features_dict",
    "DataNotFoundError",
    "DataReadError",
    "MarketDataLoaderError",
]

# Exchange por defecto para research
from ocm_platform.config.env_vars import OCM_EXCHANGE as _OCM_EXCHANGE, OCM_MARKET_TYPE as _OCM_MARKET_TYPE
_DEFAULT_EXCHANGE: str = os.environ.get(_OCM_EXCHANGE, "kucoin")
_DEFAULT_MARKET_TYPE: str = os.environ.get(_OCM_MARKET_TYPE, "spot")

# ==========================================================
# IcebergStorage cache — singleton por (exchange, market_type)
# ==========================================================

_iceberg_cache: Dict[str, IcebergStorage] = {}


def _get_storage(
    exchange:    Optional[str] = None,
    market_type: Optional[str] = None,
) -> IcebergStorage:
    """
    Devuelve IcebergStorage para el exchange dado, lazy singleton.

    La tabla silver.ohlcv es global — el exchange filtra via pushdown.
    Cache keyed por (exchange, market_type).
    """
    exc = (exchange or _DEFAULT_EXCHANGE).lower()
    mkt = (market_type or _DEFAULT_MARKET_TYPE).lower()
    key = f"{exc}:{mkt}"

    if key not in _iceberg_cache:
        _iceberg_cache[key] = IcebergStorage(exchange=exc, market_type=mkt)
        logger.debug("IcebergStorage initialized | exchange={} market_type={}", exc, mkt)

    return _iceberg_cache[key]


def _reset_storage(exchange: Optional[str] = None) -> None:
    """Reset cache — uso exclusivo en tests."""
    if exchange is None:
        _iceberg_cache.clear()
    else:
        for k in list(_iceberg_cache):
            if k.startswith(exchange.lower()):
                del _iceberg_cache[k]


# ==========================================================
# Public API — OHLCV
# ==========================================================

def get_ohlcv(
    symbol:      str,
    timeframe:   str,
    start:       Optional[str] = None,
    end:         Optional[str] = None,
    columns:     Optional[List[str]] = None,
    exchange:    Optional[str] = None,
    market_type: Optional[str] = None,
) -> pd.DataFrame:
    """
    Carga datos OHLCV desde Iceberg (Silver).

    Parameters
    ----------
    symbol      : e.g. "BTC/USDT"
    timeframe   : e.g. "1h", "4h", "1d"
    start / end : ISO 8601, e.g. "2024-01-01" (opcionales)
    columns     : subconjunto de columnas para optimizar memoria
    exchange    : exchange explícito. Si None usa OCM_EXCHANGE o "kucoin"
    market_type : "spot" | "swap". Si None usa OCM_MARKET_TYPE o "spot"

    Returns
    -------
    pd.DataFrame ordenado por timestamp.

    Raises
    ------
    DataNotFoundError : no existen datos para este símbolo/timeframe
    DataReadError     : error al leer desde Iceberg
    """
    storage = _get_storage(exchange, market_type)

    start_ts = pd.Timestamp(start, tz="UTC") if start else None
    end_ts   = pd.Timestamp(end,   tz="UTC") if end   else None

    try:
        df = storage.load_ohlcv(
            symbol=symbol,
            timeframe=timeframe,
            start=start_ts,
            end=end_ts,
        )
    except Exception as exc:
        raise DataReadError(
            f"Iceberg read failed | {symbol}/{timeframe} exchange={exchange or _DEFAULT_EXCHANGE} | {exc}"
        ) from exc

    if df is None or df.empty:
        raise DataNotFoundError(
            f"No data | {symbol}/{timeframe} exchange={exchange or _DEFAULT_EXCHANGE} "
            f"start={start} end={end}"
        )

    if columns:
        available = [c for c in columns if c in df.columns]
        df = df[available]

    logger.info(
        "Research OHLCV loaded | symbol={} timeframe={} exchange={} "
        "start={} end={} rows={}",
        symbol, timeframe, exchange or _DEFAULT_EXCHANGE,
        start, end, len(df),
    )
    return df


def get_multiple_ohlcv(
    symbols:     List[str],
    timeframe:   str,
    start:       Optional[str] = None,
    end:         Optional[str] = None,
    exchange:    Optional[str] = None,
    market_type: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Carga múltiples símbolos. Devuelve dict con los exitosos.

    Los fallos se loguean como warning — no interrumpen la carga del resto.

    Returns
    -------
    Dict[symbol, DataFrame] — solo símbolos que cargaron correctamente.
    Dict vacío si todos fallaron.
    """
    results: Dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        try:
            results[symbol] = get_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
                exchange=exchange,
                market_type=market_type,
            )
        except DataNotFoundError:
            logger.warning(
                "Research: no data | symbol={} timeframe={} exchange={}",
                symbol, timeframe, exchange or _DEFAULT_EXCHANGE,
            )
        except Exception as exc:
            logger.warning(
                "Research: load failed | symbol={} timeframe={} error={}",
                symbol, timeframe, exc,
            )
    return results


def get_ohlcv_dict(
    symbols:     List[str],
    timeframe:   str,
    start:       Optional[str] = None,
    end:         Optional[str] = None,
    exchange:    Optional[str] = None,
    market_type: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """Alias de get_multiple_ohlcv — compatibilidad con código existente."""
    return get_multiple_ohlcv(
        symbols=symbols,
        timeframe=timeframe,
        start=start,
        end=end,
        exchange=exchange,
        market_type=market_type,
    )


# ==========================================================
# Public API — Gold / Features
# ==========================================================

_gold_cache: Dict[str, GoldLoader] = {}


def _get_gold_loader(exchange: Optional[str] = None) -> GoldLoader:
    key = (exchange or _DEFAULT_EXCHANGE).lower()
    if key not in _gold_cache:
        # GoldLoader usa Iceberg — gold_path es legacy compat sin efecto.
        _gold_cache[key] = GoldLoader(exchange=key)
        logger.debug("GoldLoader initialized | exchange={}", key)
    return _gold_cache[key]


def _reset_gold_loader(exchange: Optional[str] = None) -> None:
    """Reset cache — uso exclusivo en tests."""
    if exchange is None:
        _gold_cache.clear()
    else:
        _gold_cache.pop((exchange or "").lower(), None)


def get_features(
    symbol:      str,
    timeframe:   str,
    exchange:    Optional[str] = None,
    market_type: Optional[str] = None,
    start:       Optional[str] = None,
    end:         Optional[str] = None,
    version:     str = "latest",
) -> pd.DataFrame:
    """
    Carga features Gold (OHLCV + indicadores técnicos).

    Returns
    -------
    pd.DataFrame con features ordenado por timestamp.

    Raises
    ------
    DataNotFoundError, DataReadError, VersionNotFoundError
    """
    loader = _get_gold_loader(exchange)
    mkt    = (market_type or _DEFAULT_MARKET_TYPE).lower()
    exc    = (exchange or _DEFAULT_EXCHANGE).lower()

    df = loader.load_features(
        exchange=exc,
        symbol=symbol,
        market_type=mkt,
        timeframe=timeframe,
        version=version,
    )

    if start:
        df = df[df["timestamp"] >= pd.Timestamp(start, tz="UTC")]
    if end:
        df = df[df["timestamp"] <= pd.Timestamp(end, tz="UTC")]

    logger.info(
        "Research features loaded | symbol={} timeframe={} exchange={} rows={}",
        symbol, timeframe, exc, len(df),
    )
    return df


def get_features_dict(
    symbols:     List[str],
    timeframe:   str,
    exchange:    Optional[str] = None,
    market_type: Optional[str] = None,
    version:     str = "latest",
) -> Dict[str, pd.DataFrame]:
    """Carga features para múltiples símbolos. Devuelve solo exitosos."""
    results: Dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        try:
            results[symbol] = get_features(
                symbol=symbol,
                timeframe=timeframe,
                exchange=exchange,
                market_type=market_type,
                version=version,
            )
        except Exception as exc:
            logger.warning(
                "Research: features failed | symbol={} timeframe={} error={}",
                symbol, timeframe, exc,
            )
    return results
