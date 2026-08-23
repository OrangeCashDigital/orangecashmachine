#!/usr/bin/env python3
"""
research/data/data_access.py
==============================

API de acceso a datos OHLCV y features para research y backtesting.

Fuente de datos
---------------
• OHLCV    → StorageFactoryPort → OHLCVStorage (tabla silver.ohlcv)
• Features → FeatureReaderPort (parquet gold/features/)

Implementaciones resueltas por research.data.composition_root (F-1): los
adapters concretos (IcebergStorageFactory / GoldReader) viven SOLO allí.

Uso
---
    from research.data.data_access import get_ohlcv, get_multiple_ohlcv
    from research.data.data_access import get_features

    df = get_ohlcv("BTC/USDT", "1h")
    df = get_ohlcv("BTC/USDT", "1h", start="2024-01-01", exchange="bybit")

Principios
----------
• KISS    — API simple: get_ohlcv(symbol, timeframe)
• SafeOps — errores explícitos, nunca silenciosos
• DIP     — data_access depende de ports (StorageFactoryPort / FeatureReaderPort);
            la implementación concreta la decide el composition root
• SSOT    — polars como único dtype de retorno; sin conversiones implícitas
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from market_data.ports.outbound.storage import OHLCVStorage

import polars as pl
from loguru import logger
from market_data.domain.exceptions import (
    DataNotFoundError,
    DataReadError,
    MarketDataLoaderError,
)
from market_data.ports.outbound.feature_reader import FeatureReaderPort
from market_data.ports.outbound.storage_factory import StorageFactoryPort

from research.data.composition_root import build_feature_reader, build_storage_factory

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

from ocm.config.env_vars import (
    OCM_EXCHANGE as _OCM_EXCHANGE,
)
from ocm.config.env_vars import (
    OCM_MARKET_TYPE as _OCM_MARKET_TYPE,
)

_DEFAULT_EXCHANGE: str = os.environ.get(_OCM_EXCHANGE, "kucoin")
_DEFAULT_MARKET_TYPE: str = os.environ.get(_OCM_MARKET_TYPE, "spot")

# Seam de inyección (F-1): research depende de StorageFactoryPort, no del
# adaptador concreto. El default lo resuelve el composition root; los tests
# sustituyen la instancia del módulo (port, no concreto) por un fake.
_storage_factory: StorageFactoryPort = build_storage_factory()


def _get_storage(
    exchange: Optional[str] = None,
    market_type: Optional[str] = None,
) -> "OHLCVStorage":
    exc = (exchange or _DEFAULT_EXCHANGE).lower()
    mkt = (market_type or _DEFAULT_MARKET_TYPE).lower()
    storage = _storage_factory.get_storage(exchange=exc, market_type=mkt)
    logger.debug("Storage resolved | exchange={} market_type={}", exc, mkt)
    return storage


def _parse_utc(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def get_ohlcv(
    symbol: str,
    timeframe: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    columns: Optional[List[str]] = None,
    exchange: Optional[str] = None,
    market_type: Optional[str] = None,
) -> pl.DataFrame:
    storage = _get_storage(exchange, market_type)

    start_dt = _parse_utc(start)
    end_dt = _parse_utc(end)

    try:
        df = storage.load_ohlcv(
            symbol=symbol,
            timeframe=timeframe,
            start=start_dt,
            end=end_dt,
        )
    except Exception as exc:
        raise DataReadError(
            f"Iceberg read failed | {symbol}/{timeframe} exchange={exchange or _DEFAULT_EXCHANGE} | {exc}"
        ) from exc

    if df is None:
        raise DataNotFoundError(
            f"No data | {symbol}/{timeframe} exchange={exchange or _DEFAULT_EXCHANGE} start={start} end={end}"
        )

    if df.is_empty():
        raise DataNotFoundError(
            f"No data | {symbol}/{timeframe} exchange={exchange or _DEFAULT_EXCHANGE} start={start} end={end}"
        )

    if columns:
        available = [c for c in columns if c in df.columns]
        df = df.select(available)

    logger.info(
        "Research OHLCV loaded | symbol={} timeframe={} exchange={} start={} end={} rows={}",
        symbol,
        timeframe,
        exchange or _DEFAULT_EXCHANGE,
        start,
        end,
        len(df),
    )
    return df


def get_multiple_ohlcv(
    symbols: List[str],
    timeframe: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    exchange: Optional[str] = None,
    market_type: Optional[str] = None,
) -> Dict[str, pl.DataFrame]:
    results: Dict[str, pl.DataFrame] = {}
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
                symbol,
                timeframe,
                exchange or _DEFAULT_EXCHANGE,
            )
        except Exception as exc:
            logger.warning(
                "Research: load failed | symbol={} timeframe={} error={}",
                symbol,
                timeframe,
                exc,
            )
    return results


def get_ohlcv_dict(
    symbols: List[str],
    timeframe: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    exchange: Optional[str] = None,
    market_type: Optional[str] = None,
) -> Dict[str, pl.DataFrame]:
    """Alias de get_multiple_ohlcv — compatibilidad con código existente."""
    return get_multiple_ohlcv(
        symbols=symbols,
        timeframe=timeframe,
        start=start,
        end=end,
        exchange=exchange,
        market_type=market_type,
    )


_gold_cache: Dict[str, FeatureReaderPort] = {}


def _get_gold_loader(exchange: Optional[str] = None) -> FeatureReaderPort:
    key = (exchange or _DEFAULT_EXCHANGE).lower()
    if key not in _gold_cache:
        _gold_cache[key] = build_feature_reader(exchange=key)
        logger.debug("GoldReader initialized | exchange={}", key)
    return _gold_cache[key]


def _reset_gold_loader(exchange: Optional[str] = None) -> None:
    if exchange is None:
        _gold_cache.clear()
    else:
        _gold_cache.pop((exchange or "").lower(), None)


def get_features(
    symbol: str,
    timeframe: str,
    exchange: Optional[str] = None,
    market_type: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    version: str = "latest",
) -> pl.DataFrame:
    loader = _get_gold_loader(exchange)
    mkt = (market_type or _DEFAULT_MARKET_TYPE).lower()
    exc = (exchange or _DEFAULT_EXCHANGE).lower()

    df = loader.load_features(
        exchange=exc,
        symbol=symbol,
        market_type=mkt,
        timeframe=timeframe,
        version=version,
    )

    start_dt = _parse_utc(start)
    end_dt = _parse_utc(end)
    if start_dt is not None:
        df = df.filter(pl.col("timestamp") >= start_dt)
    if end_dt is not None:
        df = df.filter(pl.col("timestamp") <= end_dt)

    logger.info(
        "Research features loaded | symbol={} timeframe={} exchange={} rows={}",
        symbol,
        timeframe,
        exc,
        len(df),
    )
    return df


def get_features_dict(
    symbols: List[str],
    timeframe: str,
    exchange: Optional[str] = None,
    market_type: Optional[str] = None,
    version: str = "latest",
) -> Dict[str, pl.DataFrame]:
    results: Dict[str, pl.DataFrame] = {}
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
                symbol,
                timeframe,
                exc,
            )
    return results
