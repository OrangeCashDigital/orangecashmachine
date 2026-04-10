# -*- coding: utf-8 -*-
"""
data_platform/platform.py
==========================

DataPlatform — facade de acceso unificado al Data Lakehouse OCM.

Punto de entrada único para research y backtesting. Oculta la topología
interna (IcebergStorage / GoldLoader / caches) detrás de una API de dominio.

Uso
---
    from data_platform.platform import DataPlatform

    data = DataPlatform(exchange="bybit", market_type="spot")

    # Silver — OHLCV crudo
    df = data.ohlcv("BTC/USDT", "1h")
    df = data.ohlcv("BTC/USDT", "1h", start="2026-01-01")

    # Gold — features técnicos
    df = data.features("BTC/USDT", "1h")
    df = data.features("BTC/USDT", "1h", as_of="2026-03-17T22:40:00Z")

    # Introspección
    data.list_datasets()
    data.list_versions("BTC/USDT", "1h")
    data.get_manifest("BTC/USDT", "1h")

Principios
----------
• SSOT  — exchange/market_type se fijan una vez en __init__, no en cada llamada.
• KISS  — API mínima orientada a consumidores (research, backtesting).
• SRP   — DataPlatform coordina, no almacena ni computa.
• Lazy  — IcebergStorage y GoldLoader se crean al primer uso.
"""
from __future__ import annotations

import os
from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

from data_platform.ohlcv_utils import DataNotFoundError, DataReadError
from data_platform.protocols import FeatureStorageProtocol, OHLCVStorageProtocol

_DEFAULT_EXCHANGE:    str = os.environ.get("OCM_EXCHANGE",     "kucoin")
_DEFAULT_MARKET_TYPE: str = os.environ.get("OCM_MARKET_TYPE",  "spot")


class DataPlatform:
    """
    Facade de acceso al Data Lakehouse OCM.

    Parameters
    ----------
    exchange    : exchange por defecto para todas las operaciones.
                  Si None usa OCM_EXCHANGE o "kucoin".
    market_type : "spot" | "swap". Si None usa OCM_MARKET_TYPE o "spot".

    Ejemplo
    -------
    data = DataPlatform(exchange="bybit", market_type="spot")
    df   = data.ohlcv("BTC/USDT", "1h", start="2026-01-01")
    """

    def __init__(
        self,
        exchange:    Optional[str] = None,
        market_type: Optional[str] = None,
    ) -> None:
        self._exchange    = (exchange    or _DEFAULT_EXCHANGE).lower()
        self._market_type = (market_type or _DEFAULT_MARKET_TYPE).lower()

        # Lazy — se crean al primer uso
        self._storage: Optional[OHLCVStorageProtocol]  = None
        self._loader:  Optional[FeatureStorageProtocol] = None

        logger.info(
            "DataPlatform initialized | exchange={} market_type={}",
            self._exchange, self._market_type,
        )

    # ── Lazy accessors ────────────────────────────────────────────────────────

    def _get_storage(self) -> OHLCVStorageProtocol:
        if self._storage is None:
            from market_data.storage.iceberg.iceberg_storage import IcebergStorage
            self._storage = IcebergStorage(
                exchange    = self._exchange,
                market_type = self._market_type,
            )
        return self._storage

    def _get_loader(self) -> FeatureStorageProtocol:
        if self._loader is None:
            from data_platform.loaders.gold_loader import GoldLoader
            self._loader = GoldLoader(exchange=self._exchange)
        return self._loader

    # ── Public API — Silver ───────────────────────────────────────────────────

    def ohlcv(
        self,
        symbol:    str,
        timeframe: str,
        start:     Optional[str] = None,
        end:       Optional[str] = None,
        columns:   Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Carga OHLCV desde Silver (Iceberg).

        Parameters
        ----------
        symbol    : e.g. "BTC/USDT"
        timeframe : e.g. "1h", "4h", "1d"
        start/end : ISO 8601 (opcionales)
        columns   : subconjunto para optimizar memoria

        Raises
        ------
        DataNotFoundError : sin datos para este símbolo/timeframe
        DataReadError     : error al leer desde Iceberg
        """
        start_ts = pd.Timestamp(start, tz="UTC") if start else None
        end_ts   = pd.Timestamp(end,   tz="UTC") if end   else None

        try:
            df = self._get_storage().load_ohlcv(
                symbol=symbol, timeframe=timeframe,
                start=start_ts, end=end_ts,
            )
        except Exception as exc:
            raise DataReadError(
                f"Silver read failed | {self._exchange}/{symbol}/{timeframe} | {exc}"
            ) from exc

        if df is None or df.empty:
            raise DataNotFoundError(
                f"No Silver data | {self._exchange}/{symbol}/{timeframe} "
                f"start={start} end={end}"
            )

        if columns:
            df = df[[c for c in columns if c in df.columns]]

        return df

    # ── Public API — Gold ─────────────────────────────────────────────────────

    def features(
        self,
        symbol:    str,
        timeframe: str,
        start:     Optional[str] = None,
        end:       Optional[str] = None,
        version:   str           = "latest",
        as_of:     Optional[str] = None,
        columns:   Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Carga features Gold (OHLCV + indicadores técnicos).

        Parameters
        ----------
        symbol/timeframe : identidad del dataset
        start/end        : filtro post-carga (no pushdown — Gold es compacto)
        version          : "latest" o snapshot_id entero
        as_of            : ISO 8601 — snapshot vigente en ese momento
        columns          : subconjunto de columnas

        Raises
        ------
        DataNotFoundError, DataReadError, VersionNotFoundError
        """
        df = self._get_loader().load_features(
            symbol      = symbol,
            market_type = self._market_type,
            timeframe   = timeframe,
            version     = version,
            as_of       = as_of,
            columns     = columns,
            exchange    = self._exchange,
        )

        if start:
            df = df[df["timestamp"] >= pd.Timestamp(start, tz="UTC")]
        if end:
            df = df[df["timestamp"] <= pd.Timestamp(end, tz="UTC")]

        return df

    # ── Introspección ─────────────────────────────────────────────────────────

    def list_datasets(self) -> List[Dict]:
        """
        Lista los datasets Gold disponibles para este exchange/market_type.

        Escanea los snapshots de gold.features y devuelve los datasets
        únicos (exchange/symbol/market_type/timeframe) con properties
        ocm.* — solo datasets construidos con GoldStorage.build()
        post-versioning feature.

        Returns
        -------
        List de dicts con keys: exchange, symbol, market_type, timeframe.
        """
        from market_data.storage.iceberg.catalog import get_catalog
        try:
            table   = get_catalog().load_table("gold.features")
            seen:   set = set()
            result: List[Dict] = []
            for entry in table.history():
                snap  = table.snapshot_by_id(entry.snapshot_id)
                if snap is None:
                    continue
                props = getattr(snap.summary, "additional_properties", {}) or {}
                exch  = props.get("ocm.exchange")
                sym   = props.get("ocm.symbol")
                mkt   = props.get("ocm.market_type")
                tf    = props.get("ocm.timeframe")
                if not all([exch, sym, mkt, tf]):
                    continue
                # Filtrar por exchange/market_type del DataPlatform
                if exch != self._exchange or mkt != self._market_type:
                    continue
                key = (exch, sym, mkt, tf)
                if key not in seen:
                    seen.add(key)
                    result.append({
                        "exchange":    exch,
                        "symbol":      sym,
                        "market_type": mkt,
                        "timeframe":   tf,
                    })
            return sorted(result, key=lambda d: (d["symbol"], d["timeframe"]))
        except Exception as exc:
            logger.warning("DataPlatform.list_datasets failed: {}", exc)
            return []

    def list_versions(
        self,
        symbol:    str,
        timeframe: str,
    ) -> List[int]:
        """
        Snapshot_ids que construyeron este dataset, en orden cronológico.

        Solo incluye snapshots con ocm.* properties — post versioning feature.
        """
        return self._get_loader().list_versions(
            exchange    = self._exchange,
            symbol      = symbol,
            market_type = self._market_type,
            timeframe   = timeframe,
        )

    def get_manifest(
        self,
        symbol:    str,
        timeframe: str,
        version:   str           = "latest",
        as_of:     Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Metadata del snapshot resuelto: identidad, lineage y estadísticas.
        """
        return self._get_loader().get_manifest(
            exchange    = self._exchange,
            symbol      = symbol,
            market_type = self._market_type,
            timeframe   = timeframe,
        )

    def get_manifest(
        self,
        symbol:    str,
        timeframe: str,
        version:   str           = "latest",
        as_of:     Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Metadata del snapshot resuelto: identidad, lineage y estadísticas.
        """
        return self._get_loader().get_manifest(
            exchange    = self._exchange,
            symbol      = symbol,
            market_type = self._market_type,
            timeframe   = timeframe,
            version     = version,
            as_of       = as_of,
        )

    def __repr__(self) -> str:
        return (
            f"DataPlatform(exchange={self._exchange!r}, "
            f"market_type={self._market_type!r})"
        )


__all__ = ["DataPlatform"]
