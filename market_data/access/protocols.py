# -*- coding: utf-8 -*-
"""
data_platform/protocols.py
===========================

Contratos estructurales (Protocol) para la capa de acceso a datos.

Implementaciones
----------------
  OHLCVStorage   → IcebergStorage  (market_data/storage/iceberg/)
  FeatureStorage → GoldLoader      (data_platform/loaders/gold_loader.py)

Principios
----------
• runtime_checkable — permite isinstance() en tests sin dependencia concreta.
• Firmas mínimas — solo lo que el consumidor (research, backtesting) necesita.
• Sin imports de implementaciones — este módulo no conoce IcebergStorage
  ni GoldLoader. Solo define el contrato.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class OHLCVStorageProtocol(Protocol):
    """
    Contrato de lectura/escritura OHLCV (Silver layer).

    Implementación única: IcebergStorage.
    Definido aquí para que research/backtesting puedan tipar sus
    dependencias sin importar la implementación concreta.
    """

    def load_ohlcv(
        self,
        symbol:    str,
        timeframe: str,
        start:     Optional[pd.Timestamp] = None,
        end:       Optional[pd.Timestamp] = None,
    ) -> Optional[pd.DataFrame]: ...

    def get_last_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]: ...

    def get_oldest_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]: ...


@runtime_checkable
class FeatureStorageProtocol(Protocol):
    """
    Contrato de lectura de features Gold (Gold layer).

    Implementación única: GoldLoader.

    Versionado
    ----------
    list_versions() devuelve solo los snapshots que construyeron
    el dataset especificado (exchange/symbol/market_type/timeframe),
    NO todos los snapshots globales de la tabla.

    get_manifest() devuelve metadata del snapshot resuelto incluyendo
    las ocm.* properties inyectadas en build() y estadísticas Iceberg.
    """

    def load_features(
        self,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        version:     str                = "latest",
        as_of:       Optional[str]      = None,
        columns:     Optional[List[str]] = None,
        exchange:    Optional[str]      = None,
    ) -> pd.DataFrame: ...

    def list_versions(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
    ) -> List[int]: ...

    def get_manifest(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        version:     str           = "latest",
        as_of:       Optional[str] = None,
    ) -> Optional[Dict]: ...


__all__ = [
    "OHLCVStorageProtocol",
    "FeatureStorageProtocol",
]
