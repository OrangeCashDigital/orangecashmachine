# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/feature_reader.py
=============================================

Puerto estructural para lectura de features Gold.

Responsabilidad
---------------
Declarar el contrato que cualquier implementación de lectura Gold debe
cumplir. data_platform y research dependen de esta abstracción — nunca
de GoldLoader ni de ningún acceso directo al catalog Iceberg (DIP).

Implementación activa
---------------------
market_data.adapters.outbound.storage.gold_reader.GoldReader

Principios
----------
DIP  — consumidores importan desde ports/, nunca desde adapters/
OCP  — agregar backend alternativo no modifica este contrato
SSOT — definición única del contrato FeatureReaderPort
SRP  — solo lectura; escritura Gold es responsabilidad de GoldStorage
"""
from __future__ import annotations

from typing import Dict, List, Optional, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class FeatureReaderPort(Protocol):
    """
    Contrato de lectura de features Gold.

    Contrato semántico
    ------------------
    load_features   — carga DataFrame Gold con filtros opcionales de versión.
    list_versions   — snapshot_ids que construyeron un dataset específico.
    list_datasets   — datasets disponibles para un exchange/market_type.
    get_manifest    — metadata del snapshot resuelto (lineage + estadísticas).

    SafeOps
    -------
    list_versions y list_datasets retornan [] ante cualquier error.
    get_manifest retorna None si el snapshot no existe.
    load_features lanza DataNotFoundError / DataReadError — nunca silencia.
    """

    def load_features(
        self,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        version:     str                 = "latest",
        as_of:       Optional[str]       = None,
        columns:     Optional[List[str]] = None,
        exchange:    Optional[str]       = None,
    ) -> pd.DataFrame:
        """
        Carga features Gold para un símbolo/timeframe.

        Parameters
        ----------
        symbol      : e.g. "BTC/USDT"
        market_type : "spot" | "swap"
        timeframe   : e.g. "1h", "4h", "1d"
        version     : "latest" o snapshot_id entero como string/int
        as_of       : ISO 8601 — snapshot vigente en ese instante
        columns     : subconjunto de columnas (optimiza memoria)
        exchange    : override del exchange del reader

        Returns
        -------
        pd.DataFrame ordenado por timestamp.

        Raises
        ------
        DataNotFoundError    : sin datos para este símbolo/timeframe
        DataReadError        : error al leer desde el backend
        VersionNotFoundError : snapshot_id no existe o as_of sin candidatos
        """
        ...

    def list_versions(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
    ) -> List[int]:
        """
        Retorna snapshot_ids que construyeron este dataset, cronológico.

        Solo incluye snapshots con ocm.* properties (post versioning feature).
        Retorna [] si no hay datos o ante cualquier error (SafeOps).
        """
        ...

    def list_datasets(
        self,
        exchange:    str,
        market_type: str,
    ) -> List[Dict]:
        """
        Lista datasets Gold disponibles para exchange/market_type.

        Returns
        -------
        List de dicts con keys: exchange, symbol, market_type, timeframe.
        Retorna [] ante cualquier error (SafeOps).
        """
        ...

    def get_manifest(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        version:     str           = "latest",
        as_of:       Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Metadata del snapshot resuelto: identidad, lineage y estadísticas.

        Returns
        -------
        Dict con keys: snapshot_id, timestamp_ms, exchange, symbol,
        market_type, timeframe, run_id, added_records, total_records.
        None si el snapshot no existe (SafeOps).
        """
        ...


__all__ = ["FeatureReaderPort"]
