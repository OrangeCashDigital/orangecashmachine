# -*- coding: utf-8 -*-
"""
market_data/storage/storage_protocol.py
========================================

Contrato estructural para el backend de storage OHLCV.

Implementación única
--------------------
- IcebergStorage : Apache Iceberg sobre SQLite catalog (único backend)

Arquitectura: Bronze → Parquet (archivos físicos) | Silver → Iceberg | Gold → Iceberg

Notas de diseño
---------------
commit_version es no-op en Iceberg (versiona en cada write automáticamente).
Existe en el contrato por compatibilidad con llamadas existentes en backfill.

find_partition_files NO forma parte del contrato — es un artefacto de
No tiene equivalente en Iceberg. Llamadas existentes reciben [].
"""
from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class OHLCVStorage(Protocol):
    """
    Interfaz mínima que IcebergStorage debe cumplir.

    Contrato semántico
    ------------------
    - save_ohlcv         : idempotente — re-escritura del mismo rango
                           no duplica filas.
    - get_last_timestamp : retorna None si no hay datos (primer backfill).
    - commit_version     : no-op — Iceberg versiona automáticamente.
    - load_ohlcv         : scan con pushdown de filtros temporales.
    - get_oldest_timestamp: timestamp más antiguo disponible.
    """

    def save_ohlcv(
        self,
        df:        pd.DataFrame,
        symbol:    str,
        timeframe: str,
        mode:      str           = "append",
        run_id:    Optional[str] = None,
    ) -> None: ...

    def get_last_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]: ...

    def commit_version(
        self,
        symbol:    str,
        timeframe: str,
        run_id:    Optional[str] = None,
    ) -> None: ...

    def load_ohlcv(
        self,
        symbol:    str,
        timeframe: str,
        start:     Optional[pd.Timestamp] = None,
        end:       Optional[pd.Timestamp] = None,
    ) -> Optional[pd.DataFrame]: ...

    def get_oldest_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]: ...
