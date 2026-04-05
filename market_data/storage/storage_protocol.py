"""
market_data/storage/storage_protocol.py
========================================

Contrato estructural para backends de storage OHLCV.

Permite intercambiar SilverStorage e IcebergStorage sin modificar
el pipeline — basta con que ambos implementen esta interfaz
(@runtime_checkable Protocol — duck typing estructural).

Implementaciones actuales
--------------------------
- SilverStorage  : parquet particionado en filesystem
- IcebergStorage : Apache Iceberg sobre SQLite catalog

Uso
---
from market_data.storage.storage_protocol import OHLCVStorage
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class OHLCVStorage(Protocol):
    """
    Interfaz mínima que todo backend de storage OHLCV debe cumplir.

    Contrato semántico
    ------------------
    - save_ohlcv        : idempotente — re-escritura del mismo rango no duplica filas
    - get_last_timestamp: retorna None si no hay datos (primer backfill)
    - find_partition_files: puede retornar [] en backends sin archivos físicos
    - commit_version    : no-op en backends con versionado automático (Iceberg)
    """

    def save_ohlcv(
        self,
        df:              pd.DataFrame,
        symbol:          str,
        timeframe:       str,
        mode:            str           = "append",
        run_id:          Optional[str] = None,
        skip_versioning: bool          = False,
    ) -> None: ...

    def get_last_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]: ...

    def find_partition_files(
        self,
        symbol:    str,
        timeframe: str,
        since:     Optional[pd.Timestamp] = None,
        until:     Optional[pd.Timestamp] = None,
    ) -> List[Path]: ...

    def commit_version(
        self,
        symbol:    str,
        timeframe: str,
        run_id:    Optional[str] = None,
    ) -> None: ...
