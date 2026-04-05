# -*- coding: utf-8 -*-
"""
market_data/storage/storage_protocol.py
========================================

Contrato estructural para backends de storage OHLCV.

Permite intercambiar SilverStorage e IcebergStorage sin modificar
el pipeline — basta con que ambos implementen esta interfaz
(@runtime_checkable Protocol — duck typing estructural).

Implementaciones actuales
--------------------------
- SilverStorage  : parquet particionado en filesystem local
- IcebergStorage : Apache Iceberg sobre SQLite catalog

Notas de diseño
---------------
skip_versioning es un detalle de implementación de SilverStorage
usado durante backfills masivos para evitar generar miles de archivos
de versión. IcebergStorage lo acepta como no-op. No forma parte del
contrato público porque:

  1. No tiene significado semántico en todos los backends.
  2. El Protocol debe expresar el mínimo común denominador.
  3. SilverStorage lo mantiene como kwarg extra con default=False,
     compatible con duck typing sin romper el Protocol.

commit_version es no-op en Iceberg (versiona en cada write).
Existe en el contrato para que el pipeline pueda llamarlo
incondicionalmente al final de un backfill masivo sin conocer
el backend concreto.
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
    - save_ohlcv         : idempotente — re-escritura del mismo rango
                           no duplica filas.
    - get_last_timestamp : retorna None si no hay datos (primer backfill).
    - find_partition_files: puede retornar [] en backends sin archivos
                           físicos (Iceberg).
    - commit_version     : no-op en backends con versionado automático.
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
