# -*- coding: utf-8 -*-
"""
market_data/ports/storage.py
=============================

Puerto estructural para el backend de storage OHLCV.

Responsabilidad
---------------
Declarar el contrato que cualquier implementación de storage debe cumplir.
El dominio y la capa de aplicación dependen de esta abstracción — nunca
de IcebergStorage ni de ningún backend concreto (DIP — SOLID).

Implementación única activa
---------------------------
market_data.adapters.outbound.storage.iceberg.IcebergStorage

Arquitectura medallion
----------------------
Bronze → Iceberg (append-only, sin dedup)
Silver → Iceberg (source of truth, OHLCVStorage)
Gold   → Iceberg (features calculadas)

Notas de diseño
---------------
commit_version es no-op en Iceberg — versiona en cada write automáticamente.
Existe en el contrato por compatibilidad semántica con el ciclo de vida
backfill → commit → verify.

find_partition_files NO forma parte del contrato — es un artefacto del
backend filesystem anterior. Iceberg no expone archivos físicos de partición.
Llamadas existentes reciben [] sin error.

Principios aplicados
--------------------
DIP  — consumidores importan desde ports/, nunca desde adapters/
OCP  — agregar un nuevo backend no modifica este contrato
SSOT — definición única del contrato OHLCVStorage
"""
from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class OHLCVStorage(Protocol):
    """
    Interfaz mínima que todo backend de storage OHLCV debe satisfacer.

    Contrato semántico
    ------------------
    save_ohlcv          — append idempotente: re-escritura del mismo rango
                          no duplica filas (dedup por timestamp + identidad).
    get_last_timestamp  — None si no hay datos (primer backfill del par).
    get_oldest_timestamp— timestamp más antiguo; usado para backfill boundary.
    load_ohlcv          — scan con pushdown de filtros temporales opcionales.
    commit_version      — no-op en Iceberg; existe por compatibilidad semántica.

    SafeOps
    -------
    Todas las implementaciones deben ser fail-soft en lecturas:
    retornar None / DataFrame vacío en lugar de lanzar excepción cuando
    los datos no existen o el backend no está disponible.
    Las escrituras pueden lanzar — son operaciones críticas.
    """

    def save_ohlcv(
        self,
        df:              pd.DataFrame,
        symbol:          str,
        timeframe:       str,
        mode:            str           = "append",
        run_id:          Optional[str] = None,
        skip_versioning: bool          = False,
    ) -> None:
        """
        Persiste un DataFrame OHLCV.

        Parameters
        ----------
        df        : DataFrame con columnas [timestamp, open, high, low, close, volume].
        symbol    : Par de trading normalizado, e.g. "BTC/USDT".
        timeframe : Intervalo canónico, e.g. "1m", "1h".
        mode      : "append" (default) — Iceberg no soporta overwrite.
        run_id    : Correlación con Bronze para trazabilidad. Generado si None.
        """
        ...

    def get_last_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        """
        Retorna el timestamp más reciente disponible para el par.

        Returns
        -------
        pd.Timestamp (tz=UTC) si hay datos, None en caso contrario.
        """
        ...

    def get_oldest_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        """
        Retorna el timestamp más antiguo disponible para el par.

        Usado por BackfillStrategy para detectar el límite histórico
        ya descargado y evitar re-ingesta masiva accidental.

        Returns
        -------
        pd.Timestamp (tz=UTC) si hay datos, None en caso contrario.
        """
        ...

    def commit_version(
        self,
        symbol:    str,
        timeframe: str,
        run_id:    Optional[str] = None,
    ) -> None:
        """
        No-op en Iceberg — versiona automáticamente en cada snapshot.

        Existe en el contrato por compatibilidad semántica con el ciclo
        backfill → commit → verify. Las implementaciones deben aceptarlo
        sin error aunque no hagan nada.
        """
        ...

    def load_ohlcv(
        self,
        symbol:    str,
        timeframe: str,
        start:     Optional[pd.Timestamp] = None,
        end:       Optional[pd.Timestamp] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Lee datos OHLCV con filtros temporales opcionales.

        Implementaciones deben aplicar partition pruning cuando sea posible.
        Retorna None (no DataFrame vacío) si no hay datos en el rango.

        Parameters
        ----------
        symbol    : Par de trading.
        timeframe : Intervalo temporal.
        start     : Límite inferior inclusivo (UTC). None = sin límite.
        end       : Límite superior inclusivo (UTC). None = sin límite.

        Returns
        -------
        pd.DataFrame ordenado por timestamp con dedup aplicado, o None.
        """
        ...
