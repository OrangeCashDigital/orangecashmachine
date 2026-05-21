# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/storage.py
=======================================

Puertos estructurales para todos los backends de storage.

DIP — Los pipelines de application dependen de estas abstracciones,
nunca de IcebergStorage, TradesStorage ni DerivativesStorage directamente.

SSOT — Cada Protocol es la única fuente de verdad del contrato de su
backend. Las implementaciones concretas deben satisfacer estructuralmente
cada Protocol (runtime_checkable: isinstance() funciona sin herencia).

ISP — Un Protocol por responsabilidad. OHLCVStorage no contamina a
TradesStoragePort; BronzeStoragePort no contamina a StorageFactoryPort.

SafeOps — Lecturas: retornar None / DataFrame vacío, nunca lanzar.
          Escrituras: pueden lanzar — son operaciones críticas.

Principios: DIP · ISP · OCP · SSOT · runtime_checkable
"""
from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable

import pandas as pd


# =========================================================================== #
# OHLCV — Silver layer                                                        #
# =========================================================================== #

@runtime_checkable
class OHLCVStorage(Protocol):
    """
    Contrato de persistencia OHLCV (Silver layer).

    Implementación canónica
    -----------------------
    market_data.adapters.outbound.storage.iceberg.IcebergStorage

    Contrato semántico
    ------------------
    save_ohlcv    — append idempotente: re-escritura del mismo rango
                    no duplica filas (dedup por timestamp + identidad).
    get_last_timestamp  — None si no hay datos (primer backfill del par).
    get_oldest_timestamp— timestamp más antiguo; backfill boundary.
    load_ohlcv    — scan con pushdown de filtros temporales opcionales.
    """

    def save_ohlcv(
        self,
        df:              pd.DataFrame,
        symbol:          str,
        timeframe:       str,
        run_id:          Optional[str] = None,
        skip_versioning: bool          = False,
    ) -> None:
        """
        Persiste un DataFrame OHLCV en modo append (Iceberg es append-only).

        Parameters
        ----------
        df        : DataFrame con columnas [timestamp, open, high, low, close, volume].
        symbol    : Par de trading normalizado, e.g. "BTC/USDT".
        timeframe : Intervalo canónico, e.g. "1m", "1h".
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
        pd.Timestamp (tz=UTC) si hay datos, None si no hay datos.
        """
        ...

    def get_oldest_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        """
        Retorna el timestamp más antiguo disponible para el par.

        Usado por BackfillStrategy para detectar el límite histórico.

        Returns
        -------
        pd.Timestamp (tz=UTC) si hay datos, None si no hay datos.
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

        Returns
        -------
        pd.DataFrame ordenado por timestamp con dedup aplicado, o None.
        """
        ...


# =========================================================================== #
# Trades — Silver layer                                                        #
# =========================================================================== #

@runtime_checkable
class TradesStoragePort(Protocol):
    """
    Contrato de persistencia de trades tick-by-tick (Silver layer).

    Implementación canónica
    -----------------------
    market_data.infrastructure.storage.silver.trades_storage.TradesStorage

    Contrato semántico
    ------------------
    append           — persiste DataFrame de trades; retorna filas escritas.
    last_timestamp_ms— último timestamp en ms; None si no hay datos.

    Nota de diseño
    --------------
    El contrato refleja la API real de TradesStorage (append / last_timestamp_ms),
    no una API imaginaria (save_trades / get_last_timestamp). SSOT: el port
    describe lo que la implementación hace, no lo que querríamos que hiciera.
    """

    def append(self, df: pd.DataFrame) -> int:
        """
        Persiste un DataFrame de trades en Silver (append-only).

        Parameters
        ----------
        df : DataFrame con columnas [trade_id, timestamp, symbol, side,
             price, amount, cost]. exchange y market_type añadidos por
             TradesStorage si no están presentes.

        Returns
        -------
        int : filas efectivamente escritas.
        """
        ...

    def last_timestamp_ms(self, symbol: str) -> int | None:
        """
        Retorna el timestamp en ms del trade más reciente para ``symbol``.

        Returns
        -------
        int si hay datos, None si la tabla está vacía o el símbolo no existe.
        """
        ...


# =========================================================================== #
# Derivatives — Silver layer                                                   #
# =========================================================================== #

@runtime_checkable
class DerivativesStoragePort(Protocol):
    """
    Contrato de persistencia de métricas de derivados (Silver layer).

    Implementación canónica
    -----------------------
    market_data.infrastructure.storage.silver.derivatives_storage.DerivativesStorage

    Contrato semántico
    ------------------
    upsert           — persiste/actualiza snapshot; retorna filas escritas.
    last_timestamp_ms— último timestamp en ms; None si no hay datos.

    Nota de diseño
    --------------
    upsert (no append) porque derivados son snapshots: el mismo timestamp
    puede llegar con datos actualizados. TradesStorage usa append porque
    los trades son inmutables.
    """

    def upsert(self, df: pd.DataFrame) -> int:
        """
        Persiste o actualiza un snapshot de derivados.

        Returns
        -------
        int : filas escritas o actualizadas.
        """
        ...

    def last_timestamp_ms(self, symbol: str) -> int | None:
        """
        Retorna el timestamp en ms del snapshot más reciente para ``symbol``.

        Returns
        -------
        int si hay datos, None si no hay datos.
        """
        ...


# =========================================================================== #
# Bronze layer                                                                 #
# =========================================================================== #

@runtime_checkable
class BronzeStoragePort(Protocol):
    """
    Contrato de persistencia de candles crudas (Bronze layer).

    Implementación canónica
    -----------------------
    market_data.infrastructure.storage.bronze.BronzeStorage
    """

    def append(
        self,
        df:        pd.DataFrame,
        symbol:    str,
        timeframe: str,
        run_id:    Optional[str] = None,
    ) -> None:
        """
        Persiste un batch de candles crudas en Bronze (append-only).

        Parameters
        ----------
        df        : DataFrame con columnas OHLCV.
        symbol    : Par de trading normalizado.
        timeframe : Intervalo canónico.
        run_id    : Correlación con EventPayload para trazabilidad.
        """
        ...

    def get_last_timestamp(self, symbol: str, timeframe: str) -> int | None:
        """Timestamp en ms del último registro. None si no hay datos."""
        ...


# =========================================================================== #
# Storage factory                                                              #
# =========================================================================== #

@runtime_checkable
class StorageFactoryPort(Protocol):
    """
    Contrato de fábrica de instancias OHLCVStorage por (exchange, market_type).

    Implementación canónica
    -----------------------
    market_data.adapters.outbound.storage.iceberg_factory.IcebergStorageFactory

    Principios: DIP · OCP · SRP · SSOT
    """

    def get_storage(
        self,
        exchange:    str,
        market_type: str  = "spot",
        dry_run:     bool = False,
    ) -> OHLCVStorage:
        """
        Retorna instancia de OHLCVStorage para (exchange, market_type).

        Implementaciones deben cachear instancias — IcebergStorage es stateful.
        Fail-Fast: lanza RuntimeError si el backend no puede inicializarse.
        """
        ...


# =========================================================================== #
# Snapshottable — extensión ISP para lineage tracking                         #
# =========================================================================== #

class SnapshottableStoragePort(OHLCVStorage, Protocol):
    """
    OHLCVStorage con capacidad de snapshot Iceberg para lineage tracking.

    ISP — get_current_snapshot() es específica de Iceberg. No pertenece al
    contrato base OHLCVStorage: no todos los backends soportan snapshots.
    Este Protocol extiende el base solo para callers que necesitan lineage.

    Usado por : GoldStorage.build() para anclar lineage de Silver.
    """

    def get_current_snapshot(self) -> Optional[dict]:
        """
        Retorna metadatos del snapshot actual.

        Returns
        -------
        dict : {"snapshot_id": int, "timestamp_ms": int}
        None : si no hay datos aún (primer ingesta).
        """
        ...
