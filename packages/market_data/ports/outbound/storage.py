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
commit_version eliminado del contrato — Iceberg versiona automáticamente.
Un método no-op en el Protocol engaña a los callers (contrato deshonesto).
Implementaciones que reciban llamadas legacy deben aceptarlas silenciosamente
pero no es obligación del contrato declararlo.

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

    Nota: commit_version eliminado del contrato — Iceberg versiona
    automáticamente en cada snapshot. Un método no-op en el Protocol
    es ruido que engaña a los callers sobre el comportamiento real.

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
        run_id:          Optional[str] = None,
        skip_versioning: bool          = False,
    ) -> None:
        """
        Persiste un DataFrame OHLCV en modo append (Iceberg es append-only).

        Fix K4: eliminado parámetro `mode` — era un contrato falso.
        Iceberg no soporta overwrite; un caller que pasara mode="overwrite"
        esperaría un comportamiento que nunca ocurriría. El contrato debe
        ser honesto: esta operación siempre es append.

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


# ---------------------------------------------------------------------------
# Trades storage port
# ---------------------------------------------------------------------------

class TradesStoragePort(Protocol):
    """Contrato de persistencia de trades. Implementación: TradesStorage (infrastructure)."""

    def save_trades(self, symbol: str, trades: object) -> None: ...
    def get_last_timestamp(self, symbol: str) -> int | None: ...


# ---------------------------------------------------------------------------
# Derivatives storage port
# ---------------------------------------------------------------------------

class DerivativesStoragePort(Protocol):
    """Contrato de persistencia de derivados. Implementación: DerivativesStorage."""

    def save(self, metric_type: str, symbol: str, data: object) -> None: ...
    def get_last_timestamp(self, metric_type: str, symbol: str) -> int | None: ...


# ---------------------------------------------------------------------------
# Bronze storage port
# ---------------------------------------------------------------------------

class BronzeStoragePort(Protocol):
    """
    Contrato de persistencia de candles crudas (capa Bronze).
    Implementación: BronzeStorage (infrastructure.storage.bronze).

    Fix K3: renombrado save_raw → append para alinear con la implementación.
    KafkaBronzeWriter llamaba self._bronze.append() pero el puerto definía
    save_raw() — el contrato estructural no se cumplía (isinstance() pasaba
    pero el método no existía en el Protocol).
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
        run_id    : Correlación con el EventPayload para trazabilidad.
        """
        ...

    def get_last_timestamp(self, symbol: str, timeframe: str) -> int | None:
        """Timestamp en ms del último registro. None si no hay datos."""
        ...


# ---------------------------------------------------------------------------
# Storage factory port
# ---------------------------------------------------------------------------

class StorageFactoryPort(Protocol):
    """
    Contrato de fábrica de instancias OHLCVStorage por (exchange, market_type).

    Responsabilidad
    ---------------
    Abstraer la creación y cache de backends de storage.
    Los pipelines no instancian IcebergStorage directamente — reciben
    una fábrica inyectada desde el composition root (DIP).

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
        Retorna una instancia de OHLCVStorage para el par (exchange, market_type).

        Implementaciones deben cachear instancias — IcebergStorage es stateful
        respecto al catalog y no debe reinstanciarse en cada llamada.

        Fail-Fast: lanza RuntimeError si el backend no puede inicializarse.
        """
        ...



# ---------------------------------------------------------------------------
# Snapshottable storage port (ISP — no contaminar OHLCVStorage base)
# ---------------------------------------------------------------------------

class SnapshottableStoragePort(OHLCVStorage, Protocol):
    """
    OHLCVStorage con capacidad de snapshot Iceberg para lineage tracking.

    Principio ISP
    -------------
    get_current_snapshot() es específica de Iceberg y NO pertenece al
    contrato base OHLCVStorage — no todos los backends de storage soportan
    snapshots. Este Protocol extiende el base solo para callers que
    necesitan lineage (GoldStorage.build()).

    Implementado por : IcebergStorage (structurally — sin herencia explícita)
    Usado por        : GoldStorage.build() para anclar el lineage de Silver

    Principios: ISP · DIP · SRP · SSOT
    """

    def get_current_snapshot(self) -> Optional[dict]:
        """
        Retorna metadatos del snapshot actual del backend.

        Returns
        -------
        dict con campos:
            snapshot_id  (int) — ID del snapshot Iceberg
            timestamp_ms (int) — timestamp epoch ms del snapshot
        None si el backend no tiene datos aún (primer ingesta).

        SafeOps: retorna None en lugar de lanzar si no hay snapshots.
        """
        ...
