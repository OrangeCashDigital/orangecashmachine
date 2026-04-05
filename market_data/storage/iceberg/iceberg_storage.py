"""
market_data/storage/iceberg/iceberg_storage.py
===============================================

Capa Silver — único backend de storage OHLCV.

Tabla: silver.ohlcv (Apache Iceberg sobre SQLite catalog)
Particionado por: exchange / market_type / symbol / timeframe / ts_month

Interfaz pública (OHLCVStorage Protocol)
-----------------------------------------
  save_ohlcv()          — append transaccional con snapshot consistency
  get_last_timestamp()  — scan con partition pruning, sin abrir archivos
  get_oldest_timestamp()— simétrico, para backfill boundary detection
  load_ohlcv()          — scan con pushdown de filtros temporales
  commit_version()      — no-op (Iceberg versiona por snapshot)

Uso
---
  storage = IcebergStorage(exchange="bybit", market_type="spot")
  fetcher = HistoricalFetcherAsync(exchange_client=..., storage=storage)

Notas de implementación
-----------------------
• row_filter usa pyiceberg.expressions (EqualTo, And, etc.) — NO pc.field().
  pc.field() es PyArrow compute — sistemas de expresiones incompatibles.
• pc (pyarrow.compute) se usa SOLO post-scan: pc.max(), pc.min().
• Timestamps normalizados a microsegundos (us) — pyiceberg 0.8 no soporta ns.
• pd.Timestamp(max_ts, tz=...) falla si el objeto ya tiene tzinfo —
  usar tz_localize solo si viene sin tz.
"""

from __future__ import annotations

import time
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThanOrEqual,
    LessThanOrEqual,
)

from core.config.paths import data_lake_root


# =============================================================================
# Catálogo — singleton por proceso
# =============================================================================

_CATALOG: Optional[SqlCatalog] = None

# Columnas OHLCV en el orden del schema Iceberg
_OHLCV_COLS = [
    "timestamp", "open", "high", "low", "close", "volume",
    "exchange", "market_type", "symbol", "timeframe",
]


def _get_catalog() -> SqlCatalog:
    """
    Devuelve el catálogo Iceberg, creándolo una sola vez por proceso.

    Paths resueltos desde data_lake_root() para garantizar
    consistencia con el resto del data platform, independientemente
    del cwd desde donde se lance el proceso.
    """
    global _CATALOG
    if _CATALOG is None:
        base      = data_lake_root().parent  # .../data_platform/
        cat_path  = base / "iceberg_catalog"
        ware_path = base / "iceberg_warehouse"
        _CATALOG  = SqlCatalog(
            "ocm",
            **{
                "uri":       f"sqlite:///{cat_path}/catalog.db",
                "warehouse": str(ware_path.absolute()),
            },
        )
    return _CATALOG


def _to_utc_timestamp(dt: object) -> Optional[pd.Timestamp]:
    """
    Convierte el resultado de pc.max() a pd.Timestamp UTC.

    pc.max() sobre columnas tz-aware devuelve datetime con tzinfo ya
    incluido. pd.Timestamp(obj, tz=...) falla en ese caso — hay que
    usar tz_convert() o simplemente wrappear sin tz y normalizar.
    """
    if dt is None:
        return None
    ts = pd.Timestamp(dt)
    return ts if ts.tzinfo is not None else ts.tz_localize("UTC")


# =============================================================================
# IcebergStorage
# =============================================================================

class IcebergStorage:
    """
    Capa Silver sobre Apache Iceberg.

    Implementación única del contrato OHLCVStorage.
    """

    def __init__(
        self,
        exchange:    Optional[str] = None,
        market_type: Optional[str] = None,
        dry_run:     bool          = False,
    ) -> None:
        self._exchange    = exchange
        self._market_type = market_type
        self._dry_run     = dry_run
        self._table       = _get_catalog().load_table("silver.ohlcv")

    # =========================================================================
    # Helpers internos
    # =========================================================================

    def _base_filter(self, symbol: str, timeframe: str):
        """
        Filtro Iceberg nativo para las cuatro columnas de identidad.

        IMPORTANTE: usa pyiceberg.expressions.EqualTo/And, NO pc.field().
        pc.field() es PyArrow compute y lanza "Cannot visit unsupported
        expression" cuando se pasa a scan(). Son sistemas distintos.
        """
        exchange    = self._exchange    or "unknown"
        market_type = self._market_type or "unknown"
        return And(
            And(
                EqualTo("exchange",    exchange),
                EqualTo("symbol",      symbol),
            ),
            And(
                EqualTo("timeframe",   timeframe),
                EqualTo("market_type", market_type),
            ),
        )

    @staticmethod
    def _normalize_df(
        df:          pd.DataFrame,
        symbol:      str,
        timeframe:   str,
        exchange:    str,
        market_type: str,
    ) -> pd.DataFrame:
        """
        Prepara el DataFrame para escritura en Iceberg:
        - Convierte timestamp a us (pyiceberg 0.8 no soporta ns)
        - Inyecta columnas de partición
        - Deduplica y ordena
        """
        df = df.copy()
        df["timestamp"] = (
            pd.to_datetime(df["timestamp"], utc=True)
            .astype("datetime64[us, UTC]")
        )
        df["exchange"]    = exchange
        df["market_type"] = market_type
        df["symbol"]      = symbol
        df["timeframe"]   = timeframe

        return (
            df[_OHLCV_COLS]
            .drop_duplicates(subset=["timestamp", "exchange", "symbol", "timeframe"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

    # =========================================================================
    # Public API — OHLCVStorage Protocol
    # =========================================================================

    def save_ohlcv(
        self,
        df:              pd.DataFrame,
        symbol:          str,
        timeframe:       str,
        mode:            str           = "append",
        run_id:          Optional[str] = None,
        skip_versioning: bool          = False,   # no-op — Iceberg versiona solo
    ) -> None:
        """
        Guarda OHLCV en la tabla Iceberg silver.ohlcv.

        Append atómico con snapshot consistency garantizada por Iceberg.
        mode='overwrite' no está soportado en pyiceberg 0.8 — se trata
        como append con dedup y se emite un warning.
        """
        if self._dry_run:
            logger.info(
                "[DRY RUN] IcebergStorage.save_ohlcv skipped | {}/{} "
                "exchange={} rows={}",
                symbol, timeframe, self._exchange or "shared", len(df),
            )
            return

        if df is None or df.empty:
            return

        if mode == "overwrite":
            logger.warning(
                "IcebergStorage: mode=overwrite no soportado en pyiceberg 0.8 "
                "— usando append con dedup | {}/{}", symbol, timeframe,
            )

        _t0      = time.monotonic()
        prepared = self._normalize_df(
            df,
            symbol      = symbol,
            timeframe   = timeframe,
            exchange    = self._exchange    or "unknown",
            market_type = self._market_type or "unknown",
        )

        self._table.append(pa.Table.from_pandas(prepared, preserve_index=False))

        logger.debug(
            "IcebergStorage saved | {}/{} exchange={} rows={} duration={}ms",
            symbol, timeframe, self._exchange or "shared",
            len(prepared), int((time.monotonic() - _t0) * 1000),
        )

    def get_last_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        """
        Obtiene el último timestamp disponible para symbol/timeframe.

        Scan Iceberg con filtros nativos (partition pruning activo).
        Solo lee la columna timestamp — mínimo I/O.
        """
        try:
            result = (
                self._table
                .scan(
                    row_filter      = self._base_filter(symbol, timeframe),
                    selected_fields = ("timestamp",),
                )
                .to_arrow()
            )

            if result.num_rows == 0:
                return None

            return _to_utc_timestamp(pc.max(result.column("timestamp")).as_py())

        except Exception as exc:
            logger.warning(
                "IcebergStorage.get_last_timestamp failed | {}/{} error={}",
                symbol, timeframe, exc,
            )
            return None


    def get_oldest_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        """
        Obtiene el timestamp más antiguo disponible para symbol/timeframe.

        Scan Iceberg con pc.min() — simétrico a get_last_timestamp.
        """
        try:
            result = (
                self._table
                .scan(
                    row_filter      = self._base_filter(symbol, timeframe),
                    selected_fields = ("timestamp",),
                )
                .to_arrow()
            )
            if result.num_rows == 0:
                return None
            return _to_utc_timestamp(pc.min(result.column("timestamp")).as_py())
        except Exception as exc:
            logger.warning(
                "IcebergStorage.get_oldest_timestamp failed | {}/{} error={}",
                symbol, timeframe, exc,
            )
            return None
    def load_ohlcv(
        self,
        symbol:    str,
        timeframe: str,
        start:     Optional[pd.Timestamp] = None,
        end:       Optional[pd.Timestamp] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Lee datos OHLCV desde Iceberg con pushdown de filtros temporales.

        Combina filtro de identidad (exchange/symbol/timeframe/market_type)
        con rango temporal opcional. Partition pruning activo en ambos ejes.
        """
        try:
            row_filter = self._base_filter(symbol, timeframe)

            if start is not None:
                row_filter = And(
                    row_filter,
                    GreaterThanOrEqual("timestamp", start.isoformat()),
                )
            if end is not None:
                row_filter = And(
                    row_filter,
                    LessThanOrEqual("timestamp", end.isoformat()),
                )

            df = (
                self._table
                .scan(row_filter=row_filter)
                .to_arrow()
                .to_pandas()
            )

            if df.empty:
                return None

            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return (
                df.sort_values("timestamp")
                .drop_duplicates(subset=["timestamp"])
                .reset_index(drop=True)
            )

        except Exception as exc:
            logger.warning(
                "IcebergStorage.load_ohlcv failed | {}/{} error={}",
                symbol, timeframe, exc,
            )
            return None

    # =========================================================================
    # Protocol stubs — no-op en Iceberg
    # =========================================================================

    def commit_version(
        self,
        symbol:    str,
        timeframe: str,
        run_id:    Optional[str] = None,
    ) -> None:
        """No-op: Iceberg versiona automáticamente por snapshot."""
        pass

    def get_version(
        self,
        symbol:    str,
        timeframe: str,
        version:   str = "latest",
    ) -> Optional[dict]:
        """Retorna metadata del snapshot actual como proxy de versión."""
        try:
            snap = self._table.current_snapshot()
            if snap is None:
                return None
            return {
                "version_id":  str(snap.snapshot_id),
                "written_at":  str(snap.timestamp_ms),
                "symbol":      symbol,
                "timeframe":   timeframe,
                "exchange":    self._exchange,
                "market_type": self._market_type,
            }
        except Exception:
            return None

    def find_partition_files(
        self,
        symbol:    str,
        timeframe: str,
        since:     Optional[pd.Timestamp] = None,
        until:     Optional[pd.Timestamp] = None,
    ) -> list:
        """
        No-op: Iceberg no expone archivos físicos de partición.
        RepairStrategy usará scan() directamente cuando soporte Iceberg.
        Retorna [] para que RepairStrategy salte silenciosamente.
        """
        return []
