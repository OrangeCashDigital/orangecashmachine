# -*- coding: utf-8 -*-
"""
market_data/storage/silver/trades_storage.py
=============================================

Storage append-only para tick data (trades) en capa Silver.

Responsabilidad única (SRP)
---------------------------
Escribir y leer trades normalizados en la tabla Iceberg
``silver.trades``.  No sabe nada de fetching, cursores ni pipelines.

Diseño
------
* Append-only    — trades son inmutables por definición de dominio.
* Idempotente    — dedup por trade_id antes de escribir (SafeOps).
* Schema-on-read — columnas fijas, sin lógica de transformación aquí.
* Particionado   — por exchange + symbol para pruning eficiente.

SafeOps
-------
* dry_run=True  → loguea sin escribir (testing / staging seguro).
* Todas las excepciones se propagan — el caller decide si reintentar.

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

import pandas as pd
from loguru import logger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiceberg.table import Table

# ---------------------------------------------------------------------------
# Schema Iceberg — definido una sola vez (DRY)
# ---------------------------------------------------------------------------
# Importamos lazy para no requerir pyiceberg en entornos sin él.
# La función _build_schema() es pura — sin efectos secundarios.
# ---------------------------------------------------------------------------

def _build_schema() -> "pyiceberg.schema.Schema":  # noqa: F821
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        NestedField, StringType, TimestamptzType, DoubleType,
    )
    return Schema(
        NestedField(1,  "trade_id",    StringType(),      required=True),
        NestedField(2,  "timestamp",   TimestamptzType(), required=True),
        NestedField(3,  "exchange",    StringType(),      required=True),
        NestedField(4,  "market_type", StringType(),      required=True),
        NestedField(5,  "symbol",      StringType(),      required=True),
        NestedField(6,  "side",        StringType(),      required=False),
        NestedField(7,  "price",       DoubleType(),      required=True),
        NestedField(8,  "amount",      DoubleType(),      required=True),
        NestedField(9,  "cost",        DoubleType(),      required=False),
    )


def _build_partition_spec() -> "pyiceberg.partitioning.PartitionSpec":  # noqa: F821
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform
    return PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name="exchange"),
        PartitionField(source_id=5, field_id=1001, transform=IdentityTransform(), name="symbol"),
    )


# ---------------------------------------------------------------------------
# TradesStorage
# ---------------------------------------------------------------------------

_TABLE_NAME = "silver.trades"


class TradesStorage:
    """
    Escribe DataFrames de trades normalizados en ``silver.trades``.

    Parameters
    ----------
    exchange    : identificador del exchange (e.g. ``"bybit"``).
    market_type : ``"spot"`` | ``"linear"`` | ``"inverse"``.
    catalog     : instancia PyIceberg RESTCatalog (inyección, testeable).
    dry_run     : si True, loguea sin escribir (SafeOps).
    """

    def __init__(
        self,
        exchange:    str,
        market_type: str,
        catalog:     object,          # pyiceberg.catalog.Catalog
        dry_run:     bool = False,
    ) -> None:
        if not exchange:
            raise ValueError("TradesStorage: exchange no puede estar vacío")
        if not market_type:
            raise ValueError("TradesStorage: market_type no puede estar vacío")
        if catalog is None:
            raise ValueError("TradesStorage: catalog es obligatorio")

        self._exchange    = exchange.lower()
        self._market_type = market_type.lower()
        self._catalog     = catalog
        self._dry_run     = dry_run
        self._log         = logger.bind(
            storage="silver.trades",
            exchange=self._exchange,
            market_type=self._market_type,
        )
        self._table: "Table | None" = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def append(self, df: pd.DataFrame) -> int:
        """
        Escribe ``df`` en ``silver.trades``.

        Garantías
        ---------
        * Dedup por ``trade_id`` antes de escribir (idempotente).
        * Añade columnas ``exchange`` y ``market_type`` si no están.
        * Retorna el número de filas efectivamente escritas.

        Parameters
        ----------
        df : DataFrame con columnas mínimas:
             trade_id, timestamp (int ms o Timestamp), side,
             price, amount, cost.

        Returns
        -------
        int : filas escritas (0 si vacío o dry_run).
        """
        if df is None or df.empty:
            self._log.debug("append: DataFrame vacío — skip")
            return 0

        df = self._normalize(df)
        df = self._dedup(df)

        if df.empty:
            self._log.debug("append: todo duplicado tras dedup — skip")
            return 0

        rows = len(df)

        if self._dry_run:
            self._log.info("DRY-RUN append | rows={}", rows)
            return rows

        table = self._get_or_create_table()
        table.append(df)
        self._log.debug("append OK | rows={}", rows)
        return rows

    def last_timestamp_ms(self, symbol: str) -> int | None:
        """
        Retorna el timestamp (ms) del último trade almacenado para
        ``symbol``, o None si la tabla está vacía / no existe.

        Usado por TradesFetcher para cursor incremental.
        """
        try:
            table = self._get_or_create_table()
            scan  = table.scan(
                row_filter=(
                    f"exchange = '{self._exchange}' "
                    f"AND symbol = '{symbol}'"
                ),
                selected_fields=("timestamp",),
            )
            arrow = scan.to_arrow()
            if arrow.num_rows == 0:
                return None
            ts_col = arrow.column("timestamp")
            # TimestamptzType → int64 microseconds UTC en Arrow
            max_us = ts_col.to_pylist()
            max_us_val = max(v.timestamp() * 1_000_000 if hasattr(v, "timestamp") else v
                            for v in max_us if v is not None)
            return int(max_us_val // 1_000)   # µs → ms
        except Exception as exc:
            self._log.warning(
                "last_timestamp_ms: no se pudo leer cursor | symbol={} error={}",
                symbol, exc,
            )
            return None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Añade columnas de partición y normaliza timestamps."""
        df = df.copy()

        # Columnas de partición (idempotente si ya existen)
        if "exchange" not in df.columns:
            df["exchange"] = self._exchange
        if "market_type" not in df.columns:
            df["market_type"] = self._market_type

        # Normalizar timestamp → datetime64[us, UTC] para Iceberg
        if "timestamp" in df.columns:
            col = df["timestamp"]
            if pd.api.types.is_integer_dtype(col):
                df["timestamp"] = pd.to_datetime(col, unit="ms", utc=True)
            elif not getattr(col.dtype, "tz", None):
                df["timestamp"] = col.dt.tz_localize("UTC")

        # Columnas requeridas con defaults seguros
        for col, default in [("side", ""), ("cost", 0.0)]:
            if col not in df.columns:
                df[col] = default

        return df[
            ["trade_id", "timestamp", "exchange", "market_type",
             "symbol", "side", "price", "amount", "cost"]
        ]

    @staticmethod
    def _dedup(df: pd.DataFrame) -> pd.DataFrame:
        """Elimina duplicados por trade_id (idempotencia)."""
        if "trade_id" not in df.columns:
            return df
        return df.drop_duplicates(subset=["trade_id"], keep="last")

    def _get_or_create_table(self) -> "Table":
        """Lazy init — crea la tabla si no existe (idempotente)."""
        if self._table is not None:
            return self._table

        try:
            self._table = self._catalog.load_table(_TABLE_NAME)
            self._log.debug("Tabla existente cargada | table={}", _TABLE_NAME)
        except Exception:
            self._log.info("Creando tabla | table={}", _TABLE_NAME)
            self._table = self._catalog.create_table(
                identifier      = _TABLE_NAME,
                schema          = _build_schema(),
                partition_spec  = _build_partition_spec(),
            )
            self._log.info("Tabla creada | table={}", _TABLE_NAME)

        return self._table

    def __repr__(self) -> str:
        return (
            f"TradesStorage("
            f"exchange={self._exchange!r}, "
            f"market_type={self._market_type!r}, "
            f"dry_run={self._dry_run})"
        )
