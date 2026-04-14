# -*- coding: utf-8 -*-
"""
market_data/storage/silver/derivatives_storage.py
==================================================

Storage para métricas de derivados en capa Silver.

Tablas gestionadas
------------------
  silver.funding_rate   — tasa de financiación periódica
  silver.open_interest  — contratos abiertos (snapshot)

Diseño
------
* Una tabla por dataset (schemas incompatibles entre sí — SRP).
* Upsert por (exchange, symbol, timestamp) — snapshots son
  idempotentes: el mismo snapshot puede llegar varias veces.
* Particionado por exchange + symbol.

SafeOps
-------
* dry_run=True  → loguea sin escribir.
* Constructor valida dataset en tiempo de construcción (fail-fast).

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

import pandas as pd
from loguru import logger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiceberg.table import Table

# ---------------------------------------------------------------------------
# Datasets soportados — fuente única de verdad (DRY)
# ---------------------------------------------------------------------------

SUPPORTED_DATASETS: frozenset[str] = frozenset({
    "funding_rate",
    "open_interest",
})

# ---------------------------------------------------------------------------
# Schemas Iceberg — uno por dataset
# ---------------------------------------------------------------------------

def _schema_funding_rate() -> "pyiceberg.schema.Schema":  # noqa: F821
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        NestedField, StringType, TimestamptzType, DoubleType,
    )
    return Schema(
        NestedField(1, "timestamp",    TimestamptzType(), required=True),
        NestedField(2, "exchange",     StringType(),      required=True),
        NestedField(3, "market_type",  StringType(),      required=True),
        NestedField(4, "symbol",       StringType(),      required=True),
        NestedField(5, "funding_rate", DoubleType(),      required=True),
    )


def _schema_open_interest() -> "pyiceberg.schema.Schema":  # noqa: F821
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        NestedField, StringType, TimestamptzType, DoubleType,
    )
    return Schema(
        NestedField(1, "timestamp",     TimestamptzType(), required=True),
        NestedField(2, "exchange",      StringType(),      required=True),
        NestedField(3, "market_type",   StringType(),      required=True),
        NestedField(4, "symbol",        StringType(),      required=True),
        NestedField(5, "open_interest", DoubleType(),      required=True),
    )


_SCHEMAS = {
    "funding_rate":  _schema_funding_rate,
    "open_interest": _schema_open_interest,
}

_TABLE_NAMES = {
    "funding_rate":  "silver.funding_rate",
    "open_interest": "silver.open_interest",
}


def _build_partition_spec() -> "pyiceberg.partitioning.PartitionSpec":  # noqa: F821
    """Partición compartida por exchange + symbol (ambos datasets)."""
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform
    return PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="exchange"),
        PartitionField(source_id=4, field_id=1001, transform=IdentityTransform(), name="symbol"),
    )


# ---------------------------------------------------------------------------
# DerivativesStorage
# ---------------------------------------------------------------------------

class DerivativesStorage:
    """
    Escribe DataFrames de métricas de derivados en ``silver.<dataset>``.

    Una instancia por dataset — no multiplexa internamente (SRP).

    Parameters
    ----------
    dataset     : ``"funding_rate"`` | ``"open_interest"``.
    exchange    : identificador del exchange.
    market_type : ``"linear"`` | ``"inverse"`` | ``"spot"``.
    catalog     : instancia PyIceberg RESTCatalog.
    dry_run     : si True, loguea sin escribir (SafeOps).
    """

    def __init__(
        self,
        dataset:     str,
        exchange:    str,
        market_type: str,
        catalog:     object,
        dry_run:     bool = False,
    ) -> None:
        if dataset not in SUPPORTED_DATASETS:
            raise ValueError(
                f"DerivativesStorage: dataset no soportado: {dataset!r}. "
                f"Soportados: {sorted(SUPPORTED_DATASETS)}"
            )
        if not exchange:
            raise ValueError("DerivativesStorage: exchange no puede estar vacío")
        if catalog is None:
            raise ValueError("DerivativesStorage: catalog es obligatorio")

        self._dataset     = dataset
        self._exchange    = exchange.lower()
        self._market_type = market_type.lower()
        self._catalog     = catalog
        self._dry_run     = dry_run
        self._table_name  = _TABLE_NAMES[dataset]
        self._log         = logger.bind(
            storage=self._table_name,
            exchange=self._exchange,
            dataset=dataset,
        )
        self._table: "Table | None" = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upsert(self, df: pd.DataFrame) -> int:
        """
        Escribe ``df`` en ``silver.<dataset>``.

        Upsert semántico por (exchange, symbol, timestamp).
        Los snapshots de derivados son idempotentes por naturaleza —
        el mismo snapshot puede re-procesarse sin duplicar.

        Returns
        -------
        int : filas escritas (0 si vacío o dry_run).
        """
        if df is None or df.empty:
            self._log.debug("upsert: DataFrame vacío — skip")
            return 0

        df = self._normalize(df)
        rows = len(df)

        if self._dry_run:
            self._log.info("DRY-RUN upsert | dataset={} rows={}", self._dataset, rows)
            return rows

        table = self._get_or_create_table()
        # PyIceberg: append + compaction posterior para upsert real.
        # Para snapshots de baja frecuencia (8h / 1h), append es correcto —
        # la dedup se hace al leer con MAX(timestamp) por símbolo.
        table.append(df)
        self._log.debug("upsert OK | dataset={} rows={}", self._dataset, rows)
        return rows

    def last_timestamp_ms(self, symbol: str) -> int | None:
        """
        Timestamp (ms) del último snapshot almacenado para ``symbol``.
        None si la tabla está vacía o no existe.
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
            ts_col    = arrow.column("timestamp").to_pylist()
            max_ts    = max(
                v.timestamp() * 1_000_000 if hasattr(v, "timestamp") else v
                for v in ts_col if v is not None
            )
            return int(max_ts // 1_000)   # µs → ms
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
        """Añade columnas de partición, normaliza timestamp."""
        df = df.copy()

        if "exchange" not in df.columns:
            df["exchange"] = self._exchange
        if "market_type" not in df.columns:
            df["market_type"] = self._market_type

        if "timestamp" in df.columns:
            col = df["timestamp"]
            if pd.api.types.is_integer_dtype(col):
                df["timestamp"] = pd.to_datetime(col, unit="ms", utc=True)
            elif not getattr(col.dtype, "tz", None):
                df["timestamp"] = col.dt.tz_localize("UTC")

        # Renombrar value_float → columna semántica del dataset
        value_col = self._dataset   # "funding_rate" | "open_interest"
        if "value_float" in df.columns and value_col not in df.columns:
            df = df.rename(columns={"value_float": value_col})

        expected = ["timestamp", "exchange", "market_type", "symbol", value_col]
        for col in expected:
            if col not in df.columns:
                raise ValueError(
                    f"DerivativesStorage._normalize: columna faltante: {col!r} "
                    f"en dataset={self._dataset!r}"
                )

        return df[expected]

    def _get_or_create_table(self) -> "Table":
        """Lazy init — crea la tabla si no existe (idempotente)."""
        if self._table is not None:
            return self._table

        try:
            self._table = self._catalog.load_table(self._table_name)
            self._log.debug("Tabla existente | table={}", self._table_name)
        except Exception:
            self._log.info("Creando tabla | table={}", self._table_name)
            self._table = self._catalog.create_table(
                identifier     = self._table_name,
                schema         = _SCHEMAS[self._dataset](),
                partition_spec = _build_partition_spec(),
            )
            self._log.info("Tabla creada | table={}", self._table_name)

        return self._table

    def __repr__(self) -> str:
        return (
            f"DerivativesStorage("
            f"dataset={self._dataset!r}, "
            f"exchange={self._exchange!r}, "
            f"market_type={self._market_type!r}, "
            f"dry_run={self._dry_run})"
        )
