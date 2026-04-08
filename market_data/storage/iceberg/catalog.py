"""
market_data/storage/iceberg/catalog.py
=======================================

Catálogo Iceberg compartido para las tres capas del medallón.

  bronze.ohlcv   — raw append-only, trazabilidad forense
  silver.ohlcv   — deduplicado, versionado por snapshot
  gold.features  — OHLCV + indicadores técnicos, overwrite por dataset

Un único SqlCatalog (SQLite), un único warehouse, tres namespaces.
Singleton por proceso — todos los storages comparten la misma instancia.

Responsabilidad
---------------
• Crear/abrir el catálogo SQLite.
• Definir schemas Iceberg para Bronze y Gold (Silver ya existe).
• Exponer ensure_*_table() — idempotente, seguro llamar en cada __init__.

Principios
----------
• SSOT — un solo lugar donde viven schemas y rutas del catálogo.
• DRY  — IcebergStorage, BronzeStorage y GoldStorage importan de aquí.
• Fail Fast — si el warehouse no puede crearse, lanza inmediatamente.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, MonthTransform
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from core.utils import repo_root

# =============================================================================
# Catálogo — singleton por proceso
# =============================================================================

_CATALOG: Optional[SqlCatalog] = None


def get_catalog() -> SqlCatalog:
    """
    Devuelve el catálogo Iceberg compartido, creándolo una sola vez.

    Paths resueltos desde repo_root() — independiente del cwd desde
    donde se lance el proceso.
    """
    global _CATALOG
    if _CATALOG is None:
        base      = repo_root() / "data_platform"
        cat_path  = base / "iceberg_catalog"
        ware_path = base / "iceberg_warehouse"
        cat_path.mkdir(parents=True, exist_ok=True)
        ware_path.mkdir(parents=True, exist_ok=True)
        _CATALOG = SqlCatalog(
            "ocm",
            **{
                "uri":       f"sqlite:///{cat_path}/catalog.db",
                "warehouse": str(ware_path.absolute()),
            },
        )
    return _CATALOG


# =============================================================================
# Schemas
# =============================================================================

# Campos OHLCV base — IDs 1-10, idénticos en las tres capas.
# El IcebergStorage (Silver) usa su propio schema definido inline;
# Bronze y Gold usan los schemas de este módulo.
_OHLCV_BASE_FIELDS = [
    NestedField(1,  "timestamp",   TimestamptzType(), required=True),
    NestedField(2,  "open",        DoubleType(),      required=True),
    NestedField(3,  "high",        DoubleType(),      required=True),
    NestedField(4,  "low",         DoubleType(),      required=True),
    NestedField(5,  "close",       DoubleType(),      required=True),
    NestedField(6,  "volume",      DoubleType(),      required=True),
    NestedField(7,  "exchange",    StringType(),      required=True),
    NestedField(8,  "market_type", StringType(),      required=True),
    NestedField(9,  "symbol",      StringType(),      required=True),
    NestedField(10, "timeframe",   StringType(),      required=True),
]

BRONZE_SCHEMA: Schema = Schema(
    *_OHLCV_BASE_FIELDS,
    # ingestion_ts: cuándo llegó el dato al sistema — trazabilidad forense.
    NestedField(11, "ingestion_ts", TimestamptzType(), required=False),
)

GOLD_SCHEMA: Schema = Schema(
    *_OHLCV_BASE_FIELDS,
    # ── Features técnicos — nullable: series cortas producen NaN ────────
    NestedField(11, "return_1",        DoubleType(), required=False),
    NestedField(12, "log_return",      DoubleType(), required=False),
    NestedField(13, "volatility_20",   DoubleType(), required=False),
    NestedField(14, "high_low_spread", DoubleType(), required=False),
    NestedField(15, "vwap",            DoubleType(), required=False),
    # ── Lineage — reproducibilidad total ────────────────────────────────
    NestedField(16, "run_id",             StringType(), required=False),
    NestedField(17, "engineer_version",   StringType(), required=False),
    NestedField(18, "silver_snapshot_id", LongType(),   required=False),
    NestedField(19, "silver_snapshot_ms", LongType(),   required=False),
)


# =============================================================================
# Partition spec
# =============================================================================

def _ohlcv_partition_spec() -> PartitionSpec:
    """
    Spec de particionado idéntico para Bronze, Silver y Gold.

    exchange / market_type / symbol / timeframe / ts_month

    ts_month (MonthTransform sobre timestamp) permite partition pruning
    en queries con rango temporal — evita abrir archivos fuera del rango.
    """
    return PartitionSpec(
        PartitionField(source_id=7,  field_id=1001, transform=IdentityTransform(), name="exchange"),
        PartitionField(source_id=8,  field_id=1002, transform=IdentityTransform(), name="market_type"),
        PartitionField(source_id=9,  field_id=1003, transform=IdentityTransform(), name="symbol"),
        PartitionField(source_id=10, field_id=1004, transform=IdentityTransform(), name="timeframe"),
        PartitionField(source_id=1,  field_id=1005, transform=MonthTransform(),    name="ts_month"),
    )


# =============================================================================
# Bootstrap — idempotente
# =============================================================================

def ensure_bronze_table() -> None:
    """Crea bronze.ohlcv si no existe. Seguro llamar en cada __init__."""
    cat = get_catalog()
    existing_ns = {tuple(ns) for ns in cat.list_namespaces()}
    if ("bronze",) not in existing_ns:
        cat.create_namespace("bronze")
    existing_tables = {t[1] for t in cat.list_tables("bronze")}
    if "ohlcv" not in existing_tables:
        cat.create_table(
            "bronze.ohlcv",
            schema         = BRONZE_SCHEMA,
            partition_spec = _ohlcv_partition_spec(),
        )


def ensure_gold_table() -> None:
    """Crea gold.features si no existe. Seguro llamar en cada __init__."""
    cat = get_catalog()
    existing_ns = {tuple(ns) for ns in cat.list_namespaces()}
    if ("gold",) not in existing_ns:
        cat.create_namespace("gold")
    existing_tables = {t[1] for t in cat.list_tables("gold")}
    if "features" not in existing_tables:
        cat.create_table(
            "gold.features",
            schema         = GOLD_SCHEMA,
            partition_spec = _ohlcv_partition_spec(),
        )


__all__ = [
    "get_catalog",
    "BRONZE_SCHEMA",
    "GOLD_SCHEMA",
    "ensure_bronze_table",
    "ensure_gold_table",
]
