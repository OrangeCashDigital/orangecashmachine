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
• Definir schemas Iceberg para Bronze, Silver y Gold (SSOT).
• Exponer ensure_*_table() — idempotente, seguro llamar en cada __init__.

Principios
----------
• SSOT  — schemas y rutas del catálogo viven exclusivamente aquí.
• DRY   — IcebergStorage, BronzeStorage y GoldStorage importan de aquí.
• Fail Fast — si el warehouse no puede crearse, lanza inmediatamente.
• Idempotencia — ensure_*_table() usa list_namespaces/list_tables antes
  de create_* para garantizar cero efectos secundarios en re-runs.

Referencias
-----------
• Apache Iceberg spec v2: https://iceberg.apache.org/spec/
• pyiceberg SqlCatalog: https://py.iceberg.apache.org/configuration/#sql-catalog
• Medallion architecture: https://www.databricks.com/glossary/medallion-architecture
"""
from __future__ import annotations

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
    """Devuelve el catálogo Iceberg compartido, creándolo una sola vez.

    Paths resueltos desde repo_root() — independiente del cwd desde
    donde se lance el proceso.

    El SQLite catalog es adecuado para desarrollo y producción single-node.
    Para producción multi-nodo usar REST catalog o Hive Metastore.
    Ref: https://py.iceberg.apache.org/configuration/#sql-catalog
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

# Campos OHLCV base — IDs 1-10, idénticos en las tres capas del medallón.
# Compartidos vía _OHLCV_BASE_FIELDS para garantizar consistencia de IDs
# y evitar drift entre capas al evolucionar el schema.
#
# Regla de schema evolution (Iceberg spec §3.5):
#   - Nunca reusar IDs de campo eliminados.
#   - Nuevos campos en Bronze/Silver: IDs >= 11 (actualmente ninguno).
#   - Nuevos campos en Gold: IDs >= 20 (11-19 reservados para features).
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
    # Optional porque datos históricos importados no tienen ingestion_ts fiable.
    NestedField(11, "ingestion_ts", TimestamptzType(), required=False),
)

# Silver: schema canónico del medallón — sin columnas extra.
# Diseño deliberado:
#   • Máxima compatibilidad hacia arriba con Gold (que extiende Silver).
#   • Mínima superficie de schema evolution en la capa más crítica.
#   • Gold lee Silver vía scan; no necesita campos adicionales en Silver.
SILVER_SCHEMA: Schema = Schema(*_OHLCV_BASE_FIELDS)

GOLD_SCHEMA: Schema = Schema(
    *_OHLCV_BASE_FIELDS,
    # ── Features técnicos — nullable: series cortas producen NaN ────────
    NestedField(11, "return_1",        DoubleType(), required=False),
    NestedField(12, "log_return",      DoubleType(), required=False),
    NestedField(13, "volatility_20",   DoubleType(), required=False),
    NestedField(14, "high_low_spread", DoubleType(), required=False),
    NestedField(15, "vwap",            DoubleType(), required=False),
    # ── Lineage — reproducibilidad total ────────────────────────────────
    # silver_snapshot_id + silver_snapshot_ms permiten reconstruir el estado
    # exacto de Silver que produjo este batch Gold (data lineage).
    NestedField(16, "run_id",             StringType(), required=False),
    NestedField(17, "engineer_version",   StringType(), required=False),
    NestedField(18, "silver_snapshot_id", LongType(),   required=False),
    NestedField(19, "silver_snapshot_ms", LongType(),   required=False),
)


# =============================================================================
# Partition spec
# =============================================================================

def _ohlcv_partition_spec() -> PartitionSpec:
    """Spec de particionado idéntico para Bronze, Silver y Gold.

    Estrategia: exchange / market_type / symbol / timeframe / ts_month

    Justificación:
    - Las cuatro columnas de identidad son los ejes naturales de consulta.
      Partition pruning elimina archivos irrelevantes antes de leer datos.
    - ts_month (MonthTransform sobre timestamp) habilita pruning temporal
      en queries con rango de fechas — el caso más frecuente en backtesting
      e ingesta histórica.
    - Orden deliberado: identidad primero, tiempo después. Optimiza queries
      del tipo "dame BTC/USDT 1h" que son más selectivas en identidad.

    Ref: https://iceberg.apache.org/spec/#partitioning
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
    """Crea bronze.ohlcv si no existe. Seguro llamar en cada __init__.

    Bronze es la capa de ingesta raw: append-only, sin deduplicación.
    Preserva trazabilidad forense completa incluyendo ingestion_ts.
    """
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


def ensure_silver_table() -> None:
    """Crea silver.ohlcv si no existe. Seguro llamar en cada __init__.

    Silver es la capa canónica del medallón: datos OHLCV deduplicados,
    versionados automáticamente por snapshot de Iceberg.

    Patrón "ensure before load": elimina la dependencia de un script de
    inicialización externo y hace el storage self-healing en cada arranque.
    Safe: no-op si la tabla ya existe.

    Ref: https://www.databricks.com/glossary/medallion-architecture
    """
    cat = get_catalog()
    existing_ns = {tuple(ns) for ns in cat.list_namespaces()}
    if ("silver",) not in existing_ns:
        cat.create_namespace("silver")
    existing_tables = {t[1] for t in cat.list_tables("silver")}
    if "ohlcv" not in existing_tables:
        cat.create_table(
            "silver.ohlcv",
            schema         = SILVER_SCHEMA,
            partition_spec = _ohlcv_partition_spec(),
        )


def ensure_gold_table() -> None:
    """Crea gold.features si no existe. Seguro llamar en cada __init__.

    Gold extiende Silver con features técnicos y columnas de lineage
    para reproducibilidad total del pipeline de features.
    """
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
    "SILVER_SCHEMA",
    "GOLD_SCHEMA",
    "ensure_bronze_table",
    "ensure_silver_table",
    "ensure_gold_table",
]
