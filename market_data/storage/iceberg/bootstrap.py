# -*- coding: utf-8 -*-
"""
market_data/storage/iceberg/bootstrap.py
==========================================

Bootstrap idempotente de tablas Iceberg — ensure_*_table().

Cada función es idempotente: no-op si ya existe, crea namespace + tabla si no.
Seguro llamar en cada __init__ de storage sin overhead significativo.

Principios: DRY · Fail-Fast · Resiliencia ante versiones de pyiceberg.
"""
from __future__ import annotations

from loguru import logger
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec

from market_data.storage.iceberg._catalog_singleton import get_catalog
from market_data.storage.iceberg.schemas import (
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    GOLD_SCHEMA,
    TRADES_SCHEMA,
    DERIVATIVES_SCHEMA,
)
from market_data.storage.iceberg.partitions import (
    ohlcv_partition_spec,
    trades_partition_spec,
    derivatives_partition_spec,
)


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _namespace_exists(cat, namespace: str) -> bool:
    """Verifica si un namespace existe de forma defensiva.

    pyiceberg puede devolver tuplas, listas o strings según la versión.
    Normalizar a str antes de comparar evita falsos negativos.
    """
    try:
        for ns in cat.list_namespaces():
            # ns puede ser: ("bronze",) | ["bronze"] | "bronze"
            ns_str = ns[0] if isinstance(ns, (list, tuple)) else str(ns)
            if ns_str == namespace:
                return True
        return False
    except Exception as exc:
        logger.warning(
            "bootstrap._namespace_exists error | namespace={} err={}",
            namespace, exc,
        )
        return False


def _table_exists(cat, namespace: str, table: str) -> bool:
    """Verifica si una tabla existe de forma defensiva."""
    try:
        tables = cat.list_tables(namespace)
        for t in tables:
            # t puede ser: ("namespace", "table") | "table"
            t_name = t[1] if isinstance(t, (list, tuple)) and len(t) > 1 else str(t)
            if t_name == table:
                return True
        return False
    except Exception as exc:
        logger.warning(
            "bootstrap._table_exists error | {}.{} err={}",
            namespace, table, exc,
        )
        return False


def _ensure_table(
    namespace: str,
    table:     str,
    schema:    Schema,
    spec:      PartitionSpec,
) -> None:
    """Crea namespace y tabla si no existen. Idempotente.

    Fail-Fast: lanza si la creación falla (no swallow silencioso).
    Cualquier error aquí es un problema de bootstrap que debe resolverse
    antes de continuar — no puede silenciarse.
    """
    cat = get_catalog()

    if not _namespace_exists(cat, namespace):
        cat.create_namespace(namespace)
        logger.debug("bootstrap | namespace_created={}", namespace)

    if not _table_exists(cat, namespace, table):
        cat.create_table(
            f"{namespace}.{table}",
            schema         = schema,
            partition_spec = spec,
        )
        logger.debug("bootstrap | table_created={}.{}", namespace, table)


# ---------------------------------------------------------------------------
# API pública — un ensure por tabla (SRP)
# ---------------------------------------------------------------------------

def ensure_bronze_table() -> None:
    """Crea bronze.ohlcv si no existe."""
    _ensure_table("bronze", "ohlcv", BRONZE_SCHEMA, ohlcv_partition_spec())


def ensure_silver_table() -> None:
    """Crea silver.ohlcv si no existe."""
    _ensure_table("silver", "ohlcv", SILVER_SCHEMA, ohlcv_partition_spec())


def ensure_gold_table() -> None:
    """Crea gold.features si no existe."""
    _ensure_table("gold", "features", GOLD_SCHEMA, ohlcv_partition_spec())


def ensure_trades_table() -> None:
    """Crea silver.trades si no existe."""
    _ensure_table("silver", "trades", TRADES_SCHEMA, trades_partition_spec())


def ensure_derivatives_table() -> None:
    """Crea silver.derivatives si no existe."""
    _ensure_table("silver", "derivatives", DERIVATIVES_SCHEMA, derivatives_partition_spec())


__all__ = [
    "ensure_bronze_table",
    "ensure_silver_table",
    "ensure_gold_table",
    "ensure_trades_table",
    "ensure_derivatives_table",
]
