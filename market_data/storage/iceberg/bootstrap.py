# -*- coding: utf-8 -*-
"""
market_data/storage/iceberg/bootstrap.py
==========================================

Bootstrap idempotente de tablas Iceberg — ensure_*_table().

Cada función es segura llamar en cada __init__ de storage:
no-op si la tabla ya existe, crea namespace + tabla si no.

Principio DRY: _ensure_table() centraliza la lógica de bootstrap.
"""
from __future__ import annotations

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


def _ensure_table(
    namespace: str,
    table:     str,
    schema:    Schema,
    spec:      PartitionSpec,
) -> None:
    """Crea namespace y tabla si no existen. Idempotente."""
    cat = get_catalog()
    if (namespace,) not in {tuple(ns) for ns in cat.list_namespaces()}:
        cat.create_namespace(namespace)
    if table not in {t[1] for t in cat.list_tables(namespace)}:
        cat.create_table(
            f"{namespace}.{table}",
            schema         = schema,
            partition_spec = spec,
        )


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
