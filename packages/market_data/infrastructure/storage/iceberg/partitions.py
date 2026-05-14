# -*- coding: utf-8 -*-
"""
market_data/storage/iceberg/partitions.py
==========================================

Partition specs para todas las tablas Iceberg del medallón.

Estrategia OHLCV: exchange / market_type / symbol / timeframe / ts_month
  - Identidad primero → pruning máximo por par/exchange
  - ts_month último   → pruning temporal en queries con rango de fechas

Ref: https://iceberg.apache.org/spec/#partitioning
"""
from __future__ import annotations

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform, MonthTransform


def ohlcv_partition_spec() -> PartitionSpec:
    """Spec compartido para bronze.ohlcv, silver.ohlcv y gold.features."""
    return PartitionSpec(
        PartitionField(source_id=7,  field_id=1001, transform=IdentityTransform(), name="exchange"),
        PartitionField(source_id=8,  field_id=1002, transform=IdentityTransform(), name="market_type"),
        PartitionField(source_id=9,  field_id=1003, transform=IdentityTransform(), name="symbol"),
        PartitionField(source_id=10, field_id=1004, transform=IdentityTransform(), name="timeframe"),
        PartitionField(source_id=1,  field_id=1005, transform=MonthTransform(),    name="ts_month"),
    )


def trades_partition_spec() -> PartitionSpec:
    """Spec para silver.trades — sin timeframe (trades no tienen resolución fija)."""
    return PartitionSpec(
        PartitionField(source_id=104, field_id=2001, transform=IdentityTransform(), name="exchange"),
        PartitionField(source_id=105, field_id=2002, transform=IdentityTransform(), name="market_type"),
        PartitionField(source_id=103, field_id=2003, transform=IdentityTransform(), name="symbol"),
        PartitionField(source_id=101, field_id=2004, transform=MonthTransform(),    name="ts_month"),
    )


def derivatives_partition_spec() -> PartitionSpec:
    """Spec para silver.derivatives — dataset como partición para pruning por tipo."""
    return PartitionSpec(
        PartitionField(source_id=202, field_id=3001, transform=IdentityTransform(), name="exchange"),
        PartitionField(source_id=205, field_id=3002, transform=IdentityTransform(), name="dataset"),
        PartitionField(source_id=204, field_id=3003, transform=IdentityTransform(), name="symbol"),
        PartitionField(source_id=201, field_id=3004, transform=MonthTransform(),    name="ts_month"),
    )


__all__ = [
    "ohlcv_partition_spec",
    "trades_partition_spec",
    "derivatives_partition_spec",
]
