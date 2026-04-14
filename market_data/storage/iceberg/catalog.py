# -*- coding: utf-8 -*-
"""
market_data/storage/iceberg/catalog.py
========================================

Facade pública del paquete iceberg — reexporta toda la API pública.

Consumers importan desde aquí sin conocer la estructura interna:

    from market_data.storage.iceberg.catalog import get_catalog, ensure_bronze_table

Estructura interna (SRP):
    _catalog_singleton.py → singleton SqlCatalog
    schemas.py            → definiciones de Schema (SSOT)
    partitions.py         → PartitionSpec por tabla
    bootstrap.py          → ensure_*_table() idempotentes

Ref: https://iceberg.apache.org/spec/
"""
from __future__ import annotations

from market_data.storage.iceberg._catalog_singleton import get_catalog
from market_data.storage.iceberg.schemas import (
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    GOLD_SCHEMA,
    TRADES_SCHEMA,
    DERIVATIVES_SCHEMA,
)
from market_data.storage.iceberg.bootstrap import (
    ensure_bronze_table,
    ensure_silver_table,
    ensure_gold_table,
    ensure_trades_table,
    ensure_derivatives_table,
)

__all__ = [
    "get_catalog",
    "BRONZE_SCHEMA",
    "SILVER_SCHEMA",
    "GOLD_SCHEMA",
    "TRADES_SCHEMA",
    "DERIVATIVES_SCHEMA",
    "ensure_bronze_table",
    "ensure_silver_table",
    "ensure_gold_table",
    "ensure_trades_table",
    "ensure_derivatives_table",
]
