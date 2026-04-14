# -*- coding: utf-8 -*-
"""
market_data/storage/iceberg/schemas.py
========================================

Schemas Iceberg para todas las capas del medallón — SSOT.

  BRONZE_SCHEMA      → bronze.ohlcv       (IDs 1–11)
  SILVER_SCHEMA      → silver.ohlcv       (IDs 1–10)
  GOLD_SCHEMA        → gold.features      (IDs 1–19)
  TRADES_SCHEMA      → silver.trades      (IDs 101–110)
  DERIVATIVES_SCHEMA → silver.derivatives (IDs 201–208)

Schema evolution (Iceberg spec §3.5)
--------------------------------------
Nunca reusar IDs de campos eliminados.
IDs reservados por rango:
  1–10   : OHLCV base (todas las capas)
  11–19  : Gold features + lineage
  101–110: Trades
  201–210: Derivatives
"""
from __future__ import annotations

from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

# Campos OHLCV base — IDs 1–10, idénticos en Bronze, Silver y Gold.
# Constante compartida para garantizar consistencia de IDs entre capas.
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

# Bronze: raw append-only. ingestion_ts opcional — históricos importados
# no tienen ingestion_ts fiable.
BRONZE_SCHEMA: Schema = Schema(
    *_OHLCV_BASE_FIELDS,
    NestedField(11, "ingestion_ts", TimestamptzType(), required=False),
)

# Silver: schema canónico — mínimo, máxima compatibilidad hacia arriba.
SILVER_SCHEMA: Schema = Schema(*_OHLCV_BASE_FIELDS)

# Gold: extiende Silver con features técnicos y lineage.
GOLD_SCHEMA: Schema = Schema(
    *_OHLCV_BASE_FIELDS,
    # ── Features técnicos ────────────────────────────────────────────────
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

# Trades: tick data — IDs 101–110.
TRADES_SCHEMA: Schema = Schema(
    NestedField(101, "timestamp",    TimestamptzType(), required=True),
    NestedField(102, "trade_id",     StringType(),      required=True),
    NestedField(103, "symbol",       StringType(),      required=True),
    NestedField(104, "exchange",     StringType(),      required=True),
    NestedField(105, "market_type",  StringType(),      required=True),
    NestedField(106, "side",         StringType(),      required=True),
    NestedField(107, "price",        DoubleType(),      required=True),
    NestedField(108, "amount",       DoubleType(),      required=True),
    NestedField(109, "cost",         DoubleType(),      required=False),
    NestedField(110, "ingestion_ts", TimestamptzType(), required=False),
)

# Derivatives: funding_rate / open_interest / liquidations — IDs 201–208.
# value_*: columnas polimórficas — solo una poblada por dataset.
DERIVATIVES_SCHEMA: Schema = Schema(
    NestedField(201, "timestamp",    TimestamptzType(), required=True),
    NestedField(202, "exchange",     StringType(),      required=True),
    NestedField(203, "market_type",  StringType(),      required=True),
    NestedField(204, "symbol",       StringType(),      required=True),
    NestedField(205, "dataset",      StringType(),      required=True),
    NestedField(206, "value_float",  DoubleType(),      required=False),
    NestedField(207, "value_str",    StringType(),      required=False),
    NestedField(208, "ingestion_ts", TimestamptzType(), required=False),
)

__all__ = [
    "BRONZE_SCHEMA",
    "SILVER_SCHEMA",
    "GOLD_SCHEMA",
    "TRADES_SCHEMA",
    "DERIVATIVES_SCHEMA",
]
