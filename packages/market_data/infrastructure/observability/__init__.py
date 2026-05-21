"""
market_data/observability/__init__.py
=======================================

Fachada pública del subsistema de métricas de dominio.

Importar desde aquí — no desde metrics.py directamente.
Esto permite reorganizar metrics.py sin romper callers (OCP).

Nombres canónicos: los de metrics.py. Este archivo NO renombra.
"""

from market_data.infrastructure.observability.metrics import (
    ACTIVE_PAIRS,  # nombre real en metrics.py
    CANDLE_DELAY_MS,
    EXCHANGE_CIRCUIT_OPEN,
    EXCHANGE_CLOCK_DRIFT,
    # ── Exchange ─────────────────────────────────────────────
    EXCHANGE_LATENCY,
    EXCHANGE_RATE_LIMIT,
    # ── Fetch ────────────────────────────────────────────────
    FETCH_ABORTS_TOTAL,
    FETCH_CHUNK_DURATION,
    FETCH_CHUNK_ERRORS_TOTAL,  # nombre real en metrics.py
    FETCH_CHUNKS_TOTAL,
    MANIFEST_CHECKSUM_FAILURES,
    PAIR_DURATION,
    PIPELINE_ERRORS,
    QUALITY_DECISIONS,
    QUALITY_GAPS_TOTAL,
    # ── Repair / gaps ────────────────────────────────────────
    REPAIR_GAPS_FOUND,
    REPAIR_GAPS_HEALED,
    REPAIR_GAPS_SKIPPED,
    RESAMPLE_DURATION_MS,
    # ── Resample ─────────────────────────────────────────────
    RESAMPLE_ROWS_TOTAL,
    # ── Pipeline ─────────────────────────────────────────────
    ROWS_INGESTED,
    STORAGE_PARTITION_SIZE_ROWS,  # nombre real en metrics.py
    # ── Storage ──────────────────────────────────────────────
    STORAGE_WRITE_DURATION,
    # ── Timestamps ───────────────────────────────────────────
    TIMESTAMP_DRIFT_CORRECTED,
    TIMESTAMP_GRID_COLLISIONS,
    WRITE_LOCK_CONFLICTS,
    WRITE_LOCK_STARVATION,
    # ── Write lock ───────────────────────────────────────────
    WRITE_LOCK_WAIT_DURATION,
    record_exchange_probe_metrics,
    # ── Functions ────────────────────────────────────────────
    record_pipeline_pair_metrics,
)

__all__ = [
    # Pipeline
    "ROWS_INGESTED",
    "PAIR_DURATION",
    "PIPELINE_ERRORS",
    "ACTIVE_PAIRS",
    "QUALITY_DECISIONS",
    # Fetch
    "FETCH_ABORTS_TOTAL",
    "FETCH_CHUNK_DURATION",
    "FETCH_CHUNKS_TOTAL",
    "FETCH_CHUNK_ERRORS_TOTAL",
    # Exchange
    "EXCHANGE_LATENCY",
    "EXCHANGE_CLOCK_DRIFT",
    "EXCHANGE_RATE_LIMIT",
    "EXCHANGE_CIRCUIT_OPEN",
    "CANDLE_DELAY_MS",
    # Storage
    "STORAGE_WRITE_DURATION",
    "STORAGE_PARTITION_SIZE_ROWS",
    # Write lock
    "WRITE_LOCK_WAIT_DURATION",
    "WRITE_LOCK_CONFLICTS",
    "WRITE_LOCK_STARVATION",
    "MANIFEST_CHECKSUM_FAILURES",
    # Repair
    "REPAIR_GAPS_FOUND",
    "REPAIR_GAPS_HEALED",
    "REPAIR_GAPS_SKIPPED",
    "QUALITY_GAPS_TOTAL",
    # Resample
    "RESAMPLE_ROWS_TOTAL",
    "RESAMPLE_DURATION_MS",
    # Timestamps
    "TIMESTAMP_DRIFT_CORRECTED",
    "TIMESTAMP_GRID_COLLISIONS",
    # Functions
    "record_pipeline_pair_metrics",
    "record_exchange_probe_metrics",
]
