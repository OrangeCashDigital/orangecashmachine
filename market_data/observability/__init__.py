# -*- coding: utf-8 -*-
"""
market_data/observability/__init__.py
======================================

Fachada pública de métricas de dominio market_data.

Re-exporta los contadores y helpers más usados para que los callers
no necesiten conocer la estructura interna del módulo.

Uso
---
    from market_data.observability import ROWS_INGESTED, PIPELINE_ERRORS
    from market_data.observability import record_exchange_probe_metrics

Principios: OCP · SSOT · DRY
"""
from market_data.observability.metrics import (
    # ── Pipeline ─────────────────────────────────────────────
    ROWS_INGESTED,
    PAIR_DURATION,
    PIPELINE_ERRORS,
    ACTIVE_PAIRS,
    QUALITY_DECISIONS,
    # ── Fetch ────────────────────────────────────────────────
    FETCH_ABORTS_TOTAL,
    FETCH_CHUNK_DURATION,
    FETCH_CHUNKS_TOTAL,
    FETCH_CHUNK_ERRORS_TOTAL,
    # ── Exchange ─────────────────────────────────────────────
    EXCHANGE_LATENCY,
    EXCHANGE_CLOCK_DRIFT,
    EXCHANGE_RATE_LIMIT,
    EXCHANGE_CIRCUIT_OPEN,
    CANDLE_DELAY_MS,
    # ── Storage ──────────────────────────────────────────────
    STORAGE_WRITE_DURATION,
    STORAGE_PARTITION_SIZE_ROWS,
    # ── Write lock ───────────────────────────────────────────
    WRITE_LOCK_WAIT_DURATION,
    WRITE_LOCK_CONFLICTS,
    WRITE_LOCK_STARVATION,
    MANIFEST_CHECKSUM_FAILURES,
    # ── Repair / gaps ────────────────────────────────────────
    REPAIR_GAPS_FOUND,
    REPAIR_GAPS_HEALED,
    REPAIR_GAPS_SKIPPED,
    QUALITY_GAPS_TOTAL,
    # ── Resample ─────────────────────────────────────────────
    RESAMPLE_ROWS_TOTAL,
    RESAMPLE_DURATION_MS,
    # ── Timestamps ───────────────────────────────────────────
    TIMESTAMP_DRIFT_CORRECTED,
    TIMESTAMP_GRID_COLLISIONS,
    # ── Functions ────────────────────────────────────────────
    record_pipeline_pair_metrics,
    record_exchange_probe_metrics,
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
