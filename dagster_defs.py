# -*- coding: utf-8 -*-
"""
dagster_defs.py
================

Punto de entrada único de Dagster para OrangeCashMachine.

SSOT de definiciones — Definitions es el único lugar donde se ensamblan:
  assets, resources, schedules, sensors, jobs.

Uso
---
  # Iniciar Dagster UI:
  uv run dagster dev -f dagster_defs.py

  # Materializar todos los assets manualmente:
  uv run dagster asset materialize -f dagster_defs.py --select "*"

  # Solo bronze:
  uv run dagster asset materialize -f dagster_defs.py --select "bronze/*"

Concurrencia
------------
Configurada declarativamente en dagster.yaml — SSOT de límites.
Los tags dagster/concurrency_key en cada asset activan los pools.
  bronze_ingestion → pools.bronze_ingestion (run_limit=2)
  silver_resample  → pools.silver_resample  (run_limit=4)
  repair_gaps      → pools.repair_gaps      (run_limit=1)

Entorno
-------
OCMResource lee OCM_ENV del entorno. No hay hardcoding de env aquí.
Fail-Fast: si OCM_ENV no está definido o es inválido, el resource
falla en setup_for_execution() antes de materializar cualquier asset.

Principios: SSOT · OCP · DIP · KISS · Fail-Fast
"""
from __future__ import annotations

import os

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from dagster_assets.bronze_ohlcv import BRONZE_OHLCV_ASSETS
from dagster_assets.repair_ohlcv import REPAIR_OHLCV_ASSETS
from dagster_assets.resample_ohlcv import RESAMPLE_OHLCV_ASSETS
from dagster_assets.asset_checks import ALL_ASSET_CHECKS
from dagster_assets.resources import OCMResource

# ==============================================================================
# Jobs — agrupan assets para ejecución coordinada
# ==============================================================================

# Job principal: ingesta completa bronze → repair → resample
ingestion_job = define_asset_job(
    name        = "ocm_ingestion_job",
    selection   = AssetSelection.groups("bronze", "silver"),
    description = (
        "Job completo de ingesta OCM: "
        "bronze (exchange) → repair (gaps) → silver (resample)."
    ),
)

# Job solo bronze — útil para smoke-test o diagnóstico rápido
bronze_only_job = define_asset_job(
    name        = "ocm_bronze_only_job",
    selection   = AssetSelection.groups("bronze"),
    description = "Solo ingesta desde exchanges — sin repair ni resample.",
)

# ==============================================================================
# Schedules
# ==============================================================================
#
# Dagster no está diseñado para schedules sub-minuto.
# bronze_schedule corre cada minuto como el schedule original de Prefect.
# ingestion_schedule corre cada 5 minutos (pipeline completo).
# Ajustar cron_schedule según rate-limits reales del exchange.

bronze_schedule = ScheduleDefinition(
    name          = "ocm_bronze_1min",
    job           = bronze_only_job,
    cron_schedule = "* * * * *",
    description   = "Ingesta incremental OHLCV desde exchanges cada minuto.",
)

ingestion_schedule = ScheduleDefinition(
    name          = "ocm_ingestion_5min",
    job           = ingestion_job,
    cron_schedule = "*/5 * * * *",
    description   = "Pipeline completo OCM cada 5 minutos.",
)

# ==============================================================================
# Definitions root — SSOT
# ==============================================================================
#
# OCMResource.env vacío → delega a RunConfig.from_env() → lee OCM_ENV.
# SSOT: el entorno activo es siempre OCM_ENV, nunca hardcodeado aquí.
# Fail-Fast: entorno inválido explota en setup_for_execution(), no mid-run.

defs = Definitions(
    assets = [
        *BRONZE_OHLCV_ASSETS,
        *REPAIR_OHLCV_ASSETS,
        *RESAMPLE_OHLCV_ASSETS,
    ],
    resources = {
        # DIP: los assets no construyen AppConfig directamente.
        # OCMResource es el único punto de acceso al RuntimeContext.
        # env="" → lee OCM_ENV del entorno (SSOT, no hardcoding).
        "ocm": OCMResource(env=os.environ.get("OCM_ENV", "")),
    },
    asset_checks = ALL_ASSET_CHECKS,
    jobs         = [ingestion_job, bronze_only_job],
    schedules    = [bronze_schedule, ingestion_schedule],
)
