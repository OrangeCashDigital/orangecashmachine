# -*- coding: utf-8 -*-
"""
dagster_defs.py
================

Punto de entrada único de Dagster para OrangeCashMachine.

SSOT de definiciones — Definitions es el único lugar donde se ensamblan:
  assets, resources, schedules, sensors, jobs.

Uso
---
  # Iniciar Dagster UI (equivalente a Prefect Server):
  uv run dagster dev -f dagster_defs.py

  # Materializar todos los assets manualmente:
  uv run dagster asset materialize -f dagster_defs.py --select "*"

  # Solo bronze:
  uv run dagster asset materialize -f dagster_defs.py --select "bronze/*"

Concurrencia
------------
Los tags dagster/concurrency_key limitan ejecuciones paralelas.
Configurar en dagster.yaml (ver abajo en DAGSTER_YAML_TEMPLATE).

Principios: SSOT · OCP · DIP · KISS
"""
from __future__ import annotations

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)

from dagster_assets.resources import OCMResource
from dagster_assets.bronze_ohlcv import BRONZE_OHLCV_ASSETS
from dagster_assets.repair_ohlcv import REPAIR_OHLCV_ASSETS
from dagster_assets.resample_ohlcv import RESAMPLE_OHLCV_ASSETS
from dagster_assets.asset_checks import ALL_ASSET_CHECKS

# ==============================================================================
# Jobs — agrupan assets para ejecución coordinada
# ==============================================================================

# Job principal: ingesta completa bronze → repair → resample
ingestion_job = define_asset_job(
    name      = "ocm_ingestion_job",
    selection = AssetSelection.groups("bronze", "silver"),
    description = (
        "Job completo de ingesta OCM: "
        "bronze (exchange) → repair (gaps) → silver (resample)."
    ),
)

# Job solo bronze — útil para smoke-test o diagnóstico
bronze_only_job = define_asset_job(
    name      = "ocm_bronze_only_job",
    selection = AssetSelection.groups("bronze"),
    description = "Solo ingesta desde exchanges — sin repair ni resample.",
)

# ==============================================================================
# Schedules — equivalente al schedule de Prefect (cada minuto)
# ==============================================================================

# NOTA: Prefect corría market_data_flow cada 60s.
# Dagster no está diseñado para schedules sub-minuto.
# Recomendación: 1m para bronze (incremental), 5m para repair.
# Ajustar según el exchange rate-limit real.

bronze_schedule = ScheduleDefinition(
    name             = "ocm_bronze_1min",
    job              = bronze_only_job,
    cron_schedule    = "* * * * *",   # cada minuto — igual que Prefect
    description      = "Ingesta incremental OHLCV desde exchanges cada minuto.",
)

ingestion_schedule = ScheduleDefinition(
    name             = "ocm_ingestion_5min",
    job              = ingestion_job,
    cron_schedule    = "*/5 * * * *",  # cada 5 minutos — bronze + repair + resample
    description      = "Pipeline completo OCM cada 5 minutos.",
)

# ==============================================================================
# Definitions root — SSOT
# ==============================================================================

defs = Definitions(
    assets   = [
        *BRONZE_OHLCV_ASSETS,
        *REPAIR_OHLCV_ASSETS,
        *RESAMPLE_OHLCV_ASSETS,
    ],
    resources = {
        # DIP: los assets no construyen AppConfig directamente.
        # OCMResource es el único punto de acceso al RuntimeContext.
        "ocm": OCMResource(env="development"),
    },
    asset_checks = ALL_ASSET_CHECKS,
    jobs      = [ingestion_job, bronze_only_job],
    # Schedules ACTIVOS — Prefect schedule desactivado en parallel.
    # SSOT: dagster_defs.py es la única fuente de schedules activos.
    schedules = [bronze_schedule, ingestion_schedule],
)


# ==============================================================================
# DAGSTER_YAML_TEMPLATE
# ==============================================================================
# Crear dagster.yaml en la raíz del proyecto con este contenido
# para activar concurrencia declarativa (Fase 3).
#
# cat > dagster.yaml << 'YAML'
# concurrency:
#   default_op_concurrency_limit: 4   # reemplaza _PIPELINE_SEMAPHORE = asyncio.Semaphore(4)
#
# run_coordinator:
#   module: dagster.core.run_coordinator
#   class: QueuedRunCoordinator
#   config:
#     max_concurrent_runs: 2          # max runs paralelos del schedule
#
# YAML
