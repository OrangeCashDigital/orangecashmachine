"""
ocm_platform/control_plane/orchestration/deploy.py
===================================================

Registra y ejecuta el deployment de ``market_data_flow`` en Prefect 3.

Modos
-----
serve   (default)  Desarrollo local — ejecuta el flow directamente
                   en foreground sin scheduling. Útil para debug.

deploy             Producción — registra el deployment en Prefect Server
                   con IntervalSchedule (1 min) y timeout de 55 s.
                   El Work Pool gestiona la concurrencia:

                       prefect work-pool update ocm-process --concurrency-limit 1

Uso
---
    # Desarrollo local (foreground, sin scheduling):
    uv run python -m ocm_platform.control_plane.orchestration.deploy

    # Producción (registra en Prefect Server):
    uv run python -m ocm_platform.control_plane.orchestration.deploy --deploy

    # Alias via run.sh:
    ./run.sh deploy

Principios
----------
SSOT    — build_context() / _build_deploy_context() como única fuente
          de construcción de RuntimeContext.
KISS    — dos modos claros, flujo lineal, sin abstracciones innecesarias.
Fail-Fast — errores de config explotan aquí, no en el Worker.
SafeOps — deployment.apply() es idempotente.

Concurrencia
------------
``concurrency_limit`` fue eliminado de ``Deployment`` en Prefect post-2024.
Configurar en el Work Pool o vía CLI (ver arriba).

Overlap protection
------------------
``timeout_seconds=55`` garantiza que Prefect cancele runs que superen
el intervalo de 60 s antes de que acumulen backlog. El Worker no lanza
la siguiente ejecución hasta que la anterior termine o sea cancelada.

Referencias
-----------
Prefect 3 deploy:    https://docs.prefect.io/latest/deploy/
Prefect schedules:   https://docs.prefect.io/latest/automate/add-schedules/
Prefect work pools:  https://docs.prefect.io/latest/deploy/infrastructure-concepts/work-pools/
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule

from ocm_platform.control_plane.orchestration.entrypoint import build_context
from ocm_platform.control_plane.orchestration.flows.batch_flow import market_data_flow
from ocm_platform.config.hydra_loader import load_appconfig_standalone
from ocm_platform.runtime.context import RuntimeContext
from ocm_platform.runtime.run_config import RunConfig


# ==============================================================================
# Context builders — SSOT de construcción de RuntimeContext
# ==============================================================================


def _build_deploy_context() -> RuntimeContext:
    """Construye RuntimeContext para registro de deployment.

    Usa ``load_appconfig_standalone`` (sin Hydra activo) y omite snapshot
    de auditoría — el deploy no es un run auditado.

    Fail-Fast: cualquier error de config explota aquí, en deploy-time,
    no en runtime del Worker.

    Returns:
        :class:`RuntimeContext` inmutable listo para serializar como
        parámetro Prefect.
    """
    run_cfg = RunConfig.from_env()
    config = load_appconfig_standalone(
        env=run_cfg.env,
        config_dir=run_cfg.config_path,
        run_id=run_cfg.run_id,   # SSOT: run_id desde RunConfig
        write_snapshot=False,    # deploy no es run auditado — snapshot omitido
    )
    return RuntimeContext(
        app_config=config,
        run_config=run_cfg,
        started_at=datetime.now(timezone.utc),
    )


# ==============================================================================
# Modos de ejecución
# ==============================================================================


def serve_local() -> None:
    """Modo desarrollo: ejecuta el flow directamente en foreground.

    No registra ningún deployment en Prefect Server.
    Usa build_context() del entrypoint canónico (con snapshot de auditoría).
    """
    ctx = build_context()
    print(f"[deploy] serve_local | env={ctx.environment} run_id={ctx.run_id}")
    asyncio.run(market_data_flow(ctx))


def register_deployment() -> None:
    """Modo producción: registra el deployment en Prefect Server.

    El flow NO se ejecuta aquí — Prefect Worker lo dispara según el schedule.

    Decisiones de diseño
    --------------------
    - ``schedules`` (lista): API moderna post-Sep-2024.
      Reemplaza el campo ``schedule`` (scalar, deprecated).

    - ``concurrency_limit``: eliminado de ``Deployment`` en Prefect 3.
      Configurar en el Work Pool:
          prefect work-pool update ocm-process --concurrency-limit 1

    - ``timeout_seconds=55``: Prefect cancela runs que excedan el intervalo
      de 60 s antes de que acumulen backlog. 55 s < 60 s → sin solapamiento.

    SafeOps: ``deployment.apply()`` es idempotente — re-ejecutar actualiza
    el deployment existente sin crear duplicados.
    """
    ctx = _build_deploy_context()
    env = ctx.environment

    deployment = Deployment.build_from_flow(
        flow=market_data_flow,
        name=f"ocm-market-data-{env}",
        work_pool_name="ocm-process",
        # ── Schedule ─────────────────────────────────────────────────────────
        # API moderna (Prefect ≥ 2.16 / 3.x): lista de dicts {schedule, active}.
        # Reemplaza el campo ``schedule`` scalar (deprecated Sep 2024).
        schedules=[
            {
                # MIGRACIÓN Dagster: schedule desactivado.
                # El scheduling ahora lo gestiona Dagster (dagster_defs.py).
                # Mantener aquí como fallback — reactivar si se revierte la migración.
                # SSOT: dagster_defs.py::bronze_schedule es el schedule canónico.
                "schedule": IntervalSchedule(interval=timedelta(minutes=1)),
                "active": False,
            }
        ],
        # ── Parámetros ───────────────────────────────────────────────────────
        # SSOT: RuntimeContext resuelto en deploy-time, serializado como dict.
        # El flow lo deserializa vía RuntimeContext.from_dict().
        parameters={
            "runtime_context": ctx.to_dict(),
        },
        description=(
            f"OCM market data ingestion — env={env}\n"
            "Cursor Redis → backfill si vacío → incremental cada 1 min.\n"
            "Concurrencia: configurar en Work Pool (--concurrency-limit 1)."
        ),
        tags=[f"env:{env}", "ocm", "market-data"],
    )

    deployment.apply()

    print(f"✓ Deployment registrado: ocm-market-data-{env}")
    print("  Pool:     ocm-process")
    print("  Schedule: cada 1 minuto")
    print("  Timeout:  55 s por run")
    print(f"  Env:      {env}")
    print()
    print("  ⚠️  Concurrencia: configurar en el Work Pool:")
    print("      prefect work-pool update ocm-process --concurrency-limit 1")
    print()
    print("  Ver en Prefect UI: http://localhost:4200/deployments")


# ==============================================================================
# CLI
# ==============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="OrangeCashMachine — Prefect deployment runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  ./run.sh serve     # Desarrollo local (foreground)\n"
            "  ./run.sh deploy    # Registrar en Prefect Server (producción)\n"
        ),
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="Registrar deployment en Prefect Server (producción)",
    )
    args = parser.parse_args()

    if args.deploy:
        register_deployment()
    else:
        serve_local()


if __name__ == "__main__":
    main()
