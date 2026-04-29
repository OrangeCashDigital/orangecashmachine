"""
deploy.py
=========

Registra y ejecuta el deployment de ``market_data_flow`` en Prefect 3.

Prefect 3 usa ``flow.serve()`` para desarrollo local o ``flow.deploy()``
para producción con work pools. Este script usa ``serve()`` para
desarrollo — en producción Prefect Worker gestiona el scheduling.

Uso
---
    # Desarrollo local (bloqueante, corre en foreground):
    uv run python deploy.py

    # Modo producción (registra en work pool):
    uv run python deploy.py --prod

Principios: DRY · SSOT · KISS — build_context() como SSOT de construcción.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone

from ocm_platform.control_plane.orchestration.entrypoint import build_context
from ocm_platform.control_plane.orchestration.flows.batch_flow import market_data_flow
from ocm_platform.config.hydra_loader import load_appconfig_standalone
from ocm_platform.runtime.context import RuntimeContext
from ocm_platform.runtime.run_config import RunConfig


# ---------------------------------------------------------------------------
# Context builder para deploy — análogo a build_context() pero sin Hydra
# ---------------------------------------------------------------------------

def _build_deploy_context() -> RuntimeContext:
    """Construye RuntimeContext para ejecución de deploy.

    Usa ``load_appconfig_standalone`` (sin Hydra) y omite snapshot de auditoría
    — deploy no es un run auditado.

    Returns:
        :class:`RuntimeContext` inmutable listo para inyectar al flow.
    """
    run_cfg = RunConfig.from_env()
    config  = load_appconfig_standalone(
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


# ---------------------------------------------------------------------------
# Modos de ejecución
# ---------------------------------------------------------------------------

def serve_local() -> None:
    """Modo desarrollo: ejecuta el flow localmente con RuntimeContext canónico."""
    ctx = _build_deploy_context()
    print(
        f"[deploy] serve_local | env={ctx.environment} run_id={ctx.run_id}"
    )
    asyncio.run(market_data_flow(ctx))


def deploy_prod() -> None:
    """Modo producción: registra deployment y ejecuta con RuntimeContext canónico."""
    ctx = _build_deploy_context()
    print(
        f"[deploy] deploy_prod | env={ctx.environment} run_id={ctx.run_id}"
    )
    asyncio.run(market_data_flow(ctx))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="OrangeCashMachine — Prefect deployment runner",
    )
    parser.add_argument(
        "--prod",
        action="store_true",
        help="Registrar en work pool (producción)",
    )
    args = parser.parse_args()

    if args.prod:
        deploy_prod()
    else:
        serve_local()


if __name__ == "__main__":
    main()
