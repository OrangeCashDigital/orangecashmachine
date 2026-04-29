"""
deploy.py — Registra el deployment de market_data_flow en Prefect 3.

Prefect 3 usa flow.serve() para desarrollo local o flow.deploy()
para producción con work pools. Este script usa serve() para
desarrollo — en producción Prefect Worker gestiona el scheduling.

Uso
---
  # Desarrollo local (bloqueante, corre en foreground):
  uv run python deploy.py

  # Producción (registrar en work pool):
  uv run python deploy.py --prod
"""

from __future__ import annotations

import argparse


def serve_local() -> None:
    """Modo desarrollo: ejecuta el flow localmente con runtime_context."""
    import asyncio
    from market_data.orchestration.flows.batch_flow import market_data_flow
    from ocm_platform.runtime.run_config import RunConfig
    from ocm_platform.config.hydra_loader import load_appconfig_standalone
    from ocm_platform.runtime.context import RuntimeContext
    from datetime import datetime, timezone
    from uuid import uuid4

    print(
        "Iniciando market_data_flow en modo serve (desarrollo) con runtime_context..."
    )
    # Construcción del contexto único de ejecución
    run_cfg = RunConfig.from_env()
    config = load_appconfig_standalone(
        env=run_cfg.env,
        config_dir=run_cfg.config_path,
        run_id=run_cfg.run_id,  # deploy: run_id desde RunConfig
        write_snapshot=False,   # deploy no es run auditado — snapshot omitido deliberadamente
    )
    started_at = datetime.now(timezone.utc)
    runtime_context = RuntimeContext(
        app_config=config,
        run_config=run_cfg,
        started_at=started_at,
    )
    # run_id y environment son @property delegadas a run_config (SSOT)
    # Ejecutar el flow directamente con el runtime_context
    asyncio.run(market_data_flow(runtime_context))


def deploy_prod() -> None:
    """Modo producción adaptado: registra deployment y ejecuta con runtime_context (prueba)."""
    import asyncio
    from market_data.orchestration.flows.batch_flow import market_data_flow
    from ocm_platform.runtime.run_config import RunConfig
    from ocm_platform.config.hydra_loader import load_appconfig_standalone
    from ocm_platform.runtime.context import RuntimeContext
    from datetime import datetime, timezone
    from uuid import uuid4

    # Construcción del contexto para ejecución real (test de despliegue)
    run_cfg = RunConfig.from_env()
    config = load_appconfig_standalone(
        env=run_cfg.env,
        config_dir=run_cfg.config_path,
        run_id=run_cfg.run_id,  # deploy: run_id desde RunConfig
        write_snapshot=False,   # deploy no es run auditado — snapshot omitido deliberadamente
    )
    started_at = datetime.now(timezone.utc)
    runtime_context = RuntimeContext(
        app_config=config,
        run_config=run_cfg,
        started_at=started_at,
    )
    # run_id y environment son @property delegadas a run_config (SSOT)

    print(
        "Desplegando market_data_flow con runtime_context (producción) para pruebas..."
    )
    asyncio.run(market_data_flow(runtime_context))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--prod", action="store_true", help="Registrar en work pool (producción)"
    )
    args = parser.parse_args()

    if args.prod:
        deploy_prod()
    else:
        serve_local()
