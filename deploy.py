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
import sys


def serve_local() -> None:
    """Modo desarrollo: flow.serve() — bloqueante, sin work pool."""
    from market_data.orchestration.flows.batch_flow import market_data_flow

    print("Iniciando market_data_flow en modo serve (desarrollo)...")
    market_data_flow.serve(
        name="market-data-hourly",
        cron="0 * * * *",
        parameters={
            "env": "development",
            "config_dir": "config",
        },
        tags=["market-data", "development"],
    )


def deploy_prod() -> None:
    """Modo producción: flow.deploy() — requiere work pool activo."""
    from market_data.orchestration.flows.batch_flow import market_data_flow

    market_data_flow.deploy(
        name="market-data-hourly",
        work_pool_name="local-pool",
        cron="0 * * * *",
        parameters={
            "env": "production",
            "config_dir": "/app/config",
        },
        tags=["market-data", "production"],
    )
    print("Deployment registrado en work pool 'local-pool'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prod", action="store_true",
                        help="Registrar en work pool (producción)")
    args = parser.parse_args()

    if args.prod:
        deploy_prod()
    else:
        serve_local()
