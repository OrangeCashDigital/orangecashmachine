from __future__ import annotations

"""
market_data/orchestration/entrypoint.py
========================================

Entrypoint LOCAL — solo para desarrollo y debug.

En producción el flow es disparado por Prefect Server/Worker
via deployment. Este archivo NO forma parte del path de producción.

Responsabilidad
---------------
Orquestar arranque del flow local + post-procesado.
No conoce detalles de storage ni de features.
"""

import asyncio
import sys
from pathlib import Path

from core.logging.setup import bind_pipeline
from core.config.runtime import RunConfig
from core.config.loader import load_config
from core.config.schema import AppConfig
from market_data.orchestration.flows.batch_flow import market_data_flow
from market_data.orchestration.post_processing import PostProcessingService
from services.observability.metrics import push_metrics

_log = bind_pipeline("entrypoint")


async def _run_flow_local(config: AppConfig, run_cfg: RunConfig) -> None:
    config_dir = str(Path("config").resolve())
    log = _log.bind(mode="local", env=run_cfg.env, config_dir=config_dir)

    log.info("flow_launching", flow="market_data_flow")
    await market_data_flow(env=run_cfg.env, config_dir=config_dir)
    log.info("flow_completed", flow="market_data_flow")

    PostProcessingService(config).execute()


def run(config: AppConfig, run_cfg: RunConfig, debug: bool = False) -> int:
    """
    Ejecuta el pipeline en modo local.

    Parameters
    ----------
    config  : AppConfig
        Configuración de aplicación ya cargada y validada.
    run_cfg : RunConfig
        Runtime ya resuelto por main.py — no se recrea aquí.
    debug   : bool
        Flag de debug resuelto por RunConfig.

    Returns
    -------
    int
        0 → éxito, 1 → error crítico, 130 → interrumpido (SIGINT).
    """
    log = _log.bind(mode="local", env=run_cfg.env)

    log.info(
        "pipeline_starting",
        exchanges=config.exchange_names,
    )

    timeout   = config.pipeline.timeouts.historical_pipeline
    exit_code = 0
    try:
        asyncio.run(
            asyncio.wait_for(
                _run_flow_local(config, run_cfg),
                timeout=timeout,
            )
        )
    except asyncio.TimeoutError:
        log.error("pipeline_timeout", timeout_s=timeout)
        exit_code = 1
    except KeyboardInterrupt:
        log.warning("pipeline_interrupted", signal="SIGINT")
        exit_code = 130
    except Exception as exc:
        log.opt(exception=True).critical(
            "pipeline_fatal",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        exit_code = 1
    finally:
        if exit_code != 130:
            for ex in config.exchanges:
                push_metrics(exchange=ex.name.value, gateway=run_cfg.pushgateway)
        else:
            log.debug("metrics_push_skipped", reason="SIGINT")
        log.info("shutdown_complete", exit_code=exit_code)

    return exit_code


if __name__ == "__main__":
    from core.logging import bootstrap_logging, configure_logging
    _run_cfg = RunConfig.from_env()
    _config  = load_config(env=_run_cfg.env, path=_run_cfg.config_path)
    bootstrap_logging(debug=_run_cfg.debug, env=_run_cfg.env)
    configure_logging(cfg=_config.observability.logging, env=_run_cfg.env, debug=_run_cfg.debug)
    sys.exit(run(config=_config, run_cfg=_run_cfg, debug=_run_cfg.debug))
