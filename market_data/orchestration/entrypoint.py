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

import hashlib
import json
import time as _time
import uuid
from datetime import datetime, timezone

from core.logging.setup import bind_pipeline
from core.config.lineage import get_git_hash
from core.config.run_registry import record_run
from core.config.runtime import RunConfig
from core.config.loader import load_config
from core.config.schema import AppConfig
from market_data.orchestration.flows.batch_flow import market_data_flow
from market_data.safety.execution_guard import ExecutionGuard, ExecutionStoppedError
from market_data.safety import guard_context
from market_data.orchestration.post_processing import PostProcessingService
from infra.observability.server import push_metrics

_log = bind_pipeline("entrypoint")


async def _run_flow_local(config: AppConfig, run_cfg: RunConfig, guard: ExecutionGuard) -> None:
    config_dir = str(Path("config").resolve())
    log = _log.bind(mode="local", env=run_cfg.env, config_dir=config_dir)

    guard.check()  # verifica kill switch antes de lanzar el flow
    log.info("flow_launching", flow="market_data_flow")
    await market_data_flow(env=run_cfg.env, config_dir=config_dir)
    log.info("flow_completed", flow="market_data_flow")
    # PostProcessingService se ejecuta en el finally de run() — fuera del timeout


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

    guard = ExecutionGuard(
        max_errors    = getattr(getattr(config, "pipeline", None), "max_consecutive_errors", 10),
        max_runtime_s = getattr(getattr(getattr(config, "pipeline", None), "timeouts", None), "historical_pipeline", 0) or 0,
    )
    guard.start()
    guard_context.set_guard(guard)  # disponible para batch_flow sin pasar por Prefect
    log.debug("execution_guard_started", max_errors=guard.max_errors, max_runtime_s=guard.max_runtime_s)

    git_hash    = get_git_hash()
    config_hash = hashlib.sha256(
        json.dumps(config.model_dump(mode="json"), sort_keys=True, default=str).encode()
    ).hexdigest()[:12]

    log.info(
        "pipeline_starting",
        exchanges=config.exchange_names,
        git_hash=git_hash,
        config_hash=config_hash,
    )

    timeout   = config.pipeline.timeouts.historical_pipeline
    run_id    = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
    exit_code = 0
    _t0       = _time.monotonic()
    try:
        asyncio.run(
            asyncio.wait_for(
                _run_flow_local(config, run_cfg, guard=guard),
                timeout=timeout,
            )
        )
    except asyncio.TimeoutError:
        log.error("pipeline_timeout", timeout_s=timeout)
        exit_code = 1
    except ExecutionStoppedError as exc:
        log.critical("pipeline_stopped_by_guard", reason=str(exc))
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
        _duration = _time.monotonic() - _t0
        _result   = {0: "success", 1: "error", 130: "interrupted"}.get(exit_code, "unknown")
        guard.stop()
        guard_context.set_guard(None)  # limpiar contexto de proceso
        _guard_summary = guard.summary()
        log.info("execution_guard_summary", **_guard_summary)
        record_run(
            run_id=run_id,
            env=run_cfg.env,
            git_hash=git_hash,
            config_hash=config_hash,
            exchanges=config.exchange_names,
            result=_result,
            duration_s=_duration,
            extra={"guard": _guard_summary},
        )
        if exit_code != 130:
            # Post-processing fuera del timeout: Gold con datos parciales > Gold vacío
            PostProcessingService(config, run_id=run_id).execute()
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
