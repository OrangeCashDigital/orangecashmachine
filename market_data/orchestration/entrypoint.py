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
import hashlib
import json
import signal
import sys
import time as _time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from core.observability import bind_pipeline
from core.config.lineage import get_git_hash
from core.config.run_registry import record_run
from core.config.runtime import RunConfig
from core.config.hydra_loader import load_appconfig_standalone
from core.config.schema import AppConfig
from market_data.orchestration.flows.batch_flow import market_data_flow
from market_data.safety.execution_guard import ExecutionGuard, ExecutionStoppedError
from market_data.safety import guard_context
from market_data.orchestration.post_processing import PostProcessingService
from market_data.ports.observability import MetricsPusherPort  # noqa: F401  # TODO(ports): inject via run(pusher=MetricsPusherPort)
from infra.observability.server        import push_metrics
from core.config.runtime_context import RuntimeContext

_log = bind_pipeline("entrypoint")


def build_context():
    """Central context builder.

    Reads configuration via the Hydra standalone path and builds the runtime
    context by resolving AppConfig and RunConfig in a single place.
    This function unifies the two startup paths into one canonical entry point
    for local execution, without touching the flow implementation yet.
    """
    run_cfg = RunConfig.from_env()
    config = load_appconfig_standalone(
        env=run_cfg.env,
        config_dir=run_cfg.config_path,
        run_id=run_cfg.run_id,  # SSOT: run_id generado por RunConfig
    )
    started_at = datetime.now(timezone.utc)
    runtime_context = RuntimeContext(
        app_config=config,
        run_config=run_cfg,
        started_at=started_at,
    )
    # run_id y environment son @property delegadas a run_config (SSOT)
    return config, run_cfg, runtime_context


async def _run_flow_local(
    config: AppConfig,
    run_cfg: RunConfig,
    runtime_context: RuntimeContext | None,
    guard: ExecutionGuard,
) -> None:
    config_dir = str(Path("config").resolve())
    log = _log.bind(mode="local", env=run_cfg.env, config_dir=config_dir)

    guard.check()  # verifica kill switch antes de lanzar el flow
    log.info("flow_launching", flow="market_data_flow")
    # Si no hay runtime_context, construirlo a partir del entrypoint
    if runtime_context is None:
        _, _, runtime_context = build_context()
    await market_data_flow(runtime_context)
    log.info("flow_completed", flow="market_data_flow")
    # PostProcessingService se ejecuta en el finally de run() — fuera del timeout


def run(
    config: AppConfig,
    run_cfg: RunConfig,
    runtime_context: RuntimeContext | None = None,
    debug: bool = False,
) -> int:
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
        max_errors=config.pipeline.max_consecutive_errors,
        max_runtime_s=config.pipeline.timeouts.historical_pipeline,
    )
    guard.start()
    guard_context.set_guard(guard)  # disponible para batch_flow sin pasar por Prefect
    log.debug(
        "execution_guard_started",
        max_errors=guard.max_errors,
        max_runtime_s=guard.max_runtime_s,
    )

    git_hash = get_git_hash()
    config_hash = hashlib.sha256(
        json.dumps(config.model_dump(mode="json"), sort_keys=True, default=str).encode()
    ).hexdigest()[:12]

    log.info(
        "pipeline_starting",
        exchanges=config.exchange_names,
        git_hash=git_hash,
        config_hash=config_hash,
    )

    timeout = config.pipeline.timeouts.historical_pipeline
    # SSoT: usar el run_id del RuntimeContext si ya fue resuelto por
    # entrypoint/main.py. Generar uno nuevo solo si run() se invoca
    # en aislamiento (tests, CLI directo sin contexto previo).
    run_id = (
        runtime_context.run_id
        if runtime_context is not None
        else f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
    )
    exit_code = 0
    _t0 = _time.monotonic()
    # ── Event loop explícito para shutdown graceful vía SIGINT/SIGTERM ──────
    # asyncio.run(wait_for(...)) no permite cancelar el task raíz antes de
    # cerrar el loop: las tareas hijas en vuelo terminan como Exception en
    # lugar de CancelledError, produciendo logs "Strategy fallida" espurios.
    # Con loop explícito instalamos signal handlers que cancelan el task raíz
    # limpiamente y esperan a que todas las coroutines terminen su finally.
    loop = asyncio.new_event_loop()
    main_task: "asyncio.Task[None] | None" = None

    def _request_shutdown(sig: int) -> None:
        sig_name = signal.Signals(sig).name
        log.warning("pipeline_interrupted", signal=sig_name)
        nonlocal exit_code
        exit_code = 130
        if main_task and not main_task.done():
            main_task.cancel()

    try:
        try:
            asyncio.set_event_loop(loop)
            for _sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(_sig, _request_shutdown, _sig)

            main_task = loop.create_task(
                asyncio.wait_for(
                    _run_flow_local(config, run_cfg, runtime_context, guard=guard),
                    timeout=timeout,
                )
            )
            loop.run_until_complete(main_task)

        except asyncio.TimeoutError:
            log.error("pipeline_timeout", timeout_s=timeout)
            exit_code = 1
        except asyncio.CancelledError:
            # Cancelación limpia vía signal handler — exit_code ya seteado en 130
            pass
        except ExecutionStoppedError as exc:
            log.critical("pipeline_stopped_by_guard", reason=str(exc))
            exit_code = 1
        except Exception as exc:
            log.opt(exception=True).critical(
                "pipeline_fatal",
                error_type=type(exc).__name__,
                error=str(exc),
            )
            exit_code = 1
        finally:
            # Cancelar tareas residuales y esperar su cleanup antes de cerrar loop
            _pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
            if _pending:
                log.debug("shutdown_cancelling_tasks", count=len(_pending))
                for t in _pending:
                    t.cancel()
                loop.run_until_complete(
                    asyncio.gather(*_pending, return_exceptions=True)
                )
            loop.close()
            asyncio.set_event_loop(None)
    finally:
        _duration = _time.monotonic() - _t0
        _result = {0: "success", 1: "error", 130: "interrupted"}.get(
            exit_code, "unknown"
        )
        guard.stop()
        guard_context.set_guard(None)  # limpiar contexto de proceso
        _guard_summary = guard.summary()
        log.info("execution_guard_summary", **_guard_summary)
        if exit_code != 130:
            # Post-processing fuera del timeout: Gold con datos parciales > Gold vacío
            PostProcessingService(config, run_id=run_id).execute()
            if config.observability.metrics.enabled:
                for ex in config.exchanges:
                    push_metrics(exchange=ex.name.value, gateway=run_cfg.pushgateway)
            else:
                log.debug("metrics_push_skipped", reason="metrics.enabled=false")
        else:
            log.debug("metrics_push_skipped", reason="SIGINT")
        # record_run al final del ciclo de vida: captura duración real incluyendo
        # post-processing y push de métricas. Envuelto en try para que un fallo
        # de persistencia no corte el log de cierre.
        try:
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
            log.info(
                "run_recorded | run_id={} result={} duration_s={:.1f}",
                run_id,
                _result,
                _duration,
            )
        except Exception as _rec_exc:
            log.warning("record_run_failed | run_id={} error={}", run_id, _rec_exc)
        log.info("shutdown_complete", exit_code=exit_code)

    return exit_code


if __name__ == "__main__":
    from core.observability import bootstrap_logging, configure_logging

    # Build centralized context using canonical resolver
    config, run_cfg, runtime_context = build_context()
    bootstrap_logging(debug=run_cfg.debug, env=run_cfg.env)
    configure_logging(
        cfg=config.observability.logging, env=run_cfg.env, debug=run_cfg.debug
    )
    sys.exit(
        run(
            config=config,
            run_cfg=run_cfg,
            runtime_context=runtime_context,
            debug=run_cfg.debug,
        )
    )
