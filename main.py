from __future__ import annotations

"""
main.py – Entrypoint principal de OrangeCashMachine

Responsabilidad: orquestar arranque → config → logging → pipeline.
No contiene lógica de negocio.

Principios: SOLID · KISS · DRY · SafeOps
"""

import sys
import uuid
from typing import Optional

from loguru import logger

import core.config.runtime
import core.config.loader
from core.config.loader.env_resolver import bootstrap_dotenv
from core.config.schema import AppConfig
from core.logging import bootstrap_logging, configure_logging
from market_data.orchestration.entrypoint import run as run_pipeline
from services.observability.metrics import start_metrics_server


# ---------------------------------------------------------------------
# Bootstrap helpers
# ---------------------------------------------------------------------
def build_run_config(explicit: Optional[core.config.runtime.RunConfig] = None) -> core.config.runtime.RunConfig:
    if explicit:
        return explicit
    bootstrap_dotenv()
    return core.config.runtime.RunConfig.from_env()


def log_runtime_context(run_cfg: core.config.runtime.RunConfig) -> None:
    logger.debug(
        "run_config_resolved",
        env=run_cfg.env,
        debug=run_cfg.debug,
        config_path=str(run_cfg.config_path) if run_cfg.config_path else None,
        pushgateway=run_cfg.pushgateway,
        source_priority="explicit>env_var>dotenv>settings_yaml>default",
    )


def initialize_config(run_cfg: core.config.runtime.RunConfig) -> AppConfig:
    try:
        return core.config.loader.load_config(env=run_cfg.env, path=run_cfg.config_path)
    except (core.config.loader.ConfigurationError, core.config.loader.ConfigValidationError):
        raise
    except Exception:
        logger.opt(exception=True).critical("unexpected_config_error")
        raise


def setup_observability(config: AppConfig, bound_logger) -> None:
    if not config.observability.metrics.enabled:
        return
    try:
        start_metrics_server(port=config.observability.metrics.port)
        bound_logger.info("metrics_server_started", port=config.observability.metrics.port)
    except OSError as exc:
        bound_logger.warning("metrics_server_failed", error=str(exc))


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main(run_cfg: Optional[core.config.runtime.RunConfig] = None) -> int:
    """
    Punto de entrada principal.

    Returns
    -------
    int
        0 → éxito
        1 → error crítico
        130 → SIGINT
    """
    run_cfg = build_run_config(run_cfg)
    run_id  = uuid.uuid4().hex[:12]

    try:
        # 1. Logging base (antes de TODO)
        bootstrap_logging(debug=run_cfg.debug, run_id=run_id, env=run_cfg.env)

        # 2. Contexto de runtime
        log_runtime_context(run_cfg)

        # 3. Configuración
        config = initialize_config(run_cfg)

        # 4. Logging final desde YAML (reemplaza sinks de Fase 1)
        configure_logging(
            cfg=config.observability.logging,
            env=run_cfg.env,
            debug=run_cfg.debug,
            run_id=run_id,
        )

        # 5. Logger contextual
        log = logger.bind(run_id=run_id)

        log.info(
            "app_started",
            env=run_cfg.env,
            debug=run_cfg.debug,
            exchanges=config.exchange_names,
        )

        # 6. Observabilidad
        setup_observability(config, log)

        # 7. Pipeline
        timeout = config.pipeline.timeouts.historical_pipeline
        log.info("pipeline_started", timeout_s=timeout, run_id=run_id)

        return run_pipeline(config=config, run_cfg=run_cfg, debug=run_cfg.debug)

    except KeyboardInterrupt:
        logger.warning("execution_interrupted", run_id=run_id)
        return 130

    except (core.config.loader.ConfigurationError, core.config.loader.ConfigValidationError) as exc:
        logger.opt(exception=True).critical(
            "config_failure",
            error_type=type(exc).__name__,
            error=str(exc),
            run_id=run_id,
        )
        return 1

    except Exception as exc:
        logger.exception(
            "critical_failure",
            error_type=type(exc).__name__,
            error=str(exc),
            run_id=run_id,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
