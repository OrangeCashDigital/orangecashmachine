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

from core.config.runtime import RunConfig
from core.config.loader import (
    load_config,
    ConfigurationError,
    ConfigValidationError,
)
from core.config.loader.env_resolver import bootstrap_dotenv
from core.config.schema import AppConfig
from core.logging import setup_logging
from market_data.orchestration.entrypoint import run as run_pipeline
from services.observability.metrics import start_metrics_server


# ---------------------------------------------------------------------
# Bootstrap helpers
# ---------------------------------------------------------------------
def build_run_config(explicit: Optional[RunConfig] = None) -> RunConfig:
    """
    Construye RunConfig de forma consistente y aislada.
    """
    if explicit:
        return explicit

    bootstrap_dotenv()
    return RunConfig.from_env()


def log_runtime_context(run_cfg: RunConfig) -> None:
    """
    Log estructurado del contexto de ejecución.
    """
    logger.debug(
        "config_source_priority | 1=explicit 2=env_var 3=dotenv 4=settings_yaml 5=default"
    )
    logger.debug(
        "run_config_resolved | env={} debug={} config_path={} pushgateway={}",
        run_cfg.env,
        run_cfg.debug,
        run_cfg.config_path,
        run_cfg.pushgateway,
    )


def initialize_config(run_cfg: RunConfig) -> AppConfig:
    """
    Carga y valida la configuración central del sistema.
    """
    try:
        return load_config(env=run_cfg.env, path=run_cfg.config_path)
    except (ConfigurationError, ConfigValidationError):
        raise
    except Exception:
        logger.opt(exception=True).critical("unexpected_config_error")
        raise


def setup_observability(config: AppConfig, bound_logger) -> None:
    """
    Inicializa métricas (no bloqueante).
    """
    if not config.observability.metrics.enabled:
        return

    try:
        start_metrics_server(port=config.observability.metrics.port)
        bound_logger.info(
            "metrics_server_started | port={}",
            config.observability.metrics.port,
        )
    except OSError as exc:
        bound_logger.warning(
            "metrics_server_failed | error={}", exc
        )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main(run_cfg: Optional[RunConfig] = None) -> int:
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

    run_id = uuid.uuid4().hex[:12]

    try:
        # 1. Logging base (antes de TODO)
        setup_logging(debug=run_cfg.debug, run_id=run_id)

        # 2. Contexto de runtime (debug crítico)
        log_runtime_context(run_cfg)

        # 3. Configuración
        config = initialize_config(run_cfg)

        # 4. Logging final desde YAML
        setup_logging(cfg=config.observability.logging, debug=run_cfg.debug)

        # 5. Logger contextual
        log = logger.bind(trace_id=run_id)

        log.info(
            "app_started | env={} debug={} exchanges={}",
            run_cfg.env,
            run_cfg.debug,
            config.exchange_names,
        )

        # 6. Observabilidad
        setup_observability(config, log)

        # 7. Pipeline
        timeout = config.pipeline.timeouts.historical_pipeline
        log.info(
            "pipeline_started | timeout_seconds={} run_id={}",
            timeout,
            run_id,
        )

        return run_pipeline(config=config, debug=run_cfg.debug)

    except KeyboardInterrupt:
        logger.warning(
            "execution_interrupted | run_id={}", run_id
        )
        return 130

    except (ConfigurationError, ConfigValidationError) as exc:
        logger.opt(exception=True).critical(
            "config_failure | type={} error={} run_id={}",
            type(exc).__name__,
            exc,
            run_id,
        )
        return 1

    except Exception as exc:
        logger.exception(
            "critical_failure | type={} error={} run_id={}",
            type(exc).__name__,
            exc,
            run_id,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())