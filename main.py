from __future__ import annotations

"""
main.py – Entrypoint principal de OrangeCashMachine

Flujo de ejecución:
    hydra_main(cfg)                          ← Hydra compone los YAMLs
        ├── bootstrap_logging()
        ├── load_appconfig_from_hydra(cfg)   → AppConfig (Pydantic validado)
        ├── configure_logging(cfg, env, ...)
        ├── setup_observability()            → MetricsRuntime (idempotente)

        ├── validate_only? → sys.exit(0)
        └── pipeline_runner()               → 🔥 lógica real

Uso CLI:
    uv run python main.py                                         # development
    uv run python main.py env=production                          # producción
    uv run python main.py pipeline.historical.backfill_mode=true  # backfill
    uv run python main.py pipeline.historical.max_concurrent_tasks=8
    uv run python main.py --cfg job                               # ver config efectivo

Principios: SOLID · KISS · DRY · SafeOps
"""

import os
import sys
import uuid
from typing import Callable

import hydra
from omegaconf import DictConfig
from loguru import logger

import core.config.loader as config_loader
from core.config.hydra_loader import load_appconfig_from_hydra
from core.config.schema import AppConfig
from core.logging import bootstrap_logging, configure_logging
from market_data.orchestration.entrypoint import run as default_pipeline_runner
from infra.observability.runtime import init_metrics_runtime


# ---------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------
def setup_observability(config: AppConfig, *, validate_only: bool = False) -> None:
    """Inicializa MetricsRuntime — idempotente, fail-soft."""
    try:
        init_metrics_runtime(config, validate_only=validate_only)
    except Exception as exc:
        logger.warning("observability_init_failed | error={}", exc)


# ---------------------------------------------------------------------
# Application lifecycle
# ---------------------------------------------------------------------
def run_application(
    config: AppConfig,
    *,
    run_id: str,
    env: str,
    debug: bool = False,
    validate_only: bool = False,
    pipeline_runner: Callable = default_pipeline_runner,
) -> int:

    # 1. Logging completo — firma: configure_logging(cfg, env, debug, run_id)
    try:
        configure_logging(config.observability.logging, env=env, debug=debug, run_id=run_id)
    except Exception as exc:
        logger.warning("logging_configure_failed | error={}", exc)

    log = logger.bind(run_id=run_id, env=env)
    log.info("application_starting | env={} run_id={} debug={}", env, run_id, debug)

    # 2. Observabilidad
    setup_observability(config, validate_only=validate_only)

    # 3. Validación del entorno
    # 3. Validación del entorno (credenciales en producción)
    if env == "production":
        missing = [ex.name for ex in config.exchanges if ex.enabled and not ex.has_credentials]
        if missing:
            log.critical("environment_validation_failed | missing_credentials={}", missing)
            return 1

    # 4. Modo validación — salida anticipada sin pipeline
    if validate_only:
        log.info("validation_complete | status=ok")
        return 0

    # 5. Pipeline (🔥 lógica de negocio)
    result = pipeline_runner(config=config, debug=debug)

    if not isinstance(result, int):
        log.error("pipeline_runner_returned_non_int | value={}", result)
        return 1

    return result


# ---------------------------------------------------------------------
# Hydra entry point
# ---------------------------------------------------------------------
@hydra.main(config_path="config", config_name="config", version_base="1.3")
def hydra_main(cfg: DictConfig) -> None:
    """
    Entry point de Hydra.
    Hydra compone: base.yaml → env/{env}.yaml → settings.yaml
    y resuelve todas las interpolaciones ${oc.env:VAR,default}.
    """
    bootstrap_logging()

    # Leer env/debug/flags desde config compuesto o env vars de override
    env_block    = cfg.get("environment", {})
    env          = str(env_block.get("name", None) or os.getenv("OCM_ENV", "development"))
    debug        = bool(env_block.get("debug", False)) or \
                   os.getenv("OCM_DEBUG", "").lower() in ("1", "true")
    validate_only = os.getenv("OCM_VALIDATE_ONLY", "").lower() in ("1", "true")
    run_id       = uuid.uuid4().hex[:12]

    try:
        config = load_appconfig_from_hydra(cfg, env=env)
    except Exception as exc:
        logger.opt(exception=True).critical(
            "config_load_failed | type={} error={}", type(exc).__name__, exc
        )
        sys.exit(1)

    exit_code = run_application(
        config,
        run_id=run_id,
        env=env,
        debug=debug,
        validate_only=validate_only,
    )
    sys.exit(exit_code)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    try:
        hydra_main()
    except KeyboardInterrupt:
        logger.warning("execution_interrupted")
        sys.exit(130)
    except (
        config_loader.ConfigurationError,
        config_loader.ConfigValidationError,
    ) as exc:
        logger.opt(exception=True).critical(
            "config_failure | type={} error={}", type(exc).__name__, exc
        )
        sys.exit(1)
    except Exception as exc:
        logger.exception(
            "critical_failure | type={} error={}", type(exc).__name__, exc
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
