from __future__ import annotations

"""
main.py
=======

Entrypoint principal de OrangeCashMachine.

Flujo de ejecución::

    hydra_main(cfg)
        ├── bootstrap_logging()
        ├── load_appconfig_from_hydra(cfg)   → AppConfig (Pydantic validado)
        ├── configure_logging(cfg, env, ...)
        ├── setup_observability()            → MetricsRuntime (idempotente)
        ├── validate_only? → sys.exit(0)
        └── pipeline_runner()               → lógica de negocio

Uso CLI::

    uv run python main.py                                          # development
    uv run python main.py env=production                           # producción
    uv run python main.py pipeline.historical.backfill_mode=true   # backfill
    uv run python main.py pipeline.historical.max_concurrent_tasks=8
    uv run python main.py --cfg job                                # ver config efectivo

Principios: SOLID · KISS · DRY · SafeOps
"""

import os
import sys
from typing import Callable

import hydra
from omegaconf import DictConfig
from loguru import logger

import core.config.loader as config_loader
from core.config.env_vars import OCM_ENV
from core.config.hydra_loader import load_appconfig_from_hydra
from core.config.runtime import RunConfig
from core.config.schema import AppConfig
from core.logging import bootstrap_logging, configure_logging
from market_data.orchestration.entrypoint import run as default_pipeline_runner
from infra.observability.runtime import init_metrics_runtime
from market_data.safety.environment_validator import (
    EnvironmentValidator,
    EnvironmentMismatchError,
)

_APP_NAME = "OrangeCashMachine"
_APP_VERSION = "0.2.0"


# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

def print_banner(env: str, version: str = _APP_VERSION) -> None:
    """Imprime el banner de inicio de la aplicación en stdout.

    Args:
        env: Nombre del entorno activo (development, production, etc.).
        version: Versión de la aplicación.
    """
    banner = f"""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   🍊  OrangeCashMachine  v{version:<34}║
║                                                              ║
║   Market-data ingestion pipeline for algorithmic trading     ║
║   Medallion architecture · Prefect · Iceberg · DuckDB        ║
║                                                              ║
║   env  : {env:<52}║
╚══════════════════════════════════════════════════════════════╝
"""
    print(banner, flush=True)


# ---------------------------------------------------------------------------
# Observabilidad
# ---------------------------------------------------------------------------

def setup_observability(config: AppConfig, *, validate_only: bool = False) -> None:
    """Inicializa MetricsRuntime de forma idempotente y fail-soft.

    No lanza excepciones — los errores de observabilidad no deben bloquear
    el pipeline principal.

    Args:
        config: Configuración validada de la aplicación.
        validate_only: Si True, inicialización mínima sin exponer métricas.
    """
    try:
        init_metrics_runtime(config, validate_only=validate_only)
    except Exception as exc:
        logger.warning("observability_init_failed | error={}", exc)


# ---------------------------------------------------------------------------
# Ciclo de vida de la aplicación
# ---------------------------------------------------------------------------

def run_application(
    config: AppConfig,
    run_cfg: RunConfig,
    *,
    pipeline_runner: Callable[..., int] = default_pipeline_runner,
) -> int:
    """Ejecuta el ciclo de vida completo de la aplicación.

    Args:
        config: Configuración Pydantic validada.
        run_cfg: Configuración de proceso inmutable (env, debug, validate_only…).
        pipeline_runner: Callable inyectable para tests.

    Returns:
        Código de salida POSIX: 0 = éxito, 1 = error.
    """
    run_id = run_cfg.run_id
    env = run_cfg.env
    debug = run_cfg.debug
    validate_only = run_cfg.validate_only
    try:
        configure_logging(
            config.observability.logging,
            env=env,
            debug=debug,
            run_id=run_id,
        )
    except Exception as exc:
        logger.warning("logging_configure_failed | error={}", exc)

    log = logger.bind(run_id=run_id, env=env)
    log.info(
        "application_starting | env={} run_id={} debug={} validate_only={}",
        env, run_id, debug, validate_only,
    )

    setup_observability(config, validate_only=validate_only)

    try:
        EnvironmentValidator().check(config, run_cfg)
    except EnvironmentMismatchError as exc:
        log.critical("environment_validation_failed | error={}", exc)
        return 1
    except Exception as exc:
        log.warning("environment_validator_skipped | error={}", exc)

    if validate_only:
        log.info("validation_complete | status=ok")
        return 0

    result: object = pipeline_runner(config=config, run_cfg=run_cfg, debug=debug)

    if not isinstance(result, int):
        log.error(
            "pipeline_runner_returned_non_int | type={} value={}",
            type(result).__name__, result,
        )
        return 1

    return result


# ---------------------------------------------------------------------------
# Hydra entry point
# ---------------------------------------------------------------------------

@hydra.main(config_path="config", config_name="config", version_base="1.3")
def hydra_main(cfg: DictConfig) -> None:
    """Entry point de Hydra.

    Hydra compone: ``base.yaml → env/{env}.yaml → settings.yaml``
    y resuelve todas las interpolaciones ``${oc.env:VAR,default}``.

    Args:
        cfg: DictConfig compuesto por Hydra.

    Variables de entorno reconocidas (prefijo ``OCM_``)::

        OCM_ENV             — entorno activo (development | production)
        OCM_DEBUG           — activa logging verboso (1 | true)
        OCM_VALIDATE_ONLY   — valida config y sale sin ejecutar pipeline (1 | true)
    """
    bootstrap_logging()

    env_block = cfg.get("environment", {})
    explicit_env: str | None = env_block.get("name", None) or os.getenv(OCM_ENV)
    run_cfg = RunConfig.from_env(explicit_env=explicit_env)

    print_banner(run_cfg.env)

    try:
        config = load_appconfig_from_hydra(cfg, env=run_cfg.env)
    except Exception as exc:
        logger.opt(exception=True).critical(
            "config_load_failed | type={} error={}", type(exc).__name__, exc
        )
        sys.exit(1)

    exit_code: int = run_application(config, run_cfg)
    sys.exit(exit_code)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Punto de entrada del script ``ocm`` definido en pyproject.toml.

    Exits:
        0   — éxito o validate-only OK.
        1   — error de configuración o fallo del pipeline.
        130 — interrupción por teclado (SIGINT / Ctrl-C).
    """
    try:
        hydra_main()  # type: ignore[call-arg]
    except KeyboardInterrupt:
        logger.warning("execution_interrupted | signal=SIGINT")
        sys.exit(130)
    except (
        config_loader.ConfigurationError,
        config_loader.ConfigValidationError,
        EnvironmentMismatchError,
    ) as exc:
        logger.opt(exception=True).critical(
            "config_failure | type={} error={}", type(exc).__name__, exc
        )
        sys.exit(1)
    except Exception as exc:
        logger.opt(exception=True).critical(
            "critical_failure | type={} error={}", type(exc).__name__, exc
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
