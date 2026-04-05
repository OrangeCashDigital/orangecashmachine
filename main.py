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
    *,
    run_id: str,
    env: str,
    debug: bool = False,
    validate_only: bool = False,
    pipeline_runner: Callable[..., int] = default_pipeline_runner,
) -> int:
    """Ejecuta el ciclo de vida completo de la aplicación.

    Args:
        config: Configuración Pydantic validada.
        run_id: Identificador único del proceso (hex de 12 chars).
        env: Nombre del entorno activo.
        debug: Activa logging verboso y diagnósticos extra.
        validate_only: Sale con código 0 tras validar config, sin ejecutar pipeline.
        pipeline_runner: Callable inyectable para tests.

    Returns:
        Código de salida POSIX: 0 = éxito, 1 = error.
    """
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

    run_cfg = None
    try:
        from core.config.runtime import RunConfig
        run_cfg = RunConfig.from_env(explicit_env=env)
        EnvironmentValidator().check(config, run_cfg)
    except EnvironmentMismatchError as exc:
        log.critical("environment_validation_failed | error={}", exc)
        return 1
    except Exception as exc:
        log.warning("environment_validator_skipped | error={}", exc)

    if validate_only:
        log.info("validation_complete | status=ok")
        return 0

    if run_cfg is None:
        log.error("run_cfg_unavailable | cannot_start_pipeline")
        return 1

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
    """
    bootstrap_logging()

    env_block = cfg.get("environment", {})
    env: str = str(
        env_block.get("name", None) or os.getenv("OCM_ENV", "development")
    )
    debug: bool = bool(env_block.get("debug", False)) or (
        os.getenv("OCM_DEBUG", "").lower() in ("1", "true")
    )
    validate_only: bool = os.getenv("OCM_VALIDATE_ONLY", "").lower() in ("1", "true")
    run_id: str = uuid.uuid4().hex[:12]

    print_banner(env)

    try:
        config = load_appconfig_from_hydra(cfg, env=env)
    except Exception as exc:
        logger.opt(exception=True).critical(
            "config_load_failed | type={} error={}", type(exc).__name__, exc
        )
        sys.exit(1)

    exit_code: int = run_application(
        config,
        run_id=run_id,
        env=env,
        debug=debug,
        validate_only=validate_only,
    )
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
