"""
main.py
=======

Entrypoint principal de OrangeCashMachine.

Flujo de ejecución::

    hydra_main(cfg)
        ├── bootstrap_logging()
        ├── RunConfig.from_env()             → RunConfig (env, debug, run_id…)
        ├── load_appconfig_from_hydra(cfg)   → AppConfig (Pydantic validado)
        ├── configure_logging(cfg, env, ...)
        ├── setup_observability()            → MetricsRuntime (idempotente)
        ├── EnvironmentValidator.check()
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
from __future__ import annotations

import sys
from datetime import datetime, timezone
from typing import Callable

import hydra
from hydra.core.config_store import ConfigStore
from omegaconf import DictConfig
from loguru import logger

from core.config.structured import (
    PipelineConfig,
    HistoricalConfig,
    ObservabilityConfig,
    ResampleConfig,
)


def _register_structured_configs() -> None:
    """Registra Structured Configs en Hydra ConfigStore.

    Debe llamarse antes de que Hydra arranque. Establece los schemas
    que Hydra usa para validar tipos durante la composición del config.

    Arquitectura:
        Hydra Structured Config  → valida tipos (int, str, bool, Optional)
        OmegaConf merge          → resuelve ${oc.env:...}
        Pydantic                 → valida reglas de negocio (ge, le, regex…)
    """
    cs = ConfigStore.instance()
    cs.store(group="pipeline", name="schema", node=PipelineConfig)
    cs.store(group="pipeline/historical", name="schema", node=HistoricalConfig)
    cs.store(group="pipeline/resample",   name="schema", node=ResampleConfig)
    cs.store(group="observability",       name="schema", node=ObservabilityConfig)


_register_structured_configs()

import core.config.loader as config_loader
from core.config.hydra_loader import load_appconfig_from_hydra
from core.config.runtime import RunConfig
from core.config.runtime_context import RuntimeContext
from core.config.schema import AppConfig
from core.logging import bootstrap_logging, configure_logging
from market_data.orchestration.entrypoint import run as default_pipeline_runner
from infra.observability.runtime import init_metrics_runtime
from market_data.safety.environment_validator import (
    EnvironmentValidator,
    EnvironmentMismatchError,
)



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

    # Bind antes del try — garantiza run_id en todos los logs incluso si
    # configure_logging falla (fail-soft: el pipeline no debe bloquearse)
    log = logger.bind(run_id=run_id, env=env)

    try:
        configure_logging(
            config.observability.logging,
            env=env,
            debug=debug,
            run_id=run_id,
        )
    except Exception as exc:
        log.warning("logging_configure_failed | error={}", exc)

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

    runtime_context = RuntimeContext(
        app_config=config,
        run_config=run_cfg,
        started_at=datetime.now(timezone.utc),
    )
    result: object = pipeline_runner(config=config, run_cfg=run_cfg, runtime_context=runtime_context, debug=debug)

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
    # explicit_env: solo el valor que Hydra/CLI aporta — RunConfig resuelve OCM_ENV
    explicit_env: str | None = env_block.get("name") or None
    run_cfg = RunConfig.from_env(explicit_env=explicit_env)

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
