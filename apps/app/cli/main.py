"""
app/cli/main.py
================

Entrypoint principal de OrangeCashMachine — market data pipeline.

Flujo de ejecución::

    hydra_main(cfg)
        ├── bootstrap_logging()
        ├── resolve_explicit_env(cfg)        → str | None (BC-51: único acceso a cfg)
        ├── RunConfig.from_env()             → RunConfig (env, debug, run_id…)
        ├── load_appconfig_from_hydra(cfg)   → AppConfig (Pydantic validado)
        ├── run_application(config, run_cfg)
        │       ├── configure_logging()
        │       ├── setup_observability()    → MetricsRuntime (idempotente, fail-soft)
        │       ├── EnvironmentValidator.check()
        │       ├── validate_only? → sys.exit(0)
        │       └── pipeline_runner(ctx, pusher)
        └── sys.exit(exit_code)

Uso CLI::

    uv run python -m app.cli.main                                        # development
    uv run python -m app.cli.main env=production                         # producción
    uv run python -m app.cli.main pipeline.historical.backfill_mode=true
    uv run python -m app.cli.main --cfg job                              # ver config efectivo

Principios: SOLID · KISS · DRY · SSOT · SafeOps
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Callable

import hydra
from hydra.core.config_store import ConfigStore
from loguru import logger
from omegaconf import DictConfig

from ocm.config.structured import (
    HistoricalConfig,
    ObservabilityConfig,
    PipelineConfig,
    ResampleConfig,
)

# ---------------------------------------------------------------------------
# Hydra Structured Config registration — debe ocurrir antes de @hydra.main
# ---------------------------------------------------------------------------


def _register_structured_configs() -> None:
    """Registra Structured Configs en Hydra ConfigStore.

    Debe llamarse antes de que Hydra arranque. Establece los schemas
    que Hydra usa para validar tipos durante la composición del config.

    Arquitectura de validación en capas:
        Hydra Structured Config  → valida tipos (int, str, bool)
        OmegaConf merge          → resuelve ``${oc.env:VAR,default}``
        Pydantic                 → valida reglas de negocio (ge, le, regex…)
    """
    cs = ConfigStore.instance()
    cs.store(group="pipeline", name="schema", node=PipelineConfig)
    cs.store(group="pipeline/historical", name="schema", node=HistoricalConfig)
    cs.store(group="pipeline/resample", name="schema", node=ResampleConfig)
    cs.store(group="observability", name="schema", node=ObservabilityConfig)


_register_structured_configs()

# Imports post-registration — Hydra requiere que ConfigStore esté listo primero
from market_data.ports.outbound.observability import MetricsPusherPort

from app.cli.entrypoint import run as _default_pipeline_runner
from ocm.config.hydra_loader import load_appconfig_from_hydra, resolve_explicit_env
from ocm.config.loader.exceptions import (
    ConfigurationError,
    ConfigValidationError,
)
from ocm.config.schema import AppConfig
from ocm.observability import bootstrap_logging, configure_logging
from ocm.observability.metrics_runtime import init_metrics_runtime
from ocm.observability.pushers import NoopPusher, PrometheusPusher
from ocm.runtime.context import RuntimeContext
from ocm.runtime.environment_validator import (
    EnvironmentMismatchError,
    EnvironmentValidator,
)
from ocm.runtime.run_config import RunConfig

# Tipo del pipeline runner — explícito para mypy y legibilidad (OCP: inyectable en tests)
PipelineRunner = Callable[[RuntimeContext, MetricsPusherPort | None], int]


# ---------------------------------------------------------------------------
# Observabilidad — fail-soft: nunca bloquea el pipeline
# ---------------------------------------------------------------------------


def setup_observability(config: AppConfig, *, validate_only: bool = False) -> None:
    """Inicializa MetricsRuntime de forma idempotente y fail-soft.

    Args:
        config:        Configuración validada de la aplicación.
        validate_only: Si ``True``, inicialización mínima sin exponer métricas.
    """
    try:
        init_metrics_runtime(config, validate_only=validate_only)
    except Exception as exc:
        logger.warning("observability_init_failed", error=str(exc))


# ---------------------------------------------------------------------------
# Ciclo de vida de la aplicación
# ---------------------------------------------------------------------------


def run_application(
    config: AppConfig,
    run_cfg: RunConfig,
    *,
    pipeline_runner: PipelineRunner = _default_pipeline_runner,
) -> int:
    """Ejecuta el ciclo de vida completo de la aplicación.

    Construye :class:`RuntimeContext` canónico y lo inyecta al pipeline runner.
    Es el único lugar donde se ensamblan config + run_cfg + pusher (Composition Root).

    Args:
        config:          Configuración Pydantic validada.
        run_cfg:         Configuración de proceso inmutable.
        pipeline_runner: Runner inyectable — default es ``entrypoint.run``.
                         Firma requerida: ``(ctx, pusher) -> int``.

    Returns:
        Código de salida POSIX: ``0`` = éxito, ``1`` = error.
    """
    log = logger.bind(run_id=run_cfg.run_id, env=run_cfg.env)

    # configure_logging fail-soft — un fallo de logging no mata el pipeline
    try:
        configure_logging(
            config.observability.logging,
            env=run_cfg.env,
            debug=run_cfg.debug,
            run_id=run_cfg.run_id,
        )
    except Exception as exc:
        log.warning("logging_configure_failed", error=str(exc))

    log.info(
        "application_starting",
        env=run_cfg.env,
        run_id=run_cfg.run_id,
        debug=run_cfg.debug,
        validate_only=run_cfg.validate_only,
    )

    setup_observability(config, validate_only=run_cfg.validate_only)

    # Fail-Fast en mismatch de entorno; Fail-Soft en errores inesperados del validator
    try:
        EnvironmentValidator().check(config, run_cfg)
    except EnvironmentMismatchError as exc:
        log.critical("environment_validation_failed", error=str(exc))
        return 1
    except Exception as exc:
        log.warning("environment_validator_skipped", error=str(exc))

    if run_cfg.validate_only:
        log.info("validation_complete", status="ok")
        return 0

    # Composition Root — único lugar donde se decide qué pusher usar.
    # DIP: el dominio (entrypoint.run) nunca importa infra directamente.
    ctx = RuntimeContext(
        app_config=config,
        run_config=run_cfg,
        started_at=datetime.now(timezone.utc),
    )
    pusher: MetricsPusherPort = PrometheusPusher() if config.observability.metrics.enabled else NoopPusher()

    result = pipeline_runner(ctx, pusher)

    if not isinstance(result, int):
        log.error(
            "pipeline_runner_returned_non_int",
            type=type(result).__name__,
            value=str(result),
        )
        return 1

    return result


# ---------------------------------------------------------------------------
# Hydra entry point
# ---------------------------------------------------------------------------

# SafeOps: path absoluto — independiente del CWD de invocación
# SSOT: repo_root() vive en shared/utils/repo.py — única definición canónica.
# Antes: _find_repo_root() duplicada aquí buscando pyproject.toml.
# Ahora: shared/utils/repo.py busca .git — más robusto (pyproject.toml puede
# existir en subdirectorios de monorepos; .git es inequívoco).
from shared.utils.repo import repo_root as _repo_root

_CONFIG_DIR = str(_repo_root() / "config")


@hydra.main(config_path=_CONFIG_DIR, config_name="config", version_base="1.3")
def hydra_main(cfg: DictConfig) -> None:
    """Entry point de Hydra.

    Hydra compone: ``base.yaml → env/{env}.yaml → settings.yaml``
    y resuelve todas las interpolaciones ``${oc.env:VAR,default}``.

    BC-51: este es el único cuerpo de función fuera de ``ocm.config`` que
    recibe un ``DictConfig`` crudo. Su única navegación directa es delegar
    a ``resolve_explicit_env()`` — el resto del flujo pasa por
    ``load_appconfig_from_hydra()``, que ya produce ``AppConfig`` tipado.

    Args:
        cfg: DictConfig compuesto por Hydra.

    Variables de entorno reconocidas (prefijo ``OCM_``)::

        OCM_ENV             — entorno activo (development | production)
        OCM_DEBUG           — activa logging verboso (true | yes | on)
        OCM_VALIDATE_ONLY   — valida config y sale sin ejecutar pipeline (true | yes | on)
    """
    bootstrap_logging()

    explicit_env = resolve_explicit_env(cfg)  # único punto de acceso a cfg fuera de ocm.config
    run_cfg = RunConfig.from_env(explicit_env=explicit_env)

    try:
        config = load_appconfig_from_hydra(
            cfg,
            env=run_cfg.env,
            run_id=run_cfg.run_id,  # SSOT: run_id generado por RunConfig
        )
    except Exception as exc:
        logger.opt(exception=True).critical(
            "config_load_failed",
            type=type(exc).__name__,
            error=str(exc),
        )
        sys.exit(1)

    sys.exit(run_application(config, run_cfg))


# ---------------------------------------------------------------------------
# CLI entry point (pyproject.toml scripts → ``ocm``)
# ---------------------------------------------------------------------------


def _reject_cfg_job_in_production() -> None:
    """B-04/H-06: bloquea `--cfg` en producción.

    Hydra imprime el DictConfig resuelto (OmegaConf pre-Pydantic) SIN redactar
    SecretStr, exponiendo credenciales a stdout (poca de antes en AGENTS.md).
    En el entorno de producción, `--cfg job`/`--cfg all` se bloquea.
    """
    if not any(a == "--cfg" or a.startswith("--cfg=") for a in sys.argv):
        return
    if os.environ.get("OCM_ENV", "development") == "production":
        logger.critical(
            "--cfg_bloqueado_en_produccion",
            reason="--cfg dumpa SecretStr sin redactar (Hydra OmegaConf pre-Pydantic)",
        )
        raise SystemExit(2)


def main() -> None:
    """Punto de entrada del script ``ocm`` definido en pyproject.toml.

    Exit codes:
        0   — éxito o validate-only OK.
        1   — error de configuración o fallo del pipeline.
        2   — `--cfg` bloqueado en producción (B-04/H-06).
        130 — interrupción por teclado (SIGINT / Ctrl-C).
    """
    _reject_cfg_job_in_production()  # Guard B-04: nunca dump de config segura en prod
    try:
        hydra_main()  # type: ignore[call-arg]
    except KeyboardInterrupt:
        logger.warning("execution_interrupted", signal="SIGINT")
        sys.exit(130)
    except (
        ConfigurationError,
        ConfigValidationError,
        EnvironmentMismatchError,
    ) as exc:
        logger.opt(exception=True).critical(
            "config_failure",
            type=type(exc).__name__,
            error=str(exc),
        )
        sys.exit(1)
    except Exception as exc:
        logger.opt(exception=True).critical(
            "critical_failure",
            type=type(exc).__name__,
            error=str(exc),
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
