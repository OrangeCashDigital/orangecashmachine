from __future__ import annotations

"""
main.py – Entrypoint principal de OrangeCashMachine

Responsabilidad:
    Orquestar el ciclo de vida de la aplicación:
        1. Setup del sistema
        2. Validación del entorno
        3. Inicialización segura
        4. Ejecución del pipeline (lógica de negocio)

⚠️ IMPORTANTE:
    Este módulo NO contiene lógica de negocio.

    Toda la lógica real (descarga de datos, conexión a exchanges,
    procesamiento, etc.) vive en el pipeline:
        market_data/orchestration/entrypoint.py

Arquitectura:

🟢 1. CAPA DE SISTEMA (este archivo)
    - Carga de configuración
    - Inicialización de logging
    - Setup de observabilidad
    - Validación del entorno
    - Control de ejecución (modo normal vs validación)

    👉 Objetivo: garantizar que el sistema está listo para operar.

🔴 2. CAPA DE NEGOCIO (pipeline_runner)
    - Conexión a exchanges
    - Descarga de datos
    - Procesamiento
    - Persistencia
    - Métricas de negocio

    👉 Se ejecuta únicamente si el sistema está correctamente inicializado.

Flujo de ejecución:

    run_application()
        ├── bootstrap_logging()        → logging mínimo seguro
        ├── load_config()             → validación de configuración
        ├── configure_logging()       → logging completo
        ├── setup_observability()     → métricas
        ├── validation_mode?          → salida anticipada (sin pipeline)
        └── pipeline_runner()         → 🔥 lógica real

Modo VALIDATE_ONLY:
    Permite validar completamente el entorno SIN ejecutar el pipeline.
    Útil para CI/CD, debugging o validaciones rápidas.

Principios:
    SOLID · KISS · DRY · SafeOps
"""

import sys
import uuid
import os
from typing import Optional, Callable

from loguru import logger

import core.config.runtime as runtime
import core.config.loader as config_loader
from core.config.loader.env_resolver import bootstrap_dotenv
from core.config.schema import AppConfig
from core.logging import bootstrap_logging, configure_logging
from market_data.orchestration.entrypoint import run as default_pipeline_runner
from market_data.safety.environment_validator import EnvironmentValidator, EnvironmentMismatchError
from infra.observability.server import start_metrics_server


# ---------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------
def build_run_config(explicit: Optional[runtime.RunConfig] = None) -> runtime.RunConfig:
    """
    Construye la configuración de ejecución.

    Prioridad:
        1. Configuración explícita (tests / overrides)
        2. Variables de entorno (.env + OS)
    """
    if explicit is not None:
        return explicit

    bootstrap_dotenv()
    return runtime.RunConfig.from_env()


def initialize_config(run_cfg: runtime.RunConfig) -> AppConfig:
    """
    Carga y valida la configuración de la aplicación.

    Falla rápido si:
        - El archivo no existe
        - La configuración es inválida
    """
    try:
        return config_loader.load_config(
            env=run_cfg.env,
            path=run_cfg.config_path,
        )
    except FileNotFoundError:
        logger.critical("config_file_missing", path=str(run_cfg.config_path))
        raise
    except Exception:
        logger.opt(exception=True).critical("config_initialization_failed")
        raise


def setup_logging_bootstrap(run_cfg: runtime.RunConfig, run_id: str) -> None:
    """
    Inicializa logging mínimo (fase temprana).

    Garantiza visibilidad antes de cargar configuración.
    """
    bootstrap_logging(
        debug=run_cfg.debug,
        run_id=run_id,
        env=run_cfg.env,
    )


def setup_logging_runtime(run_cfg: runtime.RunConfig, config: AppConfig, run_id: str):
    """
    Configura logging completo basado en YAML.

    Nota:
        Evita duplicación de logs gracias a logger.remove()
        dentro de configure_logging.
    """
    configure_logging(
        cfg=config.observability.logging,
        env=run_cfg.env,
        debug=run_cfg.debug,
        run_id=run_id,
    )

    log = logger.bind(run_id=run_id)

    log.debug(
        "run_config_resolved",
        env=run_cfg.env,
        debug=run_cfg.debug,
        config_path=str(run_cfg.config_path) if run_cfg.config_path else None,
        pushgateway=getattr(run_cfg, "pushgateway", None),
    )

    return log


def setup_observability(config: AppConfig, log) -> None:
    """
    Inicializa métricas y observabilidad.

    No bloqueante:
        Si falla, la aplicación continúa (fail-soft).
    """
    observability = getattr(config, "observability", None)

    if not observability or not observability.metrics:
        log.debug("metrics_config_missing")
        return

    metrics_cfg = observability.metrics

    if not metrics_cfg.enabled:
        log.debug("metrics_disabled")
        return

    try:
        start_metrics_server(port=metrics_cfg.port)
        log.info("metrics_server_started", port=metrics_cfg.port)
    except OSError as exc:
        log.warning("metrics_server_failed", error=str(exc))


# ---------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------
def run_application(
    run_cfg: runtime.RunConfig,
    pipeline_runner: Callable = default_pipeline_runner,
) -> int:
    """
    Orquesta la ejecución completa de la aplicación.

    Fases:
        1. Setup del sistema
        2. Validación del entorno
        3. Inicialización de observabilidad
        4. (Opcional) Validación sin ejecución
        5. Ejecución del pipeline (lógica de negocio)
    """
    run_id = uuid.uuid4().hex[:12]
    validate_only = os.getenv("VALIDATE_ONLY", "").lower() in ("1", "true", "yes")

    # -----------------------------------------------------------------
    # 1. Bootstrap logging
    # -----------------------------------------------------------------
    setup_logging_bootstrap(run_cfg, run_id)
    base_log = logger.bind(run_id=run_id)
    base_log.debug("bootstrap_initialized")

    # -----------------------------------------------------------------
    # 2. Configuración
    # -----------------------------------------------------------------
    config = initialize_config(run_cfg)

    # -----------------------------------------------------------------
    # 3. Logging completo
    # -----------------------------------------------------------------
    log = setup_logging_runtime(run_cfg, config, run_id)

    log.info(
        "app_started",
        env=run_cfg.env,
        debug=run_cfg.debug,
        exchanges=getattr(config, "exchange_names", []),
    )

    # -----------------------------------------------------------------
    # 4. Observabilidad
    # -----------------------------------------------------------------
    setup_observability(config, log)

    # -----------------------------------------------------------------
    # 5. Validación de entorno (falla rápido si incoherente)
    # -----------------------------------------------------------------
    try:
        EnvironmentValidator().check(config, run_cfg)
    except EnvironmentMismatchError as exc:
        log.critical("environment_validation_failed", error=str(exc))
        return 1

    # -----------------------------------------------------------------
    # 6. Preparación del pipeline
    # -----------------------------------------------------------------
    pipeline_cfg = getattr(config, "pipeline", None)
    timeouts_cfg = getattr(pipeline_cfg, "timeouts", None)
    timeout = getattr(timeouts_cfg, "historical_pipeline", None)

    if timeout is None:
        log.warning("pipeline_timeout_missing")

    log.info("pipeline_started", timeout_s=timeout)

    # -----------------------------------------------------------------
    # 7. Validation mode (NO ejecuta lógica de negocio)
    # -----------------------------------------------------------------
    if validate_only:
        log.info("validation_mode_enabled")
        log.info(
            "validation_successful",
            checks=[
                "config_loaded",
                "logging_configured",
                "observability_initialized",
            ],
        )
        log.info("pipeline_skipped", reason="validate_only")
        return 0

    # -----------------------------------------------------------------
    # 8. Execution boundary (🔥 lógica de negocio)
    # -----------------------------------------------------------------
    result = pipeline_runner(
        config=config,
        run_cfg=run_cfg,
        debug=run_cfg.debug,
    )

    # -----------------------------------------------------------------
    # 9. Validación defensiva
    # -----------------------------------------------------------------
    if not isinstance(result, int):
        log.error("pipeline_runner_returned_non_int", value=result)
        return 1

    return result


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main(run_cfg: Optional[runtime.RunConfig] = None) -> int:
    run_cfg = build_run_config(run_cfg)

    try:
        return run_application(run_cfg)

    except KeyboardInterrupt:
        logger.warning("execution_interrupted")
        return 130

    except (
        FileNotFoundError,
        config_loader.ConfigurationError,
        config_loader.ConfigValidationError,
        EnvironmentMismatchError,
    ) as exc:
        logger.opt(exception=True).critical(
            "config_failure",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return 1

    except Exception as exc:
        logger.exception(
            "critical_failure",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())