from __future__ import annotations

"""
main.py – Entrypoint principal de OrangeCashMachine
===================================================

Responsabilidad: orquestar arranque → config → logging → pipeline.
No contiene lógica de negocio. main() retorna int, nunca llama sys.exit().

Orden de arranque
-----------------
1. RunConfig  — lee OCM_DEBUG, OCM_ENV, OCM_CONFIG_PATH una sola vez
2. Logging    — setup_logging(debug) con defaults, ANTES de load_config
3. Config     — load_config() con loguru ya activo, sin logs perdidos
4. Logging    — setup_logging(cfg, debug) re-llamada idempotente desde YAML
5. request_id — logger.bind() propaga correlation ID a todos los sinks
6. Métricas   — start_metrics_server si config.observability.metrics.enabled
7. Pipeline   — run_pipeline(config, debug) con timeout desde config

Principios: SOLID · KISS · DRY · SafeOps
"""

import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Optional

from loguru import logger

from core.config.runtime import RunConfig
from core.config.loader import load_config, ConfigurationError, ConfigValidationError
from core.config.schema import AppConfig
from core.logging import setup_logging
from market_data.orchestration.entrypoint import run as run_pipeline
from services.observability.metrics import start_metrics_server



def initialize_config(run_cfg: RunConfig) -> AppConfig:
    """
    Carga y valida la configuración central del sistema.

    Recibe RunConfig ya construido — no lee el entorno directamente.
    Deja propagar ConfigurationError y ConfigValidationError con su tipo
    original para que main() los loggee con contexto preciso.
    """
    try:
        return load_config(env=run_cfg.env, path=run_cfg.config_path)
    except (ConfigurationError, ConfigValidationError):
        raise
    except Exception:
        logging.critical("Unexpected error loading config", exc_info=True)
        raise


def main(run_cfg: Optional[RunConfig] = None) -> int:
    """
    Punto de entrada principal.

    Parameters
    ----------
    run_cfg : RunConfig | None
        Configuración de proceso. Si None, se construye desde el entorno.
        Pasar explícitamente en tests para control total.

    Returns
    -------
    int
        0 → éxito, 1 → error crítico, 130 → interrumpido (SIGINT).
    """
    if run_cfg is None:
        run_cfg = RunConfig.from_env()

    # request_id generado una sola vez por ejecución.
    # Se propaga a todos los sinks vía logger.bind() para correlacionar
    # todos los logs de un run sin depender de threading o contextvars.
    request_id = uuid.uuid4().hex[:12]

    try:
        # 1. Logging con defaults ANTES de load_config.
        #    Garantiza que core/config/loader/* (yaml_loader, env_resolver,
        #    env_overrides, __init__) emitan a loguru desde el primer momento.
        #    setup_logging es idempotente: la segunda llamada (paso 3) solo
        #    instala el InterceptHandler, no resetea sinks.
        setup_logging(debug=run_cfg.debug)

        # 2. Config — loguru ya activo, ningún log de loader se pierde.
        config = initialize_config(run_cfg)

        # 3. Logging completo desde YAML — re-llamada idempotente.
        setup_logging(cfg=config.observability.logging, debug=run_cfg.debug)

        # 3. Bind request_id — fluye a CONSOLE y FILE automáticamente.
        bound = logger.bind(request_id=request_id)
        bound.info(
            "OrangeCashMachine starting | env={} debug={} exchanges={}",
            run_cfg.env,
            run_cfg.debug,
            config.exchange_names,
        )

        # 4. Métricas Prometheus — SafeOps: fallo no bloquea el pipeline
        if config.observability.metrics.enabled:
            try:
                start_metrics_server(port=config.observability.metrics.port)
                bound.info(
                    "Metrics server started | port={}",
                    config.observability.metrics.port,
                )
            except OSError as exc:
                bound.warning(
                    "Metrics server failed to start | error={}", exc
                )

        # 5. Pipeline con timeout desde config
        pipeline_timeout = config.pipeline.timeouts.historical_pipeline
        bound.info(
            "Launching pipeline | timeout={}s request_id={}",
            pipeline_timeout, request_id,
        )
        return run_pipeline(config=config, debug=run_cfg.debug)

    except KeyboardInterrupt:
        logger.warning(
            "Execution interrupted by user (SIGINT) | request_id={}", request_id
        )
        return 130

    except (ConfigurationError, ConfigValidationError) as exc:
        logging.critical(
            "Config failure | type=%s error=%s request_id=%s",
            type(exc).__name__, exc, request_id,
            exc_info=True,
        )
        return 1

    except Exception as exc:
        logger.exception(
            "Critical startup failure | type={} error={} request_id={}",
            type(exc).__name__, exc, request_id,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main(run_cfg=RunConfig.from_env()))
