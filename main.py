from __future__ import annotations

"""
main.py – Entrypoint principal de OrangeCashMachine
===================================================

Responsabilidad: orquestar arranque → config → logging → pipeline.
No contiene lógica de negocio. main() retorna int, nunca llama sys.exit().

Orden de arranque
-----------------
1. Config    — load_config() con stdlib logging de fallback pre-loguru
2. Logging   — setup_logging(config.observability.logging) una sola vez
3. request_id — logger.bind() propaga correlation ID a todos los sinks
4. Métricas  — start_metrics_server si config.observability.metrics.enabled
5. Pipeline  — run_pipeline(config) con timeout desde config

Principios: SOLID · KISS · DRY · SafeOps
"""

import asyncio
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Optional, Union

from loguru import logger

from core.config.loader import load_config, ConfigurationError, ConfigValidationError
from core.config.schema import AppConfig
from core.logging import setup_logging
from market_data.orchestration.entrypoint import run as run_pipeline
from services.observability.metrics import start_metrics_server

# Fallback mínimo de stdlib: captura errores ANTES de que loguru esté listo.
# setup_logging() lo reemplaza con InterceptHandler en el paso 2.
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


def initialize_config(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
) -> AppConfig:
    """
    Carga y valida la configuración central del sistema.

    Deja propagar ConfigurationError y ConfigValidationError con su tipo
    original para que main() los loggee con contexto preciso.
    Errores inesperados se loggean con stdlib (loguru aún no está listo)
    y se re-lanzan sin envolver para no perder el traceback original.
    """
    try:
        return load_config(env=env, path=Path(path) if path else None)
    except (ConfigurationError, ConfigValidationError):
        raise
    except Exception:
        logging.critical("Unexpected error loading config", exc_info=True)
        raise


def main(
    env: Optional[str] = None,
    config_path: Optional[Union[str, Path]] = None,
    debug: bool = False,
) -> int:
    """
    Punto de entrada principal.

    Returns
    -------
    int
        0 → éxito, 1 → error crítico, 130 → interrumpido (SIGINT).
    """
    # request_id generado una sola vez por ejecución.
    # Se propaga a todos los sinks vía logger.bind() para correlacionar
    # todos los logs de un run sin depender de threading o contextvars.
    request_id = uuid.uuid4().hex[:12]

    try:
        # 1. Config — stdlib logging activo como fallback hasta el paso 2
        config = initialize_config(env=env, path=config_path)

        # 2. Logging completo desde YAML — una sola inicialización.
        #    setup_logging instala InterceptHandler: a partir de aquí
        #    stdlib logging queda redirigido a loguru (prefect, ccxt, asyncio).
        setup_logging(cfg=config.observability.logging, debug=debug)

        # 3. Bind request_id — fluye a CONSOLE y FILE automáticamente.
        #    No se inyecta en PIPELINE (ese sink requiere exchange+dataset).
        bound = logger.bind(request_id=request_id)
        bound.info(
            "OrangeCashMachine starting | env={} debug={} exchanges={}",
            env or os.getenv("OCM_ENV", "development"),
            debug,
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
        return run_pipeline(config=config, debug=debug)

    except KeyboardInterrupt:
        logger.warning(
            "Execution interrupted by user (SIGINT) | request_id={}", request_id
        )
        return 130

    except (ConfigurationError, ConfigValidationError) as exc:
        # Loggeado con stdlib: loguru puede no estar listo en este punto
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
    sys.exit(
        main(
            env=os.getenv("OCM_ENV"),
            config_path=(
                Path(p) if (p := os.getenv("OCM_CONFIG_PATH")) else None
            ),
            debug=os.getenv("OCM_DEBUG", "false").lower() in ("1", "true", "yes"),
        )
    )
