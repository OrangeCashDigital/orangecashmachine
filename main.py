from __future__ import annotations

"""
main.py – Entrypoint principal de OrangeCashMachine
===================================================

Responsabilidad: orquestar arranque → config → logging → pipeline.
No contiene lógica de negocio. main() retorna int, nunca llama sys.exit().

Orden de arranque
-----------------
1. Config    — load_config() con stdlib logging de fallback
2. Logging   — setup_logging(config.observability.logging) una sola vez
3. Métricas  — start_metrics_server si config.observability.metrics.enabled
4. Pipeline  — run_pipeline(config)

Principios: SOLID · KISS · DRY · SafeOps
"""

import logging
import os
import sys
from pathlib import Path
from typing import Optional, Union

from loguru import logger

from core.config.loader import load_config
from core.config.schema import AppConfig
from core.logging import setup_logging
from market_data.orchestration.entrypoint import run as run_pipeline
from services.observability.metrics import start_metrics_server

# Logging mínimo de stdlib para capturar errores ANTES de que loguru esté listo
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


def initialize_config(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
) -> AppConfig:
    """Carga y valida la configuración central del sistema."""
    try:
        config = load_config(env=env, path=Path(path) if path else None)
        return config
    except Exception:
        logging.critical("Fallo al cargar la configuración", exc_info=True)
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
    try:
        # 1. Config primero — stdlib logging captura cualquier error aquí
        config = initialize_config(env=env, path=config_path)

        # 2. Logging completo desde YAML — una sola inicialización
        setup_logging(cfg=config.observability.logging, debug=debug)
        logger.info(
            "OrangeCashMachine starting | env={} exchanges={}",
            env or os.getenv("OCM_ENV", "development"),
            config.exchange_names,
        )

        # 3. Métricas Prometheus — SafeOps: fallo no bloquea el pipeline
        if config.observability.metrics.enabled:
            try:
                start_metrics_server(port=config.observability.metrics.port)
                logger.info(
                    "Metrics server started | port={}",
                    config.observability.metrics.port,
                )
            except OSError as exc:
                logger.warning("Metrics server failed to start | error={}", exc)

        # 4. Pipeline
        return run_pipeline(config=config, debug=debug)

    except KeyboardInterrupt:
        logger.warning("Execution interrupted by user")
        return 130
    except Exception:
        logger.exception("Error crítico durante el arranque")
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
