from __future__ import annotations

"""
main.py – Entrypoint principal de OrangeCashMachine
===================================================

Responsabilidad: orquestar arranque → logging → config → pipeline.
No contiene lógica de negocio. main() retorna int, nunca llama sys.exit().

Principios: SOLID · KISS · DRY · SafeOps
"""

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


def initialize_config(
    env: Optional[str] = None,
    path: Optional[Union[str, Path]] = None,
) -> AppConfig:
    """Carga y valida la configuración central del sistema."""
    try:
        config = load_config(env=env, path=Path(path) if path else None)
        logger.info("Configuración cargada correctamente | exchanges={}", config.exchange_names)
        return config
    except Exception:
        logger.exception("Fallo al cargar la configuración")
        raise


def main(
    env: Optional[str] = None,
    config_path: Optional[Union[str, Path]] = None,
    debug: bool = False,
) -> int:
    """
    Punto de entrada principal.

    Orden de arranque
    -----------------
    1. Bootstrap logging (consola, sin config) — para capturar errores de carga
    2. Cargar config
    3. Re-inicializar logging con LoggingConfig completo desde YAML
    4. Arrancar métricas Prometheus si habilitado
    5. Ejecutar pipeline

    Returns
    -------
    int
        0 → éxito, 1 → error crítico.
    """
    try:
        # 1. Bootstrap: solo consola, sin config todavía
        setup_logging(debug=debug)

        # 2. Cargar config
        config = initialize_config(env=env, path=config_path)

        # 3. Re-configurar logging con valores del YAML
        #    setup_logging es idempotente → resetear handlers para aplicar cfg completo
        import logging as _stdlib_logging
        from loguru import logger as _loguru
        _loguru.remove()                          # reset sinks
        _stdlib_logging.root.handlers.clear()     # reset stdlib intercept
        setup_logging(cfg=config.observability.logging, debug=debug)

        # 4. Arrancar servidor de métricas si habilitado en config
        if config.observability.metrics.enabled:
            try:
                start_metrics_server(port=config.observability.metrics.port)
                logger.info(
                    "Metrics server started | port={}",
                    config.observability.metrics.port,
                )
            except Exception as exc:
                logger.warning("Metrics server failed to start | error={}", exc)

        # 5. Ejecutar pipeline
        return run_pipeline(config=config, debug=debug)

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
