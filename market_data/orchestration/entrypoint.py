"""
orchestration/entrypoint.py
============================

Responsabilidad única
---------------------
Configurar logging local (Loguru) y lanzar el flow de ingestión.
Es el único archivo del sistema con bloque if __name__ == "__main__".

Principios aplicados
--------------------
• SOLID  – responsabilidad única; no contiene lógica de negocio
• KISS   – sin abstracciones innecesarias sobre asyncio o Prefect
• DRY    – setup_logging() centraliza toda la configuración de logs
• SafeOps – get_run_logger() nunca se llama fuera de contexto Prefect,
            sys.exit(1) en fallo garantiza código de salida correcto,
            logs de Prefect integrados via logging stdlib sin PrefectHandler
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from market_data.orchestration.flows.batch_flow import market_data_flow


# ==========================================================
# Constants
# ==========================================================

DEFAULT_CONFIG_PATH: Path = Path("config/settings.yaml")
LOG_DIR:             Path = Path("logs")

_LOG_FORMAT_CONSOLE: str = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level:<8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
    "{message}"
)
_LOG_FORMAT_FILE: str = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {name}:{line} | {message}"
)


# ==========================================================
# Loguru ↔ stdlib logging bridge
# ==========================================================

class _InterceptHandler(logging.Handler):
    """
    Redirige logs de stdlib logging (incluido Prefect) a Loguru.

    Prefect usa stdlib logging internamente. Este handler intercepta
    todos los loggers de stdlib y los envía a Loguru, unificando
    el output en un único sistema de logging.

    Referencia
    ----------
    https://loguru.readthedocs.io/en/stable/overview.html#entirely-compatible-with-standard-logging
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        # Subir en el call stack para encontrar el origen real del log
        frame, depth = logging.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


# ==========================================================
# Logging Setup
# ==========================================================

def setup_logging(
    debug:   bool           = False,
    log_dir: Optional[Path] = LOG_DIR,
) -> None:
    """
    Configura Loguru para consola y archivo rotativo.

    Integra stdlib logging (y por tanto los logs de Prefect)
    con Loguru via _InterceptHandler, sin usar PrefectHandler
    que requiere contexto de ejecución activo.

    Parameters
    ----------
    debug : bool
        Nivel DEBUG si True, INFO si False.
    log_dir : Path, optional
        Carpeta de logs rotativos. Si None, solo loguea en consola.
    """
    level = "DEBUG" if debug else "INFO"

    # 1. Limpiar handlers previos de Loguru
    logger.remove()

    # 2. Consola con colores y traceback enriquecido
    logger.add(
        sys.stderr,
        level=level,
        format=_LOG_FORMAT_CONSOLE,
        backtrace=True,
        diagnose=debug,   # diagnose=True solo en debug (expone locals)
        colorize=True,
    )

    # 3. Archivo rotativo (opcional)
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_dir / "market_data_{time:YYYY-MM-DD}.log",
            rotation="1 day",
            retention="14 days",
            level=level,
            format=_LOG_FORMAT_FILE,
            encoding="utf-8",
            backtrace=True,
            diagnose=False,   # nunca exponer locals en archivos de log
            compression="gz",
        )

    # 4. Interceptar stdlib logging → Loguru
    #    Cubre: prefect, uvicorn, ccxt, pybreaker, pandas, etc.
    intercept = _InterceptHandler()
    logging.basicConfig(handlers=[intercept], level=0, force=True)

    # Asegurar que los loggers de Prefect se propagan al root logger
    for name in ("prefect", "prefect.flow_runs", "prefect.task_runs"):
        prefect_logger = logging.getLogger(name)
        prefect_logger.handlers  = [intercept]
        prefect_logger.propagate = False

    logger.debug("Logging configured | level={} log_dir={}", level, log_dir)


# ==========================================================
# Flow Runner
# ==========================================================

async def _run_flow(config_path: Path) -> None:
    """
    Ejecuta market_data_flow de forma asíncrona.

    No usa get_run_logger() — ese método solo es válido dentro
    de un @flow o @task activo. Aquí usamos loguru directamente.

    Raises
    ------
    Exception
        Re-lanza cualquier excepción del flow para que run()
        pueda capturarla y llamar sys.exit(1).
    """
    logger.info("Launching market_data_flow | config_path={}", config_path)

    try:
        await market_data_flow(config_path=config_path)
    except Exception as exc:
        logger.exception("market_data_flow failed | error={}", exc)
        raise


# ==========================================================
# Public Entrypoint
# ==========================================================

def run(
    debug:       bool                    = False,
    config_path: Optional[Path | str]    = None,
    log_dir:     Optional[Path]          = LOG_DIR,
) -> None:
    """
    Configura el sistema y lanza el flow de ingestión.

    Diseñado para ser importable desde otros módulos (tests,
    scripts de CI, schedulers externos) sin efectos secundarios
    en el import — setup_logging() solo se llama al invocar run().

    Parameters
    ----------
    debug : bool
        Activa logging DEBUG y diagnose de Loguru.
    config_path : Path | str, optional
        Ruta al settings.yaml. Si None, usa DEFAULT_CONFIG_PATH.
    log_dir : Path, optional
        Carpeta de logs rotativos. Si None, solo consola.

    Exit codes
    ----------
    0 – flow completado sin errores
    1 – flow terminado con errores (cualquier excepción no capturada)
    """
    resolved_path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH

    setup_logging(debug=debug, log_dir=log_dir)

    logger.info(
        "OrangeCashMachine starting | config={} debug={}",
        resolved_path, debug,
    )

    try:
        asyncio.run(_run_flow(resolved_path))
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (KeyboardInterrupt)")
        sys.exit(0)
    except Exception:
        logger.error("Market Data Flow terminated with errors. Check logs above.")
        sys.exit(1)

    logger.info("OrangeCashMachine finished successfully.")


# ==========================================================
# Local Execution
# ==========================================================

if __name__ == "__main__":
    run(debug=True)

    # Producción (Prefect Cloud/Server):
    # market_data_flow.serve(
    #     name="market-data-ingestion",
    #     cron="0 * * * *",
    #     parameters={"config_path": "config/settings.yaml"},
    # )
