"""
historical_pipeline_async.py
============================

Pipeline profesional de ingestión OHLCV histórico.

Responsabilidades
-----------------
• Orquestar descarga histórica incremental
• Controlar concurrencia (semáforo único y centralizado)
• Persistir datos validados en el Data Lake

Principios aplicados
--------------------
• SOLID  – cada clase/función tiene responsabilidad única
• DRY    – lógica de concurrencia y logging centralizada
• KISS   – sin abstracciones innecesarias
• SafeOps – fallos explícitos, cierre seguro de recursos,
            tracking de resultados por par
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

import yaml
from loguru import logger

from market_data.batch.fetchers.fetcher import HistoricalFetcherAsync
from market_data.batch.storage.historical_storage import HistoricalStorage
from market_data.batch.transformers.transformer import OHLCVTransformer


# ==========================================================
# Constants
# ==========================================================

DEFAULT_MAX_WORKERS: int = 6
DEFAULT_CONFIG_PATH: Path = Path("config/settings.yaml")
LOG_ROTATION:   str = "1 day"
LOG_RETENTION:  str = "14 days"
LOG_LEVEL:      str = "INFO"


# ==========================================================
# Result Tracking
# ==========================================================

@dataclass
class PairResult:
    """
    Resultado del procesamiento de un par símbolo/timeframe.

    Permite al pipeline agregar métricas sin depender de estado
    mutable compartido entre coroutines (SafeOps).
    """
    symbol:    str
    timeframe: str
    rows:      int  = 0
    skipped:   bool = False
    error:     str  = ""

    @property
    def success(self) -> bool:
        return not self.error and not self.skipped

    def __str__(self) -> str:
        if self.error:
            return f"{self.symbol}/{self.timeframe} ERROR: {self.error}"
        if self.skipped:
            return f"{self.symbol}/{self.timeframe} SKIPPED (no new data)"
        return f"{self.symbol}/{self.timeframe} OK rows={self.rows}"


@dataclass
class PipelineSummary:
    """Agrega los resultados de todos los pares procesados."""
    results: List[PairResult] = field(default_factory=list)

    @property
    def total(self)    -> int: return len(self.results)

    @property
    def succeeded(self) -> int: return sum(1 for r in self.results if r.success)

    @property
    def skipped(self)  -> int: return sum(1 for r in self.results if r.skipped)

    @property
    def failed(self)   -> int: return sum(1 for r in self.results if r.error)

    @property
    def total_rows(self) -> int: return sum(r.rows for r in self.results)

    def log(self) -> None:
        logger.info(
            "Pipeline summary | total={} ok={} skipped={} failed={} rows={}",
            self.total, self.succeeded, self.skipped, self.failed, self.total_rows,
        )
        for r in self.results:
            if r.error:
                logger.warning("  ✗ {}", r)
            elif r.skipped:
                logger.debug("  ↷ {}", r)
            else:
                logger.debug("  ✓ {}", r)


# ==========================================================
# Config Loader
# ==========================================================

def load_config(config_file: Path) -> Dict:
    """
    Carga y devuelve la configuración YAML del pipeline.

    Raises
    ------
    FileNotFoundError
        Si el archivo no existe.
    ValueError
        Si el YAML está vacío o malformado.
    """
    if not config_file.exists():
        raise FileNotFoundError(f"Config not found → {config_file}")

    try:
        with open(config_file, "r", encoding="utf-8") as fh:
            config = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        logger.critical("Failed parsing YAML config | file={} error={}", config_file, exc)
        raise

    if not config or not isinstance(config, dict):
        raise ValueError(f"Config is empty or invalid → {config_file}")

    logger.info("Config loaded | file={}", config_file)
    return config


# ==========================================================
# Historical Pipeline
# ==========================================================

class HistoricalPipelineAsync:
    """
    Pipeline asíncrono de ingestión OHLCV histórica.

    Responsabilidades
    -----------------
    • Iterar sobre todos los pares (símbolo × timeframe)
    • Controlar concurrencia con un único semáforo (no duplicado en fetcher)
    • Delegar descarga al fetcher y persistencia al storage
    • Devolver un PipelineSummary con el resultado de cada par

    Concurrencia
    ------------
    El semáforo vive exclusivamente aquí. El HistoricalFetcherAsync
    debe ser construido con max_concurrent=None (sin límite propio)
    para evitar doble throttling. Si el fetcher tiene su propio semáforo,
    pasa max_concurrent=max_workers para que ambos sean coherentes.
    """

    def __init__(
        self,
        symbols:     List[str],
        timeframes:  List[str],
        start_date:  str,
        max_workers: int = DEFAULT_MAX_WORKERS,
        storage:     HistoricalStorage | None = None,
    ) -> None:
        _validate_pipeline_inputs(symbols, timeframes, start_date)

        self.symbols    = symbols
        self.timeframes = timeframes
        self.start_date = _parse_start_date(start_date)   # falla rápido si el formato es inválido
        self.max_workers = max_workers

        # Inyección de dependencias: facilita testing con mocks
        self._storage = storage or HistoricalStorage()

        # Semáforo único para el pipeline – el fetcher NO debe tener el suyo
        self._semaphore = asyncio.Semaphore(max_workers)

        self._fetcher = HistoricalFetcherAsync(
            storage=self._storage,
            transformer=OHLCVTransformer,
            max_concurrent=max_workers,   # coherente con el semáforo del pipeline
        )

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    async def run(self) -> PipelineSummary:
        """
        Ejecuta el pipeline completo y devuelve el resumen de resultados.

        Garantiza el cierre del fetcher incluso si alguna tarea falla
        o es cancelada (SafeOps).
        """
        pairs = [
            (symbol, timeframe)
            for symbol in self.symbols
            for timeframe in self.timeframes
        ]

        logger.info(
            "Historical pipeline starting | symbols={} timeframes={} pairs={} workers={}",
            len(self.symbols), len(self.timeframes), len(pairs), self.max_workers,
        )

        try:
            results: List[PairResult] = await asyncio.gather(
                *[self._process_pair(symbol, tf) for symbol, tf in pairs],
                return_exceptions=False,   # CancelledError se propaga limpiamente
            )
        finally:
            await self._fetcher.close()

        summary = PipelineSummary(results=list(results))
        summary.log()

        if summary.failed:
            logger.warning(
                "Pipeline finished with errors | failed={}/{}",
                summary.failed, summary.total,
            )
        else:
            logger.success(
                "Historical pipeline completed | rows={} pairs={}",
                summary.total_rows, summary.total,
            )

        return summary

    # ----------------------------------------------------------
    # Private
    # ----------------------------------------------------------

    async def _process_pair(self, symbol: str, timeframe: str) -> PairResult:
        """
        Descarga y persiste un único par símbolo/timeframe.

        Devuelve siempre un PairResult (nunca lanza excepciones al caller)
        para que asyncio.gather pueda agregar todos los resultados
        independientemente de los fallos individuales.

        La única excepción que se propaga es CancelledError (SafeOps:
        no suprimir cancelaciones del sistema).
        """
        result = PairResult(symbol=symbol, timeframe=timeframe)

        async with self._semaphore:
            try:
                logger.debug("Fetching | symbol={} timeframe={}", symbol, timeframe)

                df = await self._fetcher.download_data(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_date=str(self.start_date.date()),
                )

                if df is None or df.empty:
                    result.skipped = True
                    logger.debug("No new data | symbol={} timeframe={}", symbol, timeframe)
                    return result

                self._storage.save_ohlcv(
                    df=df,
                    symbol=symbol,
                    timeframe=timeframe,
                    mode="append",
                )

                result.rows = len(df)
                logger.info(
                    "Pair completed | symbol={} timeframe={} rows={}",
                    symbol, timeframe, result.rows,
                )

            except asyncio.CancelledError:
                # SafeOps: cancelaciones del event loop nunca se suprimen
                raise

            except Exception as exc:
                result.error = str(exc)
                logger.error(
                    "Pair failed | symbol={} timeframe={} error={}",
                    symbol, timeframe, exc,
                )

        return result


# ==========================================================
# Private Input Validation Helpers
# ==========================================================

def _validate_pipeline_inputs(
    symbols:    List[str],
    timeframes: List[str],
    start_date: str,
) -> None:
    """
    Valida los inputs del constructor en un único lugar (DRY).

    Raises
    ------
    ValueError
        Si cualquier input es inválido.
    """
    if not symbols:
        raise ValueError("symbols list cannot be empty")
    if not timeframes:
        raise ValueError("timeframes list cannot be empty")
    if not start_date or not start_date.strip():
        raise ValueError("start_date must be provided and non-empty")


def _parse_start_date(start_date: str) -> "pd.Timestamp":
    """
    Parsea y valida start_date en el constructor (fail-fast).

    Un formato inválido como '2024-13-01' fallaría silenciosamente
    más adelante en el fetcher. Lo detectamos aquí.

    Raises
    ------
    ValueError
        Si el formato de fecha no es parseable.
    """
    import pandas as pd

    try:
        ts = pd.Timestamp(start_date)
    except Exception:
        raise ValueError(
            f"start_date has invalid format: '{start_date}'. "
            "Expected ISO 8601, e.g. '2022-01-01'."
        )

    if ts > pd.Timestamp.now():
        raise ValueError(
            f"start_date '{start_date}' is in the future. "
            "Historical pipeline requires a past date."
        )

    return ts


# ==========================================================
# Runner
# ==========================================================

async def run_historical_pipeline_async(
    config_path: Path = DEFAULT_CONFIG_PATH,
) -> PipelineSummary:
    """
    Entry point funcional del pipeline.

    Acepta config_path como parámetro para facilitar testing
    y ejecución desde otros módulos (main.py, Prefect, etc.)
    sin depender de la ruta hardcodeada.
    """
    config = load_config(config_path)

    # Extraer sección correcta del YAML (coherente con main.py)
    pipeline_cfg  = config.get("pipeline", {}) or {}
    historical_cfg = pipeline_cfg.get("historical", {}) or {}

    symbols    = historical_cfg.get("symbols", [])
    timeframes = historical_cfg.get("timeframes", [])
    start_date = historical_cfg.get("start_date")
    max_workers = config.get("max_concurrent", DEFAULT_MAX_WORKERS)

    pipeline = HistoricalPipelineAsync(
        symbols=symbols,
        timeframes=timeframes,
        start_date=start_date,
        max_workers=max_workers,
    )

    return await pipeline.run()


# ==========================================================
# Entrypoint
# ==========================================================

if __name__ == "__main__":
    import sys

    logger.remove()   # elimina el handler por defecto antes de añadir los propios
    logger.add(
        sys.stderr,
        level=LOG_LEVEL,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
    )
    logger.add(
        "logs/historical_pipeline_{time}.log",
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION,
        level=LOG_LEVEL,
        encoding="utf-8",
    )

    asyncio.run(run_historical_pipeline_async())
