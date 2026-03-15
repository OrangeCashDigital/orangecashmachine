# market_data/main.py
"""
OrangeCashMachine – Market Data Orchestration (Prefect 3)
=========================================================

Orquestador principal de pipelines de market data.

Responsabilidades
-----------------
• Cargar y validar configuración con schema estricto (Pydantic)
• Validar conexión al exchange antes de lanzar pipelines (fail-fast)
• Ejecutar pipelines habilitados en paralelo con manejo de fallos parciales
• Gestionar secretos desde variables de entorno o Prefect Blocks

Principios aplicados
--------------------
• SOLID  – cada task tiene responsabilidad única; config desacoplada del flow
• DRY    – schema Pydantic como única fuente de verdad de la configuración
• KISS   – sin abstracciones innecesarias sobre Prefect
• SafeOps – fallos parciales aislados, timeouts explícitos, secretos seguros
"""

from __future__ import annotations

import asyncio
import os
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from loguru import logger as loguru_logger
from prefect import flow, task, get_run_logger
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator

from market_data.batch.flows.historical_pipeline import HistoricalPipelineAsync
from market_data.pipelines.trades_pipeline import TradesPipelineAsync
from market_data.pipelines.derivatives_pipeline import DerivativesPipelineAsync


# ==========================================================
# Constants
# ==========================================================

BASE_DIR: Path = Path(__file__).resolve().parents[1]
CONFIG_PATH: Path = Path(os.getenv("OCM_CONFIG_PATH", str(BASE_DIR / "config" / "settings.yaml")))

EXCHANGE_TASK_TIMEOUT:  int = 30   # segundos para validar conexión al exchange
PIPELINE_TASK_TIMEOUT:  int = 3600 # segundos máximo por pipeline (1 hora)

DERIVATIVE_DATASET_KEYS: tuple[str, ...] = (
    "funding_rate",
    "open_interest",
    "liquidations",
    "mark_price",
    "index_price",
)

FUTURES_EXCHANGES: frozenset[str] = frozenset({"binance", "bybit"})


# ==========================================================
# Supported Exchanges Enum
# ==========================================================

class SupportedExchange(str, Enum):
    """
    Exchanges soportados por el sistema.

    Usar un Enum en lugar de un set literal permite:
    - Autocompletado en IDEs
    - Mensajes de error de Pydantic con los valores válidos
    - Extensión sin tocar el código de validación
    """
    BINANCE = "binance"
    BYBIT   = "bybit"
    OKX     = "okx"
    KUCOIN  = "kucoin"
    GATE    = "gate"


# ==========================================================
# Pydantic Configuration Schema
# ==========================================================
# Única fuente de verdad para la estructura del YAML.
# Cualquier campo faltante, tipado incorrectamente o fuera
# de rango falla en tiempo de carga, no en mitad del pipeline.
# ==========================================================

class ExchangeConfig(BaseModel):
    """
    Configuración del exchange.

    Secretos
    --------
    api_key y api_secret se resuelven en este orden de prioridad:
      1. Variables de entorno OCM_API_KEY / OCM_API_SECRET
      2. Valor en el YAML (aceptable solo en desarrollo local)

    En producción, el YAML NO debe contener credenciales.
    Usar Prefect Blocks o variables de entorno del servidor.
    """
    name:        SupportedExchange = SupportedExchange.BINANCE
    api_key:     SecretStr
    api_secret:  SecretStr
    test_symbol: str = "BTC/USDT"

    @model_validator(mode="before")
    @classmethod
    def inject_secrets_from_env(cls, values: Dict) -> Dict:
        """
        Inyecta credenciales desde variables de entorno si están disponibles.
        Las variables de entorno tienen prioridad sobre el YAML.
        """
        if api_key := os.getenv("OCM_API_KEY"):
            values["api_key"] = api_key
        if api_secret := os.getenv("OCM_API_SECRET"):
            values["api_secret"] = api_secret
        return values


class HistoricalConfig(BaseModel):
    symbols:    List[str]
    timeframes: List[str]
    start_date: str

    @field_validator("start_date")
    @classmethod
    def validate_start_date(cls, v: str) -> str:
        import pandas as pd
        try:
            ts = pd.Timestamp(v)
        except Exception:
            raise ValueError(
                f"start_date '{v}' is not a valid date. Use ISO 8601: '2022-01-01'."
            )
        if ts > pd.Timestamp.now():
            raise ValueError(f"start_date '{v}' is in the future.")
        return v

    @field_validator("symbols", "timeframes")
    @classmethod
    def validate_non_empty_list(cls, v: List[str], info) -> List[str]:
        if not v:
            raise ValueError(f"'{info.field_name}' cannot be empty.")
        return v


class PipelineConfig(BaseModel):
    historical: HistoricalConfig


class DatasetsConfig(BaseModel):
    """
    Flags de activación por tipo de dataset.
    Todos desactivados por defecto: el operador activa explícitamente.
    """
    ohlcv:         bool = False
    trades:        bool = False
    funding_rate:  bool = False
    open_interest: bool = False
    liquidations:  bool = False
    mark_price:    bool = False
    index_price:   bool = False

    @property
    def active_derivative_datasets(self) -> List[str]:
        return [
            key for key in DERIVATIVE_DATASET_KEYS
            if getattr(self, key, False)
        ]

    @property
    def any_active(self) -> bool:
        return any([
            self.ohlcv, self.trades,
            *[getattr(self, k) for k in DERIVATIVE_DATASET_KEYS],
        ])


class AppConfig(BaseModel):
    """
    Schema completo del archivo settings.yaml.

    Ejemplo mínimo de YAML válido
    ------------------------------
    exchange:
      name: binance
      api_key: ${OCM_API_KEY}      # o dejar vacío y usar env vars
      api_secret: ${OCM_API_SECRET}
      test_symbol: BTC/USDT

    pipeline:
      historical:
        symbols: [BTC/USDT, ETH/USDT]
        timeframes: [1h, 4h]
        start_date: "2022-01-01"

    datasets:
      ohlcv: true
      trades: false

    max_concurrent: 6
    """
    exchange:       ExchangeConfig
    pipeline:       PipelineConfig
    datasets:       DatasetsConfig         = Field(default_factory=DatasetsConfig)
    max_concurrent: int                    = Field(default=6, ge=1, le=50)


# ==========================================================
# Config Loader Task
# ==========================================================

@task(
    name="load_and_validate_config",
    retries=2,
    retry_delay_seconds=[5, 30],   # backoff progresivo
    description="Loads and validates YAML config against Pydantic schema.",
)
def load_and_validate_config(path: Path) -> AppConfig:
    """
    Carga el YAML y lo valida contra AppConfig.

    Raises
    ------
    FileNotFoundError  – si el archivo no existe
    pydantic.ValidationError – si el schema no se cumple
    """
    log = get_run_logger()

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    except yaml.YAMLError as exc:
        log.critical("Failed parsing YAML | file=%s error=%s", path, exc)
        raise

    config = AppConfig.model_validate(raw)   # lanza ValidationError con detalle completo
    log.info("Config loaded and validated | file=%s", path)
    return config


# ==========================================================
# Exchange Validation Task
# ==========================================================

@task(
    name="validate_exchange_connection",
    retries=3,
    retry_delay_seconds=[10, 30, 60],   # backoff exponencial en reconexión
    timeout_seconds=EXCHANGE_TASK_TIMEOUT,
    description="Validates exchange connectivity before launching pipelines.",
)
async def validate_exchange_connection(config: AppConfig) -> None:
    """
    Verifica que el exchange responde antes de lanzar los pipelines.

    Abre y cierra su propia conexión (no comparte estado con los pipelines).
    El bloque finally garantiza el cierre incluso si fetch_ticker lanza.
    """
    log = get_run_logger()

    exc_cfg     = config.exchange
    name        = exc_cfg.name.value
    test_symbol = exc_cfg.test_symbol

    log.info("Validating exchange | name=%s symbol=%s", name, test_symbol)

    try:
        import ccxt.async_support as ccxt
    except ImportError:
        log.error("ccxt not installed. Run: pip install ccxt")
        raise

    exchange_options: Dict = {"enableRateLimit": True}
    if name in FUTURES_EXCHANGES:
        exchange_options["options"] = {"defaultType": "future"}

    exchange = getattr(ccxt, name)({
        "apiKey":    exc_cfg.api_key.get_secret_value(),
        "secret":    exc_cfg.api_secret.get_secret_value(),
        **exchange_options,
    })

    try:
        ticker = await asyncio.wait_for(
            exchange.fetch_ticker(test_symbol),
            timeout=EXCHANGE_TASK_TIMEOUT,
        )
        log.info(
            "Exchange validated | name=%s symbol=%s last_price=%s",
            name, test_symbol, ticker.get("last"),
        )
    except asyncio.TimeoutError:
        log.error("Exchange validation timed out | name=%s", name)
        raise
    except Exception as exc:
        log.error("Exchange validation failed | name=%s error=%s", name, exc)
        raise
    finally:
        await exchange.close()


# ==========================================================
# Pipeline Tasks
# ==========================================================

@task(
    name="historical_ohlcv_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    description="Ingests historical OHLCV data for all configured pairs.",
)
async def run_historical_pipeline(config: AppConfig) -> None:
    """
    Idempotente: HistoricalPipelineAsync hace append deduplicado por timestamp.
    En cada reintento de Prefect no se duplican datos.
    """
    log = get_run_logger()
    hist = config.pipeline.historical

    pipeline = HistoricalPipelineAsync(
        symbols=hist.symbols,
        timeframes=hist.timeframes,
        start_date=hist.start_date,
        max_workers=config.max_concurrent,
    )

    log.info("Historical OHLCV pipeline starting")
    summary = await pipeline.run()
    log.info(
        "Historical OHLCV pipeline finished | ok=%s failed=%s rows=%s",
        summary.succeeded, summary.failed, summary.total_rows,
    )

    # Falla la task si TODOS los pares fallaron (fallo total = bug sistémico)
    if summary.failed == summary.total and summary.total > 0:
        raise RuntimeError(
            f"Historical pipeline: all {summary.total} pairs failed. "
            "Check exchange connectivity and credentials."
        )


@task(
    name="trades_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    description="Ingests trade-level data for all configured symbols.",
)
async def run_trades_pipeline(config: AppConfig) -> None:
    log = get_run_logger()

    pipeline = TradesPipelineAsync(
        symbols=config.pipeline.historical.symbols,
        max_concurrent=config.max_concurrent,
        config=config.model_dump(),
    )

    log.info("Trades pipeline starting")
    await pipeline.run()
    log.info("Trades pipeline finished")


@task(
    name="derivatives_pipeline",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=PIPELINE_TASK_TIMEOUT,
    description="Ingests derivatives data (funding rate, OI, liquidations, etc.)",
)
async def run_derivatives_pipeline(
    config:   AppConfig,
    datasets: List[str],
) -> None:
    log = get_run_logger()

    pipeline = DerivativesPipelineAsync(
        symbols=config.pipeline.historical.symbols,
        datasets=datasets,
        max_concurrent=config.max_concurrent,
        config=config.model_dump(),
    )

    log.info("Derivatives pipeline starting | datasets=%s", datasets)
    await pipeline.run()
    log.info("Derivatives pipeline finished | datasets=%s", datasets)


# ==========================================================
# Flow Result Aggregator
# ==========================================================

async def _collect_task_results(futures: list) -> None:
    """
    Espera todas las tasks de Prefect y agrega los resultados.

    Usa return_exceptions=True para que el fallo de un pipeline
    no cancele los demás (SafeOps: fallos parciales aislados).
    Loguea cada fallo y relanza si todos los pipelines fallaron.
    """
    log = get_run_logger()

    outcomes = await asyncio.gather(*futures, return_exceptions=True)

    failures = [r for r in outcomes if isinstance(r, BaseException)]
    successes = len(outcomes) - len(failures)

    if failures:
        for exc in failures:
            log.error("Pipeline task failed | error=%s", exc)

    if failures and successes == 0:
        raise RuntimeError(
            f"All {len(failures)} pipeline(s) failed. "
            "Review individual task logs for details."
        )

    if failures:
        log.warning(
            "Flow completed with partial failures | ok=%s failed=%s",
            successes, len(failures),
        )


# ==========================================================
# Main Flow
# ==========================================================

@flow(
    name="market_data_ingestion",
    log_prints=True,
    retries=1,
    retry_delay_seconds=60,
)
async def market_data_flow(config_path: Path = CONFIG_PATH) -> None:
    """
    Orquestador principal del sistema de ingestión.

    Secuencia de ejecución
    ----------------------
    1. load_and_validate_config  – falla rápido si el YAML es inválido
    2. validate_exchange_connection – falla rápido si el exchange no responde
    3. Pipelines habilitados en paralelo con fallos parciales aislados

    El parámetro config_path permite sobreescribir la ruta en testing
    sin modificar variables globales.
    """
    log = get_run_logger()

    # Paso 1: config validada – cualquier error aquí cancela el flow completo
    config: AppConfig = load_and_validate_config(config_path)

    # Paso 2: validación de exchange – secuencial y antes de cualquier pipeline
    await validate_exchange_connection(config)

    # Paso 3: verificar que al menos un pipeline está habilitado
    if not config.datasets.any_active:
        log.warning("No pipelines enabled in configuration. Flow exiting.")
        return

    # Validación cruzada: ohlcv activo requiere symbols y timeframes
    # (ya garantizado por Pydantic, pero explicitamos el contexto)
    log.info(
        "Launching pipelines | ohlcv=%s trades=%s derivatives=%s",
        config.datasets.ohlcv,
        config.datasets.trades,
        bool(config.datasets.active_derivative_datasets),
    )

    # Paso 4: construir y lanzar tasks en paralelo
    pipeline_futures = []

    if config.datasets.ohlcv:
        pipeline_futures.append(
            run_historical_pipeline.submit(config)
        )

    if config.datasets.trades:
        pipeline_futures.append(
            run_trades_pipeline.submit(config)
        )

    derivative_datasets = config.datasets.active_derivative_datasets
    if derivative_datasets:
        pipeline_futures.append(
            run_derivatives_pipeline.submit(config, derivative_datasets)
        )

    await _collect_task_results(pipeline_futures)

    log.info("Market data flow completed successfully")


# ==========================================================
# Entrypoint
# ==========================================================

if __name__ == "__main__":
    import sys

    loguru_logger.remove()
    loguru_logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
    )

    asyncio.run(market_data_flow())

    # Producción (Prefect Cloud/Server):
    # market_data_flow.serve(
    #     name="market-data-ingestion",
    #     cron="0 * * * *",
    #     parameters={"config_path": "/app/config/settings.yaml"},
    # )
