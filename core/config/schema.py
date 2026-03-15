"""
core/config/schema.py

Schema de configuración del sistema OrangeCashMachine.

Responsabilidad
---------------
Definir y validar la estructura completa del archivo settings.yaml.
Este módulo es la única fuente de verdad para la configuración.

Todos los módulos del sistema deben importar AppConfig desde aquí.
Nunca se debe parsear el YAML directamente fuera de este módulo.

Principios aplicados
--------------------
SOLID   – SRP: este módulo solo modela configuración
OCP     – añadir exchanges o datasets sin modificar validaciones
DRY     – lógica de secretos y normalización centralizada
KISS    – modelos simples y claros
SafeOps – validaciones explícitas y uso de SecretStr
"""

from __future__ import annotations

import os
import warnings
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator


# ==========================================================
# Paths
# ==========================================================

BASE_DIR: Path = Path(__file__).resolve().parents[3]

CONFIG_PATH: Path = Path(
    os.getenv("OCM_CONFIG_PATH", str(BASE_DIR / "config" / "settings.yaml"))
)


# ==========================================================
# System constants
# ==========================================================

EXCHANGE_TASK_TIMEOUT: int = 30
PIPELINE_TASK_TIMEOUT: int = 3600

DERIVATIVE_DATASET_KEYS: tuple[str, ...] = (
    "funding_rate",
    "open_interest",
    "liquidations",
    "mark_price",
    "index_price",
)


# ==========================================================
# Enums
# ==========================================================

class SupportedExchange(str, Enum):
    """Exchanges soportados por el sistema."""

    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"
    KUCOIN = "kucoin"
    GATE = "gate"


# ==========================================================
# Exchange configuration
# ==========================================================

class MarketConfig(BaseModel):
    """Configuración de un mercado específico."""

    enabled: bool = False
    symbols: List[str] = Field(default_factory=list)
    defaultType: str = "spot"

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, symbols: List[str]) -> List[str]:
        """Normaliza símbolos a formato estándar."""
        return [s.strip().upper() for s in symbols]


class MarketsConfig(BaseModel):
    """Configuración de mercados disponibles."""

    spot: MarketConfig = Field(default_factory=MarketConfig)
    futures: MarketConfig = Field(default_factory=MarketConfig)

    @property
    def enabled_markets(self) -> Dict[str, MarketConfig]:
        """Retorna solo mercados habilitados."""
        return {
            name: market
            for name, market in (
                ("spot", self.spot),
                ("futures", self.futures),
            )
            if market.enabled
        }


class ExchangeConfig(BaseModel):
    """Configuración completa de un exchange."""

    name: SupportedExchange

    enabled: bool = True
    enableRateLimit: bool = True

    api_key: SecretStr = SecretStr("")
    api_secret: SecretStr = SecretStr("")

    test_symbol: str = "BTC/USDT"

    markets: MarketsConfig = Field(default_factory=MarketsConfig)

    # ------------------------------------------------------
    # Credential resolution
    # ------------------------------------------------------

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, values: dict) -> dict:
        """
        Resuelve credenciales desde:

        1. credentials en YAML
        2. variables de entorno específicas
        3. variables globales
        """

        name = str(values.get("name", "")).upper()

        creds = values.pop("credentials", None)
        if creds:
            values.setdefault("api_key", creds.get("apiKey", ""))
            values.setdefault("api_secret", creds.get("secret", ""))

        values["api_key"] = os.getenv(
            f"{name}_API_KEY",
            values.get("api_key", os.getenv("OCM_API_KEY", "")),
        )

        values["api_secret"] = os.getenv(
            f"{name}_API_SECRET",
            values.get("api_secret", os.getenv("OCM_API_SECRET", "")),
        )

        return values

    # ------------------------------------------------------
    # Helper properties
    # ------------------------------------------------------

    @property
    def all_symbols(self) -> List[str]:
        """Retorna todos los símbolos únicos del exchange."""
        seen: set[str] = set()
        result: List[str] = []

        for market in (self.markets.spot, self.markets.futures):
            for symbol in market.symbols:
                if symbol not in seen:
                    seen.add(symbol)
                    result.append(symbol)

        return result

    @property
    def has_credentials(self) -> bool:
        """Indica si el exchange tiene credenciales."""
        return bool(
            self.api_key.get_secret_value()
            and self.api_secret.get_secret_value()
        )


# ==========================================================
# Pipeline configuration
# ==========================================================

class RetryPolicy(BaseModel):
    """Política de reintentos."""

    max_attempts: int = 5
    backoff_factor: int = 2
    jitter: bool = True


class HistoricalConfig(BaseModel):
    """Configuración del pipeline histórico."""

    start_date: str = "2017-01-01T00:00:00Z"
    fetch_all_history: bool = False

    max_concurrent_tasks: int = Field(default=6, ge=1, le=50)

    timeframes: List[str] = Field(default_factory=list)

    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)

    @field_validator("timeframes")
    @classmethod
    def validate_timeframes(cls, value: List[str]) -> List[str]:
        if not value:
            raise ValueError("timeframes cannot be empty")
        return value

    @field_validator("start_date")
    @classmethod
    def validate_start_date(cls, value: str) -> str:
        """Valida formato ISO8601."""
        try:
            ts = pd.Timestamp(value)
        except Exception as exc:
            raise ValueError(
                f"Invalid start_date '{value}'. Use ISO 8601 format."
            ) from exc

        if ts > pd.Timestamp.now(tz="UTC"):
            raise ValueError("start_date cannot be in the future")

        return value


class RealtimeConfig(BaseModel):
    """Configuración del pipeline realtime."""

    reconnect_delay_seconds: int = 5
    heartbeat_timeout_seconds: int = 30
    snapshot_interval_seconds: int = 60
    max_stream_buffer: int = 50_000


class PipelineConfig(BaseModel):
    """Configuración global de pipelines."""

    historical: HistoricalConfig = Field(default_factory=HistoricalConfig)
    realtime: RealtimeConfig = Field(default_factory=RealtimeConfig)


# ==========================================================
# Dataset configuration
# ==========================================================

class DatasetsConfig(BaseModel):
    """Activación de datasets."""

    ohlcv: bool = False
    trades: bool = False
    orderbook: bool = False

    funding_rate: bool = False
    open_interest: bool = False
    liquidations: bool = False
    mark_price: bool = False
    index_price: bool = False

    @property
    def active_derivative_datasets(self) -> List[str]:
        return [
            key for key in DERIVATIVE_DATASET_KEYS
            if getattr(self, key, False)
        ]

    @property
    def any_active(self) -> bool:
        return (
            self.ohlcv
            or self.trades
            or self.orderbook
            or bool(self.active_derivative_datasets)
        )


# ==========================================================
# Storage configuration
# ==========================================================

class StorageConfig(BaseModel):
    """Configuración del Data Lake."""

    data_lake_path: str = "data_platform/data_lake"

    format: str = "parquet"
    compression: str = "snappy"

    partitioning: List[str] = Field(
        default_factory=lambda: [
            "exchange",
            "market",
            "symbol",
            "dataset",
            "timeframe",
            "year",
            "month",
        ]
    )


# ==========================================================
# Root configuration
# ==========================================================

class AppConfig(BaseModel):
    """Schema completo de settings.yaml."""

    exchanges: List[ExchangeConfig]

    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    datasets: DatasetsConfig = Field(default_factory=DatasetsConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)

    max_concurrent: int = Field(default=6, ge=1, le=50)

    # ------------------------------------------------------
    # Exchange parsing
    # ------------------------------------------------------

    @model_validator(mode="before")
    @classmethod
    def parse_exchanges_dict(cls, values: dict) -> dict:
        raw = values.get("exchanges", {})

        if isinstance(raw, dict):
            values["exchanges"] = [
                {"name": name, **cfg}
                for name, cfg in raw.items()
                if cfg.get("enabled", True)
            ]

        return values

    # ------------------------------------------------------
    # Validation
    # ------------------------------------------------------

    @model_validator(mode="after")
    def validate_exchanges(self) -> "AppConfig":
        if not self.exchanges:
            raise ValueError("No enabled exchanges found in configuration")
        return self

    @model_validator(mode="after")
    def validate_datasets(self) -> "AppConfig":
        if not self.datasets.any_active:
            warnings.warn(
                "No datasets are active in configuration",
                UserWarning,
                stacklevel=2,
            )
        return self

    # ------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------

    @property
    def enabled_exchanges(self) -> List[ExchangeConfig]:
        return self.exchanges

    @property
    def exchange_names(self) -> List[str]:
        return [e.name.value for e in self.exchanges]

    def get_exchange(self, name: str) -> Optional[ExchangeConfig]:
        for exc in self.exchanges:
            if exc.name.value == name.lower():
                return exc
        return None