"""
core/config/schema.py
=====================

Schema central de configuración para OrangeCashMachine.

Responsabilidad
---------------
Definir y validar la estructura completa de la configuración del sistema.

Este módulo NO carga archivos YAML.
Eso es responsabilidad de `loader.py`.

Principios aplicados
--------------------
SOLID
DRY
KISS
SafeOps
"""

from __future__ import annotations

import os
import re
import warnings
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional

from pydantic import (

    BaseModel,
    Field,
    SecretStr,
    field_validator,
    model_validator,
    ConfigDict,
)

from pathlib import Path

CONFIG_PATH: Path = Path("config/settings.yaml")
# ==========================================================
# Timeouts operativos
# ==========================================================

EXCHANGE_TASK_TIMEOUT: int  = 120   # segundos
PIPELINE_TASK_TIMEOUT: int  = 3600  # segundos


# ==========================================================
# CONSTANTES
# ==========================================================

SYMBOL_REGEX = re.compile(r"^[A-Z0-9]+/[A-Z0-9]+$")

DERIVATIVE_DATASET_KEYS: tuple[str, ...] = (
    "funding_rate",
    "open_interest",
    "liquidations",
    "mark_price",
    "index_price",
)

_EXCHANGES_WITH_PASSPHRASE: frozenset[str] = frozenset({"kucoin", "okx"})


# ==========================================================
# ENUMS
# ==========================================================

class SupportedExchange(str, Enum):
    """Exchanges soportados por el sistema."""

    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"
    KUCOIN = "kucoin"
    GATE = "gate"


# ==========================================================
# MARKET CONFIG
# ==========================================================

class MarketConfig(BaseModel):
    """Configuración de un mercado específico."""

    model_config = ConfigDict(frozen=True)

    enabled: bool = False
    symbols: List[str] = Field(default_factory=list)

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, v: List[str]) -> List[str]:

        normalized: List[str] = []

        for symbol in v:
            s = symbol.strip().upper()

            if not SYMBOL_REGEX.match(s):
                raise ValueError(
                    f"Invalid symbol format '{symbol}'. Expected BTC/USDT style."
                )

            normalized.append(s)

        return normalized


class MarketsConfig(BaseModel):
    """Mercados disponibles dentro de un exchange."""

    model_config = ConfigDict(frozen=True)

    spot: MarketConfig = Field(default_factory=MarketConfig)
    futures: MarketConfig = Field(default_factory=MarketConfig)

    @property
    def enabled_markets(self) -> Dict[str, MarketConfig]:
        """Devuelve únicamente mercados activos."""

        return {
            name: mkt
            for name, mkt in {
                "spot": self.spot,
                "futures": self.futures,
            }.items()
            if mkt.enabled
        }


# ==========================================================
# EXCHANGE CONFIG
# ==========================================================

class ExchangeConfig(BaseModel):
    """Configuración completa de un exchange."""

    model_config = ConfigDict(frozen=True)

    name: SupportedExchange
    enabled: bool = True
    enableRateLimit: bool = True

    api_key: SecretStr = SecretStr("")
    api_secret: SecretStr = SecretStr("")
    api_password: SecretStr = SecretStr("")

    test_symbol: str = "BTC/USDT"

    markets: MarketsConfig = Field(default_factory=MarketsConfig)

    # ------------------------------------------------------
    # Credenciales
    # ------------------------------------------------------

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, values: dict) -> dict:

        name = str(values.get("name", "")).upper()

        if creds := values.pop("credentials", None):

            values.setdefault("api_key", creds.get("apiKey", ""))
            values.setdefault("api_secret", creds.get("secret", ""))
            values.setdefault("api_password", creds.get("password", ""))

        values["api_key"] = os.getenv(
            f"{name}_API_KEY",
            values.get("api_key", os.getenv("OCM_API_KEY", "")),
        )

        values["api_secret"] = os.getenv(
            f"{name}_API_SECRET",
            values.get("api_secret", os.getenv("OCM_API_SECRET", "")),
        )

        passphrase = (
            os.getenv(f"{name}_PASSPHRASE")
            or os.getenv(f"{name}_PASSWORD")
            or values.get("api_password", "")
        )

        values["api_password"] = passphrase

        return values

    # ------------------------------------------------------
    # Validación de credenciales
    # ------------------------------------------------------

    @model_validator(mode="after")
    def validate_credentials(self):

        if self.enabled and not self.has_credentials:

            raise ValueError(
                f"Exchange '{self.name.value}' enabled but credentials missing."
            )

        if self.requires_passphrase and not self.has_passphrase:

            raise ValueError(
                f"Exchange '{self.name.value}' requires passphrase."
            )

        return self

    # ------------------------------------------------------
    # Propiedades útiles
    # ------------------------------------------------------

    @property
    def all_symbols(self) -> List[str]:

        seen: set[str] = set()
        result: List[str] = []

        for mkt in (self.markets.spot, self.markets.futures):
            for symbol in mkt.symbols:
                if symbol not in seen:
                    seen.add(symbol)
                    result.append(symbol)

        return result

    @property
    def has_credentials(self) -> bool:

        return bool(
            self.api_key.get_secret_value()
            and self.api_secret.get_secret_value()
        )

    @property
    def requires_passphrase(self) -> bool:

        return self.name.value in _EXCHANGES_WITH_PASSPHRASE

    @property
    def has_passphrase(self) -> bool:

        return bool(self.api_password.get_secret_value())

    def ccxt_credentials(self) -> dict:
        """Devuelve credenciales listas para ccxt."""

        creds = {
            "apiKey": self.api_key.get_secret_value(),
            "secret": self.api_secret.get_secret_value(),
            "enableRateLimit": self.enableRateLimit,
        }

        if self.requires_passphrase and self.has_passphrase:
            creds["password"] = self.api_password.get_secret_value()

        return creds


# ==========================================================
# PIPELINE
# ==========================================================

class RetryPolicy(BaseModel):

    model_config = ConfigDict(frozen=True)

    max_attempts: int = 5
    backoff_factor: int = 2
    jitter: bool = True


class HistoricalConfig(BaseModel):

    model_config = ConfigDict(frozen=True)

    start_date: str = "2017-01-01T00:00:00Z"

    fetch_all_history: bool = False

    max_concurrent_tasks: int = Field(default=6, ge=1, le=64)

    timeframes: List[str] = Field(default_factory=list)

    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)

    @field_validator("timeframes")
    @classmethod
    def validate_timeframes(cls, v: List[str]):

        if not v:
            raise ValueError("At least one timeframe must be defined.")

        return v

    @field_validator("start_date")
    @classmethod
    def validate_start_date(cls, v: str):

        try:
            ts = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:

            raise ValueError("start_date must be ISO8601.")

        if ts > datetime.now(timezone.utc):
            raise ValueError("start_date cannot be in the future.")

        return v


class RealtimeConfig(BaseModel):

    model_config = ConfigDict(frozen=True)

    reconnect_delay_seconds: int = Field(default=5, ge=1)

    heartbeat_timeout_seconds: int = Field(default=30, ge=5)

    snapshot_interval_seconds: int = Field(default=60, ge=10)

    max_stream_buffer: int = Field(default=50_000, ge=1000)


class PipelineConfig(BaseModel):

    model_config = ConfigDict(frozen=True)

    historical: HistoricalConfig = Field(default_factory=HistoricalConfig)

    realtime: RealtimeConfig = Field(default_factory=RealtimeConfig)


# ==========================================================
# DATASETS
# ==========================================================

class DatasetsConfig(BaseModel):

    model_config = ConfigDict(frozen=True)

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
            if getattr(self, key)
        ]

    @property
    def active_datasets(self) -> List[str]:

        core = [
            name
            for name in ("ohlcv", "trades", "orderbook")
            if getattr(self, name)
        ]

        return core + self.active_derivative_datasets

    @property
    def any_active(self) -> bool:

        return bool(self.active_datasets)


# ==========================================================
# STORAGE
# ==========================================================

class StorageConfig(BaseModel):

    model_config = ConfigDict(frozen=True)

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
# APP CONFIG
# ==========================================================

class AppConfig(BaseModel):
    """Schema raíz del sistema de configuración."""

    model_config = ConfigDict(frozen=True)

    exchanges: List[ExchangeConfig]

    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)

    datasets: DatasetsConfig = Field(default_factory=DatasetsConfig)

    storage: StorageConfig = Field(default_factory=StorageConfig)

    # ------------------------------------------------------

    @model_validator(mode="before")
    @classmethod
    def parse_exchanges_dict(cls, values: dict):

        raw = values.get("exchanges", {})

        if isinstance(raw, dict):

            values["exchanges"] = [
                {"name": name, **cfg}
                for name, cfg in raw.items()
                if cfg.get("enabled", True)
            ]

        return values

    # ------------------------------------------------------

    @model_validator(mode="after")
    def validate_exchanges(self):

        if not self.exchanges:
            raise ValueError("At least one exchange must be enabled.")

        return self

    @model_validator(mode="after")
    def warn_if_no_datasets(self):

        if not self.datasets.any_active:

            warnings.warn(
                "No datasets enabled.",
                UserWarning,
                stacklevel=2,
            )

        return self

    # ------------------------------------------------------

    @property
    def exchange_names(self) -> List[str]:

        return [e.name.value for e in self.exchanges]

    def get_exchange(self, name: str) -> Optional[ExchangeConfig]:

        target = name.lower()

        for exc in self.exchanges:
            if exc.name.value == target:
                return exc

        return None