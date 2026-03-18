from __future__ import annotations

"""
core/config/schema.py (PRO+)

Schema tipado, seguro y extensible para OrangeCashMachine.

Principios:
SOLID · KISS · DRY · SafeOps
"""

import os
import re
import warnings
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import (
    BaseModel,
    Field,
    SecretStr,
    field_validator,
    model_validator,
    ConfigDict,
)

CONFIG_PATH: Path = Path("config/settings.yaml")

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

SYMBOL_REGEX = re.compile(r"^[A-Z0-9]+/[A-Z0-9]+$")
_ALLOWED_TIMEFRAMES = {"1m", "5m", "15m", "1h", "4h", "1d"}
_EXCHANGES_WITH_PASSPHRASE = {"kucoin", "okx"}


# -----------------------------------------------------------------------------
# Base Model (SafeOps)
# -----------------------------------------------------------------------------

class StrictBaseModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",   # 🔥 bloquea campos inesperados
        validate_assignment=True,
    )


# -----------------------------------------------------------------------------
# Enums
# -----------------------------------------------------------------------------

class SupportedExchange(str, Enum):
    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"
    KUCOIN = "kucoin"


# -----------------------------------------------------------------------------
# Market
# -----------------------------------------------------------------------------

class MarketConfig(StrictBaseModel):
    enabled: bool = False
    symbols: List[str] = Field(default_factory=list)

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, v: List[str]) -> List[str]:
        seen = set()
        result = []

        for symbol in v:
            s = symbol.strip().upper()
            if not SYMBOL_REGEX.match(s):
                raise ValueError(f"Invalid symbol: {symbol}")
            if s not in seen:
                seen.add(s)
                result.append(s)

        return result


class MarketsConfig(StrictBaseModel):
    spot: MarketConfig = Field(default_factory=MarketConfig)
    futures: MarketConfig = Field(default_factory=MarketConfig)


# -----------------------------------------------------------------------------
# Exchange
# -----------------------------------------------------------------------------

class ExchangeConfig(StrictBaseModel):

    name: SupportedExchange
    enabled: bool = True
    enableRateLimit: bool = True

    api_key: SecretStr = SecretStr("")
    api_secret: SecretStr = SecretStr("")
    api_password: SecretStr = SecretStr("")

    markets: MarketsConfig = Field(default_factory=MarketsConfig)

    # -------------------
    # Credentials
    # -------------------

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, values: dict):

        name = str(values.get("name", "")).upper()

        creds = values.pop("credentials", {}) or {}

        values["api_key"] = os.getenv(f"{name}_API_KEY", creds.get("apiKey", ""))
        values["api_secret"] = os.getenv(f"{name}_API_SECRET", creds.get("secret", ""))

        values["api_password"] = (
            os.getenv(f"{name}_PASSPHRASE")
            or creds.get("password", "")
        )

        return values

    @model_validator(mode="after")
    def validate_credentials(self):

        if self.enabled:
            if not self.api_key.get_secret_value() or not self.api_secret.get_secret_value():
                raise ValueError(f"{self.name} missing credentials")

        if self.name.value in _EXCHANGES_WITH_PASSPHRASE:
            if not self.api_password.get_secret_value():
                raise ValueError(f"{self.name} requires passphrase")

        return self


# -----------------------------------------------------------------------------
# Pipeline
# -----------------------------------------------------------------------------

class RetryPolicy(StrictBaseModel):
    max_attempts: int = 5
    backoff_factor: int = 2
    jitter: bool = True


class HistoricalConfig(StrictBaseModel):

    start_date: str
    fetch_all_history: bool = False
    max_concurrent_tasks: int = Field(default=4, ge=1, le=32)
    timeframes: List[str]
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)

    @field_validator("timeframes")
    @classmethod
    def validate_timeframes(cls, v):
        invalid = [tf for tf in v if tf not in _ALLOWED_TIMEFRAMES]
        if invalid:
            raise ValueError(f"Invalid timeframes: {invalid}")
        return v

    @field_validator("start_date")
    @classmethod
    def validate_date(cls, v):
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        if dt > datetime.now(timezone.utc):
            raise ValueError("start_date in future")
        return v


class RealtimeConfig(StrictBaseModel):
    reconnect_delay_seconds: int = 5
    heartbeat_timeout_seconds: int = 30
    snapshot_interval_seconds: int = 60
    max_stream_buffer: int = 50000


class PipelineConfig(StrictBaseModel):
    historical: HistoricalConfig
    realtime: RealtimeConfig


# -----------------------------------------------------------------------------
# Storage (aligned with YAML)
# -----------------------------------------------------------------------------

class DataLakeConfig(StrictBaseModel):
    path: str
    format: str = "parquet"
    compression: str = "snappy"
    partitioning: List[str]


class StorageConfig(StrictBaseModel):
    data_lake: DataLakeConfig


# -----------------------------------------------------------------------------
# Datasets
# -----------------------------------------------------------------------------

class DatasetsConfig(StrictBaseModel):
    ohlcv: bool = False
    trades: bool = False
    orderbook: bool = False

    def any_active(self) -> bool:
        return any(self.model_dump().values())


# -----------------------------------------------------------------------------
# Integrations
# -----------------------------------------------------------------------------

class RedisConfig(StrictBaseModel):
    enabled: bool = False
    host: str = "localhost"
    port: int = 6379
    db: int = 0


class IntegrationsConfig(StrictBaseModel):
    redis: RedisConfig = Field(default_factory=RedisConfig)


# -----------------------------------------------------------------------------
# Observability
# -----------------------------------------------------------------------------

class LoggingConfig(StrictBaseModel):
    level: str = "INFO"
    format: str = "json"


class ObservabilityConfig(StrictBaseModel):
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


# -----------------------------------------------------------------------------
# Audit
# -----------------------------------------------------------------------------

class AuditEntry(StrictBaseModel):
    timestamp: datetime
    cache_key: str
    hash: str
    source_file: str


# -----------------------------------------------------------------------------
# AppConfig
# -----------------------------------------------------------------------------

class AppConfig(StrictBaseModel):

    exchanges: List[ExchangeConfig]
    pipeline: PipelineConfig
    storage: StorageConfig

    datasets: DatasetsConfig = Field(default_factory=DatasetsConfig)
    integrations: IntegrationsConfig = Field(default_factory=IntegrationsConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    audit_log: List[AuditEntry] = Field(default_factory=list)
    last_reload: Optional[datetime] = None

    # -------------------
    @model_validator(mode="before")
    @classmethod
    def parse_exchanges(cls, values):

        raw = values.get("exchanges", {})

        if isinstance(raw, dict):
            values["exchanges"] = [
                {"name": name, **cfg}
                for name, cfg in raw.items()
                if cfg.get("enabled", True)
            ]

        return values

    @model_validator(mode="after")
    def validate(self):

        if not self.exchanges:
            raise ValueError("No exchanges enabled")

        if not self.datasets.any_active():
            warnings.warn("No datasets enabled", stacklevel=2)

        return self

    # -------------------
    @property
    def exchange_names(self) -> List[str]:
        return [e.name.value for e in self.exchanges]