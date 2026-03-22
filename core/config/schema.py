from __future__ import annotations

"""
core/config/schema.py
=====================

Schema canónico de configuración para OrangeCashMachine.
Cubre todos los campos de base.yaml + development.yaml + settings.yaml.

Principios: SOLID · KISS · DRY · SafeOps
"""

import os
import re
import warnings
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator, ConfigDict

CONFIG_PATH: Path = Path("config/settings.yaml")

# =============================================================================
# Constants
# =============================================================================

SYMBOL_REGEX         = re.compile(r"^[A-Z0-9]+/[A-Z0-9]+(:[A-Z0-9]+)?$")
FUTURES_SYMBOL_REGEX = re.compile(r"^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$")
_ALLOWED_FUTURES_TYPES: frozenset[str] = frozenset({"swap", "future", "option"})

EXCHANGE_TASK_TIMEOUT: int = 120
PIPELINE_TASK_TIMEOUT: int = 3_600

_ALLOWED_TIMEFRAMES:        frozenset[str] = frozenset({"1m", "5m", "15m", "1h", "4h", "1d"})
_EXCHANGES_WITH_PASSPHRASE: frozenset[str] = frozenset({"kucoin", "okx"})


# =============================================================================
# Base Model
# =============================================================================

class StrictBaseModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", validate_assignment=True)


# =============================================================================
# Enums
# =============================================================================

class SupportedExchange(str, Enum):
    BINANCE = "binance"
    BYBIT   = "bybit"
    OKX     = "okx"
    KUCOIN  = "kucoin"
    GATE    = "gate"


# =============================================================================
# Environment
# =============================================================================

class EnvironmentConfig(StrictBaseModel):
    name:             str           = "base"
    version:          Optional[str] = None
    debug:            bool          = False
    last_modified_by: Optional[str] = None
    last_modified_at: Optional[Any] = None
    default_env:      Optional[str] = None  # pista local para _resolve_env — ignorado en runtime


# =============================================================================
# Market
# =============================================================================

class MarketConfig(StrictBaseModel):
    enabled:     bool          = False
    symbols:     List[str]     = Field(default_factory=list)
    defaultType: Optional[str] = None  # ccxt market type: swap | future | option

    @field_validator("defaultType")
    @classmethod
    def validate_default_type(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in _ALLOWED_FUTURES_TYPES:
            raise ValueError(
                f"Invalid defaultType: '{v}'. Allowed: {sorted(_ALLOWED_FUTURES_TYPES)}"
            )
        return v

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, v: List[str]) -> List[str]:
        seen, result = set(), []
        for symbol in v:
            s = symbol.strip().upper()
            if not SYMBOL_REGEX.match(s):
                raise ValueError(
                    f"Invalid symbol format: '{symbol}'. "
                    "Expected spot (BTC/USDT) or futures (BTC/USDT:USDT) notation."
                )
            if s not in seen:
                seen.add(s)
                result.append(s)
        return result


class MarketsConfig(StrictBaseModel):
    spot:    MarketConfig = Field(default_factory=MarketConfig)
    futures: MarketConfig = Field(default_factory=MarketConfig)

    @property
    def all_symbols(self) -> List[str]:
        """Todos los símbolos activos (spot + futures habilitados)."""
        seen, result = set(), []
        for mkt in (self.spot, self.futures):
            if mkt.enabled:
                for s in mkt.symbols:
                    if s not in seen:
                        seen.add(s)
                        result.append(s)
        return result

    @property
    def spot_symbols(self) -> List[str]:
        return self.spot.symbols if self.spot.enabled else []

    @property
    def futures_symbols(self) -> List[str]:
        return self.futures.symbols if self.futures.enabled else []

    @property
    def futures_default_type(self) -> Optional[str]:
        return self.futures.defaultType if self.futures.enabled else None


# =============================================================================
# Exchange
# =============================================================================

class ExchangeConfig(StrictBaseModel):
    name:                 SupportedExchange
    enabled:              bool           = True
    enableRateLimit:      bool           = True
    auto_discover_symbols: bool          = False
    options:              Dict[str, Any] = Field(default_factory=dict)
    test_symbol:          str            = "BTC/USDT"

    api_key:      SecretStr = SecretStr("")
    api_secret:   SecretStr = SecretStr("")
    api_password: SecretStr = SecretStr("")

    markets: MarketsConfig = Field(default_factory=MarketsConfig)

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, values: dict) -> dict:
        name  = str(values.get("name", "")).upper()
        creds = values.pop("credentials", {}) or {}
        values["api_key"]      = os.getenv(f"{name}_API_KEY",    creds.get("apiKey",    os.getenv("OCM_API_KEY", "")))
        values["api_secret"]   = os.getenv(f"{name}_API_SECRET", creds.get("secret",    os.getenv("OCM_API_SECRET", "")))
        values["api_password"] = os.getenv(f"{name}_PASSPHRASE") or os.getenv(f"{name}_PASSWORD") or creds.get("password", "")
        return values

    @model_validator(mode="after")
    def validate_credentials(self) -> "ExchangeConfig":
        if self.enabled and not self.has_credentials:
            raise ValueError(f"Exchange '{self.name.value}' is enabled but credentials are missing.")
        if self.requires_passphrase and not self.has_passphrase:
            raise ValueError(f"Exchange '{self.name.value}' requires a passphrase.")
        return self

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key.get_secret_value() and self.api_secret.get_secret_value())

    @property
    def requires_passphrase(self) -> bool:
        return self.name.value in _EXCHANGES_WITH_PASSPHRASE

    @property
    def has_passphrase(self) -> bool:
        return bool(self.api_password.get_secret_value())

    @property
    def all_symbols(self) -> List[str]:
        return self.markets.all_symbols

    @property
    def has_spot(self) -> bool:
        return self.markets.spot.enabled and bool(self.markets.spot_symbols)

    @property
    def has_futures(self) -> bool:
        return self.markets.futures.enabled and bool(self.markets.futures_symbols)

    def ccxt_credentials(self) -> dict:
        creds = {"apiKey": self.api_key.get_secret_value(), "secret": self.api_secret.get_secret_value(), "enableRateLimit": self.enableRateLimit}
        if self.requires_passphrase and self.has_passphrase:
            creds["password"] = self.api_password.get_secret_value()
        return creds


# =============================================================================
# Pipeline
# =============================================================================

class RetryPolicy(StrictBaseModel):
    max_attempts:  int  = 5
    backoff_factor: int = 2
    jitter:        bool = True


class HistoricalConfig(StrictBaseModel):
    start_date:           str       = "2017-01-01T00:00:00Z"
    fetch_all_history:    bool      = False
    max_concurrent_tasks: int       = Field(default=4, ge=1, le=64)
    timeframes:           List[str] = Field(default_factory=list)
    retry_policy:         RetryPolicy = Field(default_factory=RetryPolicy)

    @model_validator(mode="before")
    @classmethod
    def resolve_auto_concurrency(cls, values: dict) -> dict:
        if values.get("max_concurrent_tasks") == "auto":
            import os as _os
            values["max_concurrent_tasks"] = max(1, min((_os.cpu_count() or 4), 16))
        return values

    @field_validator("timeframes")
    @classmethod
    def validate_timeframes(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("At least one timeframe must be defined.")
        invalid = [tf for tf in v if tf not in _ALLOWED_TIMEFRAMES]
        if invalid:
            raise ValueError(f"Invalid timeframes: {invalid}. Allowed: {sorted(_ALLOWED_TIMEFRAMES)}")
        return v

    @field_validator("start_date")
    @classmethod
    def validate_start_date(cls, v: str) -> str:
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"start_date must be ISO 8601: '{v}'")
        if dt > datetime.now(timezone.utc):
            raise ValueError("start_date cannot be in the future.")
        return v


class RealtimeConfig(StrictBaseModel):
    reconnect_delay_seconds:   int = Field(default=5,      ge=1)
    heartbeat_timeout_seconds: int = Field(default=30,     ge=5)
    snapshot_interval_seconds: int = Field(default=60,     ge=10)
    max_stream_buffer:         int = Field(default=50_000, ge=1_000)
    drop_policy: Literal["reject", "drop_oldest", "drop_newest"] = "reject"


class TimeoutsConfig(StrictBaseModel):
    exchange_task:        int = Field(default=120,   ge=1)
    historical_pipeline:  int = Field(default=3_600, ge=1)
    trades_pipeline:      int = Field(default=1_800, ge=1)
    derivatives_pipeline: int = Field(default=2_700, ge=1)


class PipelineConfig(StrictBaseModel):
    historical: HistoricalConfig = Field(default_factory=HistoricalConfig)
    realtime:   RealtimeConfig   = Field(default_factory=RealtimeConfig)
    timeouts:   TimeoutsConfig   = Field(default_factory=TimeoutsConfig)


# =============================================================================
# Storage
# =============================================================================

class DataLakeConfig(StrictBaseModel):
    path:         str       = "data_platform/data_lake"
    format:       str       = "parquet"
    compression:  str       = "snappy"
    partitioning: List[str] = Field(default_factory=lambda: ["exchange", "symbol", "timeframe", "year", "month"])


class FeatureStoreConfig(StrictBaseModel):
    enabled: bool = False
    path:    str  = "data_platform/features"
    format:  str  = "parquet"


class StorageConfig(StrictBaseModel):
    data_lake:     DataLakeConfig     = Field(default_factory=DataLakeConfig)
    feature_store: FeatureStoreConfig = Field(default_factory=FeatureStoreConfig)


# =============================================================================
# Datasets
# =============================================================================

class DatasetsConfig(StrictBaseModel):
    ohlcv:         bool = False
    trades:        bool = False
    orderbook:     bool = False
    funding_rate:  bool = False
    open_interest: bool = False
    liquidations:  bool = False
    mark_price:    bool = False
    index_price:   bool = False

    @property
    def active_datasets(self) -> List[str]:
        return [k for k, v in self.model_dump().items() if v]

    @property
    def any_active(self) -> bool:
        return bool(self.active_datasets)

    @property
    def active_derivative_datasets(self) -> List[str]:
        derivatives = {"funding_rate", "open_interest", "liquidations", "mark_price", "index_price"}
        return [d for d in self.active_datasets if d in derivatives]


# =============================================================================
# Integrations
# =============================================================================

class RedisConfig(StrictBaseModel):
    enabled:          bool = False
    host:             str  = "localhost"
    port:             int  = Field(default=6379, ge=1, le=65535)
    db:               int  = Field(default=0,    ge=0)
    socket_timeout:   int  = Field(default=5,    ge=1)
    retry_on_timeout: bool = True


class KafkaConfig(StrictBaseModel):
    enabled:           bool = False
    bootstrap_servers: str  = "localhost:9092"


class PostgresConfig(StrictBaseModel):
    enabled:  bool          = False
    host:     str           = "localhost"
    port:     int           = Field(default=5432, ge=1, le=65535)
    user:     Optional[str] = None
    password: Optional[str] = None
    database: Optional[str] = None


class IntegrationsConfig(StrictBaseModel):
    redis:    RedisConfig    = Field(default_factory=RedisConfig)
    kafka:    KafkaConfig    = Field(default_factory=KafkaConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)


# =============================================================================
# Observability
# =============================================================================

class LoggingConfig(StrictBaseModel):
    level:     str  = "INFO"
    format:    str  = "console"
    log_dir:   str  = "logs"
    rotation:  str  = "1 day"
    retention: str  = "14 days"
    console:   bool = True
    file:      bool = True
    pipeline:  bool = True


class MetricsConfig(StrictBaseModel):
    enabled:  bool = False
    exporter: str  = "prometheus"
    port:     int  = Field(default=8000, ge=1, le=65535)


class TracingConfig(StrictBaseModel):
    enabled: bool = False


class ObservabilityConfig(StrictBaseModel):
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    tracing: TracingConfig = Field(default_factory=TracingConfig)


# =============================================================================
# Health Checks
# =============================================================================

class HealthChecksConfig(StrictBaseModel):
    enabled: bool      = True
    checks:  List[str] = Field(default_factory=lambda: ["storage", "exchanges", "redis"])


# =============================================================================
# Safety
# =============================================================================

class SafetyConfig(StrictBaseModel):
    prevent_full_reingestion: bool = True
    require_explicit_start:   bool = False


# =============================================================================
# Features (feature flags dinamicos)
# =============================================================================

class FeaturesConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="allow")


# =============================================================================
# Audit
# =============================================================================

class AuditEntry(StrictBaseModel):
    timestamp:   datetime
    cache_key:   str
    hash:        str
    source_file: str


# =============================================================================
# AppConfig
# =============================================================================

class AppConfig(StrictBaseModel):
    exchanges: List[ExchangeConfig]
    pipeline:  PipelineConfig

    storage:       StorageConfig       = Field(default_factory=StorageConfig)
    datasets:      DatasetsConfig      = Field(default_factory=DatasetsConfig)
    integrations:  IntegrationsConfig  = Field(default_factory=IntegrationsConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    environment:   EnvironmentConfig   = Field(default_factory=EnvironmentConfig)
    feature_store: FeatureStoreConfig  = Field(default_factory=FeatureStoreConfig)
    healthchecks:  HealthChecksConfig  = Field(default_factory=HealthChecksConfig)
    safety:        SafetyConfig        = Field(default_factory=SafetyConfig)
    features:      FeaturesConfig      = Field(default_factory=FeaturesConfig)

    audit_log:   List[AuditEntry]   = Field(default_factory=list)
    last_reload: Optional[datetime] = None

    @model_validator(mode="before")
    @classmethod
    def parse_exchanges(cls, values: dict) -> dict:
        raw = values.get("exchanges", {})
        if isinstance(raw, dict):
            values["exchanges"] = [
                {"name": name, **cfg}
                for name, cfg in raw.items()
                if cfg.get("enabled", True)
            ]
        return values

    @model_validator(mode="after")
    def validate_exchanges(self) -> "AppConfig":
        if not self.exchanges:
            raise ValueError("At least one exchange must be enabled.")
        return self

    @model_validator(mode="after")
    def warn_if_no_datasets(self) -> "AppConfig":
        if not self.datasets.any_active:
            warnings.warn("No datasets enabled.", UserWarning, stacklevel=2)
        return self

    @property
    def exchange_names(self) -> List[str]:
        return [e.name.value for e in self.exchanges]

    def get_exchange(self, name: str) -> Optional[ExchangeConfig]:
        target = name.lower()
        for exc in self.exchanges:
            if exc.name.value == target:
                return exc
        return None
