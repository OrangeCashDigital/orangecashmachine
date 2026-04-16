from __future__ import annotations

"""
core/config/schema.py
=====================

Schema canónico de configuración para OrangeCashMachine.

Cubre todos los campos de ``base.yaml``, ``env/{env}.yaml`` y ``settings.yaml``.
Todos los modelos extienden ``StrictBaseModel`` (frozen, extra="forbid") salvo
``FeaturesConfig`` que acepta flags dinámicos (extra="allow").

Principios: SOLID · KISS · DRY · SafeOps
"""

import os
import re
import warnings
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

from core.observability.config import LoggingConfig

CONFIG_PATH: Path = Path("config/settings.yaml")

# Constantes de coerción para model_validators de oc.env string normalization.
# oc.env resuelve SIEMPRE como string — estos sets son el contrato explícito.
# Ref: https://omegaconf.readthedocs.io/en/latest/usage.html#environment-variable-resolver
# _ENV_BOOL_TRUE/_ENV_BOOL_FALSE eliminadas — SSOT en layers/coercion.py
# Importar con alias para no romper referencias internas residuales:
from core.config.layers.coercion import BOOL_TRUE as _ENV_BOOL_TRUE   # noqa: E402
from core.config.layers.coercion import BOOL_FALSE as _ENV_BOOL_FALSE  # noqa: E402

SYMBOL_REGEX: re.Pattern[str] = re.compile(r"^[A-Z0-9]+/[A-Z0-9]+(:[A-Z0-9]+)?$")
FUTURES_SYMBOL_REGEX: re.Pattern[str] = re.compile(r"^[A-Z0-9]+/[A-Z0-9]+:[A-Z0-9]+$")

_ALLOWED_FUTURES_TYPES: frozenset[str] = frozenset({"swap", "future", "option"})
_ALLOWED_TIMEFRAMES: frozenset[str] = frozenset({"1m", "5m", "15m", "1h", "4h", "1d"})
_EXCHANGES_WITH_PASSPHRASE: frozenset[str] = frozenset({"kucoin", "kucoinfutures", "okx"})

EXCHANGE_TASK_TIMEOUT: int = 120
PIPELINE_TASK_TIMEOUT: int = 86_400


class StrictBaseModel(BaseModel):
    """BaseModel inmutable con validación estricta."""

    model_config = ConfigDict(frozen=True, extra="forbid", validate_assignment=True)


class SupportedExchange(str, Enum):
    """Exchanges soportados por OrangeCashMachine vía ccxt."""

    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"
    KUCOIN = "kucoin"
    KUCOINFUTURES = "kucoinfutures"
    GATE = "gate"


class EnvironmentConfig(StrictBaseModel):
    """Metadatos del entorno activo — solo descriptivos, no controlan comportamiento."""

    name: str = "base"
    version: Optional[str] = None
    debug: bool = False
    last_modified_by: Optional[str] = None
    last_modified_at: Optional[Any] = None
    default_env: Optional[str] = None


class MarketConfig(StrictBaseModel):
    """Configuración de un tipo de mercado (spot o futures) para un exchange."""

    enabled: bool = False
    symbols: list[str] = Field(default_factory=list)
    defaultType: Optional[str] = None

    @field_validator("defaultType")
    @classmethod
    def validate_default_type(cls, v: Optional[str]) -> Optional[str]:
        """Valida que defaultType sea un tipo de futuros reconocido por ccxt."""
        if v is not None and v not in _ALLOWED_FUTURES_TYPES:
            raise ValueError(f"Invalid defaultType: '{v}'. Allowed: {sorted(_ALLOWED_FUTURES_TYPES)}")
        return v

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, v: list[str]) -> list[str]:
        """Normaliza símbolos a uppercase, valida formato y elimina duplicados."""
        seen: set[str] = set()
        result: list[str] = []
        for symbol in v:
            s = symbol.strip().upper()
            if not SYMBOL_REGEX.match(s):
                raise ValueError(
                    f"Invalid symbol format: '{symbol}'. Expected spot (BTC/USDT) or futures (BTC/USDT:USDT) notation."
                )
            if s not in seen:
                seen.add(s)
                result.append(s)
        return result


class MarketsConfig(StrictBaseModel):
    """Agrupa configuración de mercados spot y futures de un exchange."""

    spot: MarketConfig = Field(default_factory=MarketConfig)
    futures: MarketConfig = Field(default_factory=MarketConfig)

    @property
    def all_symbols(self) -> list[str]:
        """Todos los símbolos activos (spot + futures), sin duplicados."""
        seen: set[str] = set()
        result: list[str] = []
        for mkt in (self.spot, self.futures):
            if mkt.enabled:
                for s in mkt.symbols:
                    if s not in seen:
                        seen.add(s)
                        result.append(s)
        return result

    @property
    def spot_symbols(self) -> list[str]:
        """Símbolos spot activos."""
        return self.spot.symbols if self.spot.enabled else []

    @property
    def futures_symbols(self) -> list[str]:
        """Símbolos de futuros activos."""
        return self.futures.symbols if self.futures.enabled else []

    @property
    def futures_default_type(self) -> Optional[str]:
        """Tipo de futuros por defecto (ccxt) si futuros está habilitado."""
        return self.futures.defaultType if self.futures.enabled else None


class ResilienceLimits(StrictBaseModel):
    """Límites de concurrencia y rate limit para aiometer."""

    max_concurrency: int = Field(default=5, ge=1, le=100)
    max_rate: float = Field(default=10.0, ge=0.1, description="Requests por segundo")


class ResilienceRetryPolicy(StrictBaseModel):
    """Política de retry específica por tipo de error."""

    max_attempts: int = Field(default=3, ge=1, le=10)
    backoff_factor: float = Field(default=2.0, ge=1.0)
    jitter: bool = True
    cap_seconds: float = Field(default=30.0, ge=1.0)


class ResilienceConfig(StrictBaseModel):
    """Configuración de resiliencia para un exchange.

    Define los límites de aiometer y las políticas de retry
    por tipo de error (rate_limit, timeout, network).
    """

    limits: ResilienceLimits = Field(default_factory=ResilienceLimits)
    rate_limit: ResilienceRetryPolicy = Field(
        default_factory=lambda: ResilienceRetryPolicy(max_attempts=3, backoff_factor=2.0, jitter=True, cap_seconds=30.0)
    )
    timeout: ResilienceRetryPolicy = Field(
        default_factory=lambda: ResilienceRetryPolicy(
            max_attempts=2, backoff_factor=2.0, jitter=False, cap_seconds=10.0
        )
    )
    network: ResilienceRetryPolicy = Field(
        default_factory=lambda: ResilienceRetryPolicy(max_attempts=1, backoff_factor=1.5, jitter=False, cap_seconds=5.0)
    )


class ExchangeConfig(StrictBaseModel):
    """Configuración completa de un exchange incluyendo credenciales y mercados."""

    name: SupportedExchange
    enabled: bool = True
    enableRateLimit: bool = True
    auto_discover_symbols: bool = False
    options: dict[str, Any] = Field(default_factory=dict)
    test_symbol: str = "BTC/USDT"

    api_key: SecretStr = SecretStr("")
    api_secret: SecretStr = SecretStr("")
    api_password: SecretStr = SecretStr("")

    markets: MarketsConfig = Field(default_factory=MarketsConfig)
    resilience: ResilienceConfig = Field(default_factory=ResilienceConfig)

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Resuelve credenciales desde env vars antes de instanciar el modelo."""
        from core.config.credentials import resolve_exchange_credentials

        name = str(values.get("name", "")).upper()
        creds: dict[str, Any] = values.pop("credentials", {}) or {}
        resolved = resolve_exchange_credentials(name, creds)
        values.update(resolved)
        return values

    @model_validator(mode="after")
    def validate_credentials(self) -> ExchangeConfig:
        """Valida credenciales según el entorno: error en prod, warning en dev.

        Lee OCM_ENV directamente desde os.environ para evitar el import circular
        schema → env_resolver → schema. Fail-soft: entorno desconocido no es prod.
        """
        from core.config.env_vars import OCM_ENV as _OCM_ENV  # import local: solo constante str

        env      = (os.environ.get(_OCM_ENV) or "development").strip().lower()
        is_prod  = env == "production"

        if self.enabled and not self.has_credentials:
            msg = f"Exchange '{self.name.value}' is enabled but credentials are missing."
            if is_prod:
                raise ValueError(msg)
            warnings.warn(
                f"[{env}] {msg} Will fail if pipeline attempts to connect.",
                UserWarning,
                stacklevel=1,
            )

        if self.requires_passphrase and not self.has_passphrase:
            msg = f"Exchange '{self.name.value}' requires a passphrase."
            if is_prod:
                raise ValueError(msg)
            warnings.warn(
                f"[{env}] {msg} Passphrase missing.",
                UserWarning,
                stacklevel=1,
            )
        return self

    @property
    def has_credentials(self) -> bool:
        """True si api_key y api_secret tienen valor."""
        return bool(self.api_key.get_secret_value() and self.api_secret.get_secret_value())

    @property
    def requires_passphrase(self) -> bool:
        """True si el exchange requiere passphrase (KuCoin, OKX)."""
        return self.name.value in _EXCHANGES_WITH_PASSPHRASE

    @property
    def has_passphrase(self) -> bool:
        """True si api_password tiene valor."""
        return bool(self.api_password.get_secret_value())

    @property
    def all_symbols(self) -> list[str]:
        """Todos los símbolos activos del exchange."""
        return self.markets.all_symbols

    @property
    def has_spot(self) -> bool:
        """True si spot está habilitado y tiene símbolos."""
        return self.markets.spot.enabled and bool(self.markets.spot_symbols)

    @property
    def has_futures(self) -> bool:
        """True si futures está habilitado y tiene símbolos."""
        return self.markets.futures.enabled and bool(self.markets.futures_symbols)

    def ccxt_credentials(self) -> dict[str, Any]:
        """Devuelve credenciales en el formato esperado por ccxt.

        Returns:
            Dict con ``apiKey``, ``secret``, ``enableRateLimit`` y opcionalmente ``password``.
        """
        creds: dict[str, Any] = {
            "apiKey": self.api_key.get_secret_value(),
            "secret": self.api_secret.get_secret_value(),
            "enableRateLimit": self.enableRateLimit,
        }
        if self.requires_passphrase and self.has_passphrase:
            creds["password"] = self.api_password.get_secret_value()
        return creds


class RetryPolicy(StrictBaseModel):
    """Política de reintentos con backoff exponencial y jitter opcional."""

    max_attempts: int = 5
    backoff_factor: int = 2
    jitter: bool = True


class HistoricalConfig(StrictBaseModel):
    """Configuración del pipeline histórico (backfill e incremental)."""

    start_date: str = "auto"
    auto_lookback_days: int = Field(
        default=3650,
        ge=1,
        le=36500,
        description="Días de lookback cuando start_date='auto'. El loop para al agotar datos del exchange.",
    )
    backfill_mode: bool = False
    max_concurrent_tasks: int = Field(default=4, ge=1, le=64)
    timeframes: list[str] = Field(default_factory=lambda: ["1m"])
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)

    @model_validator(mode="before")
    @classmethod
    def resolve_auto_concurrency(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Resuelve 'auto' o None → número de CPUs disponibles (máx 16).

        None viene de ``max_concurrent_tasks: null`` en YAML (valor no definido).
        'auto' es el alias explícito. Ambos delegan al runtime.
        """
        v = values.get("max_concurrent_tasks")
        if v in ("auto", None):
            values["max_concurrent_tasks"] = max(1, min((os.cpu_count() or 4), 16))
        return values

    @field_validator("timeframes")
    @classmethod
    def validate_timeframes(cls, v: list[str]) -> list[str]:
        """Valida que todos los timeframes estén en el conjunto permitido."""
        if not v:
            raise ValueError("At least one timeframe must be defined.")
        invalid = [tf for tf in v if tf not in _ALLOWED_TIMEFRAMES]
        if invalid:
            raise ValueError(f"Invalid timeframes: {invalid}. Allowed: {sorted(_ALLOWED_TIMEFRAMES)}")
        return v

    @field_validator("start_date")
    @classmethod
    def validate_start_date(cls, v: str) -> str:
        """Valida que start_date sea ISO 8601 o el literal 'auto'.

        'auto' delega la resolución de fecha al pipeline en runtime
        (e.g. último cursor conocido o fecha por defecto del exchange).
        """
        if v == "auto":
            return v
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"start_date must be ISO 8601 or 'auto', got: '{v}'")
        if dt > datetime.now(timezone.utc):
            raise ValueError("start_date cannot be in the future.")
        return v


class ResampleConfig(StrictBaseModel):
    """Configuración del pipeline de resampling local.

    SSOT de los timeframes producidos por ResamplePipeline.
    Estos TF NO se descargan del exchange — se construyen desde source_tf.

    Invariante del sistema:
        historical.timeframes[0] == resample.source_tf  (ambos deben ser "1m")
    """

    targets: list[str] = Field(
        default_factory=lambda: ["5m", "15m", "1h", "4h", "1d"],
        description="Timeframes derivados producidos por ResamplePipeline.",
    )
    source_tf: str = Field(
        default="1m",
        description="Timeframe base — debe coincidir con historical.timeframes[0].",
    )

    @field_validator("targets")
    @classmethod
    def validate_targets(cls, v: list[str]) -> list[str]:
        """Valida que todos los targets estén en el conjunto permitido."""
        if not v:
            raise ValueError("resample.targets must contain at least one timeframe.")
        invalid = [tf for tf in v if tf not in _ALLOWED_TIMEFRAMES]
        if invalid:
            raise ValueError(f"Invalid resample targets: {invalid}. Allowed: {sorted(_ALLOWED_TIMEFRAMES)}")
        return v

    @field_validator("source_tf")
    @classmethod
    def validate_source_tf(cls, v: str) -> str:
        if v not in _ALLOWED_TIMEFRAMES:
            raise ValueError(f"Invalid source_tf: '{v}'. Allowed: {sorted(_ALLOWED_TIMEFRAMES)}")
        return v


class RealtimeConfig(StrictBaseModel):
    """Configuración del pipeline en tiempo real (WebSocket / streaming)."""

    reconnect_delay_seconds: int = Field(default=5, ge=1)
    heartbeat_timeout_seconds: int = Field(default=30, ge=5)
    snapshot_interval_seconds: int = Field(default=60, ge=10)
    max_stream_buffer: int = Field(default=50_000, ge=1_000)
    drop_policy: Literal["reject", "drop_oldest", "drop_newest"] = "reject"


class TimeoutsConfig(StrictBaseModel):
    """Timeouts en segundos para cada tipo de pipeline."""

    exchange_task: int = Field(default=120, ge=1)
    historical_pipeline: int = Field(default=3_600, ge=1)
    trades_pipeline: int = Field(default=1_800, ge=1)
    derivatives_pipeline: int = Field(default=2_700, ge=1)


class PipelineConfig(StrictBaseModel):
    """Configuración agregada de todos los pipelines del sistema."""

    historical: HistoricalConfig = Field(default_factory=HistoricalConfig)
    resample: ResampleConfig = Field(
        default_factory=ResampleConfig,
        description="Timeframes producidos localmente por ResamplePipeline.",
    )
    realtime: RealtimeConfig = Field(default_factory=RealtimeConfig)
    timeouts: TimeoutsConfig = Field(default_factory=TimeoutsConfig)
    max_consecutive_errors: int = Field(
        default=10,
        ge=1,
        description="Máximo de errores consecutivos antes de ExecutionGuard.stop().",
    )


class DataLakeConfig(StrictBaseModel):
    """Path anchor del data platform — usado por paths.py para resolver el warehouse Iceberg.

    Nota: format/compression/partitioning eliminados — Iceberg gestiona esto internamente.
    """

    path: str = "data_platform/data_lake"


class FeatureStoreConfig(StrictBaseModel):
    """Configuración del feature store (capa Gold del medallion)."""

    enabled: bool = False
    path: str = "data_platform/features"
    format: str = "parquet"


class StorageConfig(StrictBaseModel):
    """Configuración de almacenamiento persistente."""

    data_lake: DataLakeConfig = Field(default_factory=DataLakeConfig)
    feature_store: FeatureStoreConfig = Field(default_factory=FeatureStoreConfig)


class DatasetsConfig(StrictBaseModel):
    """Feature flags para activar/desactivar tipos de dataset."""

    ohlcv: bool = False
    trades: bool = False
    orderbook: bool = False
    funding_rate: bool = False
    open_interest: bool = False
    liquidations: bool = False
    mark_price: bool = False
    index_price: bool = False

    @property
    def active_datasets(self) -> list[str]:
        """Lista de datasets habilitados."""
        return [k for k, v in self.model_dump().items() if v]

    @property
    def any_active(self) -> bool:
        """True si al menos un dataset está habilitado."""
        return bool(self.active_datasets)

    @property
    def active_derivative_datasets(self) -> list[str]:
        """Lista de datasets de derivados habilitados."""
        derivatives = {
            "funding_rate",
            "open_interest",
            "liquidations",
            "mark_price",
            "index_price",
        }
        return [d for d in self.active_datasets if d in derivatives]


class RedisConfig(StrictBaseModel):
    """Configuración de Redis (cursor store, estado de pipeline).

    password usa SecretStr — nunca aparece en logs ni en repr().
    Esto previene leakage accidental en tracebacks o --cfg job.
    Ref: Pydantic SecretStr — https://docs.pydantic.dev/latest/concepts/types/#secret-types

    Nota: oc.env resuelve variables de entorno siempre como strings.
    El model_validator normaliza enabled/port/db a sus tipos nativos
    antes de la validación Pydantic para evitar coerción silenciosa.
    Caso crítico: bool('False') == True — debe rechazarse explícitamente.
    """

    enabled: bool = False
    host: str = "localhost"
    port: int = Field(default=6379, ge=1, le=65535)
    db: int = Field(default=0, ge=0)
    password: Optional[SecretStr] = None
    socket_timeout: int = Field(default=5, ge=1)
    retry_on_timeout: bool = True
    ttl_days: int = Field(default=90, ge=1, description="TTL del cursor store en días")

    # coerce_env_strings ELIMINADO — L3 (coerce_scalar_values) ya convierte
    # strings a bool/int/float antes de que Pydantic vea el dict.
    # Mantener este validator duplicaba coerción con constantes locales (DRY roto).
    # Ver: config/layers/coercion.py — motor canónico único.
class KafkaConfig(StrictBaseModel):
    """Configuración de Kafka (streaming — future-ready)."""

    enabled: bool = False
    bootstrap_servers: str = "localhost:9092"

    # coerce_env_strings eliminado — ver RedisConfig (mismo principio). (DRY, SRP)


class PostgresConfig(StrictBaseModel):
    """Configuración de PostgreSQL (metadata / analytics — future-ready).

    password es Optional cuando postgres está deshabilitado.
    Si enabled=True, password vacío lanza ValueError — contraseñas vacías
    son vector directo de ataque (CWE-521: Weak Password Requirements).
    Ref: https://cwe.mitre.org/data/definitions/521.html
    """

    enabled: bool = False
    host: str = "localhost"
    port: int = Field(default=5432, ge=1, le=65535)
    user: Optional[SecretStr] = None    # SecretStr: evita leak de usuario en logs/tracebacks
    password: Optional[SecretStr] = None
    database: Optional[str] = None

    # coerce_env_strings eliminado — ver RedisConfig (mismo principio). (DRY, SRP)

    @model_validator(mode="after")
    def validate_credentials_when_enabled(self) -> PostgresConfig:
        """Falla explícitamente si postgres está habilitado sin credenciales."""
        if not self.enabled:
            return self
        if not self.password or not self.password.get_secret_value():
            raise ValueError(
                "postgres.password no puede ser vacío cuando postgres.enabled=true. "
                "Setear POSTGRES_PASSWORD en el entorno."
            )
        if not self.user or not self.user.get_secret_value():
            raise ValueError("postgres.user no puede ser vacío cuando postgres.enabled=true.")
        return self


class IntegrationsConfig(StrictBaseModel):
    """Configuración de todas las integraciones de infraestructura."""

    redis: RedisConfig = Field(default_factory=RedisConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)


class MetricsConfig(StrictBaseModel):
    """Configuración del exportador de métricas (Prometheus)."""

    enabled: bool = False
    exporter: str = "prometheus"
    port: int = Field(default=8000, ge=1, le=65535)

    # coerce_env_strings eliminado — ver RedisConfig (mismo principio). (DRY, SRP)


class TracingConfig(StrictBaseModel):
    """Configuración de distributed tracing (OpenTelemetry — future-ready)."""

    enabled: bool = False

    # coerce_env_strings eliminado — ver RedisConfig (mismo principio). (DRY, SRP)


class ObservabilityConfig(StrictBaseModel):
    """Configuración agregada de observabilidad: logging, métricas y tracing."""

    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    tracing: TracingConfig = Field(default_factory=TracingConfig)


class HealthChecksConfig(StrictBaseModel):
    """Configuración de health checks del sistema."""

    enabled: bool = True
    checks: list[str] = Field(default_factory=lambda: ["storage", "exchanges", "redis"])


class SafetyConfig(StrictBaseModel):
    """Guards de seguridad operacional para prevenir operaciones destructivas.

    dry_run es el SafeOps master switch: True en todos los entornos excepto
    production. Los módulos de storage/execution deben leerlo de aquí.
    """

    dry_run: bool = Field(
        default=True,
        description="Master safety switch. False solo en production.",
    )
    prevent_full_reingestion: bool = False  # SafeOps default: permisivo en dev/test.
    # production.yaml lo sobreescribe a True explícitamente (SSOT).
    require_explicit_start: bool = False

    # --- Paper trading / backfill guards ---
    max_backfill_days: int = Field(
        default=90,
        ge=1,
        description=(
            "Máximo de días hacia atrás permitido en backfill. Protege contra reingestas accidentales masivas."
        ),
    )
    require_confirmation: bool = Field(
        default=False,
        description=(
            "Si True, operaciones destructivas exigen confirmación explícita. "
            "Útil en staging antes de habilitar live trading."
        ),
    )


class FeaturesConfig(BaseModel):
    """Feature flags dinámicos — acepta cualquier clave booleana desde YAML.

    Usa ``extra="allow"`` intencionalmente para soportar flags experimentales
    sin modificar el schema. Los flags se definen en settings.yaml.
    """

    model_config = ConfigDict(frozen=True, extra="allow")


class RiskPositionConfig(StrictBaseModel):
    """Límites de tamaño de posición."""

    max_position_pct: float = Field(default=0.05, gt=0, le=1)
    max_open_positions: int = Field(default=3, ge=1)


class RiskStopLossConfig(StrictBaseModel):
    """Configuración de stop-loss."""

    enabled: bool = True
    default_pct: float = Field(default=0.02, gt=0, le=1)


class RiskDrawdownConfig(StrictBaseModel):
    """Límites de drawdown — halt automático si se superan."""

    max_daily_drawdown_pct: float = Field(default=0.05, gt=0, le=1)
    max_total_drawdown_pct: float = Field(default=0.15, gt=0, le=1)
    halt_on_breach: bool = True


class RiskOrderConfig(StrictBaseModel):
    """Límites de tamaño de orden en USD."""

    min_order_usd: float = Field(default=10, gt=0)
    max_order_usd: float = Field(default=1000, gt=0)


class RiskConfig(StrictBaseModel):
    """Parámetros de gestión de riesgo operacional.

    En fase data pipeline estos valores son referencia.
    Se aplican cuando execution/trading está activo.
    """

    position: RiskPositionConfig = Field(default_factory=RiskPositionConfig)
    stop_loss: RiskStopLossConfig = Field(default_factory=RiskStopLossConfig)
    drawdown: RiskDrawdownConfig = Field(default_factory=RiskDrawdownConfig)
    order: RiskOrderConfig = Field(default_factory=RiskOrderConfig)


class AppConfig(StrictBaseModel):
    """Configuración raíz de la aplicación.

    Punto de entrada único para toda la configuración del sistema.
    Construida y validada por ``hydra_cfg_to_appconfig()`` o
    ``load_appconfig_standalone()``.
    """

    exchanges: list[ExchangeConfig]
    pipeline: PipelineConfig

    storage: StorageConfig = Field(default_factory=StorageConfig)
    datasets: DatasetsConfig = Field(default_factory=DatasetsConfig)
    integrations: IntegrationsConfig = Field(default_factory=IntegrationsConfig)
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    environment: EnvironmentConfig = Field(default_factory=EnvironmentConfig)
    healthchecks: HealthChecksConfig = Field(default_factory=HealthChecksConfig)
    safety: SafetyConfig = Field(default_factory=SafetyConfig)
    features: FeaturesConfig = Field(default_factory=FeaturesConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)

    @model_validator(mode="before")
    @classmethod
    def parse_exchanges(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Convierte exchanges de dict YAML a lista, filtrando los deshabilitados."""
        raw = values.get("exchanges", {})
        if isinstance(raw, dict):
            values["exchanges"] = [{"name": name, **cfg} for name, cfg in raw.items() if cfg.get("enabled", True)]
        return values

    @model_validator(mode="after")
    def validate_exchanges(self) -> AppConfig:
        """Verifica que al menos un exchange esté habilitado."""
        if not self.exchanges:
            raise ValueError("At least one exchange must be enabled.")
        return self

    # ensure_log_dir eliminado del validator — side effect (IO) en schema validation
    # viola SRP y dificulta testing unitario. Movido a bootstrap stage:
    # core/config/hydra_loader.py::ensure_log_dir(app_config.observability.logging.log_dir)
    # Llamar desde el entrypoint DESPUÉS de pipeline.run(). (SRP, testability)

    @model_validator(mode="after")
    def warn_if_no_datasets(self) -> AppConfig:
        """Emite warning si ningún dataset está habilitado."""
        if not self.datasets.any_active:
            warnings.warn("No datasets enabled.", UserWarning, stacklevel=1)
        return self

    @property
    def exchange_names(self) -> list[str]:
        """Nombres de todos los exchanges activos."""
        return [e.name.value for e in self.exchanges]

    def get_exchange(self, name: str) -> Optional[ExchangeConfig]:
        """Devuelve la configuración de un exchange por nombre (case-insensitive).

        Args:
            name: Nombre del exchange (e.g. ``"bybit"``).

        Returns:
            ExchangeConfig si existe, None si no está en la lista activa.
        """
        target = name.lower()
        for exc in self.exchanges:
            if exc.name.value == target:
                return exc
        return None
