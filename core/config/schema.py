"""
core/config/schema.py
=====================

Schema de configuración central del sistema OrangeCashMachine.

Responsabilidad única
---------------------
Definir y validar la estructura completa del archivo settings.yaml.
Es la única fuente de verdad sobre qué campos existen, qué tipos
tienen y qué valores son válidos.

Ningún módulo debe parsear el YAML directamente.
Todos importan desde core.config:

    from core.config import AppConfig, load_config

Arquitectura multi-exchange
---------------------------
El YAML define exchanges como un dict nombrado:

    exchanges:
      bybit:
        enabled: true
        credentials:
          apiKey: \${BYBIT_API_KEY}
          secret: \${BYBIT_API_SECRET}
        markets:
          spot:
            enabled: true
            symbols: [BTC/USDT]

parse_exchanges_dict convierte ese dict a List[ExchangeConfig]
e inyecta el nombre como campo.

Resolución de credenciales (orden de prioridad)
-----------------------------------------------
1. Variables de entorno específicas: BYBIT_API_KEY / BYBIT_API_SECRET
2. Variables genéricas:              OCM_API_KEY   / OCM_API_SECRET
3. Bloque 'credentials' en el YAML  (nunca en producción)

Campos de credenciales por exchange
-------------------------------------
• Bybit   → apiKey, secret
• KuCoin  → apiKey, secret, password (passphrase requerido)
• OKX     → apiKey, secret, password (passphrase requerido)
• Binance → apiKey, secret

Principios aplicados
--------------------
• SOLID  – SRP: solo modela config, no ejecuta nada
           OCP: añadir exchange/dataset = una línea
• DRY    – resolución de credenciales centralizada en resolve_credentials
• KISS   – modelos planos, sin capas innecesarias
• SafeOps – SecretStr para credenciales, opt-in para datasets,
            fallo explícito ante config inválida, sin pandas en imports
"""

from __future__ import annotations

import os
import warnings
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator


# ==============================================================
# Constants
# ==============================================================

BASE_DIR: Path = Path(__file__).resolve().parents[3]

CONFIG_PATH: Path = Path(
    os.getenv("OCM_CONFIG_PATH", str(BASE_DIR / "config" / "settings.yaml"))
)

EXCHANGE_TASK_TIMEOUT: int = 30
PIPELINE_TASK_TIMEOUT: int = 3600

DERIVATIVE_DATASET_KEYS: tuple[str, ...] = (
    "funding_rate",
    "open_interest",
    "liquidations",
    "mark_price",
    "index_price",
)

# Exchanges que requieren passphrase además de apiKey/secret
_EXCHANGES_WITH_PASSPHRASE: frozenset[str] = frozenset({"kucoin", "okx"})


# ==============================================================
# Enums
# ==============================================================

class SupportedExchange(str, Enum):
    """
    Exchanges soportados por el sistema.

    Añadir un exchange nuevo: una línea aquí + credenciales en .env.
    No requiere tocar validaciones.
    """
    BINANCE = "binance"
    BYBIT   = "bybit"
    OKX     = "okx"
    KUCOIN  = "kucoin"
    GATE    = "gate"


# ==============================================================
# Exchange models
# ==============================================================

class MarketConfig(BaseModel):
    """Configuración de un tipo de mercado (spot o futures)."""

    enabled:     bool      = False
    symbols:     List[str] = Field(default_factory=list)
    defaultType: str       = "spot"

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, v: List[str]) -> List[str]:
        """Normaliza a mayúsculas: 'btc/usdt' → 'BTC/USDT'."""
        return [s.strip().upper() for s in v]


class MarketsConfig(BaseModel):
    """Agrupa los tipos de mercado disponibles para un exchange."""

    spot:    MarketConfig = Field(default_factory=MarketConfig)
    futures: MarketConfig = Field(default_factory=MarketConfig)

    @property
    def enabled_markets(self) -> Dict[str, MarketConfig]:
        """Dict {nombre: config} de mercados activos únicamente."""
        return {
            name: mkt
            for name, mkt in [("spot", self.spot), ("futures", self.futures)]
            if mkt.enabled
        }


class ExchangeConfig(BaseModel):
    """
    Configuración completa de un exchange individual.

    Credenciales soportadas
    -----------------------
    • api_key      → apiKey en ccxt    (todos los exchanges)
    • api_secret   → secret en ccxt    (todos los exchanges)
    • api_password → password en ccxt  (KuCoin, OKX — passphrase)

    resolve_credentials aplica la prioridad:
      env específico > env genérico > YAML
    """
    name:            SupportedExchange
    enabled:         bool          = True
    enableRateLimit: bool          = True
    api_key:         SecretStr     = SecretStr("")
    api_secret:      SecretStr     = SecretStr("")
    api_password:    SecretStr     = SecretStr("")  # passphrase para KuCoin/OKX
    test_symbol:     str           = "BTC/USDT"
    markets:         MarketsConfig = Field(default_factory=MarketsConfig)

    @model_validator(mode="before")
    @classmethod
    def resolve_credentials(cls, values: dict) -> dict:
        """
        Inyecta credenciales desde variables de entorno.

        Extrae el bloque 'credentials' del YAML si existe,
        luego sobreescribe con variables de entorno si están definidas.
        Acepta tanto KUCOIN_PASSPHRASE como KUCOIN_PASSWORD como alias.
        """
        name = str(values.get("name", "")).upper()

        # 1. Extraer credenciales del YAML
        if creds := values.pop("credentials", {}):
            values.setdefault("api_key",      creds.get("apiKey",   ""))
            values.setdefault("api_secret",   creds.get("secret",   ""))
            values.setdefault("api_password", creds.get("password", ""))

        # 2. Variables de entorno específicas (mayor prioridad)
        values["api_key"] = os.getenv(
            f"{name}_API_KEY",
            values.get("api_key", os.getenv("OCM_API_KEY", "")),
        )
        values["api_secret"] = os.getenv(
            f"{name}_API_SECRET",
            values.get("api_secret", os.getenv("OCM_API_SECRET", "")),
        )

        # Acepta KUCOIN_PASSPHRASE o KUCOIN_PASSWORD como alias
        passphrase = (
            os.getenv(f"{name}_PASSPHRASE")
            or os.getenv(f"{name}_PASSWORD")
            or values.get("api_password", "")
        )
        values["api_password"] = passphrase

        return values

    @property
    def all_symbols(self) -> List[str]:
        """
        Todos los símbolos únicos (spot + futures), orden estable.

        Deduplicados: BTC/USDT en spot y futures se descarga una sola vez.
        """
        seen:   set[str]  = set()
        result: List[str] = []
        for mkt in (self.markets.spot, self.markets.futures):
            for symbol in mkt.symbols:
                if symbol not in seen:
                    seen.add(symbol)
                    result.append(symbol)
        return result

    @property
    def has_credentials(self) -> bool:
        """True si el exchange tiene apiKey y secret configurados."""
        return bool(
            self.api_key.get_secret_value()
            and self.api_secret.get_secret_value()
        )

    @property
    def requires_passphrase(self) -> bool:
        """True si el exchange requiere passphrase (KuCoin, OKX)."""
        return self.name.value in _EXCHANGES_WITH_PASSPHRASE

    @property
    def has_passphrase(self) -> bool:
        """True si el passphrase está configurado."""
        return bool(self.api_password.get_secret_value())

    def ccxt_credentials(self) -> dict:
        """
        Devuelve el dict de credenciales listo para ccxt.

        Uso en exchange_tasks.py:
            exchange = getattr(ccxt, name)(cfg.ccxt_credentials())
        """
        creds: dict = {
            "apiKey":          self.api_key.get_secret_value(),
            "secret":          self.api_secret.get_secret_value(),
            "enableRateLimit": self.enableRateLimit,
        }
        if self.requires_passphrase and self.has_passphrase:
            creds["password"] = self.api_password.get_secret_value()
        return creds


# ==============================================================
# Pipeline models
# ==============================================================

class RetryPolicy(BaseModel):
    """Política de reintentos para pipelines de descarga."""
    max_attempts:   int  = 5
    backoff_factor: int  = 2
    jitter:         bool = True


class HistoricalConfig(BaseModel):
    """Parámetros globales del pipeline histórico OHLCV."""

    start_date:           str         = "2017-01-01T00:00:00Z"
    fetch_all_history:    bool        = False
    max_concurrent_tasks: int         = Field(default=6, ge=1, le=50)
    timeframes:           List[str]   = Field(default_factory=list)
    retry_policy:         RetryPolicy = Field(default_factory=RetryPolicy)

    @field_validator("timeframes")
    @classmethod
    def validate_timeframes(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError(
                "'timeframes' cannot be empty. "
                "Add at least one timeframe in settings.yaml."
            )
        return v

    @field_validator("start_date")
    @classmethod
    def validate_start_date(cls, v: str) -> str:
        """Valida formato ISO 8601 y que no sea fecha futura."""
        try:
            ts = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(
                f"Invalid start_date '{v}'. "
                "Use ISO 8601, e.g. '2022-01-01' or '2022-01-01T00:00:00Z'."
            )
        if ts > datetime.now(tz=timezone.utc):
            raise ValueError(f"start_date '{v}' is in the future.")
        return v


class RealtimeConfig(BaseModel):
    """Parámetros del pipeline de streaming."""
    reconnect_delay_seconds:   int = Field(default=5,      ge=1)
    heartbeat_timeout_seconds: int = Field(default=30,     ge=5)
    snapshot_interval_seconds: int = Field(default=60,     ge=10)
    max_stream_buffer:         int = Field(default=50_000, ge=1000)


class PipelineConfig(BaseModel):
    """Configuración de todos los pipelines."""
    historical: HistoricalConfig = Field(default_factory=HistoricalConfig)
    realtime:   RealtimeConfig   = Field(default_factory=RealtimeConfig)


# ==============================================================
# Datasets model
# ==============================================================

class DatasetsConfig(BaseModel):
    """
    Flags de activación por tipo de dataset.

    Todos desactivados por defecto (SafeOps: opt-in explícito).
    Habilitar solo lo que está implementado.
    """
    ohlcv:     bool = False
    trades:    bool = False
    orderbook: bool = False

    funding_rate:  bool = False
    open_interest: bool = False
    liquidations:  bool = False
    mark_price:    bool = False
    index_price:   bool = False

    @property
    def active_derivative_datasets(self) -> List[str]:
        """Lista de datasets de derivados activos."""
        return [
            key for key in DERIVATIVE_DATASET_KEYS
            if getattr(self, key, False)
        ]

    @property
    def active_datasets(self) -> List[str]:
        """Lista completa de datasets activos (core + derivados)."""
        core = [
            name for name in ("ohlcv", "trades", "orderbook")
            if getattr(self, name, False)
        ]
        return core + self.active_derivative_datasets

    @property
    def any_active(self) -> bool:
        """True si al menos un dataset está activo."""
        return bool(self.active_datasets)


# ==============================================================
# Storage model
# ==============================================================

class StorageConfig(BaseModel):
    """Configuración del Data Lake."""
    data_lake_path: str       = "data_platform/data_lake"
    format:         str       = "parquet"
    compression:    str       = "snappy"
    partitioning:   List[str] = Field(
        default_factory=lambda: [
            "exchange", "market", "symbol",
            "dataset", "timeframe", "year", "month",
        ]
    )


# ==============================================================
# AppConfig — raíz del schema
# ==============================================================

class AppConfig(BaseModel):
    """
    Schema completo de settings.yaml.

    Ejemplo mínimo válido
    ---------------------
    exchanges:
      bybit:
        enabled: true
        credentials:
          apiKey: \${BYBIT_API_KEY}
          secret: \${BYBIT_API_SECRET}
        markets:
          spot:
            enabled: true
            symbols: [BTC/USDT]

    pipeline:
      historical:
        timeframes: [1h, 4h]
        start_date: "2022-01-01"

    datasets:
      ohlcv: true
    """
    exchanges:      List[ExchangeConfig]
    pipeline:       PipelineConfig = Field(default_factory=PipelineConfig)
    datasets:       DatasetsConfig = Field(default_factory=DatasetsConfig)
    storage:        StorageConfig  = Field(default_factory=StorageConfig)
    max_concurrent: int            = Field(default=6, ge=1, le=50)

    @model_validator(mode="before")
    @classmethod
    def parse_exchanges_dict(cls, values: dict) -> dict:
        """
        Convierte el dict de exchanges del YAML a List[ExchangeConfig].

        El YAML usa nombres como claves para legibilidad humana.
        Este validator convierte e inyecta 'name' en cada item.
        Solo incluye exchanges con enabled=true (default).
        """
        raw = values.get("exchanges", {})
        if isinstance(raw, dict):
            values["exchanges"] = [
                {"name": exchange_name, **exchange_cfg}
                for exchange_name, exchange_cfg in raw.items()
                if exchange_cfg.get("enabled", True)
            ]
        return values

    @model_validator(mode="after")
    def validate_at_least_one_exchange(self) -> AppConfig:
        """Falla explícitamente si no hay exchanges habilitados."""
        if not self.exchanges:
            raise ValueError(
                "No enabled exchanges found. "
                "Enable at least one exchange in settings.yaml."
            )
        return self

    @model_validator(mode="after")
    def warn_if_no_datasets_active(self) -> AppConfig:
        """Emite warning si ningún dataset está activo."""
        if not self.datasets.any_active:
            warnings.warn(
                "No datasets are active. "
                "Enable at least one dataset in settings.yaml.",
                UserWarning,
                stacklevel=2,
            )
        return self

    @property
    def enabled_exchanges(self) -> List[ExchangeConfig]:
        return self.exchanges

    @property
    def exchange_names(self) -> List[str]:
        return [e.name.value for e in self.exchanges]

    def get_exchange(self, name: str) -> Optional[ExchangeConfig]:
        """Busca exchange por nombre (case-insensitive). Devuelve None si no existe."""
        target = name.lower()
        for exc in self.exchanges:
            if exc.name.value == target:
                return exc
        return None
