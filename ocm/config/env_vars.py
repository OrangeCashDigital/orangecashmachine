# -*- coding: utf-8 -*-
"""
core/config/env_vars.py
=======================

Single Source of Truth para los nombres de variables de entorno del proceso.

Este es el ÚNICO archivo en todo el proyecto que define estos strings estáticos.
Nadie más escribe "KAFKA_BOOTSTRAP_SERVERS" directamente — todos importan aquí.
Si una variable se renombra, se cambia aquí y solo aquí.

Alcance del SSOT
-----------------
Cubre ÚNICAMENTE variables estáticas conocidas en compile-time.
Excepción explícita: variables per-exchange dinámicas (``BINANCE_API_KEY``,
``KUCOIN_PASSPHRASE``, etc.) viven en ``core/config/credentials.py`` porque
su nombre depende del exchange activo en runtime — no pueden enumerarse aquí
sin violar OCP. Ver ``credentials.py`` para la documentación de esa excepción.

Guard de sincronía (import-time)
---------------------------------
El assert al final del módulo verifica que _ENV_VAR_NAMES contiene
exactamente las mismas constantes UPPER_CASE declaradas en el módulo.
Añadir una variable sin actualizar _ENV_VAR_NAMES falla en import.
Principio: Fail-Fast en el punto más temprano posible.
"""

from __future__ import annotations

# ALLOWED_ENVS: re-export desde ocm.observability.config (SSOT)
# La constante pertenece a observability — aquí solo para backwards compat.
from ocm.observability.config import ALLOWED_ENVS as ALLOWED_ENVS  # noqa: F401

# =============================================================================
# Proceso — leídas por RunConfig.from_env()
# =============================================================================

OCM_DEBUG: str = "OCM_DEBUG"
OCM_ENV: str = "OCM_ENV"
OCM_CONFIG_PATH: str = "OCM_CONFIG_PATH"
OCM_CONFIG_DIR: str = "OCM_CONFIG_DIR"
OCM_VALIDATE_ONLY: str = "OCM_VALIDATE_ONLY"
PUSHGATEWAY_URL: str = "PUSHGATEWAY_URL"

# =============================================================================
# Credenciales — leídas por credentials.py
# =============================================================================

OCM_API_KEY: str = "OCM_API_KEY"
OCM_API_SECRET: str = "OCM_API_SECRET"

# =============================================================================
# API service (apps/api) — leídas directamente por apps/api/settings.py vía
# pydantic-settings (env_prefix="OCM_API_"). NO pasan por AppConfig/Hydra ni
# por el protocolo estructurado OCM_SECCIÓN__CLAVE de L2 — se registran aquí
# únicamente para que L2 las reconozca como "planas conocidas" y no las marque
# como malformed_key (ver is_known_non_structured_var).
# =============================================================================

OCM_API_JWT_SECRET: str = "OCM_API_JWT_SECRET"
OCM_API_JWT_EXPIRE_MINUTES: str = "OCM_API_JWT_EXPIRE_MINUTES"

# =============================================================================
# Storage — leídas por core/config/paths.py
# =============================================================================

OCM_STORAGE__DATA_LAKE__PATH: str = "OCM_STORAGE__DATA_LAKE__PATH"
OCM_DATA_LAKE_PATH: str = "OCM_DATA_LAKE_PATH"  # DEPRECATED — legacy
OCM_GOLD_PATH: str = "OCM_GOLD_PATH"
OCM_GOLD_FEATURES_PATH: str = "OCM_GOLD_FEATURES_PATH"
OCM_EXCHANGE: str = "OCM_EXCHANGE"
OCM_MARKET_TYPE: str = "OCM_MARKET_TYPE"
OCM_OHLCV_START_DATE: str = "OCM_OHLCV_START_DATE"
"""Fecha ISO de inicio del backfill histórico. Default: 2024-01-01"""

# =============================================================================
# Market-data server
# =============================================================================

MARKET_DATA_HOST: str = "MARKET_DATA_HOST"
MARKET_DATA_PORT: str = "MARKET_DATA_PORT"
MARKET_DATA_URL: str = "MARKET_DATA_URL"
INGESTION_INTERVAL_S: str = "INGESTION_INTERVAL_S"

# =============================================================================
# Internal service ports (Docker host port mappings)
# =============================================================================

TRADING_HOST_PORT: str = "TRADING_HOST_PORT"
PORTFOLIO_HOST_PORT: str = "PORTFOLIO_HOST_PORT"

# =============================================================================
# Kafka — leídas por KafkaProducerAdapter.from_env() y KafkaConsumerAdapter
# SSOT: nadie escribe estos strings directamente fuera de este módulo.
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS: str = "KAFKA_BOOTSTRAP_SERVERS"
"""Host:puerto del broker. Múltiples separados por coma. Default: kafka:9092"""

KAFKA_CLIENT_ID_PRODUCER: str = "KAFKA_CLIENT_ID_PRODUCER"
"""Client ID del producer — visible en kafka-ui. Default: ocm-producer"""

KAFKA_COMPRESSION_TYPE: str = "KAFKA_COMPRESSION_TYPE"
"""lz4 | gzip | snappy | None. Default: lz4"""

KAFKA_ACKS: str = "KAFKA_ACKS"
"""all | 1 | 0. Default: all (máxima durabilidad)"""

KAFKA_LINGER_MS: str = "KAFKA_LINGER_MS"
"""Tiempo de espera para batch en ms. 0 = sin espera. Default: 5"""

KAFKA_MAX_BATCH_SIZE: str = "KAFKA_MAX_BATCH_SIZE"
"""Tamaño máximo del batch en bytes. Default: 16384 (16KB)"""

KAFKA_CONSUMER_SESSION_TIMEOUT_MS: str = "KAFKA_CONSUMER_SESSION_TIMEOUT_MS"
"""Timeout de sesión del consumer en ms. Default: 30000"""

KAFKA_CONSUMER_HEARTBEAT_MS: str = "KAFKA_CONSUMER_HEARTBEAT_MS"
"""Intervalo de heartbeat del consumer en ms. Default: 10000"""

KAFKA_AUTO_OFFSET_RESET: str = "KAFKA_AUTO_OFFSET_RESET"
"""earliest (replay desde inicio) | latest (solo nuevos). Default: earliest"""

KAFKA_ENABLED: str = "KAFKA_ENABLED"
"""Feature flag — activa el path Kappa (publisher → ohlcv.raw). Default: false.
Equivale a integrations.kafka.enabled en la config Hydra/YAML.
SSOT: env_vars.py es la única fuente de verdad para el nombre de la variable.
Override en producción: KAFKA_ENABLED=true en .env o env del sistema."""

# =============================================================================
# Valores permitidos y helpers
# =============================================================================

_DEBUG_DEFAULTS: dict[str, bool] = {
    "development": True,
    "test": True,
    "staging": False,
    "production": False,
}

# =============================================================================
# Registro canónico — SSOT sin heurística
# =============================================================================

_ENV_VAR_NAMES: frozenset[str] = frozenset(
    {
        # Proceso
        OCM_DEBUG,
        OCM_ENV,
        OCM_CONFIG_PATH,
        OCM_CONFIG_DIR,
        OCM_VALIDATE_ONLY,
        PUSHGATEWAY_URL,
        # Credenciales
        OCM_API_KEY,
        OCM_API_SECRET,
        # API service (apps/api) — ver bloque de declaración arriba
        OCM_API_JWT_SECRET,
        OCM_API_JWT_EXPIRE_MINUTES,
        # Storage / runtime
        OCM_STORAGE__DATA_LAKE__PATH,
        OCM_DATA_LAKE_PATH,
        OCM_GOLD_PATH,
        OCM_GOLD_FEATURES_PATH,
        MARKET_DATA_HOST,
        MARKET_DATA_PORT,
        MARKET_DATA_URL,
        INGESTION_INTERVAL_S,
        OCM_EXCHANGE,
        OCM_MARKET_TYPE,
        OCM_OHLCV_START_DATE,
        # Internal service ports
        TRADING_HOST_PORT,
        PORTFOLIO_HOST_PORT,
        # Kafka
        KAFKA_ENABLED,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_CLIENT_ID_PRODUCER,
        KAFKA_COMPRESSION_TYPE,
        KAFKA_ACKS,
        KAFKA_LINGER_MS,
        KAFKA_MAX_BATCH_SIZE,
        KAFKA_CONSUMER_SESSION_TIMEOUT_MS,
        KAFKA_CONSUMER_HEARTBEAT_MS,
        KAFKA_AUTO_OFFSET_RESET,
    }
)

# Guard de sincronía — falla en import si _ENV_VAR_NAMES diverge
_module_constants: frozenset[str] = frozenset(
    v
    for k, v in globals().items()
    if k == k.upper() and not k.startswith("_") and isinstance(v, str) and k not in ("ALLOWED_ENVS",)
)
_missing_from_registry = _module_constants - _ENV_VAR_NAMES
_missing_from_module = _ENV_VAR_NAMES - _module_constants
assert not _missing_from_registry, (
    "Constantes declaradas en env_vars.py pero ausentes de _ENV_VAR_NAMES:\n"
    + "\n".join(f"  + {v}" for v in sorted(_missing_from_registry))
    + "\nSolución: añadir a _ENV_VAR_NAMES en env_vars.py"
)
assert not _missing_from_module, (
    "Entradas en _ENV_VAR_NAMES sin constante correspondiente en env_vars.py:\n"
    + "\n".join(f"  - {v}" for v in sorted(_missing_from_module))
    + "\nSolución: declarar la constante o eliminar de _ENV_VAR_NAMES"
)
del _module_constants, _missing_from_registry, _missing_from_module


def default_debug_for(env: str) -> bool:
    return _DEBUG_DEFAULTS.get(env, False)


def is_known_non_structured_var(key: str) -> bool:
    """True si `key` es una var OCM_* registrada que NO sigue el protocolo
    estructurado OCM_SECTION__KEY de L2 (env_override.py).

    Ejemplos: OCM_VALIDATE_ONLY, OCM_DEBUG, OCM_API_KEY — banderas planas
    consumidas directamente por otras capas (RunConfig, credentials, paths),
    no por el mecanismo de override estructurado de L2.

    Usado por ocm.config.layers.env_override para distinguir "clave conocida
    pero fuera del alcance de L2" de "clave realmente malformada" — evita
    falsos positivos de warning sobre banderas planas legítimas.
    """
    return key in _ENV_VAR_NAMES and "__" not in key
