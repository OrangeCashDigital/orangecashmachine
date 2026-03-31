from __future__ import annotations

"""
core/config/env_vars.py
=======================

Single Source of Truth para los nombres de variables de entorno
del proceso OrangeCashMachine.

Este es el ÚNICO archivo en todo el proyecto que define estos strings.
Nadie más escribe "OCM_DEBUG" o "OCM_ENV" directamente — todos importan
desde aquí.

Si una variable se renombra, se cambia aquí y solo aquí.

Categorías
----------
PROCESO   — controlan cómo arranca el proceso (bootstrap)
APLICACIÓN — overrides de configuración de app (env_overrides.py los usa)
SECRETOS  — credenciales de exchanges (credentials.py los usa)
"""


# =============================================================================
# Variables de proceso — leídas por RunConfig.from_env()
# =============================================================================

OCM_DEBUG       = "OCM_DEBUG"        # "true" | "false" | "1" | "0"
OCM_ENV         = "OCM_ENV"          # "development" | "staging" | "production" | "test"
OCM_CONFIG_PATH = "OCM_CONFIG_PATH"  # path absoluto o relativo al directorio de config
OCM_CONFIG_DIR  = "OCM_CONFIG_DIR"   # alias legacy de OCM_CONFIG_PATH
PUSHGATEWAY_URL = "PUSHGATEWAY_URL"  # "host:port" del Prometheus Pushgateway


# =============================================================================
# Variables de overrides de aplicación — leídas por env_overrides.py
# =============================================================================

OCM_BACKFILL_MODE    = "OCM_BACKFILL_MODE"
OCM_FETCH_ALL_HISTORY = "OCM_FETCH_ALL_HISTORY"  # override de fetch_all_history en pipeline.historical
OCM_START_DATE       = "OCM_START_DATE"
OCM_MAX_CONCURRENT   = "OCM_MAX_CONCURRENT"
OCM_LOG_LEVEL        = "OCM_LOG_LEVEL"
OCM_SNAPSHOT_INTERVAL = "OCM_SNAPSHOT_INTERVAL"


# =============================================================================
# Variables de credenciales — leídas por credentials.py
# =============================================================================

OCM_API_KEY    = "OCM_API_KEY"     # fallback genérico si no hay {EXCHANGE}_API_KEY
OCM_API_SECRET = "OCM_API_SECRET"  # fallback genérico


# =============================================================================
# Valores permitidos
# =============================================================================

ALLOWED_ENVS:  frozenset[str] = frozenset({"development", "test", "staging", "production"})
TRUTHY_VALUES: frozenset[str] = frozenset({"1", "true", "yes"})

# Defaults seguros por entorno
_DEBUG_DEFAULTS: dict[str, bool] = {
    "development": True,
    "test":        True,
    "staging":     False,
    "production":  False,
}


def default_debug_for(env: str) -> bool:
    """
    Retorna el default seguro de debug según el entorno.
    Si OCM_DEBUG está seteada explícitamente, este valor se ignora.
    """
    return _DEBUG_DEFAULTS.get(env, False)


# =============================================================================
# Variables de storage — leídas por core/config/paths.py
# =============================================================================

OCM_DATA_LAKE_PATH = "OCM_DATA_LAKE_PATH"  # override absoluto del data lake root
OCM_GOLD_PATH      = "OCM_GOLD_PATH"       # override absoluto del gold root
OCM_EXCHANGE       = "OCM_EXCHANGE"        # exchange default para research/loaders
