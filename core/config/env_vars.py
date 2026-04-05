from __future__ import annotations

"""
core/config/env_vars.py
=======================

Single Source of Truth para los nombres de variables de entorno del proceso.

Este es el ÚNICO archivo en todo el proyecto que define estos strings.
Nadie más escribe ``"OCM_DEBUG"`` o ``"OCM_ENV"`` directamente — todos
importan desde aquí. Si una variable se renombra, se cambia aquí y solo aquí.

Categorías
----------
- **Proceso**      — controlan cómo arranca el proceso (bootstrap).
- **Aplicación**   — overrides de configuración de app (``env_overrides.py``).
- **Credenciales** — credenciales de exchanges (``credentials.py``).
- **Storage**      — rutas del data lake (``paths.py``).
"""

# =============================================================================
# Proceso — leídas por RunConfig.from_env()
# =============================================================================

OCM_DEBUG: str = "OCM_DEBUG"
"""``"true"`` | ``"false"`` | ``"1"`` | ``"0"`` — activa diagnósticos verbosos."""

OCM_ENV: str = "OCM_ENV"
"""``"development"`` | ``"test"`` | ``"staging"`` | ``"production"``."""

OCM_CONFIG_PATH: str = "OCM_CONFIG_PATH"
"""Path absoluto o relativo al directorio de config YAML."""

OCM_CONFIG_DIR: str = "OCM_CONFIG_DIR"
"""Alias legacy de ``OCM_CONFIG_PATH``. Preferir ``OCM_CONFIG_PATH``."""

PUSHGATEWAY_URL: str = "PUSHGATEWAY_URL"
"""``"host:port"`` del Prometheus Pushgateway (default: ``"localhost:9091"``)."""

# =============================================================================
# Aplicación — leídas por env_overrides.py
# =============================================================================

OCM_BACKFILL_MODE: str = "OCM_BACKFILL_MODE"
OCM_FETCH_ALL_HISTORY: str = "OCM_FETCH_ALL_HISTORY"
OCM_START_DATE: str = "OCM_START_DATE"
OCM_MAX_CONCURRENT: str = "OCM_MAX_CONCURRENT"
OCM_LOG_LEVEL: str = "OCM_LOG_LEVEL"
OCM_SNAPSHOT_INTERVAL: str = "OCM_SNAPSHOT_INTERVAL"
OCM_METRICS_ENABLED: str = "OCM_METRICS_ENABLED"
OCM_METRICS_PORT: str = "OCM_METRICS_PORT"

# =============================================================================
# Credenciales — leídas por credentials.py
# =============================================================================

OCM_API_KEY: str = "OCM_API_KEY"
"""Fallback genérico si no existe ``{EXCHANGE}_API_KEY``."""

OCM_API_SECRET: str = "OCM_API_SECRET"
"""Fallback genérico si no existe ``{EXCHANGE}_API_SECRET``."""

# =============================================================================
# Storage — leídas por core/config/paths.py
# =============================================================================

OCM_DATA_LAKE_PATH: str = "OCM_DATA_LAKE_PATH"
"""Override absoluto del root del data lake."""

OCM_GOLD_PATH: str = "OCM_GOLD_PATH"
"""Override absoluto del root del gold layer."""

OCM_EXCHANGE: str = "OCM_EXCHANGE"
"""Exchange default para scripts de research y loaders standalone."""

# =============================================================================
# Valores permitidos y helpers
# =============================================================================

ALLOWED_ENVS: frozenset[str] = frozenset(
    {"development", "test", "staging", "production"}
)

TRUTHY_VALUES: frozenset[str] = frozenset({"1", "true", "yes"})

_DEBUG_DEFAULTS: dict[str, bool] = {
    "development": True,
    "test": True,
    "staging": False,
    "production": False,
}


def default_debug_for(env: str) -> bool:
    """Retorna el valor por defecto de debug según el entorno.

    Solo se usa cuando ``OCM_DEBUG`` no está definida explícitamente.

    Args:
        env: Nombre del entorno activo.

    Returns:
        True si el entorno es development o test, False en caso contrario.
    """
    return _DEBUG_DEFAULTS.get(env, False)
