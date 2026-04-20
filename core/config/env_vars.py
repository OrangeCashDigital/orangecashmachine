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
- **Credenciales** — credenciales de exchanges (``credentials.py``).
- **Storage**      — rutas del data lake (``paths.py``).

Nota sobre overrides de aplicación
-----------------------------------
Las variables OCM_PIPELINE__*, OCM_OBSERVABILITY__*, OCM_ENVIRONMENT__*
son resueltas automáticamente por ``OcmSettings`` (pydantic-settings) via
``env_prefix="OCM_"`` y ``env_nested_delimiter="__"``.
``env_vars.py`` es el SSoT de los nombres de variables de entorno OCM_*
— no se definen como constantes aquí para evitar duplicación y desincronía.
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

OCM_VALIDATE_ONLY: str = "OCM_VALIDATE_ONLY"
"""``"1"`` | ``"true"`` — valida config y sale sin ejecutar pipeline."""

PUSHGATEWAY_URL: str = "PUSHGATEWAY_URL"
"""URL completa del Prometheus Pushgateway.
Leída por ``runtime.py``. ``PUSHGATEWAY_HOST_PORT`` fue eliminada (TD: variable huérfana).
"""

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

OCM_GOLD_FEATURES_PATH: str = "OCM_GOLD_FEATURES_PATH"
"""Override absoluto del root del gold features layer (alias de OCM_GOLD_PATH por campo)."""

# =============================================================================
# Valores permitidos y helpers
# =============================================================================

ALLOWED_ENVS: frozenset[str] = frozenset(
    {"development", "test", "staging", "production"}
)

# TRUTHY_VALUES eliminada — SSOT en core.config.layers.coercion.BOOL_TRUE
# Importar desde allí: from core.config.layers.coercion import BOOL_TRUE

_DEBUG_DEFAULTS: dict[str, bool] = {
    "development": True,
    "test":        True,
    "staging":     False,
    "production":  False,
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
