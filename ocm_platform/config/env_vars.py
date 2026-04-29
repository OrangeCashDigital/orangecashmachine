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
Las variables OCM_PIPELINE__*, OCM_OBSERVABILITY__*, OCM_SECTION__KEY, etc.
son aplicadas por apply_env_overrides() en core/config/layers/env_override.py
(L2 del ConfigPipeline), usando el separador doble guión bajo (__).
``env_vars.py`` es el SSOT de los nombres de variables de entorno OCM_*
propias del proceso — las claves OCM_SECTION__KEY de L2 no se listan aquí
porque cualquier clave del schema es un override válido por diseño.
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
"""URL completa del Prometheus Pushgateway."""

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

OCM_STORAGE__DATA_LAKE__PATH: str = "OCM_STORAGE__DATA_LAKE__PATH"
"""Override absoluto del root del data lake.

Convencion: separador __ alineado con apply_env_overrides() (L2).
  OCM_STORAGE__DATA_LAKE__PATH=/var/lib/ocm/data_lake

Reemplaza OCM_DATA_LAKE_PATH (legacy). Ver migracion en paths.py.
"""

OCM_DATA_LAKE_PATH: str = "OCM_DATA_LAKE_PATH"
"""DEPRECATED — usar OCM_STORAGE__DATA_LAKE__PATH.

Mantenida durante transicion controlada. paths.py emite DeprecationWarning
si esta variable esta presente en el entorno. Eliminar en siguiente release.
"""

OCM_GOLD_PATH: str = "OCM_GOLD_PATH"
"""Override absoluto del root del gold layer."""

OCM_EXCHANGE: str = "OCM_EXCHANGE"
"""Exchange default para scripts de research y loaders standalone."""

OCM_MARKET_TYPE: str = "OCM_MARKET_TYPE"
"""``"spot"`` | ``"swap"`` — tipo de mercado para research y loaders standalone."""

OCM_GOLD_FEATURES_PATH: str = "OCM_GOLD_FEATURES_PATH"
# ── Market-data server ────────────────────────────────────────────────────────
MARKET_DATA_HOST: str = "MARKET_DATA_HOST"
"""Interfaz de escucha del servidor HTTP de market data. Default: 0.0.0.0"""

MARKET_DATA_PORT: str = "MARKET_DATA_PORT"
"""Puerto TCP del servidor HTTP de market data. Default: 8001"""

INGESTION_INTERVAL_S: str = "INGESTION_INTERVAL_S"
"""Segundos entre ciclos de ingestión del loop principal. Default: 300"""
"""Override absoluto del root del gold features layer (alias de OCM_GOLD_PATH por campo)."""

# =============================================================================
# Valores permitidos y helpers
# =============================================================================

ALLOWED_ENVS: frozenset[str] = frozenset(
    {"development", "test", "staging", "production"}
)

# TRUTHY_VALUES eliminada — SSOT en core.config.layers.coercion.BOOL_TRUE
# Importar desde allí: from ocm_platform.config.layers.coercion import BOOL_TRUE

_DEBUG_DEFAULTS: dict[str, bool] = {
    "development": True,
    "test":        True,
    "staging":     False,
    "production":  False,
}


# =============================================================================
# Registro canónico — consumido por test_invariants.py (SSoT sin heurística)
# =============================================================================

_ENV_VAR_NAMES: frozenset[str] = frozenset({
    # Proceso
    OCM_DEBUG, OCM_ENV, OCM_CONFIG_PATH, OCM_CONFIG_DIR,
    OCM_VALIDATE_ONLY, PUSHGATEWAY_URL,
    # Credenciales
    OCM_API_KEY, OCM_API_SECRET,
    # Storage / runtime
    OCM_STORAGE__DATA_LAKE__PATH,
    OCM_DATA_LAKE_PATH,          # DEPRECATED — legacy, transicion controlada
    OCM_GOLD_PATH, OCM_GOLD_FEATURES_PATH,
    MARKET_DATA_HOST, MARKET_DATA_PORT, INGESTION_INTERVAL_S,
    OCM_EXCHANGE, OCM_MARKET_TYPE,
})
"""Registro explícito de todos los nombres de env var propios de OCM.

Es el ÚNICO lugar donde se decide qué cuenta como "env var declarada".
Los tests consumen este frozenset directamente — sin inferencia por heurística.
Principio: Explicit over Implicit (PEP 20), SSOT.
"""

# Guard de sincronía: falla en import si _ENV_VAR_NAMES diverge de las
# constantes UPPER_CASE del módulo. Hace imposible olvidar actualizar
# _ENV_VAR_NAMES al añadir una nueva variable.
# Principio: Fail-Fast en el punto más temprano posible.
_module_constants: frozenset[str] = frozenset(
    v for k, v in globals().items()
    if k == k.upper() and not k.startswith("_")
    and isinstance(v, str) and k not in ("ALLOWED_ENVS",)
)
_missing_from_registry = _module_constants - _ENV_VAR_NAMES
_missing_from_module   = _ENV_VAR_NAMES - _module_constants
assert not _missing_from_registry, (
    f"Constantes declaradas en env_vars.py pero ausentes de _ENV_VAR_NAMES:\n"
    + "\n".join(f"  + {v}" for v in sorted(_missing_from_registry))
    + "\nSolución: añadir a _ENV_VAR_NAMES en env_vars.py"
)
assert not _missing_from_module, (
    f"Entradas en _ENV_VAR_NAMES sin constante correspondiente en env_vars.py:\n"
    + "\n".join(f"  - {v}" for v in sorted(_missing_from_module))
    + "\nSolución: declarar la constante o eliminar de _ENV_VAR_NAMES"
)
del _module_constants, _missing_from_registry, _missing_from_module


def default_debug_for(env: str) -> bool:
    """Retorna el valor por defecto de debug según el entorno.

    Solo se usa cuando ``OCM_DEBUG`` no está definida explícitamente.

    Args:
        env: Nombre del entorno activo.

    Returns:
        True si el entorno es development o test, False en caso contrario.
    """
    return _DEBUG_DEFAULTS.get(env, False)
