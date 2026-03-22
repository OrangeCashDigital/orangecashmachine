from __future__ import annotations

"""core/config/loader/exceptions.py — Errores del sistema de configuración."""


class ConfigurationError(RuntimeError):
    """Error crítico de configuración."""


class ConfigValidationError(ConfigurationError):
    """Violación de regla de negocio."""
