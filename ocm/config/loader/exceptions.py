from __future__ import annotations

"""
core/config/loader/exceptions.py
=================================

Jerarquía de errores del sistema de configuración.

Todas las excepciones heredan de :exc:`ConfigurationError` para permitir
captura genérica en ``main.py`` sin silenciar otros ``RuntimeError``.
"""


class ConfigurationError(RuntimeError):
    """Error crítico de configuración que impide el arranque del sistema.

    Raise cuando un archivo YAML no existe, no se puede parsear, o la
    estructura raíz no es válida.
    """


class ConfigValidationError(ConfigurationError):
    """Violación de regla de negocio en la configuración.

    Raise cuando la configuración es sintácticamente válida pero viola
    restricciones semánticas (e.g. exchange habilitado sin credenciales
    en producción, timeframe inválido, etc.).
    """
