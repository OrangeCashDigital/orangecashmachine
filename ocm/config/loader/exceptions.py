from __future__ import annotations

"""
ocm/config/loader/exceptions.py
=================================

Jerarquía de excepciones del subsistema de configuración.

Todas las excepciones heredan de :exc:`ConfigurationError` (base de
``Exception``, no de ``RuntimeError`` — los errores de configuración
son errores de dominio, no errores de runtime del intérprete).

Árbol de herencia
-----------------
Exception
└── ConfigurationError          ← captura genérica en main.py
    ├── ConfigFileNotFoundError ← archivo YAML ausente
    ├── ConfigParseError        ← YAML inválido o raíz no es mapping
    └── ConfigValidationError   ← regla de negocio violada

Principios: SOLID (SRP por clase), DDD (errores de dominio), Fail-Fast.
"""

__all__ = [
    "ConfigurationError",
    "ConfigFileNotFoundError",
    "ConfigParseError",
    "ConfigValidationError",
]


class ConfigurationError(Exception):
    """Base para todos los errores del subsistema de configuración.

    Hereda de ``Exception`` directamente — los errores de configuración
    son errores de dominio de aplicación, no errores de runtime del
    intérprete Python.  Permite captura genérica en ``main.py`` sin
    silenciar ``RuntimeError`` del sistema.
    """


class ConfigFileNotFoundError(ConfigurationError):
    """Archivo de configuración YAML requerido no encontrado en disco.

    Raise cuando ``required=True`` y el archivo no existe.
    Fail-Fast: el sistema no puede arrancar sin su configuración base.
    """


class ConfigParseError(ConfigurationError):
    """Archivo YAML no pudo ser parseado o su raíz no es un mapping.

    Raise cuando ``yaml.safe_load`` lanza ``YAMLError`` o el resultado
    no es ``dict``.  Incluye el path y el error original como causa
    (``raise ... from exc``).
    """


class ConfigValidationError(ConfigurationError):
    """Violación de regla de negocio en la configuración validada.

    Raise cuando la configuración es sintácticamente válida pero viola
    restricciones semánticas: exchange habilitado sin credenciales en
    producción, timeframe inválido, límites de riesgo inconsistentes, etc.

    Siempre contiene un mensaje descriptivo que identifica el campo
    y el valor problemático.
    """
