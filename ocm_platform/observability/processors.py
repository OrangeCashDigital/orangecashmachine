from __future__ import annotations

"""
ocm_platform/observability/processors.py
==========================================

Processor chain de structlog para OrangeCashMachine.

Arquitectura
------------
Los processors se aplican en orden a cada evento de log antes
de enviarlo a los sinks. El resultado final es siempre un dict
JSON-serializable, listo para Loki y Prometheus.

Chain estándar (orden de aplicación)::

    1. merge_contextvars      — contexto de asyncio/threading
    2. add_log_level          — nivel como string ("info", "error")
    3. add_timestamp          — ISO 8601 UTC con milliseconds
    4. inject_service_context — service, version (SSOT: importlib.metadata)
    5. sanitize_secrets       — redacta campos sensibles
    6. JSONRenderer           — serializa a JSON string

SSOT de versión
---------------
La versión del servicio se lee desde ``importlib.metadata`` en tiempo de
importación — la única fuente de verdad es ``pyproject.toml``.
Si el paquete no está instalado (desarrollo sin ``pip install -e .``),
el fallback es ``"unknown"`` — nunca una versión hardcoded divergente.
"""

import re
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from typing import Any

import structlog
from structlog.types import EventDict, Processor


# ── Constantes ────────────────────────────────────────────────────────────────

_SERVICE = "orangecashmachine"

# SSOT: versión leída de pyproject.toml via importlib.metadata.
# Fallback "unknown" para entornos de desarrollo sin install.
try:
    _VERSION: str = _pkg_version(_SERVICE)
except PackageNotFoundError:
    _VERSION = "unknown"

# Campos cuyo valor se redacta si contienen datos sensibles.
_SECRET_KEYS: frozenset[str] = frozenset({
    "api_key", "api_secret", "api_password", "password",
    "secret", "token", "passphrase", "authorization",
    "credential", "private_key",
})

# Patrón para detectar valores que parecen secrets aunque la clave no esté listada.
_SECRET_PATTERN = re.compile(
    r"(?i)(key|secret|token|password|passphrase|credential|auth)",
)

_REDACTED = "**REDACTED**"


# ── Processors individuales ───────────────────────────────────────────────────

def _add_timestamp(
    logger: Any, method: str, event_dict: EventDict,
) -> EventDict:
    """Añade timestamp ISO 8601 UTC con milliseconds si no existe."""
    event_dict.setdefault(
        "timestamp",
        datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    )
    return event_dict


def _inject_service_context(
    logger: Any, method: str, event_dict: EventDict,
) -> EventDict:
    """Inyecta service y version como campos fijos (SSOT: importlib.metadata)."""
    event_dict.setdefault("service", _SERVICE)
    event_dict.setdefault("version", _VERSION)
    return event_dict


def _sanitize_secrets(
    logger: Any, method: str, event_dict: EventDict,
) -> EventDict:
    """Redacta valores de campos que puedan contener secretos.

    Recorre el evento un nivel de profundidad.
    Regla: si la clave está en ``_SECRET_KEYS`` o coincide con
    ``_SECRET_PATTERN``, el valor se reemplaza por ``_REDACTED``.
    """
    for key in list(event_dict.keys()):
        if key in _SECRET_KEYS or _SECRET_PATTERN.search(key):
            event_dict[key] = _REDACTED
    return event_dict


# ── Chain factory ─────────────────────────────────────────────────────────────

def build_processor_chain() -> list[Processor]:
    """Construye y retorna la chain estándar de processors.

    Returns:
        Lista de callables compatibles con structlog.
    """
    return [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        _add_timestamp,
        _inject_service_context,
        _sanitize_secrets,
        structlog.processors.JSONRenderer(),
    ]


def process_event(
    chain: list[Processor],
    level: str,
    event_dict: dict[str, Any],
) -> str:
    """Aplica la chain a un event_dict y retorna el JSON resultante.

    Usado internamente por LokiSink y en tests.

    Args:
        chain:      Chain producida por :func:`build_processor_chain`.
        level:      Nivel del log ("info", "error", etc.).
        event_dict: Dict del evento a procesar.

    Returns:
        JSON string listo para enviar a Loki o escribir en archivo.
    """
    result: Any = event_dict
    for processor in chain:
        result = processor(None, level, result)
    # JSONRenderer retorna str; garantizamos el tipo
    return result if isinstance(result, str) else str(result)
