from __future__ import annotations

"""
core/config/credentials.py
===========================

Single Source of Truth para resolución de credenciales de exchanges.

Este es el ÚNICO lugar donde se leen variables de entorno de credenciales.
:class:`~core.config.schema.ExchangeConfig` delega aquí — no llama a
``os.getenv`` directamente.

Patrón de resolución (ejemplo para BINANCE)::

    1. BINANCE_API_KEY    → máxima prioridad (por exchange)
    2. credentials.apiKey → del bloque YAML, si existe
    3. OCM_API_KEY        → fallback genérico
    4. ""                 → vacío seguro

Principios: KISS · SafeOps · Sin efectos secundarios
"""

import os
from typing import Any

from core.config.env_vars import OCM_API_KEY, OCM_API_SECRET


def resolve_exchange_credentials(
    name: str,
    credentials_yaml: dict[str, Any],
) -> dict[str, str]:
    """Resuelve credenciales para un exchange dado.

    Args:
        name: Nombre del exchange en MAYÚSCULAS (e.g. ``"BINANCE"``).
        credentials_yaml: Bloque ``credentials`` del YAML para este exchange.
            Puede ser un dict vacío o None.

    Returns:
        Dict con claves ``api_key``, ``api_secret`` y ``api_password``.
        Siempre presentes — vacíos si no se encontraron credenciales.
    """
    creds = credentials_yaml or {}

    api_key: str = (
        os.getenv(f"{name}_API_KEY")
        or creds.get("apiKey")
        or os.getenv(OCM_API_KEY)
        or ""
    )
    api_secret: str = (
        os.getenv(f"{name}_API_SECRET")
        or creds.get("secret")
        or os.getenv(OCM_API_SECRET)
        or ""
    )
    api_password: str = (
        os.getenv(f"{name}_PASSPHRASE")
        or os.getenv(f"{name}_PASSWORD")
        or creds.get("password")
        or ""
    )

    return {
        "api_key": api_key,
        "api_secret": api_secret,
        "api_password": api_password,
    }
