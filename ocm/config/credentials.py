from __future__ import annotations

"""
core/config/credentials.py
===========================

Single Source of Truth para resolución de credenciales de exchanges y
proveedores externos (CoinGlass, CoinMarketCap, ...).

Este es el ÚNICO lugar donde se leen variables de entorno de credenciales.
Los composition roots delegan aquí — no llaman a ``os.getenv`` directamente.

Patrón de resolución (ejemplo para BINANCE)::

    1. BINANCE_API_KEY    → máxima prioridad (por exchange)
    2. credentials.apiKey → del bloque YAML, si existe
    3. OCM_API_KEY        → fallback genérico
    4. ""                 → vacío seguro

Principios: KISS · SafeOps · Sin efectos secundarios

Excepción al SSOT de env_vars.py
----------------------------------
Las variables per-exchange (``BINANCE_API_KEY``, ``KUCOIN_PASSPHRASE``, etc.)
son dinámicas — su nombre depende del exchange activo en runtime y no puede
enumerarse estáticamente sin violar OCP. Esta es la única excepción documentada
al SSOT de ``env_vars.py``, que cubre únicamente variables estáticas del proceso.
Lo mismo aplica a los proveedores externos (``COINGLASS_API_KEY``,
``COINMARKETCAP_API_KEY``, ...) resueltos por :func:`resolve_provider_api_key`.

Las variables genéricas de fallback (``OCM_API_KEY``, ``OCM_API_SECRET``) sí
están en ``env_vars.py`` y se importan desde allí.
"""

import os
from typing import Any

from ocm.config.env_vars import OCM_API_KEY, OCM_API_SECRET


def resolve_provider_api_key(
    name: str,
    credentials_yaml: dict[str, Any] | None = None,
) -> str:
    """Resuelve la API key de un proveedor externo no-exchange.

    Args:
        name: Nombre del proveedor en MAYÚSCULAS (e.g. ``"COINGLASS"``).
        credentials_yaml: Bloque ``credentials`` del YAML para este proveedor.
            Puede ser un dict vacío o None.

    Returns:
        API key resuelta — vacía si no se encontró ninguna credencial.
    """
    creds = credentials_yaml or {}
    return os.getenv(f"{name}_API_KEY") or creds.get("apiKey") or os.getenv(OCM_API_KEY) or ""


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

    api_key: str = resolve_provider_api_key(name, creds)
    api_secret: str = os.getenv(f"{name}_API_SECRET") or creds.get("secret") or os.getenv(OCM_API_SECRET) or ""
    api_password: str = os.getenv(f"{name}_PASSPHRASE") or os.getenv(f"{name}_PASSWORD") or creds.get("password") or ""

    return {
        "api_key": api_key,
        "api_secret": api_secret,
        "api_password": api_password,
    }
