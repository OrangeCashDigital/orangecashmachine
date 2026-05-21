# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/rest/_cursor_factory.py
=====================================================

Helper compartido (DRY): construye un CursorStore con degradación
controlada. Centraliza el try/except de importación para que los
fetchers no lo repitan cada uno.

Principios: DRY · SafeOps · SRP
"""

from __future__ import annotations

from typing import Optional

from loguru import logger

_log = logger.bind(module="adapters.inbound.rest._cursor_factory")


def build_cursor_store(context: str = "") -> Optional[object]:
    """
    Construye y retorna un CursorStore listo para usar.

    Parámetros
    ----------
    context : str
        Nombre del caller para mensajes de warning (ej. "TradesFetcher").
        Opcional — mejora la observabilidad sin cambiar el contrato.

    Retorna
    -------
    CursorStore | None
        None si Redis / ocm.runtime no están disponibles.
        El caller debe manejar None como degradación controlada
        (fallback a storage), nunca como error fatal.

    Garantías
    ---------
    - Nunca lanza excepción.
    - El warning incluye context para trazabilidad.
    - Un None retornado es señal explícita, no silenciosa.
    """
    try:
        from ocm.runtime.state.factories import build_cursor_store as _build

        return _build()
    except Exception as exc:
        _log.warning(
            "CursorStore unavailable — degraded mode (no Redis cursor) | context={} error={}",
            context or "unknown",
            exc,
        )
        return None


__all__ = ["build_cursor_store"]
