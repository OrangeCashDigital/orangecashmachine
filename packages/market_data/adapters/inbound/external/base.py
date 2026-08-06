# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/external/base.py
=============================================

Base compartida para adapters de fuentes externas no-streaming.

Gestiona una ClientSession aiohttp compartida por instancia (evita
overhead de TCP y agotamiento de file descriptors) y timeouts por
operación. No contiene lógica de negocio — solo transporte.

Contratos implícitos
--------------------
- close() nunca debe lanzar excepción (SafeOps).
- La sesión se crea lazy (en el primer request), no en __init__.
- Las subclases implementan PollingSourcePort.fetch().

Principios: SRP · SafeOps · KISS
"""

from __future__ import annotations

from typing import Optional

import aiohttp

__all__ = ["HTTPDataSourceBase", "REQUEST_TIMEOUT"]

# Timeouts por operación (segundos).
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30.0, connect=10.0)


class HTTPDataSourceBase:
    """Cliente HTTP base para fuentes externas (sesión compartida)."""

    def __init__(self, *, base_url: str, headers: Optional[dict] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self._headers = headers or {}
        self._session: Optional[aiohttp.ClientSession] = None

    # ── sesión ────────────────────────────────────────────────────────────────

    def _get_session(self) -> aiohttp.ClientSession:
        """Devuelve la sesión compartida, creándola lazy si no existe."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=REQUEST_TIMEOUT,
                headers=self._headers,
            )
        return self._session

    async def close(self) -> None:
        """Cierra la sesión HTTP. Idempotente, nunca lanza excepción."""
        if self._session is not None and not self._session.closed:
            try:
                await self._session.close()
            except Exception:  # noqa: BLE001 — SafeOps: limpiar sin propagar
                pass
        self._session = None

    async def __aenter__(self) -> "HTTPDataSourceBase":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
