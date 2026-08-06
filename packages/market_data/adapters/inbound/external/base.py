# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/external/base.py
=============================================

Base compartida para adapters de fuentes externas no-streaming.

Gestiona una ClientSession aiohttp compartida por instancia (evita
overhead de TCP y agotamiento de file descriptors) y timeouts por
operación. No contiene lógica de negocio — solo transporte.

Clasificación de respuestas HTTP
--------------------------------
  - 401 / 403   → ExternalAuthenticationError (API key inválida)
  - 429         → ExternalRateLimitError (con Retry-After extraída)
  - otros 4xx   → ExternalRequestError (petición rechazada por config/proveedor)
  - 5xx         → ExternalSourceUnavailable (transitorio)
  - timeout/DNS → ExternalSourceUnavailable

Health check
------------
Cada adapter expone health() que valida credenciales y disponibilidad de la
fuente con un request ligero, devolviendo un HealthStatus sin lanzar. Lo usa
el orquestador para gatear el arranque del polling continuo.

Contratos implícitos
--------------------
- close() nunca debe lanzar excepción (SafeOps).
- La sesión se crea lazy (en el primer request), no en __init__.
- Las subclases implementan PollingSourcePort.fetch().

Principios: SRP · SafeOps · KISS
"""

from __future__ import annotations

import asyncio
from typing import Optional

import aiohttp

from market_data.ports.inbound.external.errors import (
    ExternalAuthenticationError,
    ExternalRateLimitError,
    ExternalRequestError,
    ExternalSourceUnavailable,
)
from market_data.ports.inbound.external.polling import HealthStatus

__all__ = ["HTTPDataSourceBase", "REQUEST_TIMEOUT"]

# Timeouts por operación (segundos).
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30.0, connect=10.0)


def _parse_retry_after(value: object) -> float | None:
    """Parsea el header Retry-After a segundos (o None si no es numérico)."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


class HTTPDataSourceBase:
    """Cliente HTTP base para fuentes externas (sesión compartida)."""

    def __init__(
        self,
        *,
        base_url: str,
        headers: Optional[dict] = None,
        health_path: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._headers = headers or {}
        self.health_path = health_path
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

    # ── clasificación de respuestas ───────────────────────────────────────────

    def classify_status(self, status: int, retry_after: object = None) -> None:
        """Lanza el error específico según el código HTTP de la respuesta.

        No retorna nada si el status es 2xx. El Retry-After solo aplica a 429.
        """
        if status == 429:
            retry_after_s = _parse_retry_after(retry_after)
            raise ExternalRateLimitError(
                f"HTTP 429 (rate limit) — reintentar en {retry_after_s or 'desconocido'}s",
                retry_after_s=retry_after_s,
            )
        if status in (401, 403):
            raise ExternalAuthenticationError(
                f"HTTP {status}: la API key fue rechazada por el proveedor (credenciales inválidas)"
            )
        if 400 <= status < 500:
            raise ExternalRequestError(f"HTTP {status}: petición rechazada por el proveedor (config/argumento)")
        if status >= 500:
            raise ExternalSourceUnavailable(f"HTTP {status}: error transitorio del proveedor")

    # ── health check ──────────────────────────────────────────────────────────

    async def health(self, *, timeout: float = 5.0) -> "HealthStatus":
        """Valida credenciales + disponibilidad con un request ligero.

        Devuelve un HealthStatus (nunca lanza). Detección: 401/403 → auth,
        4xx → config, 5xx/timeout/DNS → down/network.
        """
        started = _loop_ms()
        endpoint = self.health_path if self.health_path else self.base_url
        try:
            async with self._get_session().get(
                endpoint,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status in (200, 201, 202, 203, 204):
                    return HealthStatus(ok=True, latency_ms=_elapsed_ms(started), detail="ok")
                self.classify_status(resp.status, resp.headers.get("Retry-After") if resp.headers else None)
                # classify no retorna en 2xx/4xx lanza; si llegamos aquí es 3xx.
                return HealthStatus(ok=True, latency_ms=_elapsed_ms(started), detail=f"HTTP {resp.status}")
        except ExternalAuthenticationError as exc:
            return HealthStatus(ok=False, latency_ms=_elapsed_ms(started), detail=str(exc))
        except (ExternalRateLimitError, ExternalRequestError) as exc:
            return HealthStatus(ok=False, latency_ms=_elapsed_ms(started), detail=str(exc))
        except Exception as exc:  # noqa: BLE001 — red/DNS/timeout
            return HealthStatus(ok=False, latency_ms=_elapsed_ms(started), detail=f"unreachable: {exc}")


def _loop_ms() -> int:
    """Milisegundos transcurridos del loop (monotónico, para latencia)."""
    return int(asyncio.get_event_loop().time() * 1000)


def _elapsed_ms(started_ms: int) -> int:
    return _loop_ms() - started_ms
