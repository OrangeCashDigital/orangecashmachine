"""
market_data.application.source_manager
=====================================
Árbitro de fuente de trades — SSOT operacional.

Responsabilidades
-----------------
1. Selección de fuente  : REST (backfill) → WS (live) → Replay (testing)
2. Cursor monotónico    : garantiza que ningún trade del pasado llegue al consumer
3. Deduplicación        : clave compuesta (exchange, symbol, timestamp_ms, trade_id)
4. Fallback transparente: si WS falla/termina, continúa con REST desde el cursor

Principios aplicados
--------------------
- SSOT        : cursor y dedup centralizados aquí
- DIP         : depende de TradesSourceProtocol (ports/inbound), no de implementaciones
- Clean Arch  : orquestación entre domain y adapters
- Bounded ctx : decisión de fuente es responsabilidad exclusiva de este componente
- Fail-fast   : constructor valida invariantes
- SafeOps     : stop() nunca lanza
"""

from __future__ import annotations

from collections import OrderedDict
from enum import Enum
from typing import AsyncIterator, Optional

from loguru import logger

# DIP: depende del port canónico, no de una implementación concreta
from market_data.ports.inbound.trades_source import TradesSourceProtocol


# ─── Constantes ──────────────────────────────────────────────────────────────

_DEDUP_MAX_SIZE: int = 10_000


# ─── Enum de fuente ───────────────────────────────────────────────────────────

class TradesSourceKind(str, Enum):
    """Identifica el rol semántico de la fuente activa."""
    REST      = "rest"
    WEBSOCKET = "ws"
    REPLAY    = "replay"


# ─── Manager ─────────────────────────────────────────────────────────────────

class TradesSourceManager:
    """
    Árbitro de fuente de trades. Expone un único AsyncIterator al consumer.

    Política de selección de fuente
    --------------------------------
    1. Replay  — si está configurado (testing / reconstrucción)
    2. WS      — si está configurado y emite datos (live stream)
    3. REST    — fallback universal (backfill / recovery)

    Si WS levanta StopAsyncIteration, el manager hace fallback a REST
    preservando el cursor — sin pérdida de posición.

    Parámetros
    ----------
    rest_source    : fuente REST (requerida — fallback universal)
    ws_source      : fuente WS   (opcional — live stream)
    replay_source  : fuente Replay (opcional — testing)
    """

    def __init__(
        self,
        *,
        rest_source: TradesSourceProtocol,
        ws_source: Optional[TradesSourceProtocol] = None,
        replay_source: Optional[TradesSourceProtocol] = None,
    ) -> None:
        # Fail-fast
        if rest_source is None:
            raise ValueError("rest_source es requerido (fallback universal)")
        if not isinstance(rest_source, TradesSourceProtocol):
            raise TypeError(
                f"rest_source debe implementar TradesSourceProtocol, "
                f"recibido: {type(rest_source)!r}"
            )

        self._rest   = rest_source
        self._ws     = ws_source
        self._replay = replay_source

        # Estado interno
        self._cursor_ms: int = 0
        self._dedup: OrderedDict[str, None] = OrderedDict()
        self._active: Optional[TradesSourceProtocol] = None
        self._kind:   Optional[TradesSourceKind]     = None

        self._log = logger.bind(component="TradesSourceManager")

    # ------------------------------------------------------------------ #
    # Propiedades públicas  (satisfacen TradesSourceProtocol)              #
    # ------------------------------------------------------------------ #

    @property
    def source_id(self) -> str:
        kind      = self._kind.value if self._kind else "uninitialized"
        active_id = self._active.source_id if self._active else "none"
        return f"manager:{kind}:{active_id}"

    @property
    def is_running(self) -> bool:
        return self._active is not None and self._active.is_running

    @property
    def cursor_ms(self) -> int:
        """Último timestamp confirmado. Útil para métricas y supervisión."""
        return self._cursor_ms

    @property
    def active_kind(self) -> Optional[TradesSourceKind]:
        return self._kind

    # ------------------------------------------------------------------ #
    # AsyncIterator                                                        #
    # ------------------------------------------------------------------ #

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self):
        # Inicialización lazy — selecciona fuente en el primer __anext__
        if self._active is None:
            self._active, self._kind = self._select_source()
            self._log.info(
                "fuente seleccionada | kind={} source_id={}",
                self._kind.value,
                self._active.source_id,
            )

        while True:
            trade = await self._fetch_next_with_fallback()

            # ── Cursor monotónico — descarta solapamientos de backfill ──
            if trade.timestamp_ms < self._cursor_ms:
                self._log.debug(
                    "descartado trade estale | ts={} cursor={}",
                    trade.timestamp_ms, self._cursor_ms,
                )
                continue

            # ── Deduplicación ───────────────────────────────────────────
            if self._is_duplicate(trade):
                self._log.debug(
                    "descartado duplicado | trade_id={} ts={}",
                    getattr(trade, "trade_id", "?"), trade.timestamp_ms,
                )
                continue

            # ── Avanzar cursor ──────────────────────────────────────────
            self._advance_cursor(trade)
            return trade

    # ------------------------------------------------------------------ #
    # Lógica interna                                                       #
    # ------------------------------------------------------------------ #

    def _select_source(self) -> tuple[TradesSourceProtocol, TradesSourceKind]:
        """Árbitro. Orden de prioridad: Replay > WS > REST."""
        if self._replay is not None:
            self._log.info("modo replay activado")
            return self._replay, TradesSourceKind.REPLAY
        if self._ws is not None:
            self._log.info("WS disponible — intentando fuente live")
            return self._ws, TradesSourceKind.WEBSOCKET
        self._log.info("usando REST (backfill / fallback)")
        return self._rest, TradesSourceKind.REST

    async def _fetch_next_with_fallback(self):
        """
        Obtiene el siguiente trade. Si WS termina, hace fallback a REST
        preservando el cursor.
        """
        try:
            return await self._active.__anext__()
        except StopAsyncIteration:
            if self._kind is TradesSourceKind.WEBSOCKET:
                self._log.warning(
                    "WS terminó inesperadamente — fallback a REST | cursor_ms={}",
                    self._cursor_ms,
                )
                self._active = self._rest
                self._kind   = TradesSourceKind.REST
                return await self._active.__anext__()
            raise  # REST o Replay agotados → propaga StopAsyncIteration

    def _make_dedup_key(self, trade) -> str:
        return (
            f"{getattr(trade, 'exchange', '')}:"
            f"{getattr(trade, 'symbol', '')}:"
            f"{trade.timestamp_ms}:"
            f"{getattr(trade, 'trade_id', '')}"
        )

    def _is_duplicate(self, trade) -> bool:
        key = self._make_dedup_key(trade)
        if key in self._dedup:
            return True
        if len(self._dedup) >= _DEDUP_MAX_SIZE:
            self._dedup.popitem(last=False)
        self._dedup[key] = None
        return False

    def _advance_cursor(self, trade) -> None:
        if trade.timestamp_ms > self._cursor_ms:
            self._cursor_ms = trade.timestamp_ms

    # ------------------------------------------------------------------ #
    # SafeOps                                                              #
    # ------------------------------------------------------------------ #

    async def stop(self) -> None:
        """SafeOps: detiene la fuente activa. Nunca lanza."""
        try:
            if self._active is not None:
                await self._active.stop()
            self._log.debug(
                "TradesSourceManager detenido | kind={} cursor_ms={}",
                self._kind, self._cursor_ms,
            )
        except Exception:
            pass  # SafeOps

    def __repr__(self) -> str:
        return (
            f"TradesSourceManager("
            f"kind={self._kind!r}, "
            f"cursor_ms={self._cursor_ms}, "
            f"dedup_size={len(self._dedup)}, "
            f"active={self._active!r})"
        )


__all__ = ["TradesSourceManager", "TradesSourceKind"]
