# -*- coding: utf-8 -*-
"""
market_data/application/source_manager
=======================================

TradesSourceManager — árbitro de fuente de trades y SSOT operacional.

Responsabilidades
-----------------
1. Selección de modo    : Backfill (REST) → Live (WS/GapAwareStream) → Replay
2. Cursor monotónico    : garantiza que ningún trade del pasado llegue al consumer
3. Deduplicación        : clave compuesta (exchange, symbol, timestamp_ms, trade_id)

División de responsabilidades con GapAwareStream
-------------------------------------------------
  GapAwareStream        — resiliencia del TRANSPORTE WS
                          (timeout, desconexión, gap recovery vía REST_RECOVERY)
                          Nunca lanza StopAsyncIteration si puede recuperar.

  TradesSourceManager   — árbitro de MODO operacional
                          (Backfill vs Live vs Replay)
                          Cursor monotónico + dedup cross-source.

Por esta razón, el manager NO hace fallback WS→REST internamente:
GapAwareStream ya absorbió esa responsabilidad. Si GapAwareStream
lanza StopAsyncIteration, significa que agotó todos sus reintentos
y el stream ya no puede continuar — el manager para limpiamente.

Política de selección de fuente
--------------------------------
  1. Replay  — si está configurado (testing / reconstrucción histórica)
  2. Live WS — si ws_source es provisto (GapAwareStream envuelve WSTradesSource)
  3. Backfill REST — si solo rest_source (TradesBackfillFetcher)

Parámetros
----------
rest_source   : TradesBackfillFetcher — bootstrap histórico (obligatorio).
                Fallback si ws_source no está disponible.
ws_source     : GapAwareStream(WSTradesSource) — live stream con auto-recovery.
                Opcional — ausente en modo backfill puro.
replay_source : ReplayTradesSource — testing / reconstrucción.
                Opcional.

Principios: SSOT · DIP · Clean Arch · Bounded Ctx · Fail-Fast · SafeOps
"""

from __future__ import annotations

from collections import OrderedDict
from enum import Enum
from typing import AsyncIterator, Optional

from loguru import logger

from market_data.ports.inbound.trades_source import TradesSourceProtocol


# ─── Constantes ───────────────────────────────────────────────────────────────

_DEDUP_MAX_SIZE: int = 10_000


# ─── Enum de modo ─────────────────────────────────────────────────────────────


class TradesSourceKind(str, Enum):
    """
    Modo operacional de la fuente activa.

    BACKFILL  — REST paginado histórico (TradesBackfillFetcher).
    WEBSOCKET — Live stream con auto-recovery (GapAwareStream).
    REPLAY    — Replay de eventos almacenados (testing / reconstrucción).
    """

    BACKFILL = "backfill"
    WEBSOCKET = "ws"
    REPLAY = "replay"


# ─── Manager ──────────────────────────────────────────────────────────────────


class TradesSourceManager:
    """
    Árbitro de fuente de trades. Expone un único AsyncIterator al consumer.

    El consumer hace:

        async for trade in manager:
            process(trade)

    y no conoce si el trade vino de REST, WS o replay.

    Ciclo de vida
    -------------
    1. __aiter__()  → selecciona fuente (lazy, en primer __anext__)
    2. __anext__()  → cursor monotónico + dedup + yield
    3. stop()       → SafeOps, para la fuente activa
    """

    def __init__(
        self,
        *,
        rest_source: TradesSourceProtocol,
        ws_source: Optional[TradesSourceProtocol] = None,
        replay_source: Optional[TradesSourceProtocol] = None,
    ) -> None:
        # -- Fail-fast: rest_source es obligatorio — fallback universal --------
        if rest_source is None:
            raise ValueError(
                "TradesSourceManager: rest_source es obligatorio. "
                "Proveer TradesBackfillFetcher como fallback universal."
            )
        if not isinstance(rest_source, TradesSourceProtocol):
            raise TypeError(
                f"TradesSourceManager: rest_source debe implementar "
                f"TradesSourceProtocol, recibido: {type(rest_source)!r}"
            )

        self._rest = rest_source
        self._ws = ws_source
        self._replay = replay_source

        # -- Estado interno ----------------------------------------------------
        self._cursor_ms: int = 0
        self._dedup: OrderedDict[str, None] = OrderedDict()
        self._active: Optional[TradesSourceProtocol] = None
        self._kind: Optional[TradesSourceKind] = None

        self._log = logger.bind(component="TradesSourceManager")

    # ------------------------------------------------------------------ #
    # Propiedades públicas                                                  #
    # ------------------------------------------------------------------ #

    @property
    def source_id(self) -> str:
        kind = self._kind.value if self._kind else "uninitialized"
        active_id = self._active.source_id if self._active else "none"
        return f"manager:{kind}:{active_id}"

    @property
    def is_running(self) -> bool:
        return self._active is not None and self._active.is_running

    @property
    def cursor_ms(self) -> int:
        """Último timestamp confirmado. SSOT del offset del consumer."""
        return self._cursor_ms

    @property
    def active_kind(self) -> Optional[TradesSourceKind]:
        """Modo operacional activo. Útil para métricas y observabilidad."""
        return self._kind

    # ------------------------------------------------------------------ #
    # AsyncIterator                                                        #
    # ------------------------------------------------------------------ #

    def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self):
        """
        Retorna el siguiente RawTrade validado.

        Loop interno:
          1. Selección lazy de fuente en primer __anext__
          2. Fetch del siguiente trade de la fuente activa
          3. Descarte por cursor monotónico (trade estale)
          4. Descarte por dedup (trade duplicado cross-source)
          5. Avance de cursor + yield
        """
        # Selección lazy de fuente — una sola vez por sesión
        if self._active is None:
            self._active, self._kind = self._select_source()
            self._log.info(
                "fuente seleccionada | kind={} source_id={}",
                self._kind.value,
                self._active.source_id,
            )

        while True:
            # Propaga StopAsyncIteration si la fuente se agotó.
            # GapAwareStream solo lanza SAI si agotó todos los reintentos.
            trade = await self._active.__anext__()

            # ── Cursor monotónico: descarta solapamientos de backfill ──────
            if trade.timestamp_ms < self._cursor_ms:
                self._log.debug(
                    "descartado trade estale | ts={} cursor={}",
                    trade.timestamp_ms,
                    self._cursor_ms,
                )
                continue

            # ── Deduplicación cross-source ─────────────────────────────────
            if self._is_duplicate(trade):
                self._log.debug(
                    "descartado duplicado | trade_id={} ts={}",
                    getattr(trade, "trade_id", "?"),
                    trade.timestamp_ms,
                )
                continue

            # ── Avanzar cursor y retornar ──────────────────────────────────
            self._advance_cursor(trade)
            return trade

    # ------------------------------------------------------------------ #
    # Lógica interna                                                       #
    # ------------------------------------------------------------------ #

    def _select_source(self) -> tuple[TradesSourceProtocol, TradesSourceKind]:
        """
        Árbitro de modo. Orden de prioridad: Replay > WS > Backfill REST.

        La decisión se toma una sola vez — el manager no cambia de fuente
        en runtime. GapAwareStream es responsable de la resiliencia interna
        del WS (fallback a REST_RECOVERY ante gaps o desconexiones).
        """
        if self._replay is not None:
            self._log.info("modo replay activado")
            return self._replay, TradesSourceKind.REPLAY
        if self._ws is not None:
            self._log.info(
                "modo live WS activado | source_id={}",
                getattr(self._ws, "source_id", "ws"),
            )
            return self._ws, TradesSourceKind.WEBSOCKET
        self._log.info(
            "modo backfill REST | source_id={}",
            getattr(self._rest, "source_id", "rest"),
        )
        return self._rest, TradesSourceKind.BACKFILL

    def _make_dedup_key(self, trade) -> str:
        """Clave compuesta para dedup cross-source determinista."""
        return (
            f"{getattr(trade, 'exchange', '')}:"
            f"{getattr(trade, 'symbol', '')}:"
            f"{trade.timestamp_ms}:"
            f"{getattr(trade, 'trade_id', '')}"
        )

    def _is_duplicate(self, trade) -> bool:
        """LRU dedup en memoria. O(1) por operación."""
        key = self._make_dedup_key(trade)
        if key in self._dedup:
            return True
        if len(self._dedup) >= _DEDUP_MAX_SIZE:
            self._dedup.popitem(last=False)  # FIFO eviction
        self._dedup[key] = None
        return False

    def _advance_cursor(self, trade) -> None:
        """Cursor monotónico — solo avanza, nunca retrocede."""
        if trade.timestamp_ms > self._cursor_ms:
            self._cursor_ms = trade.timestamp_ms

    # ------------------------------------------------------------------ #
    # SafeOps                                                              #
    # ------------------------------------------------------------------ #

    async def stop(self) -> None:
        """SafeOps: para la fuente activa. Nunca lanza. Idempotente."""
        try:
            if self._active is not None:
                await self._active.stop()
            self._log.debug(
                "TradesSourceManager detenido | kind={} cursor_ms={}",
                self._kind,
                self._cursor_ms,
            )
        except Exception as _stop_exc:  # SafeOps — nunca propaga
            self._log.warning("stop() raised unexpectedly | error={}", _stop_exc)

    def __repr__(self) -> str:
        return (
            f"TradesSourceManager("
            f"kind={self._kind!r}, "
            f"cursor_ms={self._cursor_ms}, "
            f"dedup_size={len(self._dedup)}, "
            f"active={self._active!r})"
        )


__all__ = ["TradesSourceManager", "TradesSourceKind"]
