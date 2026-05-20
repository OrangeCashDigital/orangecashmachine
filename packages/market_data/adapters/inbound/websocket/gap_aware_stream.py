# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/gap_aware_stream.py
==========================================================

GapAwareStream — wrapper de resiliencia para streams de trades.

Responsabilidad
---------------
Envolver cualquier TradesSourceProtocol (normalmente WSTradesSource)
y detectar automáticamente dos tipos de gaps:

  1. Silencio temporal — el stream no produce trades durante
     más de ``gap_threshold_ms`` milisegundos.
  2. Desconexión — el stream lanza StopAsyncIteration inesperadamente
     mientras debería seguir produciendo (``reconnect=True``).

Ante cualquier gap detectado:
  a. Calcula el rango [last_trade_ms + 1, now_ms].
  b. Instancia GapRecoveryFetcher vía ``recovery_factory``.
  c. Emite los trades de recovery con source=REST_RECOVERY.
  d. Intenta reiniciar el stream principal (si ``reconnect=True``).

Kappa Architecture
------------------
El pipeline downstream siempre lee desde GapAwareStream — nunca
distingue si un trade viene del WS o del recovery. La coherencia
del stream es responsabilidad de este wrapper, no del consumer.

  WSTradesSource ──► GapAwareStream ──► consumers
                           ▲
                    GapRecoveryFetcher
                    (REST, source=REST_RECOVERY)

Diseño de composición (OCP)
----------------------------
GapAwareStream no conoce WSTradesSource directamente — recibe
cualquier TradesSourceProtocol por inyección (DIP). Cuando el WS
real se implemente, solo cambia el argumento ``source``.

``recovery_factory`` es un callable que recibe (gap_start_ms, gap_end_ms)
y retorna un GapRecoveryFetcher listo para iterar. Esto desacopla
el wrapper de la implementación concreta del recovery (OCP, DIP).

Gap threshold
-------------
``gap_threshold_ms`` (default 30s) es el tiempo de silencio tolerable
antes de considerar que hay un gap. Para WS de exchanges con baja
liquidez, aumentarlo. Para mercados líquidos (BTC/USDT perp), 5-10s.

SafeOps
-------
- stop() idempotente, nunca lanza.
- Errores en recovery → logueados, no propagados.
- Errores en restart  → logueados, stream marcado como stopped.
- gap_threshold_ms=0  → deshabilita detección por silencio.

Principios: DIP · OCP · Kappa · DRY · SafeOps · KISS · SRP
"""
from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator, Callable, Optional

from loguru import logger

from market_data.domain.value_objects.raw_trade import RawTrade, TradeSource
from market_data.ports.inbound.trades_source import TradesSourceProtocol


# ---------------------------------------------------------------------------
# Tipos
# ---------------------------------------------------------------------------

# Factory que construye un GapRecoveryFetcher dado un rango temporal.
# Signature: (gap_start_ms: int, gap_end_ms: int) -> TradesSourceProtocol
RecoveryFactory = Callable[[int, int], TradesSourceProtocol]


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_DEFAULT_GAP_THRESHOLD_MS: int = 30_000   # 30s de silencio → gap
_MAX_RESTART_ATTEMPTS:     int = 3        # reintentos de reconexión WS
_RESTART_BACKOFF_S:        float = 2.0   # segundos entre reintentos


# ---------------------------------------------------------------------------
# GapAwareStream
# ---------------------------------------------------------------------------

class GapAwareStream:
    """
    Wrapper de resiliencia sobre cualquier TradesSourceProtocol.
    Implementa TradesSourceProtocol.

    Parámetros
    ----------
    source            : stream principal (normalmente WSTradesSource).
    recovery_factory  : callable (gap_start_ms, gap_end_ms) →
                        TradesSourceProtocol listo para iterar.
    exchange_id       : para logging y source_id.
    symbol            : para logging y source_id.
    gap_threshold_ms  : silencio máximo antes de trigger recovery (ms).
                        0 = deshabilitar detección por silencio.
    reconnect         : si True, intenta reiniciar el stream principal
                        tras una desconexión (StopAsyncIteration).
    """

    def __init__(
        self,
        source:           TradesSourceProtocol,
        recovery_factory: RecoveryFactory,
        exchange_id:      str,
        symbol:           str,
        gap_threshold_ms: int   = _DEFAULT_GAP_THRESHOLD_MS,
        reconnect:        bool  = True,
    ) -> None:
        # -- fail-fast -------------------------------------------------------
        if source is None:
            raise ValueError("GapAwareStream: source es obligatorio")
        if recovery_factory is None:
            raise ValueError("GapAwareStream: recovery_factory es obligatoria")
        if not exchange_id:
            raise ValueError("GapAwareStream: exchange_id no puede ser vacío")
        if not symbol:
            raise ValueError("GapAwareStream: symbol no puede ser vacío")
        if gap_threshold_ms < 0:
            raise ValueError(
                f"GapAwareStream: gap_threshold_ms debe ser >= 0, got {gap_threshold_ms}"
            )

        self._source           = source
        self._recovery_factory = recovery_factory
        self._exchange_id      = exchange_id
        self._symbol           = symbol
        self._gap_threshold_ms = gap_threshold_ms
        self._reconnect        = reconnect

        # -- estado interno --------------------------------------------------
        self._last_trade_ms:  Optional[int]  = None
        self._running:        bool           = False
        self._stop_event:     asyncio.Event  = asyncio.Event()
        # Buffer de trades de recovery pendientes de emitir
        self._recovery_buffer: list[RawTrade] = []

        self._log = logger.bind(
            component    = "GapAwareStream",
            exchange     = exchange_id,
            symbol       = symbol,
            threshold_ms = gap_threshold_ms,
        )

    # ------------------------------------------------------------------
    # TradesSourceProtocol
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def source_id(self) -> str:
        # Refleja el source_id del stream principal — transparencia total.
        return getattr(self._source, "source_id", f"ws:{self._exchange_id}:{self._symbol}")

    def __aiter__(self) -> AsyncIterator[RawTrade]:
        self._running      = True
        self._last_trade_ms = None
        self._recovery_buffer = []
        self._stop_event.clear()
        self._log.info(
            "GapAwareStream start | gap_threshold_ms={} reconnect={}",
            self._gap_threshold_ms, self._reconnect,
        )
        return self

    async def __anext__(self) -> RawTrade:
        """
        Retorna el siguiente RawTrade del stream con resiliencia de gap.

        Orden de prioridad:
          1. stop() llamado                → StopAsyncIteration
          2. Buffer de recovery disponible → emitir sin fetch
          3. Intentar obtener trade del source principal:
             a. Con timeout si gap_threshold_ms > 0
             b. Sin timeout si gap_threshold_ms == 0
          4. Timeout (silencio)            → trigger recovery
          5. StopAsyncIteration del source → trigger recovery + reconnect
        """
        if self._stop_event.is_set():
            self._running = False
            raise StopAsyncIteration

        # 2. Emitir trades de recovery pendientes
        if self._recovery_buffer:
            return self._recovery_buffer.pop(0)

        # 3-5. Obtener trade del source principal
        while not self._stop_event.is_set():

            # Emitir recovery buffer si se rellenó en iteración anterior
            if self._recovery_buffer:
                return self._recovery_buffer.pop(0)

            try:
                trade = await self._next_from_source()

                # Trade válido — actualizar cursor y retornar
                self._last_trade_ms = trade.timestamp_ms
                return trade

            except asyncio.TimeoutError:
                # Silencio > gap_threshold_ms → gap detectado
                await self._handle_silence_gap()
                # Continuar loop — recovery buffer rellenado

            except StopAsyncIteration:
                if not self._reconnect:
                    self._running = False
                    raise
                # Desconexión del WS → recovery + restart
                reconnected = await self._handle_disconnection()
                if not reconnected:
                    self._running = False
                    raise StopAsyncIteration

            except Exception as exc:
                # Error inesperado del source — loguear y continuar
                self._log.warning(
                    "source error inesperado | error={}", exc
                )
                await asyncio.sleep(1.0)

        self._running = False
        raise StopAsyncIteration

    async def stop(self) -> None:
        """SafeOps: señaliza fin del stream. Nunca lanza. Idempotente."""
        try:
            self._stop_event.set()
            self._running = False
            await self._source.stop()
            self._log.debug("GapAwareStream stopped")
        except Exception as _stop_exc:  # SafeOps — nunca propaga
            self._log.warning("stop() raised unexpectedly | error={}", _stop_exc)

    # ------------------------------------------------------------------
    # Private — lógica de gap
    # ------------------------------------------------------------------

    async def _next_from_source(self) -> RawTrade:
        """
        Obtiene el siguiente trade del source.

        Si gap_threshold_ms > 0: envuelve con asyncio.wait_for para
        detectar silencio. Lanza TimeoutError si supera el umbral.
        """
        if self._gap_threshold_ms > 0:
            return await asyncio.wait_for(
                self._source.__anext__(),
                timeout=self._gap_threshold_ms / 1_000.0,
            )
        return await self._source.__anext__()

    async def _handle_silence_gap(self) -> None:
        """
        Trigger de gap por silencio: no hubo trades en gap_threshold_ms.

        Calcula el rango y ejecuta recovery.
        """
        now_ms       = int(time.time() * 1_000)
        gap_start_ms = (self._last_trade_ms + 1) if self._last_trade_ms else (
            now_ms - self._gap_threshold_ms
        )
        gap_end_ms   = now_ms

        if gap_start_ms >= gap_end_ms:
            # Gap demasiado pequeño — no vale la pena
            return

        self._log.warning(
            "gap por silencio detectado | "
            "gap=[{}, {}] duration_ms={}",
            gap_start_ms, gap_end_ms, gap_end_ms - gap_start_ms,
        )
        await self._run_recovery(gap_start_ms, gap_end_ms)

    async def _handle_disconnection(self) -> bool:
        """
        Trigger de gap por desconexión WS.

        Ejecuta recovery del gap y luego intenta reiniciar el source.

        Returns
        -------
        bool : True si el source se reinició con éxito, False si no.
        """
        now_ms = int(time.time() * 1_000)

        if self._last_trade_ms is not None:
            gap_start_ms = self._last_trade_ms + 1
            gap_end_ms   = now_ms
            if gap_start_ms < gap_end_ms:
                self._log.warning(
                    "gap por desconexión WS | "
                    "gap=[{}, {}] duration_ms={}",
                    gap_start_ms, gap_end_ms, gap_end_ms - gap_start_ms,
                )
                await self._run_recovery(gap_start_ms, gap_end_ms)

        # Intentar reiniciar el source principal
        return await self._restart_source()

    async def _run_recovery(self, gap_start_ms: int, gap_end_ms: int) -> None:
        """
        Ejecuta GapRecoveryFetcher para el rango dado y rellena el buffer.

        Fail-soft: errores en recovery → logueados, stream no interrumpido.
        """
        try:
            recovery = self._recovery_factory(gap_start_ms, gap_end_ms)
            count = 0
            async for trade in recovery:
                # Verificar que el trade es REST_RECOVERY (SSOT check)
                if trade.source is not TradeSource.REST_RECOVERY:
                    self._log.warning(
                        "recovery trade con source inesperado | "
                        "expected=REST_RECOVERY got={}", trade.source.value,
                    )
                self._recovery_buffer.append(trade)
                count += 1
            self._log.info(
                "recovery completo | gap=[{}, {}] trades={}",
                gap_start_ms, gap_end_ms, count,
            )
        except Exception as exc:
            self._log.error(
                "recovery falló (fail-soft) | "
                "gap=[{}, {}] error={}",
                gap_start_ms, gap_end_ms, exc,
            )

    async def _restart_source(self) -> bool:
        """
        Intenta reiniciar el stream principal con backoff exponencial.

        Returns
        -------
        bool : True si reiniciado con éxito, False si agotó intentos.
        """
        for attempt in range(1, _MAX_RESTART_ATTEMPTS + 1):
            if self._stop_event.is_set():
                return False
            try:
                self._source.__aiter__()   # reset del iterador
                self._log.info(
                    "WS source reiniciado | intento={}/{}", attempt, _MAX_RESTART_ATTEMPTS
                )
                return True
            except Exception as exc:
                backoff = _RESTART_BACKOFF_S * attempt
                self._log.warning(
                    "restart falló (intento {}/{}) | error={} retry_in={:.1f}s",
                    attempt, _MAX_RESTART_ATTEMPTS, exc, backoff,
                )
                await asyncio.sleep(backoff)

        self._log.error(
            "WS source no pudo reiniciarse tras {} intentos — stream detenido",
            _MAX_RESTART_ATTEMPTS,
        )
        return False

    def __repr__(self) -> str:
        return (
            f"GapAwareStream("
            f"source={self._source!r}, "
            f"exchange={self._exchange_id!r}, "
            f"symbol={self._symbol!r}, "
            f"gap_threshold_ms={self._gap_threshold_ms}, "
            f"running={self._running})"
        )


__all__ = ["GapAwareStream", "RecoveryFactory"]
