# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/rest/gap_recovery_fetcher.py
=========================================================

GapRecoveryFetcher — relleno urgente de gaps en el stream de trades.

Responsabilidad
---------------
Producir un stream acotado de RawTrade via REST para rellenar el hueco
temporal [gap_start_ms, gap_end_ms) cuando el WebSocket cae o se detecta
una discontinuidad en el stream canónico.

Diferencia semántica con TradesBackfillFetcher
----------------------------------------------
  TradesBackfillFetcher  → source=REST_BACKFILL
                           Rango opcional. Uso: bootstrap histórico.
                           Prioridad baja en dedup downstream.

  GapRecoveryFetcher     → source=REST_RECOVERY
                           Rango OBLIGATORIO en ambos extremos.
                           Uso: reparar gaps activos post-WS-failure.
                           Prioridad alta en dedup downstream
                           (WS > REST_RECOVERY > REST_BACKFILL).

Kappa Architecture
------------------
Cuando WsTradesStream detecta un gap (último trade WS + silencio > umbral):

  1. WsTradesStream calcula [gap_start_ms, gap_end_ms]
  2. Instancia GapRecoveryFetcher con ese rango
  3. GapRecoveryFetcher publica RawTrade(source=REST_RECOVERY) al stream
  4. Consumers deducan por trade_id — WS events tienen prioridad si solapan

  WsTradesStream ─── gap detectado ───► GapRecoveryFetcher
                                             ↓
                                    trades.raw (Kafka)
                                             ↓
                                       consumers

Paginación
----------
Idéntica a TradesBackfillFetcher:
  1. fetch_trades(symbol, since=cursor_ms, limit=PAGE_LIMIT)
  2. ACL: raw → RawTrade(source=REST_RECOVERY)
  3. Filtrar trades >= gap_end_ms
  4. Avanzar cursor: max(timestamp_ms) + 1ms
  5. Página incompleta o gap_end_ms alcanzado → StopAsyncIteration

SafeOps
-------
- gap_start_ms y gap_end_ms: ambos requeridos, fail-fast si inválidos.
- stop() idempotente, nunca lanza.
- MAX_PAGES guard anti-loop.
- Retry delegado a _retry.retry_async (DRY).

Principios: SOLID · DIP · ACL · Kappa · DRY · SafeOps · KISS · SRP
"""

from __future__ import annotations

import asyncio
from decimal import Decimal, InvalidOperation
from typing import AsyncIterator, Optional

from loguru import logger

from market_data.domain.value_objects.raw_trade import RawTrade, TradeSide, TradeSource
from market_data.ports.outbound.exchange import ExchangeAdapter

# ---------------------------------------------------------------------------
# Constantes operacionales
# ---------------------------------------------------------------------------

_DEFAULT_PAGE_LIMIT: int = 1_000  # trades por request REST
_MAX_PAGES: int = 500  # guard anti-loop (SafeOps)
_RETRY_ATTEMPTS: int = 3
_BACKOFF_BASE: float = 1.5
_MAX_BACKOFF_S: float = 15.0


# ---------------------------------------------------------------------------
# ACL — conversión CCXT raw → RawTrade  (pura, sin efectos secundarios)
# ---------------------------------------------------------------------------


def _parse_raw_trade(
    raw: dict,
    exchange_id: str,
    market_type: str,
    symbol: str,
) -> Optional[RawTrade]:
    """
    ACL: convierte un trade CCXT crudo a RawTrade del dominio.

    source=REST_RECOVERY declara que este trade proviene de un gap recovery
    activo, no de WS ni de backfill histórico. Prioridad en dedup:
    WS > REST_RECOVERY > REST_BACKFILL.

    Fail-soft: retorna None si el trade es inválido — el fetcher lo omite.
    No lanza — un trade malformado no debe abortar el recovery.
    """
    try:
        trade_id = str(raw.get("id") or "").strip()
        timestamp_ms = int(raw.get("timestamp") or 0)
        price_raw = raw.get("price")
        amount_raw = raw.get("amount")
        side_raw = raw.get("side", "")

        if not trade_id or timestamp_ms <= 0:
            return None

        price = Decimal(str(price_raw))
        amount = Decimal(str(amount_raw))

        if price <= 0 or amount <= 0:
            return None

        return RawTrade(
            exchange=exchange_id,
            market_type=market_type,
            symbol=symbol,
            trade_id=trade_id,
            timestamp_ms=timestamp_ms,
            price=price,
            amount=amount,
            side=TradeSide.from_raw(side_raw),
            source=TradeSource.REST_RECOVERY,
        )
    except (InvalidOperation, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# GapRecoveryFetcher — AsyncIterator[RawTrade]
# ---------------------------------------------------------------------------


class GapRecoveryFetcher:
    """
    Stream acotado de RawTrade para rellenar gaps activos.
    Implementa TradesSourceProtocol.

    Parámetros
    ----------
    exchange_adapter : ExchangeAdapter configurado (DIP).
    exchange_id      : identificador del exchange ("bybit", "kucoin"…).
    symbol           : par de trading ("BTC/USDT").
    market_type      : "spot" | "linear" | "inverse".
    gap_start_ms     : inicio del gap (inclusive). Obligatorio.
    gap_end_ms       : fin del gap (exclusivo). Obligatorio.
                       Normalmente = timestamp del primer trade WS
                       recibido tras la reconexión.
    page_limit       : máximo de trades por request REST.

    Ciclo de vida
    -------------
    1. __aiter__() → inicia iteración, resetea estado.
    2. __anext__() → retorna RawTrade del buffer; si vacío, fetcha página.
    3. StopAsyncIteration cuando: gap_end_ms alcanzado, sin más datos, o stop().
    """

    def __init__(
        self,
        exchange_adapter: ExchangeAdapter,
        exchange_id: str,
        symbol: str,
        market_type: str = "spot",
        gap_start_ms: int = 0,
        gap_end_ms: int = 0,
        page_limit: int = _DEFAULT_PAGE_LIMIT,
    ) -> None:
        # -- fail-fast: invariantes de construcción --------------------------
        if exchange_adapter is None:
            raise ValueError("GapRecoveryFetcher: exchange_adapter es obligatorio")
        if not exchange_id:
            raise ValueError("GapRecoveryFetcher: exchange_id no puede ser vacío")
        if not symbol:
            raise ValueError("GapRecoveryFetcher: symbol no puede ser vacío")
        if gap_start_ms <= 0:
            raise ValueError(f"GapRecoveryFetcher: gap_start_ms debe ser > 0, got {gap_start_ms}")
        if gap_end_ms <= 0:
            raise ValueError(f"GapRecoveryFetcher: gap_end_ms debe ser > 0, got {gap_end_ms}")
        if gap_end_ms <= gap_start_ms:
            raise ValueError(f"GapRecoveryFetcher: gap_end_ms ({gap_end_ms}) debe ser > gap_start_ms ({gap_start_ms})")

        self._adapter = exchange_adapter
        self._exchange_id = exchange_id
        self._symbol = symbol
        self._market_type = market_type.lower()
        self._gap_start_ms = gap_start_ms
        self._gap_end_ms = gap_end_ms
        self._page_limit = page_limit

        # -- estado mutable del iterador (reset en __aiter__) ----------------
        self._cursor_ms: int = gap_start_ms
        self._buffer: list[RawTrade] = []
        self._exhausted: bool = False
        self._running: bool = False
        self._pages_read: int = 0
        self._stop_event: asyncio.Event = asyncio.Event()

        self._log = logger.bind(
            component="GapRecoveryFetcher",
            exchange=exchange_id,
            symbol=symbol,
            market_type=market_type,
            gap_start_ms=gap_start_ms,
            gap_end_ms=gap_end_ms,
        )

    # ------------------------------------------------------------------
    # TradesSourceProtocol
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def source_id(self) -> str:
        return f"recovery:{self._exchange_id}:{self._symbol}"

    def __aiter__(self) -> AsyncIterator[RawTrade]:
        # Reset de estado — permite reusar la instancia (idempotente)
        self._cursor_ms = self._gap_start_ms
        self._buffer = []
        self._exhausted = False
        self._pages_read = 0
        self._running = True
        self._stop_event.clear()
        self._log.info(
            "gap recovery start | gap=[{}, {}] duration_ms={}",
            self._gap_start_ms,
            self._gap_end_ms,
            self._gap_end_ms - self._gap_start_ms,
        )
        return self

    async def __anext__(self) -> RawTrade:
        """
        Retorna el siguiente RawTrade del gap recovery.

        Loop interno:
          1. stop() llamado             → StopAsyncIteration
          2. Buffer tiene trades        → retorna el primero (FIFO)
          3. Stream agotado (exhausted) → StopAsyncIteration
          4. Guard anti-loop            → StopAsyncIteration + warning
          5. Fetcha página siguiente    → rellena buffer
          6. gap_end_ms alcanzado       → marca exhausted
        """
        # 1. Stop externo
        if self._stop_event.is_set():
            self._running = False
            raise StopAsyncIteration

        # 2. Buffer disponible — retornar sin fetch
        if self._buffer:
            return self._buffer.pop(0)

        # 3. Stream ya marcado como agotado
        if self._exhausted:
            self._running = False
            raise StopAsyncIteration

        # 4. Guard anti-loop
        if self._pages_read >= _MAX_PAGES:
            self._log.warning(
                "gap recovery: límite de páginas alcanzado | symbol={} max_pages={} — gap demasiado grande",
                self._symbol,
                _MAX_PAGES,
            )
            self._exhausted = True
            self._running = False
            raise StopAsyncIteration

        # 5. Fetch página desde cursor actual
        raw_page = await self._fetch_page_with_retry(self._cursor_ms)
        self._pages_read += 1

        if not raw_page:
            self._log.info(
                "gap recovery exhausted (página vacía) | symbol={} pages={} gap_end_ms={}",
                self._symbol,
                self._pages_read,
                self._gap_end_ms,
            )
            self._exhausted = True
            self._running = False
            raise StopAsyncIteration

        # 6. ACL + filtro de rango [gap_start_ms, gap_end_ms)
        new_trades: list[RawTrade] = []
        max_ts = self._cursor_ms
        reached_end = False

        for raw in raw_page:
            trade = _parse_raw_trade(raw, self._exchange_id, self._market_type, self._symbol)
            if trade is None:
                continue
            # Corte superior: gap_end_ms exclusivo
            if trade.timestamp_ms >= self._gap_end_ms:
                reached_end = True
                continue
            new_trades.append(trade)
            max_ts = max(max_ts, trade.timestamp_ms)

        # Ordenar cronológicamente (CCXT no garantiza orden)
        new_trades.sort(key=lambda t: t.timestamp_ms)
        self._buffer.extend(new_trades)

        # 7. Avanzar cursor — +1ms evita re-fetch del último trade
        if max_ts > self._cursor_ms:
            self._cursor_ms = max_ts + 1

        # 8. Condición de fin del gap
        page_incomplete = len(raw_page) < self._page_limit
        if reached_end or page_incomplete or not self._buffer:
            if not self._buffer:
                self._exhausted = True
                self._running = False
                reason = "gap_end_ms" if reached_end else "page_incomplete"
                self._log.info(
                    "gap recovery complete | symbol={} pages={} reason={}",
                    self._symbol,
                    self._pages_read,
                    reason,
                )
                raise StopAsyncIteration
            # Hay trades en buffer pero no vendrán más páginas
            self._exhausted = True

        self._log.debug(
            "gap recovery page | symbol={} page={} trades={} cursor_ms={}",
            self._symbol,
            self._pages_read,
            len(new_trades),
            self._cursor_ms,
        )

        # 9. Retornar primer trade del buffer recién rellenado
        if self._buffer:
            return self._buffer.pop(0)

        self._running = False
        raise StopAsyncIteration

    async def stop(self) -> None:
        """SafeOps: señaliza fin del stream. Nunca lanza. Idempotente."""
        try:
            self._stop_event.set()
            self._running = False
            self._log.debug("GapRecoveryFetcher stopped")
        except Exception as _stop_exc:  # SafeOps — nunca propaga
            self._log.warning("stop() raised unexpectedly | error={}", _stop_exc)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _fetch_page_with_retry(self, since_ms: int) -> list:
        """Fetcha una página delegando retry a _retry.retry_async (DRY)."""
        from market_data.adapters.inbound.rest._retry import retry_async

        async def _call() -> list:
            return (
                await self._adapter.fetch_trades(
                    symbol=self._symbol,
                    since=since_ms,
                    limit=self._page_limit,
                )
                or []
            )

        return await retry_async(
            _call,
            attempts=_RETRY_ATTEMPTS,
            backoff_base=_BACKOFF_BASE,
            backoff_cap=_MAX_BACKOFF_S,
            context=f"GapRecoveryFetcher.fetch_trades symbol={self._symbol}",
        )

    def __repr__(self) -> str:
        return (
            f"GapRecoveryFetcher("
            f"exchange={self._exchange_id!r}, "
            f"symbol={self._symbol!r}, "
            f"market_type={self._market_type!r}, "
            f"gap=[{self._gap_start_ms}, {self._gap_end_ms}), "
            f"running={self._running})"
        )


__all__ = ["GapRecoveryFetcher"]
