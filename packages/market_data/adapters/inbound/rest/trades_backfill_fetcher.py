# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/rest/trades_backfill_fetcher.py
============================================================

TradesBackfillFetcher — stream histórico paginado de RawTrade.

Responsabilidad
---------------
Producir un stream acotado de RawTrade via REST paginado, implementando
TradesSourceProtocol. Cubre dos casos de uso:

  1. Bootstrap histórico — desde since_ms hasta until_ms (o fin de datos).
  2. Gap recovery — rango acotado [gap_start_ms, gap_end_ms] cuando el
     WS cae y hay que rellenar el hueco.

Diferencia con RESTTradesPoller
--------------------------------
RESTTradesPoller es un stream infinito de polling (live fallback).
TradesBackfillFetcher es un stream finito y acotado — termina naturalmente
cuando los datos se agotan o se alcanza until_ms.

Diferencia con TradesFetcher
-----------------------------
TradesFetcher escribe directamente a storage (capa silver — DataFrame).
TradesBackfillFetcher produce RawTrade domain objects para el event stream
(Kappa producer). No conoce storage.

Kappa Architecture
------------------
Todos los productores — WS, backfill, recovery — publican RawTrade al
mismo event stream (topic trades.raw en Kafka). Los consumers leen siempre
desde el stream, nunca desde los productores directamente.

  WsTradesStream          → source=TradeSource.WS          (live)
  TradesBackfillFetcher   → source=TradeSource.REST_BACKFILL (histórico)
  GapRecoveryFetcher      → source=TradeSource.REST_RECOVERY (gaps)

Paginación
----------
  1. fetch_trades(symbol, since=cursor_ms, limit=PAGE_LIMIT)
  2. Parsear batch → List[RawTrade]
  3. Filtrar trades que superen until_ms (si definido)
  4. Avanzar cursor: max(timestamp_ms) + 1ms
  5. Página incompleta → stream agotado → StopAsyncIteration
  6. Página vacía → stream agotado → StopAsyncIteration

Deduplicación
--------------
No se aplica dedup por trade_id en este fetcher — el consumer downstream
es responsable del dedup entre fuentes (WS vs backfill).
Esto mantiene SRP: el fetcher produce, el consumer decide.

SafeOps
-------
- stop()   → idempotente, nunca lanza.
- Retry    → delegado a _retry.retry_async (DRY).
- MAX_PAGES → guard anti-loop (SafeOps).
- until_ms → corte temporal explícito para gaps acotados.

Principios: SOLID · DIP · ACL · Kappa · DRY · SafeOps · KISS
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

    source=REST_BACKFILL declara que este trade proviene de backfill
    histórico, no de WS. Permite dedup determinista en consumers.

    Fail-soft: retorna None si el trade es inválido — el fetcher lo omite.
    No lanza — un trade malformado no debe abortar el stream.
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
            source=TradeSource.REST_BACKFILL,
        )
    except (InvalidOperation, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# TradesBackfillFetcher — AsyncIterator[RawTrade]
# ---------------------------------------------------------------------------


class TradesBackfillFetcher:
    """
    Stream histórico paginado de RawTrade. Implementa TradesSourceProtocol.

    Parámetros
    ----------
    exchange_adapter : ExchangeAdapter configurado (DIP).
    exchange_id      : identificador del exchange ("bybit", "kucoin"…).
    symbol           : par de trading ("BTC/USDT").
    market_type      : "spot" | "linear" | "inverse".
    since_ms         : inicio del rango. None = desde el más antiguo disponible.
    until_ms         : fin del rango (exclusivo). None = hasta agotar datos.
    page_limit       : máximo de trades por request REST.

    Ciclo de vida
    -------------
    1. __aiter__() → inicia iteración, resetea estado.
    2. __anext__() → retorna RawTrade del buffer interno; si vacío, fetcha página.
    3. StopAsyncIteration cuando: sin más datos, until_ms alcanzado, o stop().
    """

    def __init__(
        self,
        exchange_adapter: ExchangeAdapter,
        exchange_id: str,
        symbol: str,
        market_type: str = "spot",
        since_ms: Optional[int] = None,
        until_ms: Optional[int] = None,
        page_limit: int = _DEFAULT_PAGE_LIMIT,
    ) -> None:
        # -- fail-fast: invariantes de construcción --------------------------
        if exchange_adapter is None:
            raise ValueError("TradesBackfillFetcher: exchange_adapter es obligatorio")
        if not exchange_id:
            raise ValueError("TradesBackfillFetcher: exchange_id no puede ser vacío")
        if not symbol:
            raise ValueError("TradesBackfillFetcher: symbol no puede ser vacío")
        if until_ms is not None and since_ms is not None and until_ms <= since_ms:
            raise ValueError(f"TradesBackfillFetcher: until_ms ({until_ms}) debe ser > since_ms ({since_ms})")

        self._adapter = exchange_adapter
        self._exchange_id = exchange_id
        self._symbol = symbol
        self._market_type = market_type.lower()
        self._since_ms = since_ms
        self._until_ms = until_ms
        self._page_limit = page_limit

        # -- estado mutable del iterador (reset en __aiter__) ----------------
        self._cursor_ms: Optional[int] = since_ms
        self._buffer: list[RawTrade] = []
        self._exhausted: bool = False
        self._running: bool = False
        self._pages_read: int = 0
        self._stop_event: asyncio.Event = asyncio.Event()

        self._log = logger.bind(
            component="TradesBackfillFetcher",
            exchange=exchange_id,
            symbol=symbol,
            market_type=market_type,
        )

    # ------------------------------------------------------------------
    # TradesSourceProtocol
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def source_id(self) -> str:
        return f"backfill:{self._exchange_id}:{self._symbol}"

    def __aiter__(self) -> AsyncIterator[RawTrade]:
        # Reset de estado — permite reusar la instancia (idempotente)
        self._cursor_ms = self._since_ms
        self._buffer = []
        self._exhausted = False
        self._pages_read = 0
        self._running = True
        self._stop_event.clear()
        self._log.debug(
            "backfill start | since_ms={} until_ms={}",
            self._since_ms,
            self._until_ms,
        )
        return self

    async def __anext__(self) -> RawTrade:
        """
        Retorna el siguiente RawTrade del stream histórico.

        Loop interno:
          1. stop() llamado                → StopAsyncIteration
          2. Buffer tiene trades           → retorna el primero (FIFO)
          3. Stream agotado (exhausted)    → StopAsyncIteration
          4. Fetcha página siguiente       → rellena buffer
          5. Página vacía o hasta until_ms → marca exhausted, StopAsyncIteration
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
                "backfill: límite de páginas alcanzado | symbol={} max_pages={} — verificar volumen",
                self._symbol,
                _MAX_PAGES,
            )
            self._exhausted = True
            self._running = False
            raise StopAsyncIteration

        # 5. Fetch página
        raw_page = await self._fetch_page_with_retry(self._cursor_ms)
        self._pages_read += 1

        if not raw_page:
            self._log.debug(
                "backfill exhausted (página vacía) | symbol={} pages={}",
                self._symbol,
                self._pages_read,
            )
            self._exhausted = True
            self._running = False
            raise StopAsyncIteration

        # 6. ACL + filtro temporal
        new_trades: list[RawTrade] = []
        max_ts = self._cursor_ms or 0
        reached_until = False

        for raw in raw_page:
            trade = _parse_raw_trade(raw, self._exchange_id, self._market_type, self._symbol)
            if trade is None:
                continue
            # Corte temporal: until_ms exclusivo
            if self._until_ms is not None and trade.timestamp_ms >= self._until_ms:
                reached_until = True
                continue
            new_trades.append(trade)
            max_ts = max(max_ts, trade.timestamp_ms)

        # Ordenar cronológicamente antes de encolar (SafeOps: CCXT no garantiza orden)
        new_trades.sort(key=lambda t: t.timestamp_ms)
        self._buffer.extend(new_trades)

        # 7. Avanzar cursor — +1ms evita re-fetch del último trade
        if max_ts > (self._cursor_ms or 0):
            self._cursor_ms = max_ts + 1

        # 8. Condición de fin: página incompleta, until_ms alcanzado, o buffer vacío post-filtro
        page_incomplete = len(raw_page) < self._page_limit
        if page_incomplete or reached_until or not self._buffer:
            if not self._buffer:
                self._exhausted = True
                self._running = False
                self._log.debug(
                    "backfill exhausted | symbol={} pages={} reason={}",
                    self._symbol,
                    self._pages_read,
                    "until_ms" if reached_until else "page_incomplete",
                )
                raise StopAsyncIteration
            # Hay trades en buffer pero no vendrán más páginas
            self._exhausted = True

        self._log.debug(
            "backfill page | symbol={} page={} trades={} cursor_ms={}",
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
            self._log.debug("TradesBackfillFetcher stopped")
        except Exception as _stop_exc:  # SafeOps — nunca propaga
            self._log.warning("stop() raised unexpectedly | error={}", _stop_exc)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _fetch_page_with_retry(
        self,
        since_ms: Optional[int],
    ) -> list:
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
            context=f"TradesBackfillFetcher.fetch_trades symbol={self._symbol}",
        )

    def __repr__(self) -> str:
        return (
            f"TradesBackfillFetcher("
            f"exchange={self._exchange_id!r}, "
            f"symbol={self._symbol!r}, "
            f"market_type={self._market_type!r}, "
            f"since_ms={self._since_ms}, "
            f"until_ms={self._until_ms}, "
            f"running={self._running})"
        )


__all__ = ["TradesBackfillFetcher"]
