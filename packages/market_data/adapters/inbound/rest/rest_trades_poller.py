# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/rest/rest_trades_poller.py
=======================================================

RESTTradesPoller — implementación REST de TradesSourceProtocol.

Responsabilidad
---------------
Producir un stream continuo de RawTrade usando REST polling via
ExchangeAdapter.fetch_trades(). Implementa TradesSourceProtocol,
lo que lo hace intercambiable con WebSocketTradesStream (futuro).

Kappa Architecture
------------------
REST polling produce eventos RawTrade incrementales e inmutables —
semánticamente idénticos a los de WebSocket. El OHLCV builder y
el OrderBook builder no distinguen el transporte.

Diseño del polling
------------------
  1. fetch_trades(symbol, since=cursor_ms, limit=PAGE_LIMIT)
  2. Para cada trade raw → convertir a RawTrade (ACL)
  3. Yield cada RawTrade → consumer los recibe via AsyncIterator
  4. Actualizar cursor al max(timestamp_ms) del batch
  5. Si batch vacío → esperar poll_interval_s antes de reintentar
  6. Backoff exponencial en errores de red

Cursor
------
Persiste en memoria (last_trade_ms). Para persistencia entre reinicios,
el caller inyecta since_ms en el constructor. El cursor avanza +1ms
después de cada batch para evitar re-fetch del último trade.

Deduplicación
--------------
Los exchanges pueden devolver el mismo trade en polls consecutivos
si el timestamp cae en el borde del cursor. SeenFilter L1 (en memoria)
descarta duplicados por trade_id dentro de la sesión.

ACL — conversión raw → RawTrade
--------------------------------
fetch_trades() devuelve List[Dict] de CCXT. La conversión a RawTrade
ocurre aquí, en la frontera del adapter — el dominio no conoce CCXT.

SafeOps
-------
- Errores de red → backoff + retry, nunca propagan al consumer
- stop() → SafeOps, idempotente
- StopAsyncIteration cuando stop() es llamado

Principios: DIP · ACL · Kappa · SafeOps · Fail-Soft · SSOT
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from decimal import Decimal, InvalidOperation
from typing import AsyncIterator, Optional

from loguru import logger

from market_data.domain.value_objects.raw_trade import RawTrade, TradeSide, TradeSource
from market_data.ports.inbound.trades_source import TradesSourceProtocol
from market_data.ports.outbound.exchange import ExchangeAdapter


# ---------------------------------------------------------------------------
# Constantes operacionales
# ---------------------------------------------------------------------------

_DEFAULT_POLL_INTERVAL_S: float = 1.0    # polling cada 1s en condiciones normales
_DEFAULT_PAGE_LIMIT:      int   = 500    # trades por request REST
_DEFAULT_DEDUP_SIZE:      int   = 5_000  # LRU de trade_ids vistos
_MAX_BACKOFF_S:           float = 60.0   # backoff máximo en errores
_BACKOFF_BASE:            float = 2.0    # base exponencial del backoff


# ---------------------------------------------------------------------------
# ACL — conversión CCXT raw → RawTrade
# ---------------------------------------------------------------------------

def _parse_raw_trade(
    raw:         dict,
    exchange_id: str,
    market_type: str,
    symbol:      str,
) -> Optional[RawTrade]:
    """
    ACL: convierte un trade CCXT crudo a RawTrade del dominio.

    Fail-Soft: retorna None si el trade es inválido — el poller lo omite.
    No lanza — un trade malformado no debe abortar el stream.

    CCXT trade format:
        {
            "id":        "trade_id_string",
            "timestamp": 1700000000000,   # ms UTC
            "price":     30000.5,
            "amount":    0.05,
            "side":      "buy" | "sell",
        }
    """
    try:
        trade_id     = str(raw.get("id") or "")
        timestamp_ms = int(raw.get("timestamp") or 0)
        price_raw    = raw.get("price")
        amount_raw   = raw.get("amount")
        side_raw     = raw.get("side", "")

        if not trade_id or timestamp_ms <= 0:
            return None

        price  = Decimal(str(price_raw))
        amount = Decimal(str(amount_raw))

        if price <= 0 or amount <= 0:
            return None

        return RawTrade(
            exchange     = exchange_id,
            market_type  = market_type,
            symbol       = symbol,
            trade_id     = trade_id,
            timestamp_ms = timestamp_ms,
            price        = price,
            amount       = amount,
            side         = TradeSide.from_raw(side_raw),
            # TECH-DEBT: REST_RECOVERY temporal hasta migración a
            # GapRecoveryFetcher, que recibirá source como parámetro.
            source       = TradeSource.REST_RECOVERY,
        )
    except (InvalidOperation, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# RESTTradesPoller — AsyncIterator[RawTrade]
# ---------------------------------------------------------------------------

class RESTTradesPoller:
    """
    Stream de trades via REST polling. Implementa TradesSourceProtocol.

    Parámetros
    ----------
    exchange_adapter  : ExchangeAdapter configurado (DIP).
    exchange_id       : identificador del exchange ("bybit", "kucoin"…).
    symbol            : par de trading ("BTC/USDT").
    market_type       : "spot" | "linear" | "inverse".
    since_ms          : cursor inicial. None = desde el más reciente.
    poll_interval_s   : segundos entre polls cuando no hay datos nuevos.
    page_limit        : máximo de trades por request.
    run_id            : correlación con el run actual.
    """

    def __init__(
        self,
        exchange_adapter: ExchangeAdapter,
        exchange_id:      str,
        symbol:           str,
        market_type:      str   = "spot",
        since_ms:         Optional[int] = None,
        poll_interval_s:  float = _DEFAULT_POLL_INTERVAL_S,
        page_limit:       int   = _DEFAULT_PAGE_LIMIT,
        run_id:           str   = "",
    ) -> None:
        if exchange_adapter is None:
            raise ValueError("RESTTradesPoller: exchange_adapter es obligatorio")
        if not exchange_id:
            raise ValueError("RESTTradesPoller: exchange_id no puede ser vacío")
        if not symbol:
            raise ValueError("RESTTradesPoller: symbol no puede ser vacío")

        self._adapter        = exchange_adapter
        self._exchange_id    = exchange_id
        self._symbol         = symbol
        self._market_type    = market_type.lower()
        self._cursor_ms      = since_ms
        self._poll_interval  = poll_interval_s
        self._page_limit     = page_limit
        self._run_id         = run_id

        self._running        = False
        self._stop_event     = asyncio.Event()
        # OrderedDict: LRU manual via popitem(last=False).
        # dict normal no garantiza popitem FIFO en CPython < 3.8.
        self._dedup: OrderedDict[str, None] = OrderedDict()

        self._log = logger.bind(
            component   = "RESTTradesPoller",
            exchange    = exchange_id,
            symbol      = symbol,
            market_type = market_type,
        )

    # ------------------------------------------------------------------
    # TradesSourceProtocol
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def source_id(self) -> str:
        return f"rest:{self._exchange_id}:{self._symbol}"

    def __aiter__(self) -> AsyncIterator[RawTrade]:
        self._running = True
        self._stop_event.clear()
        return self

    # TECH-DEBT: __anext__ mezcla fetch, parsing/dedup y yield (SRP).
    # Pendiente: extraer _fetch_batch() y _process_batch() como helpers.
    async def __anext__(self) -> RawTrade:
        """
        Retorna el siguiente RawTrade del stream REST.

        Loop interno:
          1. Si stop() fue llamado → StopAsyncIteration
          2. fetch_trades(since=cursor_ms)
          3. Para cada trade válido no duplicado → yield
          4. Si batch vacío → esperar poll_interval_s
          5. En error → backoff exponencial, retry
        """
        backoff = _DEFAULT_POLL_INTERVAL_S

        while not self._stop_event.is_set():
            try:
                raw_trades = await self._adapter.fetch_trades(
                    symbol = self._symbol,
                    since  = self._cursor_ms,
                    limit  = self._page_limit,
                )
            except Exception as exc:
                self._log.warning(
                    "fetch_trades error | backoff={:.1f}s error={}",
                    backoff, exc,
                )
                await asyncio.sleep(min(backoff, _MAX_BACKOFF_S))
                backoff = min(backoff * _BACKOFF_BASE, _MAX_BACKOFF_S)
                continue

            backoff = _DEFAULT_POLL_INTERVAL_S  # reset backoff en éxito

            if not raw_trades:
                # Sin datos nuevos — esperar antes de reintentar
                await asyncio.sleep(self._poll_interval)
                continue

            # ACL: convertir batch completo, filtrar inválidos y duplicados
            new_trades: list[RawTrade] = []
            # None or 0 → arranca en 0; cualquier trade.timestamp_ms > 0
            # garantiza que max_ts avanza en el primer batch.
            max_ts = self._cursor_ms or 0

            for raw in raw_trades:
                trade = _parse_raw_trade(
                    raw, self._exchange_id, self._market_type, self._symbol
                )
                if trade is None:
                    continue
                if trade.trade_id in self._dedup:
                    continue
                # LRU dedup en memoria
                self._dedup[trade.trade_id] = None
                if len(self._dedup) > _DEFAULT_DEDUP_SIZE:
                    self._dedup.popitem(last=False)
                new_trades.append(trade)
                max_ts = max(max_ts, trade.timestamp_ms)

            if not new_trades:
                await asyncio.sleep(self._poll_interval)
                continue

            # Avanzar cursor — +1ms evita re-fetch del último trade
            self._cursor_ms = max_ts + 1

            self._log.debug(
                "fetched | trades={} cursor_ms={}", len(new_trades), self._cursor_ms
            )

            # Yield uno a uno — el consumer controla el ritmo
            for trade in sorted(new_trades, key=lambda t: t.timestamp_ms):
                if self._stop_event.is_set():
                    self._running = False
                    raise StopAsyncIteration
                return trade

        self._running = False
        raise StopAsyncIteration

    async def stop(self) -> None:
        """SafeOps: señaliza el fin del stream. Nunca lanza."""
        try:
            self._stop_event.set()
            self._running = False
            self._log.debug("RESTTradesPoller stopped")
        except Exception as _stop_exc:  # SafeOps — nunca propaga
            self._log.warning("stop() raised unexpectedly | error={}", _stop_exc)

    def __repr__(self) -> str:
        return (
            f"RESTTradesPoller("
            f"exchange={self._exchange_id!r}, "
            f"symbol={self._symbol!r}, "
            f"market_type={self._market_type!r}, "
            f"running={self._running})"
        )


__all__ = ["RESTTradesPoller"]
