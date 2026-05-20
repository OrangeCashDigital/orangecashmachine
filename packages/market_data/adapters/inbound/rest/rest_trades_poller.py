# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/rest/rest_trades_poller.py
=======================================================

.. deprecated::
    RESTTradesPoller está DEPRECADO desde el ciclo de implementación
    de GapAwareStream (Paso 4). Usar en su lugar:

      ┌─────────────────────────────────────────────────────────────┐
      │  Live (continuo)  →  GapAwareStream(WSTradesSource, ...)   │
      │  Backfill         →  TradesBackfillFetcher                  │
      │  Gap recovery     →  GapRecoveryFetcher                     │
      └─────────────────────────────────────────────────────────────┘

    RESTTradesPoller permanece en el codebase SOLO como alias de
    compatibilidad durante la transición. Será eliminado en el siguiente
    ciclo de limpieza una vez que GapAwareStream esté en producción.

    Razón de deprecación
    --------------------
    RESTTradesPoller mezclaba dos responsabilidades semánticas distintas:
      1. Live fallback continuo  → ahora: GapAwareStream
      2. source=REST_RECOVERY    → BUG: debería ser REST_POLLING para live

    El sistema Kappa requiere que cada fuente declare su source de forma
    semánticamente correcta. REST_RECOVERY está reservado para
    GapRecoveryFetcher (gaps acotados). Un poller continuo no es recovery.

Historial
---------
    Creado  : Ciclo OHLCV bootstrap — live fallback sobre REST
    Deprecado: Ciclo Paso 4 — GapAwareStream + GapRecoveryFetcher
    Eliminar : Siguiente ciclo de limpieza (post-producción de GapAwareStream)

Principios: DIP · ACL · Kappa · SafeOps · Fail-Soft · SSOT
"""
from __future__ import annotations

import asyncio
import warnings
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
_DEFAULT_DEDUP_SIZE:      int   = 5_000  # LRU de trade_ids vistos (ventana de sesión)
_MAX_BACKOFF_S:           float = 60.0   # backoff máximo en errores de red
_BACKOFF_BASE:            float = 2.0    # base exponencial del backoff


# ---------------------------------------------------------------------------
# ACL — conversión CCXT raw → RawTrade  (pura, sin efectos secundarios)
# ---------------------------------------------------------------------------

def _parse_raw_trade(
    raw:         dict,
    exchange_id: str,
    market_type: str,
    symbol:      str,
) -> Optional[RawTrade]:
    """
    ACL: convierte un trade CCXT crudo a RawTrade del dominio.

    source=REST_POLLING declara que este trade proviene de REST polling
    continuo (live fallback), no de WebSocket ni de recovery.

    Semántica SSOT de source:
      REST_POLLING  → polling continuo (este poller, live fallback)
      REST_BACKFILL → histórico acotado (TradesBackfillFetcher)
      REST_RECOVERY → gap recovery acotado (GapRecoveryFetcher)
      WS            → stream live (WSTradesSource / GapAwareStream)

    Fail-soft: retorna None si el trade es inválido — el poller lo omite.
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
        trade_id     = str(raw.get("id") or "").strip()
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
            source       = TradeSource.REST_POLLING,  # SSOT: live polling ≠ recovery
        )
    except (InvalidOperation, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# RESTTradesPoller — AsyncIterator[RawTrade]  [DEPRECATED]
# ---------------------------------------------------------------------------

class RESTTradesPoller:
    """
    .. deprecated::
        Usar GapAwareStream(WSTradesSource, recovery_factory, ...) en su lugar.
        Ver módulo docstring para la ruta de migración completa.

    Stream continuo de trades via REST polling. Implementa TradesSourceProtocol.

    Permanece funcional durante la transición. El constructor emite
    DeprecationWarning para que los callers sean conscientes.

    Parámetros
    ----------
    exchange_adapter  : ExchangeAdapter configurado (DIP).
    exchange_id       : identificador del exchange ("bybit", "kucoin"…).
    symbol            : par de trading ("BTC/USDT").
    market_type       : "spot" | "linear" | "inverse".
    since_ms          : cursor inicial. None = desde el más reciente disponible.
    poll_interval_s   : segundos entre polls cuando no hay datos nuevos.
    page_limit        : máximo de trades por request REST.
    run_id            : correlación con el run actual (observabilidad).

    Ciclo de vida
    -------------
    1. __aiter__() → inicia iteración, resetea stop_event.
    2. __anext__() → retorna RawTrade; bloquea con backoff si no hay datos.
    3. stop()      → SafeOps, idempotente.
    """

    def __init__(
        self,
        exchange_adapter: ExchangeAdapter,
        exchange_id:      str,
        symbol:           str,
        market_type:      str          = "spot",
        since_ms:         Optional[int] = None,
        poll_interval_s:  float        = _DEFAULT_POLL_INTERVAL_S,
        page_limit:       int          = _DEFAULT_PAGE_LIMIT,
        run_id:           str          = "",
    ) -> None:
        # -- Deprecation warning observable en logs y en warnings ----------------
        warnings.warn(
            "RESTTradesPoller está DEPRECADO. "
            "Migrar a GapAwareStream(WSTradesSource, recovery_factory=...) "
            "para live streaming con auto-recovery. "
            "Ver: market_data/adapters/inbound/websocket/gap_aware_stream.py",
            DeprecationWarning,
            stacklevel=2,
        )

        # -- Fail-fast: invariantes de construcción ------------------------------
        if exchange_adapter is None:
            raise ValueError("RESTTradesPoller: exchange_adapter es obligatorio")
        if not exchange_id:
            raise ValueError("RESTTradesPoller: exchange_id no puede ser vacío")
        if not symbol:
            raise ValueError("RESTTradesPoller: symbol no puede ser vacío")
        if poll_interval_s <= 0:
            raise ValueError(
                f"RESTTradesPoller: poll_interval_s debe ser > 0, got {poll_interval_s}"
            )
        if page_limit <= 0:
            raise ValueError(
                f"RESTTradesPoller: page_limit debe ser > 0, got {page_limit}"
            )

        self._adapter       = exchange_adapter
        self._exchange_id   = exchange_id
        self._symbol        = symbol
        self._market_type   = market_type.lower()
        self._cursor_ms     = since_ms
        self._poll_interval = poll_interval_s
        self._page_limit    = page_limit
        self._run_id        = run_id

        self._running    = False
        self._stop_event = asyncio.Event()

        # OrderedDict: LRU manual via popitem(last=False) — O(1) por operación.
        # Mantiene ventana de _DEFAULT_DEDUP_SIZE trade_ids vistos en sesión.
        # Propósito: descartar duplicados de borde de cursor entre polls.
        self._dedup: OrderedDict[str, None] = OrderedDict()

        self._log = logger.bind(
            component   = "RESTTradesPoller[DEPRECATED]",
            exchange    = exchange_id,
            symbol      = symbol,
            market_type = market_type,
        )
        self._log.warning(
            "RESTTradesPoller instanciado [DEPRECATED] | "
            "exchange={} symbol={} — migrar a GapAwareStream",
            exchange_id, symbol,
        )

    # ------------------------------------------------------------------
    # TradesSourceProtocol
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def source_id(self) -> str:
        # prefix "rest:" identifica live REST polling en observabilidad
        return f"rest:{self._exchange_id}:{self._symbol}"

    def __aiter__(self) -> AsyncIterator[RawTrade]:
        self._running = True
        self._stop_event.clear()
        self._log.debug(
            "polling start | since_ms={} interval={}s",
            self._cursor_ms, self._poll_interval,
        )
        return self

    async def __anext__(self) -> RawTrade:
        """
        Retorna el siguiente RawTrade del stream REST polling.

        Loop interno:
          1. stop() llamado              → StopAsyncIteration
          2. fetch_trades(since=cursor)  → batch de trades crudos
          3. Error de red               → backoff exponencial, retry
          4. Batch vacío                → esperar poll_interval_s, retry
          5. ACL + dedup LRU            → filtrar inválidos y duplicados
          6. Yield uno a uno (FIFO)     → el consumer controla el ritmo
          7. Avanzar cursor: max_ts + 1 → evita re-fetch del último trade
        """
        backoff = _DEFAULT_POLL_INTERVAL_S

        while not self._stop_event.is_set():
            # -- fetch con backoff exponencial en error ----------------------
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

            backoff = _DEFAULT_POLL_INTERVAL_S  # reset en fetch exitoso

            if not raw_trades:
                await asyncio.sleep(self._poll_interval)
                continue

            # -- ACL: convertir batch, dedup, avanzar cursor -----------------
            new_trades: list[RawTrade] = []
            # cursor_ms None implica "desde el más reciente" — max_ts arranca en 0
            # y avanza con el primer trade válido recibido.
            max_ts = self._cursor_ms or 0

            for raw in raw_trades:
                trade = _parse_raw_trade(
                    raw, self._exchange_id, self._market_type, self._symbol
                )
                if trade is None:
                    continue
                if trade.trade_id in self._dedup:
                    continue

                # LRU dedup: insertar y evictar oldest si supera la ventana
                self._dedup[trade.trade_id] = None
                if len(self._dedup) > _DEFAULT_DEDUP_SIZE:
                    self._dedup.popitem(last=False)  # O(1) — FIFO eviction

                new_trades.append(trade)
                max_ts = max(max_ts, trade.timestamp_ms)

            if not new_trades:
                await asyncio.sleep(self._poll_interval)
                continue

            # Avanzar cursor — +1ms garantiza que el siguiente poll no
            # re-fetche el trade con timestamp max_ts (borde inclusivo CCXT).
            self._cursor_ms = max_ts + 1

            self._log.debug(
                "polled | trades={} cursor_ms={}",
                len(new_trades), self._cursor_ms,
            )

            # -- Yield uno a uno en orden cronológico ------------------------
            # sorted() garantiza orden independientemente del orden de CCXT.
            for trade in sorted(new_trades, key=lambda t: t.timestamp_ms):
                if self._stop_event.is_set():
                    self._running = False
                    raise StopAsyncIteration
                return trade

        self._running = False
        raise StopAsyncIteration

    async def stop(self) -> None:
        """SafeOps: señaliza fin del stream. Nunca lanza. Idempotente."""
        try:
            self._stop_event.set()
            self._running = False
            self._log.debug("RESTTradesPoller stopped")
        except Exception as _stop_exc:  # SafeOps — nunca propaga
            self._log.warning("stop() raised unexpectedly | error={}", _stop_exc)

    def __repr__(self) -> str:
        return (
            f"RESTTradesPoller[DEPRECATED]("
            f"exchange={self._exchange_id!r}, "
            f"symbol={self._symbol!r}, "
            f"market_type={self._market_type!r}, "
            f"running={self._running})"
        )


__all__ = ["RESTTradesPoller"]
