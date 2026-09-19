# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/websocket/cryptofeed_orderbook_stream.py
=======================================================================

CryptofeedOrderBookStream — ACL de cryptofeed para L2 order book (canal L2_BOOK).

Este módulo ES la Anti-Corruption Layer de cryptofeed para order book.
Todos los tipos vendor (FeedHandler, OrderBook, L2_BOOK) están confinados
aquí — nunca cruzan esta frontera hacia el dominio.

Cadena
------
  CryptofeedOrderBookStream
      → on_snapshot(...) → OrderBookKafkaProducer.on_snapshot → orderbook.raw
      → on_delta(...)    → OrderBookKafkaProducer.on_delta    → orderbook.raw

Uso
---
    producer = OrderBookKafkaProducer(kafka_port)
    stream = CryptofeedOrderBookStream(
        exchange="bybit",
        symbols=["BTC-USDT-PERP"],
        on_snapshot=producer.on_snapshot,
        on_delta=producer.on_delta,
    )
    await stream.start()
    ...
    await stream.stop()

Verificado contra cryptofeed 2.4.1 (source real de exchanges/bybit.py)
-----------------------------------------------------------------------
- book.delta es None en el snapshot inicial; en updates incrementales es
  {BID: [[price, size], ...], ASK: [[price, size], ...]} — listas crudas
  tal como las manda el exchange (BID='bid', ASK='ask').
- book.book es una instancia de order_book.OrderBook (extensión C).
  Su .to_dict() devuelve {'bid': {price: size}, 'ask': {price: size}} —
  forma más simple y confiable de extraer el snapshot completo.
- book.book.bids / .asks son SortedDict sin .items(); no iterar
  directamente, usar to_dict() del wrapper completo en su lugar.
- book.raw es el mensaje Bybit v5 JSON completo (ver _book en bybit.py:
  book_callback(..., raw=msg, ...)). De ahí extraemos u/seq/cts:
      u   → msg['data']['u']        (token de continuidad monótono)
      seq → msg['data']['seq']
      cts → msg['cts']
  Ver _bybit_sequence(). Estas son FUENTES DE EVIDENCIA del Discovery
  Profile de Bybit (ADR-0017 §83-89) — documentado en el perfil.
- El delta v2 se emite como UN mensaje atómico multinivel (bids+asks del
  mismo mensaje), NO aplanado nivel a nivel (D-7a). La atomicidad del
  mensaje Bybit se preserva.

Principios: SRP · DIP · ACL · SafeOps · Kappa
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from cryptofeed import FeedHandler
from cryptofeed.defines import L2_BOOK
from cryptofeed.exchanges import Bybit, KuCoin
from loguru import logger

# ── ACL: exchange slug → clase cryptofeed ─────────────────────────────────────
# Añadir nuevos exchanges aquí y solo aquí.
_EXCHANGE_CLASSES: dict[str, type] = {
    "bybit": Bybit,
    "kucoin": KuCoin,
}

OnSnapshotCallback = Callable[..., Awaitable[None]]
OnDeltaCallback = Callable[..., Awaitable[None]]


class CryptofeedOrderBookStream:
    """
    Runner WebSocket de order book L2 sobre cryptofeed.

    A diferencia de BybitCryptofeedRunner/KuCoinCryptofeedRunner (específicos
    por exchange, traducen TRADES), esta clase es genérica por exchange
    (parametrizada) y traduce el canal L2_BOOK.

    Parámetros
    ----------
    exchange     : slug del exchange ('bybit', 'kucoin')
    symbols      : símbolos en formato cryptofeed (ej. 'BTC-USDT-PERP')
    on_snapshot  : callback async — ver OrderBookKafkaProducer.on_snapshot
    on_delta     : callback async — ver OrderBookKafkaProducer.on_delta
    max_depth    : niveles de profundidad a mantener/publicar por lado
    """

    def __init__(
        self,
        exchange: str,
        symbols: list[str],
        on_snapshot: OnSnapshotCallback,
        on_delta: OnDeltaCallback,
        max_depth: int = 50,
    ) -> None:
        key = exchange.lower().strip()
        exchange_cls = _EXCHANGE_CLASSES.get(key)
        if exchange_cls is None:
            raise ValueError(
                f"CryptofeedOrderBookStream: exchange no soportado: {exchange!r}. "
                f"Disponibles: {sorted(_EXCHANGE_CLASSES)}"
            )
        if not symbols:
            raise ValueError("CryptofeedOrderBookStream: symbols no puede estar vacío")

        self._exchange = key
        self._exchange_cls = exchange_cls
        self._symbols = symbols
        self._on_snapshot = on_snapshot
        self._on_delta = on_delta
        self._max_depth = max_depth
        self._handler: FeedHandler | None = None
        self._log = logger.bind(component="CryptofeedOrderBookStream", exchange=key)

    # ── lifecycle ──────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Arranca el FeedHandler. No bloquea — cryptofeed corre en el loop actual."""
        self._handler = FeedHandler()
        self._handler.add_feed(
            self._exchange_cls(
                symbols=self._symbols,
                channels=[L2_BOOK],
                callbacks={L2_BOOK: self._translate_and_dispatch},
                max_depth=self._max_depth,
            )
        )
        self._log.info(
            "orderbook_stream_starting | symbols={} max_depth={}",
            self._symbols,
            self._max_depth,
        )
        self._handler.run(start_loop=False, install_signal_handlers=False)

    async def stop(self) -> None:
        """SafeOps: detiene el FeedHandler. Nunca lanza."""
        try:
            if self._handler is not None:
                stop_coro = getattr(self._handler, "stop_async", None)
                if stop_coro is not None:
                    await stop_coro()
                self._log.info("orderbook_stream_stopped")
        except Exception as exc:
            self._log.warning("orderbook_stream_stop_error", error=str(exc))

    # ── ACL interna ────────────────────────────────────────────────────────

    async def _translate_and_dispatch(
        self,
        book: Any,
        receipt_timestamp: float,
    ) -> None:
        """
        ACL: cryptofeed OrderBook -> llamadas a on_snapshot / on_delta.

        Firma real confirmada EN VIVO (cryptofeed 2.4.1, traceback de
        produccion contra Bybit WS real, 2026-07-30):
            Feed.callback() invoca:  await cb(obj, receipt_timestamp)
        Son 2 posicionales exactos. book_type se consume internamente
        en Feed.callback() y nunca llega al callback de usuario -- incluirlo
        como parametro produce TypeError y cryptofeed reconecta en loop
        silencioso sin nunca entregar datos (visto en produccion).

        delta, timestamp, raw, checksum, sequence_number NO llegan como
        kwargs -- Feed.book_callback() los asigna como atributos de book
        antes de invocar el callback:
            book.delta, book.timestamp, book.raw, book.checksum, book.sequence_number
        book.delta es None en el snapshot inicial; en updates incrementales es
        {BID: [[price, size], ...], ASK: [[price, size], ...]} -- listas crudas
        tal como las manda el exchange (BID='bid', ASK='ask').

        UNIDADES de timestamp -- confirmadas por observacion directa, no
        por asuncion (ValueError: year 58548 is out of range con el bug viejo):
            receipt_timestamp : segundos (float), estandar time.time(). Requiere *1000.
            book.timestamp    : para Bybit, YA viene en milisegundos (int),
                                 ver cryptofeed/exchanges/bybit.py:405 --
                                 timestamp=int(msg['ts']). NO multiplicar de nuevo.
        Si se reutiliza este adapter para otro exchange (ej. KuCoin),
        verificar sus unidades de book.timestamp con el mismo tipo de
        smoke test antes de asumir milisegundos.

        Todos los accesos a atributos vendor ocurren exclusivamente aqui.
        """
        symbol: str = book.symbol
        delta = getattr(book, "delta", None)
        raw = getattr(book, "raw", None)
        exchange_ts = getattr(book, "timestamp", None)

        if exchange_ts is not None:
            timestamp_ms = int(exchange_ts)  # ya en ms para Bybit
        else:
            timestamp_ms = int(receipt_timestamp * 1000)  # segundos -> ms

        update_id, cross_seq, cts_ms = self._bybit_sequence(raw)

        if delta is None:
            # Snapshot inicial — extraer via to_dict() del wrapper, que ya
            # resuelve el SortedDict interno a {price: size} plano.
            snapshot = book.book.to_dict()
            bids = self._sorted_levels(snapshot.get("bid", {}), descending=True)
            asks = self._sorted_levels(snapshot.get("ask", {}), descending=False)
            try:
                await self._on_snapshot(
                    exchange=self._exchange,
                    symbol=symbol,
                    timestamp_ms=timestamp_ms,
                    bids=bids,
                    asks=asks,
                    depth=self._max_depth,
                    checksum=getattr(book, "checksum", None),
                    update_id=update_id,
                    cross_seq=cross_seq,
                    cts_ms=cts_ms,
                )
            except Exception as exc:
                self._log.bind(symbol=symbol, error=str(exc)).warning("snapshot_dispatch_failed")
            return

        # Delta incremental: {BID: [[price, size], ...], ASK: [[price, size], ...]}
        # Emitimos UN delta atómico multinivel por mensaje (schema v2, D-7a).
        # No aplanamos nivel por nivel: la atomicidad del mensaje Bybit se
        # preserva. "0" como size = eliminar el nivel (lo decide el dominio).
        # Bybit ya ordena b DESC / a ASC, así que preservamos el orden del
        # exchange (canónico) sin re-ordenar.
        bids = self._levels_from_pairs(delta.get("bid", []))
        asks = self._levels_from_pairs(delta.get("ask", []))
        try:
            await self._on_delta(
                exchange=self._exchange,
                symbol=symbol,
                timestamp_ms=timestamp_ms,
                bids=bids,
                asks=asks,
                update_id=update_id,
                cross_seq=cross_seq,
                cts_ms=cts_ms,
            )
        except Exception as exc:
            self._log.bind(symbol=symbol, error=str(exc)).warning("delta_dispatch_failed")

    @staticmethod
    def _bybit_sequence(raw: Any) -> tuple[int, int | None, int | None]:
        """
        Extrae (u, seq, cts) del mensaje Bybit crudo (libre de librería vendor).

        cryptofeed pasa el mensaje JSON completo como ``book.raw`` (Bybit v5):
            { "topic": ..., "type": ..., "ts": ..., "cts": ...,
              "data": { "s": ..., "b": [...], "a": [...], "u": ..., "seq": ... } }
        Devolvemos: (update_id='u', cross_seq='seq', cts_ms='cts').
        Si raw no está disponible (otro exchange o versión de librería), se
        devuelve (0, None, None) — fail-soft, sin cascada.
        """
        if not raw:
            return 0, None, None
        data = raw.get("data") if isinstance(raw, dict) else None
        update_id = int(data.get("u", 0)) if isinstance(data, dict) else 0
        cross_seq = int(data["seq"]) if isinstance(data, dict) and data.get("seq") is not None else None
        cts = raw.get("cts")
        cts_ms = int(cts) if cts is not None else None
        return update_id, cross_seq, cts_ms

    @staticmethod
    def _sorted_levels(price_size_map: dict, descending: bool) -> list[tuple[str, str]]:
        """Convierte {price: size} en lista ordenada de (price_str, size_str)."""
        items = sorted(price_size_map.items(), key=lambda kv: kv[0], reverse=descending)
        return [(str(price), str(size)) for price, size in items]

    @staticmethod
    def _levels_from_pairs(pairs: list) -> list[tuple[str, str]]:
        """Convierte [[price, size], ...] crudo de cryptofeed en [(price_str, size_str), ...].

        Preserva el orden recibido (Bybit ya emite b DESC / a ASC). Los valores
        pueden ser Decimal (parse_float=Decimal en cryptofeed) → se convierten a
        str exacto para preservar precisión (D-7c).
        """
        return [(str(price), str(size)) for price, size in pairs]

    def __repr__(self) -> str:
        return f"CryptofeedOrderBookStream(exchange={self._exchange!r}, symbols={self._symbols!r})"


__all__ = ["CryptofeedOrderBookStream"]
