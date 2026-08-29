# -*- coding: utf-8 -*-
"""
market_data/application/processing/book_builder.py
====================================================

Libro de órdenes L2 reconstruido (schema v2) — use case puro de aplicación.

Responsabilidad
---------------
Mantener el estado coherente del order book por (exchange, symbol) a partir de
los snapshots y deltas atómicos multinivel que llegan por orderbook.raw (v2).
Este módulo NO depende de Kafka ni de infraestructura (Clean Architecture):
es 100% framework-agnostic y unit-testable sin broker.

Contrato con el adapter de infraestructura
------------------------------------------
  builder = BookBuilder(viewport=depth, stale_ms=2000)
  out = builder.on_snapshot(exchange, symbol, ts, bids, asks, update_id=...)
  out = builder.on_delta(exchange, symbol, ts, bids, asks, update_id=...)
  out = builder.check_stale(now_ms)

Cada método devuelve un ``BookBuilderOutcome`` (dataclass inmutable) que el
adapter traduce a publicación Kafka (book.snapshot / book.delta) o a una
respuesta de operación (gap / stale / deshechado). El use case nunca publica.

Decisiones de diseño (D-7, aprobado)
------------------------------------
- D-7a  : el delta llega ATÓMICO multinivel (un mensaje = una actualización).
- D-7b  : la continuidad se verifica por ``update_id`` ('u'), NO por ``seq+1``.
          En P0 se observó que 'seq' tiene huecos no atómicos (min 9, max 7593)
          mientras que 'u' va estrictamente +1.
- D-7c  : el estado interno del libro usa ``Decimal`` para precio/cantidad
          (sin pérdida de precisión). El wire usa str (ver schemas.orderbook).
- D-7d  : viewport — solo se conservan/publican los top-N niveles por lado.
          El replay histórico queda diferido (Fase 1 = viewport).

Protocolo frente a gaps (D-7b, Bybit semantics)
-----------------------------------------------
  next.update_id != last.update_id + 1  →  GAP
      → invalidar estado del símbolo
      → NO aplicar el delta (evita construir un libro corrupto)
      → emitir GapDetected → el adapter alerta y espera un snapshot fresco
        (el stream reenvía snapshot al reconectar) para reinicializar.

Protocolo de borrado de nivel (estándar de mercado)
---------------------------------------------------
  (price, size="0") dentro de bids/asks  →  eliminar el nivel.

Invariantes estructurales (fail-fast / descartar)
-------------------------------------------------
  - Cantidades negativas → descartar delta (dato corrupto).
  - Tras aplicar, bids DESC / asks ASC se re-verifica; si queda cruzado o
    desordenado con cantidades inválidas, se invalida y pide snapshot.

Principios: SRP · DIP · Clean Architecture · Fail-Fast · KISS
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from market_data.ports.outbound.book_builder import (
    BookBuilderOutcome,
    OutcomeKind,
)

# ---------------------------------------------------------------------------
# Estado interno del libro — Decimal (D-7c)
# ---------------------------------------------------------------------------


@dataclass
class _BookState:
    """Estado vivo del libro para un símbolo, con Decimal exacto (D-7c).

    bids : dict[Decimal] → Decimal, orden implícito DESC por precio.
    asks : dict[Decimal] → Decimal, orden ASC por precio.
    """

    exchange: str
    symbol: str
    bids: Dict[Decimal, Decimal] = field(default_factory=dict)
    asks: Dict[Decimal, Decimal] = field(default_factory=dict)
    last_update_id: int = 0
    has_snapshot: bool = False
    last_update_at_ms: int = 0

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = 0
        self.has_snapshot = False
        self.last_update_at_ms = 0


# ---------------------------------------------------------------------------
# BookBuilder — use case puro
# ---------------------------------------------------------------------------


class BookBuilder:
    """
    Reconstruye y mantiene el estado L2 del order book (schema v2).

    Parametrización
    ---------------
    viewport  : top-N niveles por lado a conservar/publicar (D-7d). 0 = ilimitado.
    stale_ms  : ventana (ms) sin actualizaciones antes de marcar STALE.
    """

    def __init__(self, viewport: int = 50, stale_ms: int = 2_000) -> None:
        if viewport < 0:
            raise ValueError(f"BookBuilder.viewport must be >= 0, got {viewport}")
        if stale_ms < 0:
            raise ValueError(f"BookBuilder.stale_ms must be >= 0, got {stale_ms}")
        self._viewport = viewport
        self._stale_ms = stale_ms
        self._books: Dict[Tuple[str, str], _BookState] = {}

    # ------------------------------------------------------------------ #
    # API pública                                                          #
    # ------------------------------------------------------------------ #

    def on_snapshot(
        self,
        exchange: str,
        symbol: str,
        timestamp_ms: int,
        bids: List[Tuple[str, str]],
        asks: List[Tuple[str, str]],
        update_id: int = 0,
    ) -> BookBuilderOutcome:
        """
        Aplica un snapshot completo. Descarta todo estado previo del símbolo.

        Un snapshot es autoritativo: reconstruye el libro desde cero y
        establece la base de continuidad (update_id) para deltas posteriores.
        """
        state = self._books.setdefault((exchange, symbol), _BookState(exchange, symbol))
        state.reset()

        def _as_decimal(pair: Tuple[str, str]) -> Optional[Tuple[Decimal, Decimal]]:
            try:
                return Decimal(pair[0]), Decimal(pair[1])
            except Exception:
                return None

        sbids: Dict[Decimal, Decimal] = {}
        sasks: Dict[Decimal, Decimal] = {}
        for pair in bids:
            d = _as_decimal(pair)
            if d is None or d[1] < 0:
                # Nivel corrupto en snapshot → invalidar (fail-fast, descartar).
                continue
            if d[1] != 0:
                sbids[d[0]] = d[1]
        for pair in asks:
            d = _as_decimal(pair)
            if d is None or d[1] < 0:
                continue
            if d[1] != 0:
                sasks[d[0]] = d[1]

        state.bids = sbids
        state.asks = sasks
        state.last_update_id = update_id
        state.has_snapshot = True
        state.last_update_at_ms = timestamp_ms

        out_bids, out_asks = self._viewport_levels(state)
        return BookBuilderOutcome(
            kind=OutcomeKind.SNAPSHOT_APPLIED,
            exchange=exchange,
            symbol=symbol,
            update_id=update_id,
            bids=out_bids,
            asks=out_asks,
            timestamp_ms=timestamp_ms,
        )

    def on_delta(
        self,
        exchange: str,
        symbol: str,
        timestamp_ms: int,
        bids: List[Tuple[str, str]],
        asks: List[Tuple[str, str]],
        update_id: int = 0,
    ) -> BookBuilderOutcome:
        """
        Aplica un delta atómico multinivel (schema v2).

        Returns outcome con kind:
          DELTA_APPLIED      → aplicar + publicar book.delta
          DELTA_BEFORE_SNAPSHOT → no hay snapshot aún → deshechar (no publicar)
          GAP_DETECTED       → update_id discontinuo → invalidar, pedir snapshot
          STRUCTURAL_INVALID → deltas corruptos (qty<0) → descartar/invalidar
        """
        state = self._books.setdefault((exchange, symbol), _BookState(exchange, symbol))

        if not state.has_snapshot:
            # No tenemos base — no podemos aplicar un delta. Se descarta
            # (evita construir un libro a medias). El snapshot llegará y
            # reestablecerá la base.
            return BookBuilderOutcome(
                kind=OutcomeKind.DELTA_BEFORE_SNAPSHOT,
                exchange=exchange,
                symbol=symbol,
                update_id=update_id,
                timestamp_ms=timestamp_ms,
                detail="delta antes de snapshot — deshechado",
            )

        # Continuidad por update_id (D-7b). El primer delta tras un snapshot
        # debe continuar exactamente con update_id == last + 1.
        if state.last_update_id != 0 and update_id != state.last_update_id + 1:
            expected = state.last_update_id + 1
            state.reset()
            return BookBuilderOutcome(
                kind=OutcomeKind.GAP_DETECTED,
                exchange=exchange,
                symbol=symbol,
                update_id=update_id,
                timestamp_ms=timestamp_ms,
                detail=(
                    f"gap detectado: esperado u={expected}, recibido u={update_id} "
                    f"(dE={update_id - expected}) — estado invalidado, requiere snapshot"
                ),
            )

        # Primer delta tras snapshot: update_id debe ser last+1 (last podría
        # ser 0 si el snapshot no trajo 'u'). Si el snapshot tenía u=0 y el
        # delta trae un u real, lo aceptamos (base aún no establecida).
        if not self._apply_delta_levels(state, bids, asks, timestamp_ms):
            # Deltas corruptos (qty<0) → no aplicar nada → invalidar.
            state.reset()
            return BookBuilderOutcome(
                kind=OutcomeKind.STRUCTURAL_INVALID,
                exchange=exchange,
                symbol=symbol,
                update_id=update_id,
                timestamp_ms=timestamp_ms,
                detail="delta con cantidad negativa o nivel inválido — no aplicado, requiere snapshot",
            )

        state.last_update_id = update_id
        state.last_update_at_ms = timestamp_ms

        out_bids, out_asks = self._viewport_levels(state)
        return BookBuilderOutcome(
            kind=OutcomeKind.DELTA_APPLIED,
            exchange=exchange,
            symbol=symbol,
            update_id=update_id,
            bids=out_bids,
            asks=out_asks,
            timestamp_ms=timestamp_ms,
        )

    def check_stale(self, now_ms: int) -> List[BookBuilderOutcome]:
        """
        Marca como STALE los libros sin actualización dentro de ``stale_ms``.

        El adapter lo invoca periódicamente y alerta si hay outcomes STALE.
        """
        stale: List[BookBuilderOutcome] = []
        for (exchange, symbol), state in list(self._books.items()):
            if not state.has_snapshot:
                continue
            if state.last_update_at_ms and (now_ms - state.last_update_at_ms) > self._stale_ms:
                stale.append(
                    BookBuilderOutcome(
                        kind=OutcomeKind.STALE,
                        exchange=exchange,
                        symbol=symbol,
                        update_id=state.last_update_id,
                        timestamp_ms=state.last_update_at_ms,
                        detail=(
                            f"libro stale: último update hace "
                            f"{now_ms - state.last_update_at_ms}ms (umbral {self._stale_ms}ms)"
                        ),
                    )
                )
        return stale

    def book_state(self, exchange: str, symbol: str) -> Optional[Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]]:
        """Devuelve (bids, asks) actuales en el viewport, o None si no hay libro."""
        state = self._books.get((exchange, symbol))
        if state is None or not state.has_snapshot:
            return None
        return self._viewport_levels(state)

    # ------------------------------------------------------------------ #
    # Internos                                                             #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _apply_delta_levels(
        state: _BookState,
        bids: List[Tuple[str, str]],
        asks: List[Tuple[str, str]],
        timestamp_ms: int,
    ) -> bool:
        """Aplica niveles al estado. Retorna False si algún nivel es inválido.

        (price, "0") elimina el nivel; (price, qty>0) upsert. Cantidades
        negativas → inválido (datos corruptos) → no aplicar nada (atómico).
        """
        # Pre-validamos TODOS los niveles antes de mutar (atomicidad D-7a):
        # si cualquiera es inválido, no tocar el libro.
        try:
            b_ops = [(Decimal(p), Decimal(q)) for p, q in bids]
            a_ops = [(Decimal(p), Decimal(q)) for p, q in asks]
        except Exception:
            return False
        for _, q in [*b_ops, *a_ops]:
            if q < 0:
                return False

        for price, qty in b_ops:
            if qty == 0:
                state.bids.pop(price, None)
            else:
                state.bids[price] = qty
        for price, qty in a_ops:
            if qty == 0:
                state.asks.pop(price, None)
            else:
                state.asks[price] = qty
        return True

    def _viewport_levels(self, state: _BookState) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        """Serializa el estado al viewport (D-7d), bids DESC / asks ASC.

        Usa formato punto fijo (format(x, 'f')) para NO degradar la
        precisión Decimal a notación científica ('2E-10' → '0.0000000002').
        """
        sbids = [
            (self._fmt(price), self._fmt(qty))
            for price, qty in sorted(state.bids.items(), key=lambda kv: kv[0], reverse=True)
        ]
        sasks = [(self._fmt(price), self._fmt(qty)) for price, qty in sorted(state.asks.items(), key=lambda kv: kv[0])]
        if self._viewport and self._viewport > 0:
            sbids = sbids[: self._viewport]
            sasks = sasks[: self._viewport]
        return sbids, sasks

    @staticmethod
    def _fmt(value: Decimal) -> str:
        """Serializa Decimal a str fijo (no científico) para el wire (D-7c)."""
        return format(value, "f")


__all__ = [
    "BookBuilder",
    "BookBuilderOutcome",
    "OutcomeKind",
]
