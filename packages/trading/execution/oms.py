# -*- coding: utf-8 -*-
"""
trading/execution/oms.py
=========================

OMS — Order Management System.

Responsabilidad única (SRP)
---------------------------
Gestionar el ciclo de vida completo de las órdenes:
  crear → validar riesgo → submit → fill/reject → notificar

No genera señales, no accede a exchanges directamente.

Ciclo correcto de posiciones (importante)
------------------------------------------
  _open_positions = posiciones HELD (abiertas y tenidas).
  submit()  → record_open() SOLO si BUY (abre la posición y la mantiene)
  _fill()   → record_close() SOLO si SELL (cierra la posición realizada)
  _reject() → record_close() SOLO si era BUY (revertir: no se mantuvo abierta)
  request_cancel() → CANCELLING; resolve_cancel() con estado del exchange:
      CANCELLED → record_close() SOLO si era BUY (revertir: no se completó)

S1 — integridad de P&L y fills reales
--------------------------------------
  - execute() retorna OrderResult (no bool): el OMS propaga el fill real del
    exchange (fill_price, filled_qty, fees) vía OrderState.
  - `_entry_positions` (symbol → (open_qty, avg_entry)) es la fuente de la
    entrada real como weighted average cost (ADR-0025/F4b): el P&L realizado
    del SELL se calcula contra ese avg (determinista, INV-04), no contra
    signal.price (criterio G — la vía paralela Order.pnl_pct fue eliminada) y
    no contra la última entrada (last-entry-wins eliminado).
  - record_close(pnl_pct) recibe el P&L real; el drawdown-halt del RiskManager
    se computa sobre él (criterio F). Un cierre parcial reduce la qty abierta
    preservando el avg; la posición se da por cerrada a qty = 0.

F1 — Execution Quantity / SELL sizing (ADR-0025/0026/0027)
-----------------------------------------------------------
  - La cantidad económica canónica es la de la posición (Position.quantity,
    SSOT en PortfolioService; `_entry_positions` es su espejo local para las
    órdenes que pasan por este OMS, alimentado por los mismos fills).
  - En submit(), un SELL deriva su cantidad REQUESTED de esa posición:
      target = signal.quantity (TARGET del productor) si lo hay, si no la
               cantidad abierta local (cierre completo);
      clamp  = min(target, cantidad abierta local) cuando la local es > 0
               (INV-08: NUNCA se pide más de lo disponible).
  - Si no hay cantidad económica disponible (ni local ni TARGET) el SELL se
    RECHAZA (return None, patrón existente de rechazo) — fail-closed, nunca
    se vende de una posición desconocida.
  - El TARGET de la señal solo lo produce el TradingEngine (stop-loss, leído
    del snapshot del portfolio — la SSOT); cuando el OMS no tiene espejo local
    (p. ej. tras un restart) confía en ese TARGET. Sin TARGET y sin local →
    rechazo.
  - Un fill parcial reduce la cantidad económica disponible (remaining en
    `_entry_positions` y en la posición del portfolio), así el siguiente SELL
    nunca vuelve a pedir la cantidad original (requested ≠ executed no
    contamina el sizing posterior).
  - fill_price UNKNOWN (exchange sin average): NO se sustituye por
    signal.price (INV-10, ADR-0026). La cantidad ejecutada SÍ se contabiliza
    (reduce la disponible); el P&L queda UNKNOWN y se loguea.

F2 — Risk basado en exposición económica real (ADR-0025/0026/0027)
------------------------------------------------------------------
  - El OMS alimenta el espejo económico de RiskManager con los mismos fills
    que su espejo local `_entry_positions` (record_position): la cantidad
    ejecutada real y el WAC. Así Risk representa la exposición económica real
    (qty × avg_entry), no signal.quantity/requested/size_pct.
  - Un cierre PARCIAL (remaining > 0) reduce la cantidad/exposición pero NO
    cierra la posición: el conteo de posiciones HELD de Risk solo decrementa
    en cierre completo (record_close(close_position=remaining <= 0)).
  - BUY con fill_price UNKNOWN: la cantidad ejecutada se registra en Risk con
    precio UNKNOWN (INV-F2-06: nunca exposición 0).
  - SELL sin entrada WAC (posición desconocida): se limpia el espejo de Risk
    y el conteo decrementa (máx. 0) — semántica held-position preservada.

SafeOps
-------
- Thread-safe con RLock (reentrante — fill puede llamarse desde submit).
- guard.should_stop() antes de cada submit.
- Errores de executor → orden REJECTED, no excepción al caller.
- transition(SUBMITTED) protegido con try/except.

Principios: SOLID · KISS · DRY · SafeOps
"""

from __future__ import annotations

import threading
from typing import Callable, Optional, Protocol, runtime_checkable

from loguru import logger

from ocm.runtime.guard import ExecutionGuard
from shared.contracts.boundaries import (
    SignalProtocol,
)  # DIP — execution depende de abstraccion
from trading.execution.order import Order, OrderSide, OrderStatus
from trading.execution.settlement import FeeStatus, Settlement
from trading.execution.transport import (
    OrderResult,
    OrderState,
    OrderTransport,
)
from trading.execution.transport import (
    OrderStatus as ExchangeStatus,
)
from trading.risk.manager import RiskManager

# ---------------------------------------------------------------------------
# Executor protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class OrderExecutor(Protocol):
    """
    Contrato mínimo del executor de órdenes.

    PaperExecutor  → loguea sin tocar dinero real.
    LiveExecutor   → llama al exchange via CCXT.

    execute() retorna OrderResult (S1): expone el estado real del exchange
    (order_id, fill_price, filled_qty, fees) para que el OMS propague el
    fill real — antes devolvía bool y el OMS rellenaba con signal.price.
    """

    def execute(self, order: Order) -> OrderResult:
        """
        Ejecuta la orden y retorna el resultado con el estado del exchange.

        Returns
        -------
        OrderResult con accepted (bool) y, si procede, state (OrderState) con
        el fill confirmado. No lanza excepciones — errores se capturan
        internamente.
        """
        ...


# ---------------------------------------------------------------------------
# OMS
# ---------------------------------------------------------------------------


class OMS:
    """
    Order Management System.

    Parameters
    ----------
    risk_manager : RiskManager     — decide si la señal puede ejecutarse.
    executor     : OrderExecutor   — paper o live.
    guard        : ExecutionGuard, optional — kill switch.
    on_fill      : callable, optional — callback(order) al fill.
    on_reject    : callable, optional — callback(order) al rechazo.
    """

    def __init__(
        self,
        risk_manager: RiskManager,
        executor: OrderExecutor,
        guard: Optional[ExecutionGuard] = None,
        on_fill: Optional[Callable[[Order], None]] = None,
        on_reject: Optional[Callable[[Order], None]] = None,
    ) -> None:
        if risk_manager is None:
            raise ValueError("OMS: risk_manager es obligatorio")
        if executor is None:
            raise ValueError("OMS: executor es obligatorio")

        self._risk = risk_manager
        self._executor = executor
        self._guard = guard
        self._on_fill = on_fill
        self._on_reject = on_reject

        self._orders: dict[str, Order] = {}
        self._open: dict[str, Order] = {}
        # symbol → (open_qty, avg_entry) — acumulador WAC de la entrada real
        # (ADR-0025/F4b). Sustituye a `_entry_prices` (last-entry-wins): la
        # entrada media de una posición multi-entry es el weighted average
        # cost, determinista (INV-04) y preservado en cierres parciales. El
        # P&L realizado del SELL se calcula contra este avg, no contra
        # signal.price ni contra la última entrada (criterio G).
        self._entry_positions: dict[str, tuple[float, float]] = {}
        self._lock = threading.RLock()
        self._log = logger.bind(component="OMS")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def submit(self, signal: SignalProtocol) -> Optional[Order]:
        """
        Procesa una señal: valida riesgo → crea orden → ejecuta.

        Returns
        -------
        Order  : si fue aceptada y enviada al executor.
        None   : si fue rechazada por riesgo o guard activo.
        """
        # Kill switch — usar propiedad pública stop_reason
        if self._guard is not None and self._guard.should_stop():
            self._log.warning(
                "submit bloqueado — guard activo | reason={}",
                self._guard.stop_reason,
            )
            return None

        # Validación de riesgo
        decision = self._risk.validate(signal)
        if decision.rejected:
            self._log.debug(
                "Señal rechazada por riesgo | symbol={} reason={}",
                signal.symbol,
                decision.reason,
            )
            return None

        # Crear orden
        raw_side = signal.direction
        if raw_side not in ("buy", "sell"):
            self._log.error(
                "submit: side inválido | symbol={} side={}",
                signal.symbol,
                raw_side,
            )
            return None

        # F1 (ADR-0025/0026/0027) — sizing de la orden de reducción/cierre.
        # Para una posición existente, la cantidad REQUESTED de un SELL se
        # deriva de la cantidad económica disponible (Position.quantity, SSOT;
        # `_entry_positions` es el espejo local), NUNCA de signal.quantity/
        # requested/size_pct cuando difieran. El TARGET de la señal solo lo
        # produce el TradingEngine (stop-loss, leído del snapshot del
        # portfolio). Clamp INV-08: nunca se pide más de lo disponible. Un
        # SELL sin cantidad (ni local ni TARGET) se rechaza — fail-closed.
        target_qty: Optional[float] = getattr(signal, "quantity", None)
        if raw_side == "sell":
            with self._lock:
                available, _ = self._entry_positions.get(signal.symbol, (0.0, 0.0))
            if available > 0.0:
                target_qty = min(target_qty if target_qty is not None else available, available)
            if target_qty is None or target_qty <= 0.0:
                self._log.warning(
                    "SELL sin cantidad económica disponible — rechazado | symbol={} requested={}",
                    signal.symbol,
                    getattr(signal, "quantity", None),
                )
                return None

        order = Order(
            symbol=signal.symbol,
            side=OrderSide(raw_side),
            size_pct=decision.size_pct,
            signal=signal,  # type: ignore[arg-type]  # SignalProtocol satisface Signal estructuralmente
            quantity=target_qty,
        )

        with self._lock:
            self._orders[order.order_id] = order
            self._open[order.order_id] = order

        self._log.info(
            "Order created | {} {} {} size={:.1%}",
            order.order_id,
            order.side.value,
            order.symbol,
            order.size_pct,
        )

        # B-03: BUY abre posición HELD y la mantiene hasta SELL. SELL NO abre.
        # `_open_positions` = posiciones tenidas (capping max_open_positions).
        if order.side == OrderSide.BUY:
            self._risk.record_open()

        # Transición a SUBMITTED — protegida ante bugs de estado
        try:
            order.transition(OrderStatus.SUBMITTED)
        except ValueError as exc:
            self._log.error(
                "transition(SUBMITTED) falló | order={} error={}",
                order.order_id,
                exc,
            )
            self._reject(order, reason=f"state_error:{exc}", revert_open=True)
            return order

        # Enviar al executor
        try:
            result = self._executor.execute(order)
        except Exception as exc:
            self._log.error(
                "executor.execute falló | order={} error={}",
                order.order_id,
                exc,
            )
            result = OrderResult(accepted=False, reason=f"{type(exc).__name__}: {exc}")

        # Captura del ID real de exchange en cuanto esté disponible — incluso
        # si accepted=False (p.ej. reconciliación no confirmada): el executor
        # ya pudo haber creado la orden en el exchange. Sin esto,
        # manage_open_orders (ADR-0029 paso 5) no tiene ID sobre el que operar.
        if result.state is not None and result.state.order_id:
            order.exchange_order_id = result.state.order_id

        if result.accepted:
            self._fill(order, result.state)
        else:
            reason = result.reason or "executor_rejected"
            self._reject(order, reason=reason, revert_open=True)

        return order

    def request_cancel(self, order_id: str) -> bool:
        """
        Solicita la cancelación de una orden (ADR-0029, B-MD-008 paso 4).

        Transiciona SUBMITTED → CANCELLING (idempotente: no-op si la orden no
        existe, ya es terminal o ya está en CANCELLING). El estado final lo
        resuelve `resolve_cancel()` contra el estado real del exchange — NUNCA
        se decreta CANCELLED sin confirmación (fail-closed).

        Returns True si la orden quedó (o ya estaba) en CANCELLING.
        """
        with self._lock:
            order = self._orders.get(order_id)
            if order is None or order.is_terminal:
                return False
            if order.status == OrderStatus.CANCELLING:
                return True  # idempotente — ya en transición
            if order.status != OrderStatus.SUBMITTED:
                return False
            order.transition(OrderStatus.CANCELLING)
        self._log.info("Order CANCELLING requested | {}", order_id)
        return True

    def resolve_cancel(self, order_id: str, state: Optional[OrderState]) -> OrderStatus:
        """
        Resuelve la carrera CANCEL/FILL de una orden en CANCELLING (ADR-0029).

        Regla determinista: el fill SIEMPRE prevalece sobre el cancel.

        - state.status == FILLED   → aplica el flujo _fill existente (WAC,
          settlement, fill_sync, portfolio) → FILLED terminal.
        - state.status == CANCELLED → CANCELLED definitivo; revierte la
          posición HELD (record_close).
        - state.status == REJECTED → REJECTED (cancel rechazado / not found
          concluyente).
        - state.status == ERROR / timeout / sin confirmación → permanece
          CANCELLING (fail-closed: no se da por cancelada ni llenada).

        Returns el estado resultante de la orden (domain status).
        """
        with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                return OrderStatus.CANCELLED  # sin tracking local → no-op
        if order.status != OrderStatus.CANCELLING:
            return order.status  # ya resuelta o no transitoria — no-op

        if state is None or state.status == ExchangeStatus.ERROR:
            # Sin confirmación recuperable del exchange → permanece transitorio.
            self._log.warning(
                "cancel sin confirmación — permanece CANCELLING | {} error={}",
                order_id,
                state.error if state is not None else "sin estado",
            )
            return OrderStatus.CANCELLING

        if state.status == ExchangeStatus.FILLED:
            # El fill real prevalece — reutiliza OMS._fill (SSOT de asentamiento).
            self._fill(order, state)
            return OrderStatus.FILLED

        if state.status == ExchangeStatus.REJECTED:
            # Cancel rechazado / orden no cancelable de forma concluyente.
            self._reject(order, reason=state.error or "cancel_rejected", revert_open=True)
            return OrderStatus.REJECTED

        if state.status == ExchangeStatus.CANCELLED:
            with self._lock:
                order.transition(OrderStatus.CANCELLED)
                self._open.pop(order_id, None)
            if order.side == OrderSide.BUY:
                self._risk.record_close(pnl_usd=None)
            self._log.info("Order CANCELLED (confirmado) | {}", order_id)
            return OrderStatus.CANCELLED

        # Otros estados (SUBMITTED/open, sin decisión) → permanece CANCELLING.
        self._log.warning(
            "cancel: estado exchange sin decisión final — permanece CANCELLING | {} status={}",
            order_id,
            state.status.value,
        )
        return OrderStatus.CANCELLING

    # ------------------------------------------------------------------
    # State inspection
    # ------------------------------------------------------------------

    @property
    def open_orders(self) -> list[Order]:
        with self._lock:
            return list(self._open.values())

    @property
    def all_orders(self) -> list[Order]:
        with self._lock:
            return list(self._orders.values())

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._orders.get(order_id)

    def validate_signal(self, signal) -> object:
        """
        Expone la validación de riesgo como API pública del OMS.

        Permite que TradingEngine valide señales sin acceder a
        self._risk directamente (Law of Demeter). El OMS es el punto
        de entrada correcto para cualquier interacción con el RiskManager.

        Returns
        -------
        RiskDecision con campos: rejected (bool), reason (str), size_pct (float).
        """
        return self._risk.validate(signal)

    def summary(self) -> dict:
        with self._lock:
            total = len(self._orders)
            open_ = len(self._open)
            filled = sum(1 for o in self._orders.values() if o.status == OrderStatus.FILLED)
            rejected = sum(1 for o in self._orders.values() if o.status == OrderStatus.REJECTED)
        return {
            "total": total,
            "open": open_,
            "filled": filled,
            "rejected": rejected,
            "risk": self._risk.state(),
        }

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _fill(self, order: Order, state: Optional[OrderState] = None) -> None:
        """Transiciona a FILLED con el fill real del exchange. record_open ya
        fue llamado en submit.

        Semántica held-position (ADR-0003/B-03): `risk._open_positions` cuenta
        posiciones TENIDAS. submit() abre solo si BUY. Aquí un SELL (cerrar la
        posición abierta por un BUY previo) hace record_close. Sin este cierre,
        el contador fuga y `max_open_positions` se agota tras el primer BUY→SELL.

        S1 — propagación del fill real: el precio/cantidad/costes provienen del
        OrderState devuelto por el executor (exchange), NO de signal.price.
        En paper, el state trae fill_price = signal.price (fill al precio de
        señal). El P&L realizado del SELL se calcula contra el weighted average
        cost del BUY que abrió la posición (self._entry_positions, ADR-0025),
        no contra signal.price.

        F1 — fill_price UNKNOWN: si el executor no reporta el precio económico
        del fill, NO se sustituye por signal.price (INV-10, ADR-0026): el
        precio queda UNKNOWN y el WAC/P&L no se inventan. La cantidad
        ejecutada (filled_qty) SÍ se contabiliza: reduce la cantidad económica
        disponible (la invariante F1 es de cantidad). El P&L UNKNOWN se loguea
        y la contabilidad de cierre usa el patrón existente (record_close 0.0).
        """
        fill_price = state.fill_price if state is not None else None
        filled_qty = state.filled_qty if state is not None else None
        fees = state.fees if state is not None else None

        order.transition(
            OrderStatus.FILLED,
            fill_price=fill_price,
            filled_qty=filled_qty,
            fees=fees,
        )
        with self._lock:
            self._open.pop(order.order_id, None)

        if order.side == OrderSide.BUY:
            # ADR-0025/F4b — acumular WAC (qty, avg). La cantidad viene del
            # fill real (filled_qty). Si el executor no la reporta, no se
            # inventa: se loguea y no se acumula (INV-01). Igual con el
            # precio: sin fill_price no hay avg (INV-10), no se usa
            # signal.price.
            if filled_qty is not None and filled_qty > 0:
                with self._lock:
                    # `fill_price or 0.0` solo alimenta el default de prev_avg,
                    # que solo se usa cuando prev_qty == 0 (else → fill_price).
                    prev_qty, prev_avg = self._entry_positions.get(order.symbol, (0.0, fill_price or 0.0))
                if fill_price is not None and fill_price > 0:
                    new_qty = prev_qty + filled_qty
                    new_avg = (prev_qty * prev_avg + filled_qty * fill_price) / new_qty if prev_qty > 0 else fill_price
                    with self._lock:
                        self._entry_positions[order.symbol] = (new_qty, new_avg)
                    # F2 — espejo económico real en Risk: cantidad ejecutada + WAC.
                    self._risk.record_position(order.symbol, new_qty, new_avg)
                else:
                    self._log.warning(
                        "BUY fill sin fill_price — WAC NO acumulado (precio UNKNOWN, INV-10) | {} symbol={}",
                        order.order_id,
                        order.symbol,
                    )
                    # F2 (INV-F2-06): la cantidad ejecutada se registra con
                    # precio UNKNOWN (nunca exposición 0); el avg queda None.
                    self._risk.record_position(order.symbol, prev_qty + filled_qty, None)
            else:
                self._log.warning(
                    "BUY fill sin filled_qty — no se acumula WAC | {} symbol={}",
                    order.order_id,
                    order.symbol,
                )
        elif order.side == OrderSide.SELL:
            with self._lock:
                entry = self._entry_positions.get(order.symbol)
            if entry is not None:
                prev_qty, avg = entry
                # F1 (INV-08): defensiva — la cantidad cerrada nunca supera lo
                # mantenido, aunque el exchange reporte un fill mayor.
                closed_qty = min(filled_qty, prev_qty) if filled_qty is not None else prev_qty
                remaining = prev_qty - closed_qty
                with self._lock:
                    if remaining > 0:
                        # Cierre parcial: se reduce qty y se preserva el WAC.
                        self._entry_positions[order.symbol] = (remaining, avg)
                    else:
                        self._entry_positions.pop(order.symbol, None)
                # F2 (INV-F2-02/03): un cierre parcial reduce la cantidad y la
                # exposición pero NO cierra la posición — el conteo de posiciones
                # HELD solo decrementa en cierre completo (remaining <= 0). El
                # espejo económico de Risk refleja la cantidad restante real.
                self._risk.record_position(order.symbol, remaining if remaining > 0 else None, avg)
                if fill_price is not None and avg > 0:
                    # --- settlement canónico ---
                    settlement = Settlement.compute(
                        order_id=order.order_id,
                        symbol=order.symbol,
                        closed_qty=closed_qty,
                        avg_entry_price=avg,
                        exit_price=fill_price,
                        fee_amount_usd=fees,
                        fee_currency=None,  # GAP F7: fee_currency no disponible en F3
                        fee_status=FeeStatus.KNOWN if fees is not None else FeeStatus.UNKNOWN,
                    )
                    order.settlement = settlement
                    # P&L monetario neto: puede ser None si fee_status UNKNOWN
                    net_realized_usd = settlement.net_realized_usd
                    self._risk.record_close(
                        pnl_usd=net_realized_usd,
                        close_position=remaining <= 0,
                    )
                    self._log.debug(
                        "SELL realizó P&L | {} pnl_usd={:+.2f} gross={:+.2f} net={:+.2f} "
                        "avg={:.2f} exit={:.2f} closed_qty={} remaining={} fee_status={}",
                        order.order_id,
                        net_realized_usd,
                        settlement.gross_realized_usd,
                        net_realized_usd,
                        avg,
                        fill_price,
                        closed_qty,
                        remaining,
                        settlement.fee_status,
                    )
                else:
                    # SELL sin precio real: P&L UNKNOWN, no se inventa.
                    # settlement None: downstream consumirá condicionalmente.
                    order.settlement = None
                    net_realized_usd = None
                    self._risk.record_close(
                        pnl_usd=net_realized_usd,
                        close_position=remaining <= 0,
                    )
                    self._log.warning(
                        "SELL fill sin fill_price — P&L UNKNOWN, no se inventa | "
                        "{} symbol={} closed_qty={} remaining={}",
                        order.order_id,
                        order.symbol,
                        closed_qty,
                        remaining,
                    )
            else:
                # SELL sin BUY previo (posición desconocida) — sin entrada WAC.
                # F2: si Risk aún rastrea el símbolo (p. ej. BUY con precio
                # UNKNOWN que OMS no acumuló), se limpia: la posición se da
                # por cerrada; el conteo held-position decrementa (máx. 0).
                # P&L: sin entrada WAC, P&L UNKNOWN; settlement None.
                order.settlement = None
                self._risk.record_close(pnl_usd=None, close_position=True)
                self._risk.record_position(order.symbol, None, None)
                self._log.debug(
                    "SELL sin entrada WAC registrada | {} avg=None exit={}",
                    order.order_id,
                    fill_price,
                )

        self._log.info(
            "Order FILLED | {} {} @ {} qty={} fees={}",
            order.order_id,
            order.symbol,
            order.fill_price,
            filled_qty,
            fees,
        )
        if self._on_fill:
            try:
                self._on_fill(order)
            except Exception as exc:
                self._log.warning("on_fill callback error | {}", exc)

    def _reject(
        self,
        order: Order,
        reason: str,
        revert_open: bool = False,
    ) -> None:
        """
        Transiciona a REJECTED.

        revert_open=True → la posición había sido contabilizada en record_open
        y debe revertirse porque no se completó.
        """
        order.transition(OrderStatus.REJECTED, reject_reason=reason)
        with self._lock:
            self._open.pop(order.order_id, None)

        if revert_open and order.side == OrderSide.BUY:
            self._risk.record_close(pnl_usd=None)

        self._log.warning(
            "Order REJECTED | {} reason={}",
            order.order_id,
            reason,
        )
        if self._on_reject:
            try:
                self._on_reject(order)
            except Exception as exc:
                self._log.warning("on_reject callback error | {}", exc)

    def __repr__(self) -> str:
        s = self.summary()
        return f"OMS(open={s['open']} filled={s['filled']} rejected={s['rejected']} halted={self._risk.is_halted})"


# ---------------------------------------------------------------------------
# manage_open_orders — gate de reconciliacion (ADR-0029, B-MD-008 paso 5)
# ---------------------------------------------------------------------------


def manage_open_orders(oms: "OMS", transport: "OrderTransport") -> None:
    """Reconcilia el estado local del OMS contra el exchange (ADR-0029).

    NO es un loop de proceso: OCM opera one-shot por ciclo (execute_live.py
    no tiene while/sleep — el "loop" es el reinicio externo del proceso,
    igual que paper_hydra.py). Por eso esta funcion corre como GATE al
    inicio de cada ciclo (execute_live.execute(), antes de run_once()), no
    como tarea en background: "arranque" = cada invocacion del proceso.

    Dos responsabilidades separadas (F-SUB-01/F-SUB-02, auditoria 2026-08-18):

    1. Resolver ordenes CANCELLING pendientes (SUBMITTED es estado
       test-only — F-SUB-01, nunca persiste en el flujo real; solo
       CANCELLING puede sobrevivir entre ciclos si el proceso murio
       durante una cancelacion en curso). Reusa resolve_cancel — misma
       logica de la carrera CANCEL/FILL, no se duplica.
    2. Detectar huerfanas (F-SUB-02): ordenes que el exchange reporta
       abiertas via fetch_open_orders() pero que el OMS no reconoce
       (perdidas por timeout de create_order, crash, reinicio previo).
       Politica (NautilusTrader/Hummingbot, F-SUB-03): NUNCA se inventa
       un reject ni se cancela automaticamente ante lo desconocido — solo
       se alerta. Decidir que hacer con una huerfana es una decision de
       negocio fuera del alcance de esta reconciliacion mecanica.

    SafeOps: no lanza. Un fallo de transporte se loguea y la funcion
    retorna — el ciclo sigue (fail-soft a nivel gate, consistente con
    execute_live.execute() que nunca deja que un fallo de reconciliacion
    tumbe el ciclo completo).
    """
    log = logger.bind(component="manage_open_orders")

    # 1. Resolver CANCELLING pendientes de ciclos anteriores.
    cancelling = [o for o in oms.open_orders if o.status == OrderStatus.CANCELLING]
    for order in cancelling:
        if not order.exchange_order_id:
            log.warning(
                "orden CANCELLING sin exchange_order_id — no se puede reconciliar | {}",
                order.order_id,
            )
            continue
        try:
            state = transport.fetch_state(order.exchange_order_id)
        except Exception as exc:
            log.warning(
                "fetch_state fallo durante reconciliacion CANCELLING | {} id={} err={}",
                order.order_id,
                order.exchange_order_id,
                exc,
            )
            continue
        oms.resolve_cancel(order.order_id, state)

    # 2. Detectar huerfanas — comparar lo que el exchange reporta abierto
    # contra lo que el OMS reconoce localmente.
    try:
        exchange_open = transport.fetch_open_orders()
    except Exception as exc:
        log.warning("fetch_open_orders fallo — huerfanas no verificables | {}", exc)
        return

    known_exchange_ids = {o.exchange_order_id for o in oms.all_orders if o.exchange_order_id}
    for state in exchange_open:
        if state.order_id and state.order_id not in known_exchange_ids:
            # Politica: alerta, NUNCA auto-cancelar ni inventar estado local
            # (F-SUB-03 / NautilusTrader "unknown -> mantener in-flight").
            log.error(
                "ORDEN HUERFANA detectada — el exchange la reporta abierta pero "
                "OCM no la reconoce | exchange_order_id={} status={}",
                state.order_id,
                state.status.value,
            )
