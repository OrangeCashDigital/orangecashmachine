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
-----------------------------------------
  submit()  → record_open()  (se abre la posición al aceptar)
  _fill()   → (posición ya contabilizada en submit)
  _reject() → record_close() (revertir: la posición no se abrió)
  cancel()  → record_close() (revertir: la posición no se completó)

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

from ocm_platform.boundaries import SignalProtocol  # DIP — execution depende de abstraccion
from market_data.safety.execution_guard import ExecutionGuard
from trading.execution.order import Order, OrderSide, OrderStatus
from trading.risk.manager import RiskManager


# ---------------------------------------------------------------------------
# Executor protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class OrderExecutor(Protocol):
    """
    Contrato mínimo del executor de órdenes.

    PaperExecutor  → loguea sin tocar dinero real.
    LiveExecutor   → llama al exchange via CCXT (futuro).
    """

    def execute(self, order: Order) -> bool:
        """
        Ejecuta la orden.

        Returns True si fue aceptada, False si rechazada.
        No lanza excepciones — errores se capturan internamente.
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
        executor:     OrderExecutor,
        guard:        Optional[ExecutionGuard] = None,
        on_fill:      Optional[Callable[[Order], None]] = None,
        on_reject:    Optional[Callable[[Order], None]] = None,
    ) -> None:
        if risk_manager is None:
            raise ValueError("OMS: risk_manager es obligatorio")
        if executor is None:
            raise ValueError("OMS: executor es obligatorio")

        self._risk     = risk_manager
        self._executor = executor
        self._guard    = guard
        self._on_fill  = on_fill
        self._on_reject = on_reject

        self._orders: dict[str, Order] = {}
        self._open:   dict[str, Order] = {}
        self._lock    = threading.RLock()
        self._log     = logger.bind(component="OMS")

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
                signal.symbol, decision.reason,
            )
            return None

        # Crear orden
        order = Order(
            symbol   = signal.symbol,
            side     = OrderSide(signal.signal),
            size_pct = decision.size_pct,
            signal   = signal,
        )

        with self._lock:
            self._orders[order.order_id] = order
            self._open[order.order_id]   = order

        self._log.info(
            "Order created | {} {} {} size={:.1%}",
            order.order_id, order.side.value, order.symbol, order.size_pct,
        )

        # record_open aquí — la posición se considera abierta al aceptarla
        self._risk.record_open()

        # Transición a SUBMITTED — protegida ante bugs de estado
        try:
            order.transition(OrderStatus.SUBMITTED)
        except ValueError as exc:
            self._log.error(
                "transition(SUBMITTED) falló | order={} error={}",
                order.order_id, exc,
            )
            self._reject(order, reason=f"state_error:{exc}", revert_open=True)
            return order

        # Enviar al executor
        try:
            accepted = self._executor.execute(order)
        except Exception as exc:
            self._log.error(
                "executor.execute falló | order={} error={}",
                order.order_id, exc,
            )
            accepted = False

        if accepted:
            self._fill(order)
        else:
            self._reject(order, reason="executor_rejected", revert_open=True)

        return order

    def cancel(self, order_id: str) -> bool:
        """
        Cancela una orden por ID.

        Returns True si fue cancelada, False si no existe o ya es terminal.
        """
        with self._lock:
            order = self._orders.get(order_id)
            if order is None or order.is_terminal:
                return False
            order.transition(OrderStatus.CANCELLED)
            self._open.pop(order_id, None)

        self._risk.record_close(pnl_pct=0.0)
        self._log.info("Order cancelled | {}", order_id)
        return True

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

    def summary(self) -> dict:
        with self._lock:
            total    = len(self._orders)
            open_    = len(self._open)
            filled   = sum(
                1 for o in self._orders.values()
                if o.status == OrderStatus.FILLED
            )
            rejected = sum(
                1 for o in self._orders.values()
                if o.status == OrderStatus.REJECTED
            )
        return {
            "total":    total,
            "open":     open_,
            "filled":   filled,
            "rejected": rejected,
            "risk":     self._risk.state(),
        }

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _fill(self, order: Order) -> None:
        """Transiciona a FILLED. record_open ya fue llamado en submit."""
        order.transition(
            OrderStatus.FILLED,
            fill_price=order.signal.price,
        )
        with self._lock:
            self._open.pop(order.order_id, None)

        self._log.info(
            "Order FILLED | {} {} @ {:.2f}",
            order.order_id, order.symbol, order.fill_price,
        )
        if self._on_fill:
            try:
                self._on_fill(order)
            except Exception as exc:
                self._log.warning("on_fill callback error | {}", exc)

    def _reject(
        self,
        order:        Order,
        reason:       str,
        revert_open:  bool = False,
    ) -> None:
        """
        Transiciona a REJECTED.

        revert_open=True → la posición había sido contabilizada en record_open
        y debe revertirse porque no se completó.
        """
        order.transition(OrderStatus.REJECTED, reject_reason=reason)
        with self._lock:
            self._open.pop(order.order_id, None)

        if revert_open:
            self._risk.record_close(pnl_pct=0.0)

        self._log.warning(
            "Order REJECTED | {} reason={}",
            order.order_id, reason,
        )
        if self._on_reject:
            try:
                self._on_reject(order)
            except Exception as exc:
                self._log.warning("on_reject callback error | {}", exc)

    def __repr__(self) -> str:
        s = self.summary()
        return (
            f"OMS(open={s['open']} filled={s['filled']} "
            f"rejected={s['rejected']} halted={self._risk.is_halted})"
        )
