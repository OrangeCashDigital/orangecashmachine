# -*- coding: utf-8 -*-
"""
trading/execution/live_executor.py
=====================================

LiveExecutor — executor real de órdenes sobre un OrderTransport.

Responsabilidad única (SRP)
--------------------------
Recibir una Order, convertir su tamaño relativo (size_pct) a notional/qty,
enviarla por el transporte y RECONCILIAR el fill contra el estado del exchange.
NO genera señales, NO valida riesgo, NO gestiona estado de portfolio.

Implementa el OrderExecutor Protocol definido en trading/execution/oms.py.

DIP / BC-50
-----------
Este módulo NO importa market_data ni ccxt (BC-50): opera contra el port
`OrderTransport` (trading/execution/transport.py). El adapter real con CCXT
se inyecta desde trading/bootstrap/composition_root (único punto autorizado).

Reconciliación de fills (ADR-0016/R10)
--------------------------------------
Tras submit, el executor consulta el estado real del exchange (fetch_state)
y solo da la orden por aceptada si se confirma FILLED. Si NO se confirma
(timeout, still open, error), NO llena internamente — política fail-closed:
sin fill confirmado, sin countdown de posición (B-03). El caller (OMS) trata
un retorno accepted=False como REJECTED y revierte el open. S1: execute()
retorna el OrderResult completo (accepted + OrderState) para propagar el
fill real (fill_price, filled_qty, fees) al OMS.

Kill switch / reintentos
------------------------
- guard.should_stop() ANTES de cada submit: un kill activo aborta la orden.
  guard.record_error en errores → el breaker (max_errors) activa el guard.
- Reintentos con backoff ante errores de transporte (submit_state/error),
  acotados por max_retries y respetando idempotencia (clientOrderId).

Principios: SRP · DIP · SafeOps · KISS
"""

from __future__ import annotations

import time
from typing import ClassVar, Optional

from loguru import logger

from ocm.runtime.guard import ExecutionGuard
from trading.execution.order import Order
from trading.execution.transport import (
    OrderResult,
    OrderState,
    OrderStatus,
    OrderTransport,
)

__all__ = ["LiveExecutor", "OrderResult"]


class LiveExecutor:
    """
    Ejecutor de órdenes sobre un OrderTransport inyectado.

    Parameters
    ----------
    capital_usd    : float — capital efectivo para convertir size_pct→qty.
    transport      : OrderTransport — transporte hacia el exchange (ccxt via
                     composition_root, o PaperTransport en modo paper).
    exchange       : str — ID del exchange (solo logging).
    market_type    : str — "spot" | "linear" | "inverse" (solo logging).
    guard          : ExecutionGuard | None — kill switch (obligatorio en live).
    max_retries    : int — reintentos con backoff ante errores de transporte.
    backoff_s      : float — backoff base entre reintentos.
    timeout_s      : int — presupuesto total del ciclo submit+reconciliación.
    """

    # Guard R1 / B-01: fail-closed. Con el transport real commiteado y con
    # pruebas (F3/B-12), IS_STUB pasa a False. assembly_root lo usa para decidir.
    IS_STUB: ClassVar[bool] = False

    def __init__(
        self,
        capital_usd: float,
        transport: OrderTransport,
        exchange: str = "bybit",
        market_type: str = "spot",
        guard: ExecutionGuard | None = None,
        max_retries: int = 1,
        backoff_s: float = 0.5,
        timeout_s: int = 10,
    ) -> None:
        if transport is None:
            raise ValueError("LiveExecutor: transport es obligatorio (OrderTransport).")
        if capital_usd <= 0:
            raise ValueError(f"LiveExecutor: capital_usd debe ser > 0, recibido {capital_usd}")
        self._transport = transport
        self._capital_usd = float(capital_usd)
        self._guard = guard
        self._max_retries = max_retries
        self._backoff_s = backoff_s
        self._timeout_s = timeout_s
        self._log = logger.bind(
            component="LiveExecutor",
            exchange=exchange,
            market_type=market_type,
        )

    # ------------------------------------------------------------------
    # OrderExecutor Protocol
    # ------------------------------------------------------------------

    def execute(self, order: Order) -> OrderResult:
        """Envía la orden por el transport, reconcilia y devuelve el resultado.

        S1: retorna el OrderResult completo (no solo el bool accepted) para que
        el OMS propague el fill real del exchange (OrderState.fill_price,
        filled_qty, fees).

        SafeOps: nunca lanza — errores capturados y devueltos como rejected.
        """
        try:
            return self._submit(order)
        except Exception as exc:
            self._log.error("execute: error inesperado | order={} error={}", order.order_id, exc)
            self._record_failure()
            return OrderResult(accepted=False, reason=f"{type(exc).__name__}: {exc}")

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _kill_guard_blocker(self) -> Optional[str]:
        """Razón si el guard activo bloquea, None si se puede operar."""
        if self._guard is None:
            return None
        if self._guard.should_stop():
            return self._guard.stop_reason or "kill_switch_activo"
        return None

    def _notional_qty(self, order: Order) -> float:
        """Cantidad REQUESTED para el exchange.

        F1 (Execution Quantity): si la orden lleva `quantity` (SELL/cierre —
        el OMS la deriva de la cantidad económica de la posición y la clampa:
        nunca se pide más de lo disponible), se usa esa cantidad exacta —
        NO el sizing por capital ni signal.price.

        Sin quantity (BUY por asignación): notional_usd = capital * size_pct;
        qty = notional / precio de señal.
        """
        if order.quantity is not None:
            if order.quantity <= 0:
                raise ValueError(f"LiveExecutor: order.quantity inválido {order.quantity}")
            return float(order.quantity)
        price = float(order.signal.price)
        if price <= 0:
            raise ValueError(f"LiveExecutor: precio de señal inválido {price}")
        return (self._capital_usd * order.size_pct) / price

    def _submit(self, order: Order) -> OrderResult:
        """Ciclo submit + reconciliación con reintentos con backoff."""
        blocker = self._kill_guard_blocker()
        if blocker is not None:
            self._log.warning(
                "submit bloqueado — guard activo | order={} reason={}",
                order.order_id,
                blocker,
            )
            self._record_failure()
            return OrderResult(accepted=False, reason=f"kill_switch:{blocker}")

        qty = self._notional_qty(order)
        client_order_id = order.order_id  # idempotencia de orden

        state: OrderState | None = None
        last_error: Optional[str] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                state = self._transport.submit(
                    symbol=order.symbol,
                    side=order.side.value,
                    qty=qty,
                    client_order_id=client_order_id,
                )
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                self._log.warning(
                    "transport.submit error | order={} attempt={} err={}",
                    order.order_id,
                    attempt,
                    last_error,
                )
                if attempt < self._max_retries:
                    time.sleep(self._backoff_s * attempt)
                continue

            if state is not None and state.status != OrderStatus.ERROR:
                break
            last_error = state.error if state else "transporte sin estado"
            if attempt < self._max_retries:
                time.sleep(self._backoff_s * attempt)

        if state is None or state.status == OrderStatus.ERROR:
            self._log.error("submit agotó reintentos | order={} err={}", order.order_id, last_error)
            self._record_failure()
            return OrderResult(accepted=False, reason=last_error)

        # Reconciliación: estado inmediato aún no filled
        if state.confirmed_filled:
            self._log.info(
                "orden FILLED confirmada | order={} exchange_id={} fill_price={}",
                order.order_id,
                state.order_id,
                state.fill_price,
            )
            self._record_success()
            return OrderResult(accepted=True, state=state)

        # Estado SUBMITTED/CANCELLED/REJECTED o sin fill — reconciliar por fetch.
        reconciled = self._reconcile(order, state)
        if reconciled is None:
            self._record_failure()
            return OrderResult(accepted=False, reason="reconciliation_no_confirmed")

        self._record_success()
        return OrderResult(accepted=True, state=reconciled)

    def _reconcile(self, order: Order, state: OrderState) -> OrderState | None:
        """Consulta el estado del exchange y retorna el estado si confirma fill.

        Fail-closed: si el exchange no confirma FILLED (SUBMITTED, timeout,
        error), retorna None — el caller NO registra fill ni countdown.
        """
        exchange_order_id = state.order_id
        if not exchange_order_id:
            self._log.warning(
                "reconciliacion: sin exchange_order_id | order={}",
                order.order_id,
            )
            return None
        try:
            fetched = self._transport.fetch_state(exchange_order_id)
        except Exception as exc:
            self._log.warning(
                "reconciliacion fetch error | order={} id={} err={}",
                order.order_id,
                exchange_order_id,
                exc,
            )
            return None
        if fetched.status != OrderStatus.ERROR and fetched.confirmed_filled:
            return fetched
        self._log.warning(
            "reconciliacion NO confirma fill | order={} id={} status={} err={}",
            order.order_id,
            exchange_order_id,
            fetched.status.value,
            fetched.error,
        )
        return None

    # ------------------------------------------------------------------
    # Guard / observabilidad
    # ------------------------------------------------------------------

    def _record_failure(self) -> None:
        if self._guard is not None:
            self._guard.record_error("live_executor")

    def _record_success(self) -> None:
        if self._guard is not None:
            self._guard.record_success()

    def close(self) -> None:
        self._transport.close()

    def __repr__(self) -> str:
        return "LiveExecutor(transport={})".format(type(self._transport).__name__)
