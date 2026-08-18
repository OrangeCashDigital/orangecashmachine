# -*- coding: utf-8 -*-
"""
trading/execution/transport.py
==============================

OrderTransport — contrato de transporte de órdenes (límite → el exchange).

Responsabilidad
---------------
Separar el OMS (estado de órdenes) del TRANSPORTE (I/O hacia un exchange).
El OMS ya define OrderExecutor; este port define el transporte concreto de
una orden y su reconciliación: submit + fetch del estado real del exchange.

Por qué un port propio (DIP / BC-50)
---------------------------------------
BC-50 prohíbe a trading.* importar market_data: solo
trading/bootstrap/composition_root puede tocar el adaptador CCXT
(mismo patrón que _GoldFeatureSource para datos, ADR-0004/ADR-0016).
Este módulo es framework-agnóstico (sin ccxt, sin market_data) — define SOLO
el contrato. El adapter _BybitTransport (composición_root) implementa FETCH
con CCXTAdapter y se inyecta en LiveExecutor.

Magnitud size→qty
-----------------
OrderTransport opera con *notional* (USD) — el LiveExecutor calcula el qty
real a partir del precio de señal y la pasa al adapter. El adapter hace la
conversión final a moneda del exchange solo donde es padre (Bybit). Mantiene
el dominio sin conocer cantidades de exchange.

SafeOps
-------
- fetch_state() nunca lanza: devuelve OrderState con error.
- La reconciliación es fail-closed: fasill si el estado no confirma FILLED.

Principios: SRP · DIP · SafeOps · BC-50
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Protocol, runtime_checkable

__all__ = [
    "OrderStatus",
    "OrderState",
    "OrderResult",
    "OrderTransport",
    "PaperTransport",
]


class OrderStatus(str, Enum):
    """Estado de una orden en el exchange (vista transporte)."""

    SUBMITTED = "submitted"  # aceptada, pendiente de fill
    FILLED = "filled"  # totalmente ejecutada
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    ERROR = "error"  # no se pudo determinar / transporte falló


@dataclass(frozen=True)
class OrderState:
    """Estado confirmado de una orden desde el exchange (o transporte).

    Se usa tanto como respuesta de submit() (estado inmediato) como de
    fetch_state() (reconciliación).

    fees: coste total del fill en moneda de cotización (USD), cuando el
    exchange lo reporte. None si no hay información de costes (paper).
    """

    order_id: Optional[str] = None
    status: OrderStatus = OrderStatus.SUBMITTED
    filled_qty: Optional[float] = None
    fill_price: Optional[float] = None
    fees: Optional[float] = None
    error: Optional[str] = None

    @property
    def confirmed_filled(self) -> bool:
        return self.status == OrderStatus.FILLED and self.filled_qty is not None


@dataclass(frozen=True)
class OrderResult:
    """Resultado de execute(): cooperación local + estado del exchange."""

    accepted: bool
    state: Optional[OrderState] = None
    reason: Optional[str] = None


@runtime_checkable
class OrderTransport(Protocol):
    """Contrato de transporte de órdenes hacia un exchange.

    Vive en trading/execution (framework-agnóstico). La implementación
    con ccxt vive en trading/bootstrap/composition_root (BC-50) y se
    inyecta en LiveExecutor.
    """

    def submit(
        self,
        symbol: str,
        side: str,
        qty: float,
        *,
        client_order_id: str,
    ) -> OrderState:
        """Envía una orden al exchange y devuelve su estado inmediato.

        SafeOps: nunca lanza — si falla, retorna OrderState status=ERROR.
        """
        ...

    def fetch_state(self, exchange_order_id: str) -> OrderState:
        """Consulta el estado real de la orden desde el exchange (reconciliación).

        Si no se puede confirmar (timeout/red/404), retorna ERROR — el caller NO
        da la orden por filled internamente (fail-closed).
        """
        ...

    def cancel(self, symbol: str, exchange_order_id: str) -> OrderState:
        """Cancela una orden en el exchange (ADR-0029, B-MD-008).

        SafeOps: nunca lanza. Si el cancel no se puede confirmar (timeout/red),
        retorna ERROR — el caller NO declara CANCELLED sin confirmación
        (fail-closed). El ack de cancel es asíncrono en el exchange: el estado
        final (cancelado vs ejecutado) lo confirma fetch_state/WS; el caller es
        quien resuelve la carrera CANCEL/FILL (el fill SIEMPRE prevalece).
        """
        ...

    def close(self) -> None:
        """Cierra recursos del transporte. SafeOps: nunca lanza."""
        ...


class PaperTransport:
    """Transporte simulado — sigue el flujo orden→fill→estado sin I/O.

    Usado en el modo paper del LiveExecutor para validar el ciclo completo
    (orden→fill→reconciliación) con el mismo pipeline que live, pero sin
    dinero real. Siempre confirma FILLED al precio de señal.
    """

    def submit(
        self,
        symbol: str,
        side: str,
        qty: float,
        *,
        client_order_id: str,
    ) -> OrderState:
        return OrderState(
            order_id=client_order_id,
            status=OrderStatus.FILLED,
            filled_qty=qty,
        )

    def fetch_state(self, exchange_order_id: str) -> OrderState:
        # Papel no mantiene estado persistente; cualquier id es del ciclo actual.
        return OrderState(order_id=exchange_order_id, status=OrderStatus.FILLED)

    def cancel(self, symbol: str, exchange_order_id: str) -> OrderState:
        # Sin I/O: el papel confirma el cancel inmediatamente (ADR-0029).
        return OrderState(order_id=exchange_order_id, status=OrderStatus.CANCELLED)

    def close(self) -> None:
        return None

    def __repr__(self) -> str:
        return "PaperTransport()"
