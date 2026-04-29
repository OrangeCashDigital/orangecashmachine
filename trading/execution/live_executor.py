# -*- coding: utf-8 -*-
"""
trading/execution/live_executor.py
=====================================

LiveExecutor — executor real que envía órdenes al exchange via CCXT.

Responsabilidad única (SRP)
---------------------------
Recibir una Order y enviarla al exchange real.
NO genera señales, NO valida riesgo, NO gestiona estado de portfolio.

Implementa el OrderExecutor Protocol definido en trading/execution/oms.py.

SafeOps — reglas de producción
-------------------------------
- execute() NUNCA lanza al caller — errores retornan False.
- Timeout configurable en cada llamada CCXT (asyncio.wait_for).
- Logging completo: orden enviada, respuesta del exchange, errores.
- La orden se marca REJECTED si CCXT lanza — el OMS maneja el reintento.

Estado actual: STUB con lógica real de CCXT comentada y documentada.
La integración completa con CCXT async va en el ticket de websockets.

Principios: SRP · DIP · SafeOps · KISS
"""
from __future__ import annotations

from loguru import logger

from trading.execution.order import Order, OrderStatus


class LiveExecutor:
    """
    Ejecutor de órdenes reales en el exchange.

    Parameters
    ----------
    exchange    : str — ID del exchange CCXT (e.g. "bybit", "kucoin")
    market_type : str — "spot" | "linear" | "inverse"
    timeout_s   : int — timeout por llamada CCXT en segundos (default: 10)
    """

    def __init__(
        self,
        exchange:    str,
        market_type: str = "spot",
        timeout_s:   int = 10,
    ) -> None:
        self._exchange    = exchange
        self._market_type = market_type
        self._timeout_s   = timeout_s
        self._log         = logger.bind(
            component   = "LiveExecutor",
            exchange    = exchange,
            market_type = market_type,
        )
        self._log.info(
            "LiveExecutor inicializado | exchange={} market_type={}",
            exchange, market_type,
        )

    # ------------------------------------------------------------------
    # OrderExecutor Protocol
    # ------------------------------------------------------------------

    def execute(self, order: Order) -> bool:
        """
        Envía la orden al exchange real.

        Returns True si fue aceptada, False si rechazada o error.
        SafeOps: nunca lanza — errores capturados y logueados.

        Implementación actual: STUB — loguea la intención sin enviar.
        Activar el bloque CCXT comentado cuando el módulo websocket
        esté implementado y el ccxt_adapter async esté disponible.
        """
        try:
            return self._submit(order)
        except Exception as exc:
            self._log.error(
                "execute: error inesperado | order={} {}", order.order_id, exc
            )
            return False

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _submit(self, order: Order) -> bool:
        """
        Lógica real de submit al exchange.

        ESTADO ACTUAL: STUB — imprime la orden y retorna True simulando éxito.

        ACTIVAR cuando ccxt_adapter async esté disponible:
        ---------------------------------------------------
        from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter
        import asyncio

        try:
            adapter = CCXTAdapter(exchange_id=self._exchange)
            result  = asyncio.run(
                asyncio.wait_for(
                    adapter.create_order(
                        symbol     = order.symbol,
                        order_type = "market",
                        side       = order.side.value,
                        amount     = order.size_pct,   # convertir a qty real antes
                    ),
                    timeout=self._timeout_s,
                )
            )
            self._log.info(
                "Order enviada al exchange | order={} exchange_id={}",
                order.order_id, result.get("id"),
            )
            return True
        except asyncio.TimeoutError:
            self._log.error(
                "CCXT timeout | order={} timeout={}s",
                order.order_id, self._timeout_s,
            )
            return False
        except Exception as exc:
            self._log.error("CCXT error | order={} {}", order.order_id, exc)
            return False
        """
        # STUB — loguea sin enviar al exchange
        self._log.warning(
            "[LIVE-STUB] Orden simulada (CCXT no activo) | {} {} {} @ ~{:.4f} size={:.1%}",
            order.order_id,
            order.side.value.upper(),
            order.symbol,
            order.signal.price,
            order.size_pct,
        )
        return True

    def __repr__(self) -> str:
        return (
            f"LiveExecutor(exchange={self._exchange!r}"
            f" market_type={self._market_type!r})"
        )
