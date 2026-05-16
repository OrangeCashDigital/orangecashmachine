# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/resilience.py
=========================================

Puerto del circuit breaker — DIP · SRP.

OHLCVPipeline depende de este protocolo para consultar el estado
del breaker sin conocer CCXTAdapter ni su implementación interna.

Implementación concreta: adapters/outbound/exchange/ccxt_adapter.py
  (get_breaker_state + ExchangeCircuitOpenError)

Por qué port separado y no en ExchangeClientPort
-------------------------------------------------
ExchangeClientPort define el contrato de comunicación con el exchange
(fetch_ohlcv, fetch_trades, etc.). El circuit breaker es un concern
de resiliencia ortogonal — meterlo en ExchangeClientPort violaría SRP
y contaminaría todos los fakes/mocks de test que implementan el port.
"""
from __future__ import annotations

from typing import Any, Dict, Protocol, runtime_checkable


class ExchangeCircuitOpenError(Exception):
    """
    Señal de dominio: el circuit breaker del exchange está abierto.

    Esta excepción vive en el port (dominio de resiliencia), no en el adapter.
    CCXTAdapter la importa desde aquí y la lanza cuando el breaker activa.
    OHLCVPipeline la captura desde aquí — sin importar ccxt_adapter.
    """


@runtime_checkable
class CircuitBreakerPort(Protocol):
    """Contrato mínimo para consultar el estado del circuit breaker."""

    def get_breaker_state(self, exchange_id: str) -> Dict[str, Any]:
        """
        Retorna el estado actual del breaker para el exchange dado.

        Claves garantizadas:
          fail_counter          : int   — fallos consecutivos
          cooldown_remaining_ms : float — ms hasta que el breaker se cierre
          is_open               : bool  — True si el breaker está abierto
        """
        ...
