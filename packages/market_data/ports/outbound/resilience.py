# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/resilience.py
=========================================

Puerto de resiliencia — contratos de fallos del adapter layer.

DIP: application y domain dependen de estas excepciones/protocolos,
     nunca de las implementaciones concretas (ccxt, circuit-breaker internals).

SSOT: ExchangeCircuitOpenError es el único lugar donde se define el
      contrato de "circuit abierto". Lleva su propio estado para que
      application nunca necesite consultar al adapter (get_breaker_state).

SRP: este módulo sólo define contratos de resiliencia, no lógica.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


class ExchangeCircuitOpenError(Exception):
    """Circuit breaker abierto — raised por adapter, consumido por application.

    Lleva el estado del breaker en el momento de la excepción:
    - ``cooldown_remaining_ms``: tiempo hasta que el breaker se cierre.
    - ``fail_counter``: número de fallos consecutivos registrados.

    Contrato DIP: application.pipelines usa estos atributos directamente.
    El pipeline nunca llama get_breaker_state() — eso es conocimiento de adapter.

    Ejemplo de uso en el adapter (CCXTAdapter):

        raise ExchangeCircuitOpenError(
            self.exchange_id,
            cooldown_remaining_ms=state["cooldown_remaining_ms"],
            fail_counter=state["fail_counter"],
        )

    Ejemplo de uso en application (OHLCVPipeline):

        except ExchangeCircuitOpenError as exc:
            cooldown = max(1.0, exc.cooldown_remaining_ms / 1000)
            log.bind(failures=exc.fail_counter).warning("Circuit open")
    """

    def __init__(
        self,
        exchange_id: str,
        *,
        cooldown_remaining_ms: float = 0.0,
        fail_counter: int = 0,
    ) -> None:
        self.exchange_id = exchange_id
        self.cooldown_remaining_ms = cooldown_remaining_ms
        self.fail_counter = fail_counter
        super().__init__(
            f"Circuit open for {exchange_id!r} (fails={fail_counter}, cooldown={cooldown_remaining_ms:.0f}ms)"
        )


@runtime_checkable
class CircuitBreakerPort(Protocol):
    """Contrato para consultar estado del circuit breaker.

    Pendiente de implementación en CCXTAdapter.
    Cuando esté implementado, get_breaker_state() desaparece del adapter.
    """

    def get_state(self, exchange_id: str) -> dict[str, object]:
        """Retorna estado actual: fail_counter, cooldown_remaining_ms, is_open."""
        ...

    def reset(self, exchange_id: str) -> None:
        """Resetea el breaker manualmente (uso en tests / admin)."""
        ...
