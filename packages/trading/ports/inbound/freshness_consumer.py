# -*- coding: utf-8 -*-
"""
market_data/ports/inbound/freshness_consumer.py
===============================================

FreshnessConsumer — puerto INBOUND de consumo de freshness en trading.

Trading consume el estado de freshness de market_data a través de este
puerto, sin importar directamente la implementación de GapAwareStream
(respeta BC-10: trading no importa sibling bounded contexts).

Responsabilidad
---------------
Definir el contrato por el cual trading (OMS, RiskManager) consulta la
freshness de un stream de datos de mercado antes de tomar decisiones de
ejecución o riesgo.

No implementa lógica de enforcement — solo declara el puerto de consulta.
El enforcement pre-orden (bloquear orden si stale) se define en la capa
de aplicación de trading (Fase 5 del ADR-0059).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from shared.contracts.boundaries import FreshnessSnapshot


@runtime_checkable
class FreshnessConsumerPort(Protocol):
    """
    Contrato para consumir freshness de datos de mercado en trading.

    Implementado por: adaptadores que envuelven FreshnessPort de
    market_data (ej. en composition_root de trading).
    Consumido por: OMS (pre-submit), RiskManager (sizing/drawdown).

    Fail-closed: si no puede obtener freshness, lanza.
    """

    def get_freshness(self, stream_id: str) -> FreshnessSnapshot:
        """
        Retorna el estado de freshness de un stream de market_data.

        Parámetros
        ----------
        stream_id : identificador del stream (ej. "ws:bybit:BTC/USDT")

        Retorna
        -------
        FreshnessSnapshot con last_seen_ms, lag_ms, is_stale, etc.

        Fail-closed: lanza si no puede determinar la freshness.
        """
        ...


__all__ = ["FreshnessConsumerPort"]

# Umbral de lag por defecto para freshness en streams de trading (ms).
# Símbolo de freshness consumido por trading vía FreshnessConsumerPort.
lag_ms: int = 30_000
