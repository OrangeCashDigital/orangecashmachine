# -*- coding: utf-8 -*-
"""
trading/application/freshness_enforcement_port.py
=================================================

FreshnessEnforcementPort — puerto de enforcement pre-orden en trading.

Define el contrato por el cual trading bloquea órdenes cuando los datos
de mercado están obsoletos (stale), sin cambiar el comportamiento actual
de OMS.submit() (esa implementación queda para la fase 5 del ADR-0059,
requiere autorización del owner).

Responsabilidad
---------------
Declarar el contrato de enforcement pre-orden: antes de enviar una orden,
consultar freshness; si está stale, bloquear + alerta (fail-closed).

Este puerto se implementa en la capa de aplicación de trading
(OMS.submit() consulta FreshnessPort antes de enviar).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    pass  # FreshnessSnapshot no se usa directamente en este módulo


@runtime_checkable
class FreshnessEnforcementPort(Protocol):
    """
    Contrato de enforcement pre-orden basado en freshness.

    Implementado por: OMS (en su método submit, fase 5 del ADR-0059).
    Consumido por: composición de trading (composition_root).

    Fail-closed: si la freshness no puede determinarse, la orden se bloquea.
    """

    def enforce_freshness_before_submit(
        self,
        stream_id: str,
        max_lag_ms: int,
    ) -> bool:
        """
        Verifica si se puede enviar una orden dado el estado de freshness.

        Parámetros
        ----------
        stream_id : identificador del stream de market_data
        max_lag_ms : lag máximo aceptable en milisegundos

        Retorna
        -------
        True si se puede enviar la orden (freshness OK).
        False si la orden debe bloquearse (datos obsoletos).

        Fail-closed: si no puede determinarse la freshness, retorna False.
        """
        ...


__all__ = ["FreshnessEnforcementPort"]

# Umbral de lag por defecto para enforcement pre-orden (ms).
# Símbolo de freshness usado en capa de aplicación de trading.
lag_ms: int = 30_000
