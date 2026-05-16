# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/exchange_quirks.py
====================================================

Conocimiento sobre capacidades específicas por exchange — value object puro.

Por qué está en domain/
-----------------------
ExchangeQuirks afecta la lógica de paginación y fechas de origen del backfill.
Es conocimiento de negocio, no de infraestructura: ninguna dependencia externa.
Value object: inmutable, comparable por valor, sin identidad ni efectos.

El módulo adapters/outbound/exchange/exchange_quirks.py re-exporta desde aquí
para backward compat durante la transición.

Principios: DDD (Value Object) · SSOT · DIP · KISS
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExchangeQuirks:
    """
    Capacidades específicas de un exchange que afectan paginación y llamadas REST.

    backward_pagination  : True → el exchange devuelve velas anteriores a `endAt`
                           en lugar de responder a `since`. Paginar decrementando end_ms.
    requires_end_at      : True → el exchange exige el parámetro `endAt` (en segundos).
    reject_zero_since    : True → since=0 se convierte a None antes de la llamada.
    origin_fallback_date : Fecha ISO 8601 usada como origen cuando el exchange
                           no soporta since=1. SSOT para _discover_origin().
    """
    backward_pagination:  bool = False
    requires_end_at:      bool = False
    reject_zero_since:    bool = False
    origin_fallback_date: str  = "2017-01-01"


# SSOT por exchange_id. Exchanges no listados usan defaults (todo False, 2017-01-01).
EXCHANGE_QUIRKS: dict[str, ExchangeQuirks] = {
    "kucoin": ExchangeQuirks(
        backward_pagination  = True,
        requires_end_at      = True,
        reject_zero_since    = True,
        origin_fallback_date = "2018-01-01",
    ),
    "kucoinfutures": ExchangeQuirks(
        backward_pagination  = True,
        requires_end_at      = True,
        reject_zero_since    = True,
        origin_fallback_date = "2020-01-01",
    ),
    "bybit": ExchangeQuirks(
        origin_fallback_date = "2021-04-01",
    ),
    "bybit_futures": ExchangeQuirks(
        origin_fallback_date = "2019-10-01",
    ),
}

_DEFAULT_QUIRKS = ExchangeQuirks()


def get_quirks(exchange_id: str) -> ExchangeQuirks:
    """Devuelve las quirks del exchange, o los defaults si no está registrado."""
    return EXCHANGE_QUIRKS.get(exchange_id, _DEFAULT_QUIRKS)


def get_origin_fallback_ms(exchange_id: str, market_type: str = "spot") -> int:
    """
    Timestamp mínimo de historia disponible para el exchange (epoch ms).

    Usado en _discover_origin() cuando since=1 devuelve near-now en lugar del más antiguo.
    Lookup compuesto: exchange_id para spot, exchange_id_futures para non-spot.
    Fallback conservador a 2017-01-01 para exchanges no registrados.
    """
    import pandas as _pd  # pandas es herramienta de datos, no infraestructura
    _is_futures = market_type.lower() not in ("spot", "")
    lookup_key  = f"{exchange_id}_futures" if _is_futures else exchange_id
    quirks      = EXCHANGE_QUIRKS.get(lookup_key) or get_quirks(exchange_id)
    return int(_pd.Timestamp(quirks.origin_fallback_date, tz="UTC").value // 1_000_000)


__all__ = [
    "ExchangeQuirks",
    "EXCHANGE_QUIRKS",
    "get_quirks",
    "get_origin_fallback_ms",
]
