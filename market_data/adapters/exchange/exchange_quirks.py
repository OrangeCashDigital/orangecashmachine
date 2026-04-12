"""
market_data/adapters/exchange/exchange_quirks.py
================================================

Capacidades y comportamientos especiales por exchange.

En lugar de dispersar bloques `if exchange_id == "kucoin"` por el código,
este módulo centraliza las diferencias de API en un dict de reglas.
Agregar soporte a un nuevo exchange = añadir una entrada aquí.
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class ExchangeQuirks:
    """
    Capacidades específicas de un exchange que afectan la paginación y
    la construcción de parámetros de la llamada REST.

    backward_pagination : bool
        True si el exchange devuelve velas ANTERIORES a `endAt` en lugar
        de responder a `since`. La paginación debe ir hacia atrás
        decrementando end_ms en cada chunk.

    requires_end_at : bool
        True si el exchange requiere el parámetro `endAt` (en segundos)
        para acotar el rango temporal. KuCoin/KuCoinFutures lo exigen;
        otros exchanges lo ignoran o no lo soportan.

    reject_zero_since : bool
        True si el exchange rechaza since=0 como valor inválido.
        Cuando True, since=0 debe ser convertido a None antes de la llamada.
    """
    backward_pagination: bool = False
    requires_end_at:     bool = False
    reject_zero_since:   bool = False


# Reglas por exchange_id. Los exchanges no listados usan los defaults (todo False).
EXCHANGE_QUIRKS: dict[str, ExchangeQuirks] = {
    "kucoin": ExchangeQuirks(
        backward_pagination = True,
        requires_end_at     = True,
        reject_zero_since   = True,
    ),
    "kucoinfutures": ExchangeQuirks(
        backward_pagination = True,
        requires_end_at     = True,
        reject_zero_since   = True,
    ),
}

_DEFAULT_QUIRKS = ExchangeQuirks()


def get_quirks(exchange_id: str) -> ExchangeQuirks:
    """Devuelve las quirks del exchange, o los defaults si no está registrado."""
    return EXCHANGE_QUIRKS.get(exchange_id, _DEFAULT_QUIRKS)
