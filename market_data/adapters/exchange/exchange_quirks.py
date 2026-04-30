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

    origin_fallback_date : str
        Fecha ISO 8601 (YYYY-MM-DD) usada como origen cuando el exchange
        no soporta since=1 y devuelve el candle más reciente en su lugar.
        SSOT para _discover_origin() en backfill.py.
        El default conservador (2017-01-01) cubre todos los exchanges no
        listados explícitamente.
    """
    backward_pagination: bool = False
    requires_end_at:     bool = False
    reject_zero_since:   bool = False
    origin_fallback_date: str = "2017-01-01"


# Reglas por exchange_id. Los exchanges no listados usan los defaults (todo False).
EXCHANGE_QUIRKS: dict[str, ExchangeQuirks] = {
    "kucoin": ExchangeQuirks(
        backward_pagination  = True,
        requires_end_at      = True,
        reject_zero_since    = True,
        origin_fallback_date = "2018-01-01",  # KuCoin spot lanzó en 2018
    ),
    "kucoinfutures": ExchangeQuirks(
        backward_pagination  = True,
        requires_end_at      = True,
        reject_zero_since    = True,
        origin_fallback_date = "2020-01-01",  # KuCoin Futures lanzó en 2020
    ),
    "bybit": ExchangeQuirks(
        origin_fallback_date = "2021-04-01",  # Bybit spot lanzó en Abril 2021
    ),
    "bybit_futures": ExchangeQuirks(
        origin_fallback_date = "2019-10-01",  # Bybit USDT perpetuals: Oct 2019
    ),
}

_DEFAULT_QUIRKS = ExchangeQuirks()


def get_quirks(exchange_id: str) -> ExchangeQuirks:
    """Devuelve las quirks del exchange, o los defaults si no está registrado."""
    return EXCHANGE_QUIRKS.get(exchange_id, _DEFAULT_QUIRKS)


def get_origin_fallback_ms(exchange_id: str, market_type: str = "spot") -> int:
    """
    Timestamp mínimo de historia disponible para el exchange (epoch ms).

    Usado en _discover_origin() cuando since=1 no está soportado y el
    exchange devuelve un candle near-now en lugar del más antiguo.

    La clave de lookup es `exchange_id` para spot y `exchange_id + "_futures"`
    para cualquier market_type no-spot (futures, swap, perpetual).
    Cae de forma conservadora a 2017-01-01 para exchanges no registrados.

    Parameters
    ----------
    exchange_id : str
        ID del exchange (e.g. "bybit", "kucoin").
    market_type : str
        Tipo de mercado del pipeline (e.g. "spot", "futures", "swap").
        Default "spot" — compatible con todas las llamadas existentes.
    """
    import pandas as _pd
    _is_futures = market_type.lower() not in ("spot", "")
    lookup_key  = f"{exchange_id}_futures" if _is_futures else exchange_id
    # Fallback a exchange_id base si la clave compuesta no existe
    quirks    = EXCHANGE_QUIRKS.get(lookup_key) or get_quirks(exchange_id)
    date_str  = quirks.origin_fallback_date
    return int(_pd.Timestamp(date_str, tz="UTC").value // 1_000_000)
