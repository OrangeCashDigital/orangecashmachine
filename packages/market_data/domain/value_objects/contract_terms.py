# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/contract_terms.py
====================================================

ContractTerms — términos identity-defining de una Listing Identity.

Representa la parte DESCRIPTIVA de la identidad: los términos que, según
G1-D05..D08 (ADR-0049), definen si estamos ante una entidad negociable
distinta. Los términos se comparan por valor para discriminar identidades.

Qué términos y para qué categoría
----------------------------------
- underlying / quote         : identifica el activo subyacente y su moneda
                               (G1-D05; aplicable a no-spot).
- settlement                 : moneda de settlement contractual (G1-D05).
- margin_convention          : linear/inverse/coin-margined cuando diferencia
                               mercados negociables coexist (G1-D05).
- expiry                     : vencimiento — FUTURE y OPTION (G1-D06).
- strike / option_right      : solo OPTION (G1-D07).
- contract_multiplier        : FUTURE y OPTION cuando diferencia (G1-D08).

Categorías de contrato
----------------------
- SPOT      : sin expiry/strike/multiplier.
- PERPETUAL : sin expiry/strike/multiplier (G1-D06: perpetuo no tiene expiry).
- FUTURE    : expiry (D06); multiplier (D08).
- OPTION    : expiry (D06); strike + right (D07); multiplier (D08).

Notas de representación (D19 BLOCKED)
-------------------------------------
strike, multiplier y expiry se conservan aquí en representación neutra
(Decimal/date, comparación por valor) SIN decidir encoding/serialización/
formato físico — eso pertenece a D19, que permanece bloqueado. Este
módulo NO define wire format, UUID, hash canónico ni serialización.

Principios
----------
DDD   — VO puro: frozen dataclass, definido por valor
SSOT  — única definición de los términos identity-defining en el dominio
FAIL-CLOSED — categoría/término incoherente → ValueError al construir
KISS  — comparación estructural por dataclass equality
D19   — sin decisión de representación física
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum

from market_data.domain.value_objects.instrument_type import InstrumentType


class MarginConvention(str, Enum):
    """
    Convención de margen/settlement de un contrato derivado (G1-D05).

    LINEAR / INVERSE / COIN_MARGINED son las convenciones declaradas. La
    distinción es identity-defining cuando diferencia mercados negociables
    coexist (por ejemplo, BTCUSDT linear vs BTCUSD inverse en la misma venue).
    """

    LINEAR = "linear"
    INVERSE = "inverse"
    COIN_MARGINED = "coin_margined"


class OptionRight(str, Enum):
    """
    Derecho de opción (G1-D07) — solo OPTION.

    CALL o PUT. Una combinación distinta de strike/right representa una
    Listing Identity distinta para la categoría OPTION.
    """

    CALL = "call"
    PUT = "put"


# ===========================================================================
# ContractTerms
# ===========================================================================


@dataclass(frozen=True)
class ContractTerms:
    """
    Términos identity-defining de una Listing Identity (G1-D05..D08).

    Los términos opcionales (Optional) solo aplican a ciertas categorías.
    La coherencia categoría/término se valida en __post_init__ (Fail-Fast);
    una combinación incoherente nunca se construye silenciosamente.
    """

    instrument_type: InstrumentType
    underlying: str = ""
    quote: str = ""
    settlement: str = ""
    margin_convention: MarginConvention | None = None
    expiry: date | None = None
    strike: Decimal | None = None
    option_right: OptionRight | None = None
    contract_multiplier: Decimal | None = None

    def __post_init__(self) -> None:
        if self.instrument_type is InstrumentType.OPTION:
            self._validate_option()
        elif self.instrument_type is InstrumentType.FUTURE:
            self._validate_future()
        else:
            self._validate_spot_or_perpetual()

    def _validate_option(self) -> None:
        if self.strike is None or self.option_right is None or self.expiry is None:
            raise ValueError(
                "OPTION requires expiry, strike and option_right (G1-D06/D07) — "
                f"got expiry={self.expiry!r}, strike={self.strike!r}, right={self.option_right!r}"
            )

    def _validate_future(self) -> None:
        if self.expiry is None:
            raise ValueError("FUTURE requires expiry (G1-D06) — got expiry=None")

    def _validate_spot_or_perpetual(self) -> None:
        if (
            self.expiry is not None
            or self.strike is not None
            or self.option_right is not None
            or self.contract_multiplier is not None
        ):
            raise ValueError(
                f"{self.instrument_type.value} must not carry expiry/strike/right/multiplier "
                "(G1-D06/D07/D08) — SPOT y PERPETUAL no tienen vencimiento"
            )


__all__ = ["ContractTerms", "MarginConvention", "OptionRight"]
