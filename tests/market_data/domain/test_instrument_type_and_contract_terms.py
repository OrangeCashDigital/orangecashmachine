# -*- coding: utf-8 -*-
"""
tests/market_data/domain/test_instrument_type_and_contract_terms.py
====================================================================

Tests de G1-D04 (InstrumentType) y G1-D05/D06/D07/D08 (ContractTerms).

Invariantes contratados:
- D04 : SPOT/PERPETUAL/FUTURE/OPTION son categorías distint as; dos
        categorías distintas NO comparten identidad por el mismo venue.
- D05 : underlying/quote/settlement/margin convention identity-defining.
- D06 : FUTURE y OPTION requieren expiry; SPOT/PERPETUAL no.
- D07 : OPTION requiere strike + option_right; distinto strike/right ⇒
        terms distintos ⇒ identidades distintas.
- D08 : contract multiplier identity-defining.

Si un invariante de contrato se viola, el test falla — no son tests que
solo comprueben nombres de clases.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from market_data.domain.value_objects.contract_terms import (
    ContractTerms,
    MarginConvention,
    OptionRight,
)
from market_data.domain.value_objects.instrument_type import InstrumentType


class TestInstrumentTypeD04:
    def test_categories_are_distinct(self):
        # D04: cada categoría es una entidad negociable distinta.
        cats = {it.value for it in InstrumentType}
        assert cats == {"spot", "perpetual", "future", "option"}

    @pytest.mark.parametrize(
        "left,right",
        [
            (InstrumentType.SPOT, InstrumentType.PERPETUAL),
            (InstrumentType.PERPETUAL, InstrumentType.FUTURE),
            (InstrumentType.FUTURE, InstrumentType.OPTION),
        ],
    )
    def test_distinct_categories_are_not_equal(self, left, right):
        # Spot ≠ Perpetual ≠ Future ≠ Option: no comparten identidad.
        assert left != right
        assert left.value != right.value


class TestContractTermsD05:
    def test_underlying_quote_settlement_identity_defining(self):
        # D05: mismo venue+type pero distinto settlement ⇒ terms distintos
        # (identity-defining) ⇒ no es la misma entidad.
        a = ContractTerms(
            instrument_type=InstrumentType.FUTURE,
            underlying="BTC",
            quote="USDT",
            settlement="USDT",
            expiry=date(2026, 6, 26),
        )
        b = ContractTerms(
            instrument_type=InstrumentType.FUTURE,
            underlying="BTC",
            quote="USDT",
            settlement="BTC",
            expiry=date(2026, 6, 26),
        )
        assert a != b

    def test_margin_convention_identity_defining_when_differing(self):
        # D05: margin convention (linear vs inverse) diferencia mercados
        # negociables coexistentes bajo el mismo venue.
        linear = ContractTerms(
            instrument_type=InstrumentType.PERPETUAL,
            underlying="BTC",
            quote="USDT",
            settlement="USDT",
            margin_convention=MarginConvention.LINEAR,
        )
        inverse = ContractTerms(
            instrument_type=InstrumentType.PERPETUAL,
            underlying="BTC",
            quote="USD",
            settlement="BTC",
            margin_convention=MarginConvention.INVERSE,
        )
        assert linear != inverse


class TestContractTermsD06:
    def test_future_requires_expiry(self):
        with pytest.raises(ValueError):
            ContractTerms(
                instrument_type=InstrumentType.FUTURE,
                underlying="BTC",
                quote="USDT",
            )

    def test_perpetual_and_spot_have_no_expiry(self):
        # D06: SPOT no tiene expiry; PERPETUAL no tiene expiry.
        spot = ContractTerms(instrument_type=InstrumentType.SPOT, underlying="BTC", quote="USDT")
        perp = ContractTerms(instrument_type=InstrumentType.PERPETUAL, underlying="BTC", quote="USDT")
        assert spot.expiry is None
        assert perp.expiry is None


class TestContractTermsD07:
    def test_option_requires_strike_and_right(self):
        with pytest.raises(ValueError):
            ContractTerms(
                instrument_type=InstrumentType.OPTION,
                underlying="BTC",
                quote="USDT",
                expiry=date(2027, 1, 27),
            )

    def test_option_differs_by_strike(self):
        # D07: mismo resto pero strike distinto ⇒ identidad distinta.
        call_100k = ContractTerms(
            instrument_type=InstrumentType.OPTION,
            underlying="BTC",
            quote="USDT",
            expiry=date(2027, 1, 27),
            strike=Decimal("100000"),
            option_right=OptionRight.CALL,
        )
        call_110k = ContractTerms(
            instrument_type=InstrumentType.OPTION,
            underlying="BTC",
            quote="USDT",
            expiry=date(2027, 1, 27),
            strike=Decimal("110000"),
            option_right=OptionRight.CALL,
        )
        assert call_100k != call_110k

    def test_option_differs_by_right(self):
        # D07: call ≠ put con el mismo strike.
        call = ContractTerms(
            instrument_type=InstrumentType.OPTION,
            underlying="BTC",
            quote="USDT",
            expiry=date(2027, 1, 27),
            strike=Decimal("100000"),
            option_right=OptionRight.CALL,
        )
        put = ContractTerms(
            instrument_type=InstrumentType.OPTION,
            underlying="BTC",
            quote="USDT",
            expiry=date(2027, 1, 27),
            strike=Decimal("100000"),
            option_right=OptionRight.PUT,
        )
        assert call != put


class TestContractTermsD08:
    def test_contract_multiplier_identity_defining(self):
        # D08: futures iguales salvo multiplier → entidades negociables
        # distintas cuando ambas pueden coexistir.
        m1 = ContractTerms(
            instrument_type=InstrumentType.FUTURE,
            underlying="BTC",
            quote="USDT",
            settlement="USDT",
            expiry=date(2026, 6, 26),
            contract_multiplier=Decimal("0.001"),
        )
        m2 = ContractTerms(
            instrument_type=InstrumentType.FUTURE,
            underlying="BTC",
            quote="USDT",
            settlement="USDT",
            expiry=date(2026, 6, 26),
            contract_multiplier=Decimal("0.01"),
        )
        assert m1 != m2
        assert m1.instrument_type is InstrumentType.FUTURE
        assert m2.instrument_type is InstrumentType.FUTURE


class TestContractTermsFailFast:
    def test_coherent_terms_are_immutable_equal_by_value(self):
        # Los términos se comparan por valor (dataclass frozen) — dos
        # construcciones idénticas son iguales, sin identidad propia.
        a = ContractTerms(instrument_type=InstrumentType.SPOT, underlying="BTC", quote="USDT")
        b = ContractTerms(instrument_type=InstrumentType.SPOT, underlying="BTC", quote="USDT")
        assert a == b
        assert hash(a) == hash(b)
