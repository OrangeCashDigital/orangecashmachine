import dataclasses
from decimal import Decimal

import pytest

from shared.types.values import Money, Price, Quantity


def test_price_is_frozen_and_decimal_only():
    p = Price(Decimal("100.50"), Decimal("0.01"))
    with pytest.raises(dataclasses.FrozenInstanceError):
        p.value = Decimal("200.00")  # type: ignore[misc]
    with pytest.raises(TypeError):
        Price(100.50, Decimal("0.01"))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Price(Decimal("100.50"), 0.01)  # type: ignore[arg-type]


def test_price_validates_tick_and_value():
    with pytest.raises(ValueError):
        Price(Decimal("-1"), Decimal("0.01"))
    with pytest.raises(ValueError):
        Price(Decimal("1"), Decimal("0"))
    with pytest.raises(ValueError):
        Price(Decimal("1.005"), Decimal("0.01"))


def test_quantity_non_negative_decimal_and_step():
    q = Quantity(Decimal("0"), Decimal("0.001"))
    assert q.value == Decimal("0")
    with pytest.raises(ValueError):
        Quantity(Decimal("-1"), Decimal("0.001"))
    with pytest.raises(ValueError):
        Quantity(Decimal("1.0005"), Decimal("0.001"))
    with pytest.raises(TypeError):
        Quantity(1.0, Decimal("0.001"))  # type: ignore[arg-type]


def test_money_currency_and_arithmetic():
    a = Money(Decimal("10.5"), "usdt")
    b = Money(Decimal("2.5"), "USDT")
    assert a.currency == "USDT"
    assert (a + b).amount == Decimal("13.0")
    assert (a - b).amount == Decimal("8.0")
    with pytest.raises(ValueError):
        Money(Decimal("1"), "")
    with pytest.raises(ValueError):
        a + Money(Decimal("1"), "BTC")
    with pytest.raises(TypeError):
        Money(10.5, "USDT")  # type: ignore[arg-type]


def test_value_objects_hashable_and_equal():
    p1 = Price(Decimal("100"), Decimal("0.01"))
    p2 = Price(Decimal("100"), Decimal("0.01"))
    assert p1 == p2
    assert hash(p1) == hash(p2)
    {p1: "ok"}
