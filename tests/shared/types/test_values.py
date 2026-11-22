"""Tests golden de los value objects del Shared Kernel (P1)."""

import dataclasses
from decimal import Decimal

import pytest

from shared.types.values import Money, Price, Quantity


def test_float_is_rejected():
    with pytest.raises(TypeError):
        Price(1.5)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Price("1", tick_size=0.01)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Quantity(1.0)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Quantity("1", step_size=0.001)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Money(1.0, "USD")  # type: ignore[arg-type]


def test_accepts_decimal_int_and_str_coercing_to_decimal():
    assert isinstance(Price(1).value, Decimal)
    assert isinstance(Price("1.5").value, Decimal)
    assert isinstance(Price(Decimal("2")).value, Decimal)
    assert isinstance(Quantity(0).value, Decimal)
    assert isinstance(Quantity("2").value, Decimal)
    assert isinstance(Money("-1", "usd").amount, Decimal)
    assert Price(1).value == Decimal("1")
    assert Price("1.5").value == Decimal("1.5")


def test_value_objects_are_immutable():
    price = Price("1.23")
    quantity = Quantity("1")
    money = Money("1", "USD")
    with pytest.raises(dataclasses.FrozenInstanceError):
        price.value = Decimal("2")  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        quantity.value = Decimal("2")  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        money.amount = Decimal("2")  # type: ignore[misc]


def test_equality_and_hashing():
    assert Price("1.0") == Price(1)
    assert Price(Decimal("1.0"), tick_size="0.1") == Price("1.0", tick_size=Decimal("0.1"))
    assert Quantity("0") == Quantity(0)
    assert Money("1", "usd") == Money(Decimal("1"), "USD")
    assert hash(Price("100")) == hash(Price(Decimal("100")))
    {Price("100"): "ok"}


def test_decimal_precision_golden():
    assert Decimal("0.1") + Decimal("0.2") == Decimal("0.3")
    assert Money("0.1", "USD") + Money("0.2", "USD") == Money("0.3", "USD")


def test_price_must_be_positive_and_tick_aligned():
    with pytest.raises(ValueError):
        Price(0)
    with pytest.raises(ValueError):
        Price("-1")
    assert Price("1.00", tick_size="0.01").value == Decimal("1.00")
    with pytest.raises(ValueError):
        Price("1.005", tick_size="0.01")
    with pytest.raises(ValueError):
        Price("1", tick_size="0")


def test_quantity_non_negative_and_step_aligned():
    assert Quantity(0).value == Decimal("0")
    with pytest.raises(ValueError):
        Quantity("-0.1")
    assert Quantity("1.000", step_size="0.001").value == Decimal("1.000")
    with pytest.raises(ValueError):
        Quantity("1.0005", step_size="0.001")
    with pytest.raises(ValueError):
        Quantity("1", step_size="0")


def test_money_may_be_negative_and_requires_same_currency_for_arithmetic():
    assert Money("-10.5", "usd").amount == Decimal("-10.5")
    assert Money("-10.5", "usd").currency == "USD"
    assert (Money("1", "USD") + Money("2", "USD")).amount == Decimal("3")
    assert (Money("3", "USD") - Money("1", "USD")).amount == Decimal("2")
    with pytest.raises(ValueError):
        Money("1", "")
    with pytest.raises(ValueError):
        Money("1", "   ")
    with pytest.raises(ValueError):
        Money("1", "USD") + Money("1", "EUR")
    with pytest.raises(ValueError):
        Money("1", "USD") - Money("1", "EUR")
