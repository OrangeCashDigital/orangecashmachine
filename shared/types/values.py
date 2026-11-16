from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


def _require_decimal(name: str, value: Any) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal, got {type(value).__name__}")
    return value


@dataclass(frozen=True, slots=True)
class Price:
    """Precio monetario inmutable con precisión explícita (tick_size)."""

    value: Decimal
    tick_size: Decimal

    def __post_init__(self) -> None:
        value = _require_decimal("value", self.value)
        tick_size = _require_decimal("tick_size", self.tick_size)
        if value < 0:
            raise ValueError("value must be >= 0")
        if tick_size <= 0:
            raise ValueError("tick_size must be > 0")
        if value % tick_size != 0:
            raise ValueError("value must align to tick_size")

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class Quantity:
    """Cantidad económica inmutable y no negativa con precisión explícita (step_size)."""

    value: Decimal
    step_size: Decimal

    def __post_init__(self) -> None:
        value = _require_decimal("value", self.value)
        step_size = _require_decimal("step_size", self.step_size)
        if value < 0:
            raise ValueError("value must be >= 0")
        if step_size <= 0:
            raise ValueError("step_size must be > 0")
        if value % step_size != 0:
            raise ValueError("value must align to step_size")


@dataclass(frozen=True, slots=True)
class Money:
    """Monto monetario inmutable en una divisa."""

    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        amount = _require_decimal("amount", self.amount)
        currency = self.currency.strip().upper()
        if not currency:
            raise ValueError("currency must be non-empty")
        # frozen dataclass: use object.__setattr__ for normalization
        object.__setattr__(self, "currency", currency)

    def __add__(self, other: object) -> "Money":
        if not isinstance(other, Money):
            return NotImplemented
        if self.currency != other.currency:
            raise ValueError("currency mismatch")
        return Money(self.amount + other.amount, self.currency)

    def __sub__(self, other: object) -> "Money":
        if not isinstance(other, Money):
            return NotImplemented
        if self.currency != other.currency:
            raise ValueError("currency mismatch")
        return Money(self.amount - other.amount, self.currency)


__all__ = ["Price", "Quantity", "Money"]
