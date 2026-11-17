"""Value objects monetarios y de cantidad del Shared Kernel.

Reglas (ver docs/plans/shared-kernel.md, P1):

- ``frozen=True`` (inmutables) y ``slots=True``.
- Nunca ``float`` para dinero/precio/cantidad: se rechaza con ``TypeError``.
- Se aceptan ``Decimal``, ``int`` y ``str``; se normalizan a ``Decimal``.
- ``Price.value > 0``; ``Quantity.value >= 0``; ``Money.amount`` puede ser negativo.
- ``tick_size`` / ``step_size`` son opcionales; si se aportan, el valor debe ser
  un múltiplo exacto.
- ``Money`` normaliza ``currency`` a mayúsculas y exige misma divisa para ``+``/``-``.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


def _coerce_decimal(name: str, value: Any) -> Decimal:
    """Convierte a ``Decimal`` rechazando ``float`` (pérdida de precisión)."""
    if isinstance(value, float):
        raise TypeError(f"{name} must not be float; use Decimal, int, or str")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, str):
        try:
            return Decimal(value)
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"{name} is not a valid decimal string: {value!r}") from exc
    raise TypeError(f"{name} must be Decimal, int, or str; got {type(value).__name__}")


@dataclass(frozen=True, slots=True)
class Price:
    """Precio monetario inmutable y estrictamente positivo."""

    value: Decimal
    tick_size: Decimal | None = None

    def __post_init__(self) -> None:
        value = _coerce_decimal("value", self.value)
        tick_size = _coerce_decimal("tick_size", self.tick_size) if self.tick_size is not None else None
        if value <= 0:
            raise ValueError("Price.value must be > 0")
        if tick_size is not None:
            if tick_size <= 0:
                raise ValueError("tick_size must be > 0")
            if value % tick_size != 0:
                raise ValueError("Price.value must be an exact multiple of tick_size")
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "tick_size", tick_size)

    def __str__(self) -> str:
        return str(self.value)


@dataclass(frozen=True, slots=True)
class Quantity:
    """Cantidad económica inmutable y no negativa."""

    value: Decimal
    step_size: Decimal | None = None

    def __post_init__(self) -> None:
        value = _coerce_decimal("value", self.value)
        step_size = _coerce_decimal("step_size", self.step_size) if self.step_size is not None else None
        if value < 0:
            raise ValueError("Quantity.value must be >= 0")
        if step_size is not None:
            if step_size <= 0:
                raise ValueError("step_size must be > 0")
            if value % step_size != 0:
                raise ValueError("Quantity.value must be an exact multiple of step_size")
        object.__setattr__(self, "value", value)
        object.__setattr__(self, "step_size", step_size)


@dataclass(frozen=True, slots=True)
class Money:
    """Monto monetario inmutable en una divisa concreta (puede ser negativo)."""

    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        amount = _coerce_decimal("amount", self.amount)
        if not isinstance(self.currency, str) or not self.currency.strip():
            raise ValueError("currency must be a non-empty string")
        currency = self.currency.strip().upper()
        object.__setattr__(self, "amount", amount)
        object.__setattr__(self, "currency", currency)

    def __add__(self, other: object) -> Money:
        if not isinstance(other, Money):
            return NotImplemented
        if self.currency != other.currency:
            raise ValueError(f"currency mismatch: {self.currency} != {other.currency}")
        return Money(self.amount + other.amount, self.currency)

    def __sub__(self, other: object) -> Money:
        if not isinstance(other, Money):
            return NotImplemented
        if self.currency != other.currency:
            raise ValueError(f"currency mismatch: {self.currency} != {other.currency}")
        return Money(self.amount - other.amount, self.currency)


__all__ = ["Price", "Quantity", "Money"]
