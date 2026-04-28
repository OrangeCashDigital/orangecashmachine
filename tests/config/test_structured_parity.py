"""
tests/config/test_structured_parity.py
========================================
Verifica que Hydra Structured Config y Pydantic schema
tengan exactamente los mismos campos.

REGLA: este test FALLA en CI si alguien añade un campo
a una capa sin actualizar la otra.
CI gate — no skipear sin actualizar ambas capas.
"""

from __future__ import annotations

import dataclasses

import pytest

from core.config.structured.observability import (
    LoggingConfig as HydraLoggingConfig,
    MetricsConfig as HydraMetricsConfig,
)
from core.observability.config import LoggingConfig as PydanticLoggingConfig
from core.config.schema import MetricsConfig as PydanticMetricsConfig


def _hydra_fields(cls) -> set[str]:
    return {f.name for f in dataclasses.fields(cls)}


def _pydantic_fields(cls) -> set[str]:
    return set(cls.model_fields.keys())


@pytest.mark.parametrize("hydra_cls,pydantic_cls,label", [
    (HydraLoggingConfig, PydanticLoggingConfig, "LoggingConfig"),
    (HydraMetricsConfig, PydanticMetricsConfig, "MetricsConfig"),
])
def test_hydra_pydantic_field_parity(hydra_cls, pydantic_cls, label):
    """Ambas capas deben tener exactamente los mismos campos (SSOT)."""
    hydra = _hydra_fields(hydra_cls)
    pydantic = _pydantic_fields(pydantic_cls)

    only_hydra = hydra - pydantic
    only_pydantic = pydantic - hydra

    assert not only_hydra, (
        f"{label}: campos fantasma en Hydra (sin Pydantic): {only_hydra}\n"
        "→ Añádelos a la clase Pydantic correspondiente."
    )
    assert not only_pydantic, (
        f"{label}: campos sin cobertura Hydra: {only_pydantic}\n"
        "→ Añádelos a core/config/structured/observability.py."
    )
