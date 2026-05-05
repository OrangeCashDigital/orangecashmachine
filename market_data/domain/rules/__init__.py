# -*- coding: utf-8 -*-
"""
market_data/domain/rules/__init__.py
======================================

Reglas de negocio del bounded context market_data.

Reglas presentes
----------------
CandleValidator     — clasifica velas como CLEAN/SUSPECT/CORRUPT (C0–C6, S1–S3)
CandleNormalizer    — transforma ValidationResult[] → DataFrame Silver tipado
InvariantResult     — resultado de verificación de invariantes de dataset
check_dataset_invariants — verifica consistencia de un dataset Silver completo

Sobre la distinción Rules vs Services (DDD)
-------------------------------------------
- Rules:    lógica de dominio pura, sin I/O, sin estado externo.
            CandleValidator es una regla: dada una vela, determina su calidad.
            check_dataset_invariants es una regla: dado un manifest, valida consistencia.
- Services: orquestan múltiples reglas y/o entidades con posible I/O.
            Ver domain/services/.

Principios
----------
SRP    — cada regla verifica exactamente una cosa
SSOT   — re-exports desde owners; sin duplicar lógica
Fail-Fast  — reglas CORRUPT (C0–C6) retornan al primer fallo
Fail-Soft  — reglas SUSPECT (S1–S3) acumulan sin abortar
"""
from __future__ import annotations

from market_data.processing.validation.candle_validator import (  # noqa: F401
    CandleValidator,
)
from market_data.processing.validation.candle_normalizer import (  # noqa: F401
    CandleNormalizer,
    SILVER_DTYPE_MAP,
)
from market_data.quality.invariants.invariants import (  # noqa: F401
    InvariantResult,
    check_dataset_invariants,
)

__all__ = [
    "CandleValidator",
    "CandleNormalizer",
    "SILVER_DTYPE_MAP",
    "InvariantResult",
    "check_dataset_invariants",
]
