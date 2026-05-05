# -*- coding: utf-8 -*-
"""
market_data/processing/validation/candle_validator.py
======================================================

Data Quality Layer — validación estructural y semántica de velas OHLCV.

Responsabilidad
---------------
Única: clasificar cada vela como CLEAN, SUSPECT o CORRUPT.
No escribe, no transforma, no hace I/O — SRP estricto.

Contratos de clasificación
--------------------------
  CLEAN   → vela válida, pasa a Silver sin flag
  SUSPECT → anomalía detectada, se escribe con quality_flag='suspect'
  CORRUPT → inválida, no se escribe en Silver, va a quarantine log

Reglas CORRUPT (fail-fast, C0–C6) — retornan al primer fallo:
  C0  tupla con menos de 6 campos
  C1  timestamp no es entero positivo
  C2  cualquier campo OHLCV es None o NaN
  C3  high < low
  C4  close fuera de [low, high]
  C5  open fuera de [low, high]
  C6  volume < 0

Reglas SUSPECT (fail-soft, S1–S3) — acumulativas, no abortan:
  S1  volume == 0
  S2  high == low (mercado congelado)
  S3  delta temporal != timeframe esperado (gap detectado)

Dependencias de dominio
-----------------------
RawCandle y QualityLabel se importan desde domain/value_objects (DIP).
Este módulo depende del dominio, nunca al revés.

Principios
----------
SOLID/SRP  — solo clasifica, no actúa sobre la clasificación
SSOT       — ValidationResult es el único contrato de salida
Fail-Fast  — C0–C6 retornan inmediatamente en el primer fallo
Fail-Soft  — S1–S3 acumulan, no abortan
DIP        — RawCandle y QualityLabel importados desde domain/value_objects
KISS       — sin dependencias externas salvo timeframe_to_ms y domain VOs
DRY        — reglas encapsuladas en métodos privados

Ref: CCXT OHLCV schema — https://docs.ccxt.com/#/?id=ohlcv-structure
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

# Tipos de dominio — importar desde domain, nunca definir aquí (DIP · SSOT)
from market_data.domain.value_objects import QualityLabel, RawCandle
from market_data.processing.utils.timeframe import timeframe_to_ms


# ===========================================================================
# Resultado de validación
# ===========================================================================

@dataclass(frozen=True)
class ValidationResult:
    """
    Resultado de validar una sola vela OHLCV.

    Immutable (frozen=True) — garantiza trazabilidad sin mutación posterior.

    Attributes
    ----------
    label      : clasificación final (CLEAN / SUSPECT / CORRUPT)
    candle     : vela original — siempre presente para trazabilidad
    violations : códigos de regla violada, e.g. ["C3", "S1"]
    reason     : descripción del primer fallo determinante (None si CLEAN)
    """
    label:      QualityLabel
    candle:     RawCandle
    violations: List[str]     = field(default_factory=list)
    reason:     Optional[str] = None

    @property
    def is_clean(self)   -> bool: return self.label == QualityLabel.CLEAN
    @property
    def is_suspect(self) -> bool: return self.label == QualityLabel.SUSPECT
    @property
    def is_corrupt(self) -> bool: return self.label == QualityLabel.CORRUPT


# ===========================================================================
# Resumen de validación de lote
# ===========================================================================

@dataclass
class ValidationSummary:
    """
    Agrega resultados de validate_batch() para logging y métricas.

    Usage
    -----
    results = validator.validate_batch(candles)
    summary = ValidationSummary.from_results(results)
    logger.info(
        "quality | clean=%s suspect=%s corrupt=%s ratio=%.1f%%",
        summary.clean, summary.suspect, summary.corrupt,
        summary.quality_ratio * 100,
    )
    """
    total:           int
    clean:           int
    suspect:         int
    corrupt:         int
    corrupt_results: List[ValidationResult] = field(default_factory=list)
    suspect_results: List[ValidationResult] = field(default_factory=list)

    @classmethod
    def from_results(cls, results: List[ValidationResult]) -> "ValidationSummary":
        clean_r   = [r for r in results if r.is_clean]
        suspect_r = [r for r in results if r.is_suspect]
        corrupt_r = [r for r in results if r.is_corrupt]
        return cls(
            total           = len(results),
            clean           = len(clean_r),
            suspect         = len(suspect_r),
            corrupt         = len(corrupt_r),
            corrupt_results = corrupt_r,
            suspect_results = suspect_r,
        )

    @property
    def quality_ratio(self) -> float:
        """Fracción CLEAN/total. 1.0 = sin anomalías."""
        return self.clean / self.total if self.total > 0 else 1.0

    @property
    def has_critical_corruption(self) -> bool:
        """True si >10% de velas son CORRUPT."""
        return (self.corrupt / self.total) > 0.10 if self.total > 0 else False


# ===========================================================================
# Validador
# ===========================================================================

class CandleValidator:
    """
    Valida velas OHLCV individuales contra las reglas C0–C6 y S1–S3.

    Stateless — segura para uso concurrente sin locks.
    prev_timestamp_ms se pasa por el caller, no se almacena en la instancia.

    Usage
    -----
    validator = CandleValidator(timeframe="1m")
    result    = validator.validate(candle, prev_timestamp_ms=prev_ts)
    results   = validator.validate_batch(candles)
    summary   = ValidationSummary.from_results(results)
    """

    def __init__(self, timeframe: str) -> None:
        self._timeframe    = timeframe
        self._timeframe_ms = timeframe_to_ms(timeframe)

    # ── API pública ───────────────────────────────────────────────────────────

    def validate(
        self,
        candle:            RawCandle,
        prev_timestamp_ms: Optional[int] = None,
    ) -> ValidationResult:
        """
        Valida una vela individual.

        Orden de evaluación: C0–C6 (fail-fast) → S1–S3 (fail-soft acumulativo).
        """
        corrupt = self._check_corrupt(candle)
        if corrupt is not None:
            code, reason = corrupt
            return ValidationResult(
                label      = QualityLabel.CORRUPT,
                candle     = candle,
                violations = [code],
                reason     = reason,
            )

        suspect = self._check_suspect(candle, prev_timestamp_ms)
        if suspect:
            return ValidationResult(
                label      = QualityLabel.SUSPECT,
                candle     = candle,
                violations = suspect,
                reason     = f"Suspect flags: {', '.join(suspect)}",
            )

        return ValidationResult(label=QualityLabel.CLEAN, candle=candle)

    def validate_batch(
        self,
        candles: List[RawCandle],
    ) -> List[ValidationResult]:
        """
        Valida una lista ordenada de velas.

        Pasa prev_timestamp_ms automáticamente entre velas consecutivas.
        Las velas CORRUPT no actualizan prev_ts — evita propagación de error.
        """
        results: List[ValidationResult] = []
        prev_ts: Optional[int] = None
        for candle in candles:
            result = self.validate(candle, prev_timestamp_ms=prev_ts)
            results.append(result)
            if not result.is_corrupt:
                ts = candle[0]
                if isinstance(ts, (int, float)) and ts > 0:
                    prev_ts = int(ts)
        return results

    # ── Reglas CORRUPT (fail-fast) ────────────────────────────────────────────

    def _check_corrupt(
        self,
        candle: RawCandle,
    ) -> Optional[Tuple[str, str]]:
        """
        Evalúa reglas C0–C6 en orden.

        Retorna (código, razón) del primer fallo, o None si pasa todo.
        """
        if len(candle) < 6:
            return ("C0", f"Malformed candle: expected 6 fields, got {len(candle)}")

        ts, o, h, lo, c, v = candle[0], candle[1], candle[2], candle[3], candle[4], candle[5]

        if not isinstance(ts, (int, float)) or ts <= 0:
            return ("C1", f"Invalid timestamp: {ts!r}")

        for name, val in (("open", o), ("high", h), ("low", lo), ("close", c), ("volume", v)):
            if val is None or (isinstance(val, float) and math.isnan(val)):
                return ("C2", f"NaN/None in '{name}': {val!r}")

        if h < lo:
            return ("C3", f"high ({h}) < low ({lo})")

        if not (lo <= c <= h):
            return ("C4", f"close ({c}) outside [low={lo}, high={h}]")

        if not (lo <= o <= h):
            return ("C5", f"open ({o}) outside [low={lo}, high={h}]")

        if v < 0:
            return ("C6", f"Negative volume: {v}")

        return None

    # ── Reglas SUSPECT (fail-soft) ────────────────────────────────────────────

    def _check_suspect(
        self,
        candle:            RawCandle,
        prev_timestamp_ms: Optional[int],
    ) -> List[str]:
        """
        Evalúa reglas S1–S3 acumulativamente.

        Retorna lista de códigos violados (vacía si ninguno).
        """
        violations: List[str] = []
        ts, _o, h, lo, _c, v = candle[0], candle[1], candle[2], candle[3], candle[4], candle[5]

        if v == 0:
            violations.append("S1")

        if h == lo:
            violations.append("S2")

        if prev_timestamp_ms is not None:
            delta = int(ts) - int(prev_timestamp_ms)
            if abs(delta - self._timeframe_ms) > 1:
                violations.append("S3")

        return violations


__all__ = [
    # Tipos re-exportados desde domain/value_objects (backward-compat)
    "RawCandle",
    "QualityLabel",
    # Tipos propios de este módulo
    "ValidationResult",
    "ValidationSummary",
    "CandleValidator",
]
