# -*- coding: utf-8 -*-
"""
market_data/quality/policies/data_quality_policy.py
=====================================================

Domain Service — política de aceptación/rechazo de datos OHLCV.

Responsabilidad
---------------
Única: dada una DataQualityReport, producir una decisión de negocio
(ACCEPT / ACCEPT_WITH_FLAGS / REJECT) con score de calidad y desglose.

No valida datos crudos, no escribe, no hace I/O — SRP estricto.

Scoring
-------
score = 100.0 - Σ(weight_i × row_pct_i × severity_mult_i × 100)

  weight         : gravedad del tipo de issue (ver _SCORE_WEIGHTS)
  row_pct        : fracción de filas afectadas sobre el total
  severity_mult  : 2.0 para "critical", 1.0 para el resto

Umbrales
--------
  score < REJECT_THRESHOLD  → REJECT  (datos inutilizables)
  score < FLAG_THRESHOLD    → ACCEPT_WITH_FLAGS (usable con precaución)
  score >= FLAG_THRESHOLD   → ACCEPT  (sin issues significativos)

  FLAG_THRESHOLD calibrado al scoring proporcional:
    1 flatline / 500 velas → penalty=0.002% → score≈100 → ACCEPT (correcto)
    84 outliers MAD / 500  → penalty≈5%     → score≈95  → FLAG  (correcto)

Decisión de REJECT adicional
-----------------------------
Si has_critical_issues Y critical_rows > MAX_CRITICAL_ROWS_PCT,
el batch se rechaza independientemente del score (fail-fast de seguridad).

Principios
----------
SRP    — solo decide; no valida, no escribe
OCP    — agregar checks no modifica esta política
DIP    — depende de DataQualityReport (abstracción), no de implementaciones
SSOT   — _SCORE_WEIGHTS y umbrales declarados como constantes de módulo
KISS   — flujo lineal: compute_score → collect_reasons → decide
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List

from loguru import logger

from market_data.quality.validators.data_quality import DataQualityReport


# ===========================================================================
# Tipos públicos
# ===========================================================================

class QualityDecision(str, Enum):
    """
    Decisión de negocio sobre un batch de datos.

    ACCEPT            — datos limpios, sin reservas
    ACCEPT_WITH_FLAGS — datos usables, con anomalías registradas
    REJECT            — datos inutilizables, no escribir en Silver
    """
    ACCEPT            = "accept"
    ACCEPT_WITH_FLAGS = "accept_with_flags"
    REJECT            = "reject"


@dataclass
class PolicyResult:
    """
    Resultado estructurado de aplicar DataQualityPolicy a un report.

    Attributes
    ----------
    decision          : decisión final de negocio
    score             : calidad en [0.0, 100.0] (100 = sin issues)
    reasons           : descripción legible de cada issue detectado
    report            : report de origen (para trazabilidad)
    penalty_breakdown : desglose de penalización por tipo de issue
    """
    decision:          QualityDecision
    score:             float
    reasons:           List[str]
    report:            DataQualityReport
    penalty_breakdown: Dict[str, float] = field(default_factory=dict)

    @property
    def accepted(self) -> bool:
        """True si la decisión NO es REJECT."""
        return self.decision != QualityDecision.REJECT

    @property
    def flagged(self) -> bool:
        """True si la decisión es ACCEPT_WITH_FLAGS."""
        return self.decision == QualityDecision.ACCEPT_WITH_FLAGS

    def summary(self) -> str:
        breakdown_str = " ".join(
            f"{k}={v:.3f}" for k, v in self.penalty_breakdown.items()
        )
        suffix = f" penalties=[{breakdown_str}]" if breakdown_str else ""
        return (
            f"PolicyResult | decision={self.decision.value} "
            f"score={self.score:.1f} reasons={self.reasons}{suffix}"
        )

    def to_dict(self) -> Dict:
        return {
            "decision":          self.decision.value,
            "score":             round(self.score, 4),
            "accepted":          self.accepted,
            "flagged":           self.flagged,
            "reasons":           self.reasons,
            "penalty_breakdown": self.penalty_breakdown,
        }


# ===========================================================================
# Constantes de política — SSOT
# ===========================================================================

_SCORE_WEIGHTS: Dict[str, float] = {
    "future_timestamps":     1.0,
    "ohlc_inconsistencies":  0.9,
    "temporal_gaps":         0.5,
    "price_outliers_mad":    0.3,
    "price_outliers_zscore": 0.2,
    "flatline_candles":      0.1,
    "empty_dataset":         1.0,
}

_REJECT_THRESHOLD:      float = 40.0
_FLAG_THRESHOLD:        float = 99.0
_MAX_CRITICAL_ROWS_PCT: float = 0.10


# ===========================================================================
# Domain Service
# ===========================================================================

class DataQualityPolicy:
    """
    Política de aceptación/rechazo de datos OHLCV.

    Stateless — segura para uso concurrente y como singleton de módulo.

    Usage
    -----
    policy = DataQualityPolicy()
    result = policy.evaluate(report)
    if not result.accepted:
        raise QualityError(result.summary())
    """

    def evaluate(self, report: DataQualityReport) -> PolicyResult:
        """
        Evalúa un DataQualityReport y produce una decisión de negocio.

        Orden: guard (empty) → score → reasons → decide → log.
        """
        if not report.rows:
            return PolicyResult(
                decision          = QualityDecision.REJECT,
                score             = 0.0,
                reasons           = ["empty dataset"],
                report            = report,
                penalty_breakdown = {},
            )

        score, breakdown = self._compute_score(report)
        reasons          = self._collect_reasons(report)
        decision         = self._decide(report, score)

        result = PolicyResult(
            decision          = decision,
            score             = score,
            reasons           = reasons,
            report            = report,
            penalty_breakdown = breakdown,
        )
        logger.debug(
            "Policy | {}/{} exchange={} {}",
            report.symbol, report.timeframe, report.exchange, result.summary(),
        )
        return result

    # ── Internos (SRP — cada método hace una sola cosa) ───────────────────────

    def _decide(self, report: DataQualityReport, score: float) -> QualityDecision:
        """Traduce score + issues críticos a una decisión. Separado por SRP."""
        if self._should_reject(report, score):
            return QualityDecision.REJECT
        if score < _FLAG_THRESHOLD:
            return QualityDecision.ACCEPT_WITH_FLAGS
        return QualityDecision.ACCEPT

    def _compute_score(
        self,
        report: DataQualityReport,
    ) -> tuple[float, Dict[str, float]]:
        """
        Calcula score de calidad y desglose de penalización.

        score = 100.0 - Σ(weight × row_pct × severity_mult × 100)
        Nunca baja de 0.0.
        """
        if not report.issues:
            return 100.0, {}

        penalty:   float            = 0.0
        breakdown: Dict[str, float] = {}

        for issue in report.issues:
            weight        = _SCORE_WEIGHTS.get(issue.check, 0.3)
            row_pct       = issue.affected_rows / max(report.rows, 1)
            severity_mult = 2.0 if issue.severity == "critical" else 1.0
            p             = weight * row_pct * severity_mult * 100
            penalty      += p
            breakdown[issue.check] = round(
                breakdown.get(issue.check, 0.0) + p, 4
            )

        return max(0.0, 100.0 - penalty), breakdown

    def _should_reject(self, report: DataQualityReport, score: float) -> bool:
        """
        True si el batch debe rechazarse.

        Condiciones (OR):
        1. score < REJECT_THRESHOLD
        2. issues críticos superan MAX_CRITICAL_ROWS_PCT del total
        """
        if score < _REJECT_THRESHOLD:
            return True
        if report.has_critical_issues:
            critical_rows = sum(i.affected_rows for i in report.criticals)
            if critical_rows / max(report.rows, 1) > _MAX_CRITICAL_ROWS_PCT:
                return True
        return False

    def _collect_reasons(self, report: DataQualityReport) -> List[str]:
        """Genera lista de razones legibles desde los issues del report."""
        return [
            f"{i.check}({i.severity}): {i.description}"
            for i in report.issues
        ]


# ===========================================================================
# Singleton de módulo — injectable para tests
# ===========================================================================

default_policy: DataQualityPolicy = DataQualityPolicy()
"""
Instancia compartida para uso en producción.

Inyectable en QualityPipeline:
    pipeline = QualityPipeline(policy=custom_policy)  # tests
    pipeline = QualityPipeline()                       # usa default_policy
"""


__all__ = [
    "QualityDecision",
    "PolicyResult",
    "DataQualityPolicy",
    "default_policy",
]
