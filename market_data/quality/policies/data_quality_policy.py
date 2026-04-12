from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from loguru import logger
from market_data.quality.validators.data_quality import DataQualityReport

class QualityDecision(str, Enum):
    ACCEPT = "accept"
    ACCEPT_WITH_FLAGS = "accept_with_flags"
    REJECT = "reject"

@dataclass
class PolicyResult:
    decision: QualityDecision
    score: float
    reasons: list[str]
    report: DataQualityReport
    penalty_breakdown: dict[str, float] = field(default_factory=dict)

    @property
    def accepted(self) -> bool:
        return self.decision != QualityDecision.REJECT

    @property
    def flagged(self) -> bool:
        return self.decision == QualityDecision.ACCEPT_WITH_FLAGS

    def summary(self) -> str:
        breakdown_str = " ".join(f"{k}={v:.3f}" for k, v in self.penalty_breakdown.items())
        suffix = f" penalties=[{breakdown_str}]" if breakdown_str else ""
        return f"PolicyResult | decision={self.decision.value} score={self.score:.1f} reasons={self.reasons}{suffix}"

    def to_dict(self) -> dict:
        return {
            "decision":          self.decision.value,
            "score":             round(self.score, 4),
            "accepted":          self.accepted,
            "flagged":           self.flagged,
            "reasons":           self.reasons,
            "penalty_breakdown": self.penalty_breakdown,
        }

_SCORE_WEIGHTS: dict[str, float] = {
    "future_timestamps":    1.0,
    "ohlc_inconsistencies": 0.9,
    "temporal_gaps":        0.5,
    "price_outliers_mad":   0.3,
    "price_outliers_zscore":0.2,
    "flatline_candles":     0.1,
    "empty_dataset":        1.0,
}

_REJECT_THRESHOLD  = 40.0
# FLAG_THRESHOLD calibrado al scoring proporcional:
# - 1 flatline / 500 velas → penalty=0.002% → score=99.998 → ACCEPT (correcto)
# - 84 outliers MAD / 500 velas → penalty~=5% → score~=95 → FLAG (correcto)
# - Antes (75.0 + "or warnings"): cualquier issue, por mínimo, activaba FLAG
#   produciendo score=100.0 con ACCEPT_WITH_FLAGS — semánticamente incoherente.
_FLAG_THRESHOLD    = 99.0
_MAX_CRITICAL_ROWS_PCT = 0.10

class DataQualityPolicy:
    def evaluate(self, report: DataQualityReport) -> PolicyResult:
        if not report.rows:
            return PolicyResult(decision=QualityDecision.REJECT, score=0.0, reasons=["empty dataset"], report=report, penalty_breakdown={})

        score, breakdown = self._compute_score(report)
        reasons = self._collect_reasons(report)

        decision = self._decide(report, score)

        result = PolicyResult(decision=decision, score=score, reasons=reasons, report=report, penalty_breakdown=breakdown)
        logger.debug("Policy | {}/{} exchange={} {}", report.symbol, report.timeframe, report.exchange, result.summary())
        return result

    def _decide(self, report: DataQualityReport, score: float) -> QualityDecision:
        """Traduce score + issues a una decisión. Separado de evaluate() por SRP."""
        if self._should_reject(report, score):
            return QualityDecision.REJECT
        # El score penaliza proporcionalmente — no hace falta "or report.warnings"
        # que causaba ACCEPT_WITH_FLAGS con score=100.0 (semánticamente incoherente).
        if score < _FLAG_THRESHOLD:
            return QualityDecision.ACCEPT_WITH_FLAGS
        return QualityDecision.ACCEPT

    def _compute_score(self, report: DataQualityReport) -> tuple[float, dict[str, float]]:
        if not report.issues:
            return 100.0, {}
        penalty = 0.0
        breakdown: dict[str, float] = {}
        for issue in report.issues:
            weight = _SCORE_WEIGHTS.get(issue.check, 0.3)
            row_pct = issue.affected_rows / max(report.rows, 1)
            severity_mult = 2.0 if issue.severity == "critical" else 1.0
            p = weight * row_pct * severity_mult * 100
            penalty += p
            breakdown[issue.check] = round(breakdown.get(issue.check, 0.0) + p, 4)
        return max(0.0, 100.0 - penalty), breakdown

    def _should_reject(self, report: DataQualityReport, score: float) -> bool:
        if score < _REJECT_THRESHOLD:
            return True
        if report.has_critical_issues:
            critical_rows = sum(i.affected_rows for i in report.criticals)
            if critical_rows / max(report.rows, 1) > _MAX_CRITICAL_ROWS_PCT:
                return True
        return False

    def _collect_reasons(self, report: DataQualityReport) -> list[str]:
        return [f"{i.check}({i.severity}): {i.description}" for i in report.issues]

default_policy = DataQualityPolicy()
