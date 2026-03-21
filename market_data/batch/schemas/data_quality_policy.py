from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional
from loguru import logger
from market_data.batch.schemas.data_quality import DataQualityReport, QualityIssue

class QualityDecision(str, Enum):
    ACCEPT = "accept"
    ACCEPT_WITH_FLAGS = "accept_with_flags"
    REJECT = "reject"

@dataclass
class PolicyResult:
    decision: QualityDecision
    score: float
    reasons: List[str]
    report: DataQualityReport

    @property
    def accepted(self) -> bool: return self.decision != QualityDecision.REJECT

    @property
    def flagged(self) -> bool: return self.decision == QualityDecision.ACCEPT_WITH_FLAGS

    def summary(self) -> str:
        return f"PolicyResult | decision={self.decision.value} score={self.score:.1f} reasons={self.reasons}"

_SCORE_WEIGHTS: Dict[str, float] = {
    "future_timestamps":    1.0,
    "ohlc_inconsistencies": 0.9,
    "temporal_gaps":        0.5,
    "price_outliers_mad":   0.3,
    "price_outliers_zscore":0.2,
    "flatline_candles":     0.1,
    "empty_dataset":        1.0,
}

_REJECT_THRESHOLD  = 40.0
_FLAG_THRESHOLD    = 75.0
_MAX_CRITICAL_ROWS_PCT = 0.10

class DataQualityPolicy:
    def evaluate(self, report: DataQualityReport) -> PolicyResult:
        if not report.rows:
            return PolicyResult(decision=QualityDecision.REJECT, score=0.0, reasons=["empty dataset"], report=report)

        score = self._compute_score(report)
        reasons = self._collect_reasons(report)

        if self._should_reject(report, score):
            decision = QualityDecision.REJECT
        elif score < _FLAG_THRESHOLD or report.warnings:
            decision = QualityDecision.ACCEPT_WITH_FLAGS
        else:
            decision = QualityDecision.ACCEPT

        result = PolicyResult(decision=decision, score=score, reasons=reasons, report=report)
        logger.debug("Policy | {}/{} exchange={} {}", report.symbol, report.timeframe, report.exchange, result.summary())
        return result

    def _compute_score(self, report: DataQualityReport) -> float:
        if not report.issues: return 100.0
        penalty = 0.0
        for issue in report.issues:
            weight = _SCORE_WEIGHTS.get(issue.check, 0.3)
            row_pct = issue.affected_rows / max(report.rows, 1)
            severity_mult = 2.0 if issue.severity == "critical" else 1.0
            penalty += weight * row_pct * severity_mult * 100
        return max(0.0, 100.0 - penalty)

    def _should_reject(self, report: DataQualityReport, score: float) -> bool:
        if score < _REJECT_THRESHOLD: return True
        if report.has_critical_issues:
            critical_rows = sum(i.affected_rows for i in report.criticals)
            if critical_rows / max(report.rows, 1) > _MAX_CRITICAL_ROWS_PCT: return True
        return False

    def _collect_reasons(self, report: DataQualityReport) -> List[str]:
        return [f"{i.check}({i.severity}): {i.description}" for i in report.issues]

default_policy = DataQualityPolicy()
