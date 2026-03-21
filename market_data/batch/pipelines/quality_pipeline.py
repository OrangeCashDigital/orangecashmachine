from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Optional
import pandas as pd
from loguru import logger
from market_data.batch.schemas.data_quality import DataQualityChecker, DataQualityReport
from market_data.batch.schemas.data_quality_policy import DataQualityPolicy, PolicyResult, QualityDecision, default_policy

class DataTier(str, Enum):
    RAW     = "raw"
    CLEAN   = "clean"
    REJECTED = "rejected"

@dataclass
class QualityPipelineResult:
    df:     pd.DataFrame
    report: DataQualityReport
    policy: PolicyResult
    tier:   DataTier

    @property
    def accepted(self) -> bool: return self.tier != DataTier.REJECTED

    @property
    def score(self) -> float: return self.policy.score

class QualityPipeline:
    def __init__(self, policy: Optional[DataQualityPolicy] = None) -> None:
        self._policy = policy or default_policy

    def run(self, df: pd.DataFrame, symbol: str, timeframe: str, exchange: str) -> QualityPipelineResult:
        checker = DataQualityChecker(timeframe=timeframe, exchange=exchange)
        report  = checker.check(df, symbol=symbol)
        result  = self._policy.evaluate(report)

        if result.decision == QualityDecision.REJECT:
            tier = DataTier.REJECTED
            logger.warning("QualityPipeline REJECT | {}/{} exchange={} score={:.1f} reasons={}",
                symbol, timeframe, exchange, result.score, result.reasons)
        elif result.decision == QualityDecision.ACCEPT_WITH_FLAGS:
            tier = DataTier.CLEAN
            logger.info("QualityPipeline ACCEPT_WITH_FLAGS | {}/{} exchange={} score={:.1f}",
                symbol, timeframe, exchange, result.score)
        else:
            tier = DataTier.CLEAN
            logger.debug("QualityPipeline ACCEPT | {}/{} exchange={} score={:.1f}",
                symbol, timeframe, exchange, result.score)

        return QualityPipelineResult(df=df, report=report, policy=result, tier=tier)

default_quality_pipeline = QualityPipeline()
