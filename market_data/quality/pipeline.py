from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd
from loguru import logger

from market_data.quality.validators.data_quality import DataQualityChecker, DataQualityReport
from market_data.observability.metrics import QUALITY_GAPS_TOTAL, PIPELINE_ERRORS
from market_data.processing.utils.gap_utils import scan_gaps
from market_data.quality.anomaly_registry import AnomalyRegistry, _registry
from market_data.lineage.tracker import (
    LineageEvent, LineageStatus, PipelineLayer, lineage_tracker,
)
from market_data.quality.policies.data_quality_policy import (
    DataQualityPolicy, PolicyResult, QualityDecision, default_policy,
)


class DataTier(str, Enum):
    CLEAN    = "clean"    # ACCEPT: sin issues
    FLAGGED  = "flagged"  # ACCEPT_WITH_FLAGS: warnings presentes, usable con precaucion
    REJECTED = "rejected" # REJECT: datos inutilizables


@dataclass
class QualityPipelineResult:
    df:     pd.DataFrame
    report: DataQualityReport
    policy: PolicyResult
    tier:   DataTier

    @property
    def accepted(self) -> bool: return self.tier != DataTier.REJECTED

    @property
    def flagged(self) -> bool: return self.tier == DataTier.FLAGGED

    @property
    def score(self) -> float: return self.policy.score


# ----------------------------------------------------------
# Anomaly Registry
# ----------------------------------------------------------
# Delegado a market_data.quality.anomaly_registry (SQLite persistente).
# _registry importado arriba como singleton de módulo.
# ----------------------------------------------------------


class QualityPipeline:
    def __init__(
        self,
        policy:   Optional[DataQualityPolicy] = None,
        registry: Optional[_AnomalyRegistry]  = None,
    ) -> None:
        self._policy   = policy or default_policy
        self._registry = registry or _registry  # shared by default, injectable for tests

    def run(
        self,
        df:           pd.DataFrame,
        symbol:       str,
        timeframe:    str,
        exchange:     str,
        run_id:       str | None = None,
        rows_removed: int = 0,
    ) -> QualityPipelineResult:
        # rows_removed propagado al checker para que _check_gaps pueda
        # distinguir gaps de pipeline vs gaps reales de fuente (SSOT).
        checker = DataQualityChecker(
            timeframe=timeframe,
            exchange=exchange,
            rows_removed=rows_removed,
        )
        report  = checker.check(df, symbol=symbol)
        result  = self._policy.evaluate(report)

        # Gap scan post-ingesta: detecta huecos temporales silenciosos.
        # rows_removed: velas CORRUPT eliminadas upstream (transformer).
        # Los gaps causados por nuestra propia remoción no son dato faltante
        # del exchange — se descuentan del conteo para evitar falsos positivos.
        # Ref: Mangala (2022) — distinguir "pipeline-induced gaps" de "source gaps".
        _gaps_raw = scan_gaps(df, timeframe)
        _gaps     = _gaps_raw[rows_removed:] if rows_removed > 0 else _gaps_raw
        if _gaps:
            _high   = sum(1 for g in _gaps if g.severity == "high")
            _medium = sum(1 for g in _gaps if g.severity == "medium")
            _low    = len(_gaps) - _high - _medium
            _lvl    = logger.warning if _high > 0 else logger.info
            _suffix = f" (excluded {rows_removed} pipeline-removed)" if rows_removed else ""
            _lvl(
                "Gap scan | total={} high={} medium={} low={}{} | {}/{} exchange={}",
                len(_gaps), _high, _medium, _low, _suffix, symbol, timeframe, exchange,
            )
            # Métricas Prometheus — permite alertas y dashboards por severidad.
            for _sev, _cnt in (("high", _high), ("medium", _medium), ("low", _low)):
                if _cnt:
                    QUALITY_GAPS_TOTAL.labels(
                        exchange=exchange, symbol=symbol,
                        timeframe=timeframe, severity=_sev,
                    ).inc(_cnt)

        if result.decision == QualityDecision.REJECT:
            tier = DataTier.REJECTED
            PIPELINE_ERRORS.labels(exchange=exchange, error_type="quality_reject").inc()
            logger.warning(
                "QualityPipeline REJECT | {}/{} exchange={} score={:.1f} reasons={}",
                symbol, timeframe, exchange, result.score, result.reasons,
            )
            if run_id is not None:
                lineage_tracker.record(LineageEvent(
                    run_id        = run_id,
                    layer         = PipelineLayer.SILVER,
                    exchange      = exchange,
                    symbol        = symbol,
                    timeframe     = timeframe,
                    rows_in       = len(df),
                    rows_out      = 0,
                    status        = LineageStatus.FAILED,
                    quality_score = result.score,
                    params        = {"reasons": result.reasons},
                ))

        elif result.decision == QualityDecision.ACCEPT_WITH_FLAGS:
            tier = DataTier.FLAGGED
            # Log principal siempre visible
            logger.info(
                "QualityPipeline ACCEPT_WITH_FLAGS | {}/{} exchange={} score={:.1f}",
                symbol, timeframe, exchange, result.score,
            )
            # Loguear cada reason solo la primera vez que aparece en esta sesión
            for reason in result.reasons:
                if self._registry.is_new(exchange, symbol, timeframe, reason):
                    logger.info(
                        "Quality anomaly (first occurrence) | {}/{} exchange={} | {}",
                        symbol, timeframe, exchange, reason,
                    )
                else:
                    logger.debug(
                        "Quality anomaly (recurring) | {}/{} exchange={} | {}",
                        symbol, timeframe, exchange, reason,
                    )

        else:
            tier = DataTier.CLEAN
            logger.debug(
                "QualityPipeline ACCEPT | {}/{} exchange={} score={:.1f}",
                symbol, timeframe, exchange, result.score,
            )

        # Lineage QUALITY para CLEAN y FLAGGED (REJECT ya registrado arriba)
        if run_id is not None and tier != DataTier.REJECTED:
            lineage_tracker.record(LineageEvent(
                run_id        = run_id,
                layer         = PipelineLayer.SILVER,
                exchange      = exchange,
                symbol        = symbol,
                timeframe     = timeframe,
                rows_in       = len(df),
                rows_out      = len(df),
                status        = (
                    LineageStatus.PARTIAL
                    if tier == DataTier.FLAGGED
                    else LineageStatus.SUCCESS
                ),
                quality_score = result.score,
                params        = {"tier": tier.value, "reasons": result.reasons},
            ))

        return QualityPipelineResult(df=df, report=report, policy=result, tier=tier)


default_quality_pipeline = QualityPipeline()
