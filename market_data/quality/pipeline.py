# -*- coding: utf-8 -*-
"""
market_data/quality/pipeline.py
=================================

Domain Service — pipeline de calidad de datos OHLCV.

Responsabilidad
---------------
Orquestar el flujo completo de calidad para un DataFrame Silver:
  1. Ejecutar DataQualityChecker → DataQualityReport
  2. Aplicar DataQualityPolicy   → PolicyResult (score + decision)
  3. Escanear gaps temporales    → métricas Prometheus
  4. Registrar anomalías nuevas  → AnomalyRegistry (fail-soft)
  5. Registrar lineage           → LineageTracker (fail-soft)
  6. Retornar QualityPipelineResult

No valida velas crudas (eso es CandleValidator), no escribe en Silver
(eso es IcebergStorage) — SRP estricto.

Dependencias de dominio
-----------------------
DataTier importado desde domain/entities (DIP · SSOT).
QualityDecision importado desde quality/policies (dominio).

Gap scan
--------
rows_removed: velas CORRUPT eliminadas upstream por el transformer.
Los gaps causados por nuestra propia remoción no son dato faltante
del exchange — se descuentan para evitar falsos positivos.
Ref: Mangala (2022) — distinguir "pipeline-induced gaps" de "source gaps".

Principios
----------
SRP    — orquesta, no implementa lógica de calidad
DIP    — depende de puertos (AnomalyRegistryPort), no de infra concreta
OCP    — agregar validators no modifica este pipeline
SafeOps — lineage y registry: fail-soft, nunca bloquean el pipeline
SSOT   — DataTier definida en domain/entities (no duplicada aquí)
"""
from __future__ import annotations

# stdlib
from dataclasses import dataclass
from typing import Optional

# terceros
import pandas as pd
from loguru import logger

# dominio
from market_data.domain.entities import DataTier
from market_data.lineage.tracker import (
    LineageEvent,
    LineageStatus,
    PipelineLayer,
    lineage_tracker,
)
from market_data.ports.quality import AnomalyRegistryPort
from market_data.quality.anomaly_registry import default_registry
from market_data.quality.policies.data_quality_policy import (
    DataQualityPolicy,
    PolicyResult,
    QualityDecision,
    default_policy,
)
from market_data.quality.validators.data_quality import (
    DataQualityChecker,
    DataQualityReport,
)

# infra / utils
from market_data.observability.metrics import PIPELINE_ERRORS, QUALITY_GAPS_TOTAL
from market_data.processing.utils.gap_utils import scan_gaps


# ===========================================================================
# Resultado del pipeline
# ===========================================================================

@dataclass
class QualityPipelineResult:
    """
    Resultado completo de ejecutar QualityPipeline sobre un DataFrame.

    Attributes
    ----------
    df     : DataFrame original (no modificado — inmutabilidad de dominio)
    report : DataQualityReport con todos los issues detectados
    policy : PolicyResult con score, decision y desglose de penalización
    tier   : DataTier resultante (CLEAN / FLAGGED / REJECTED)
    """
    df:     pd.DataFrame
    report: DataQualityReport
    policy: PolicyResult
    tier:   DataTier

    @property
    def accepted(self) -> bool:
        """True si el tier NO es REJECTED."""
        return self.tier != DataTier.REJECTED

    @property
    def flagged(self) -> bool:
        """True si el tier es FLAGGED."""
        return self.tier == DataTier.FLAGGED

    @property
    def score(self) -> float:
        """Score de calidad en [0.0, 100.0]."""
        return self.policy.score


# ===========================================================================
# Pipeline
# ===========================================================================

class QualityPipeline:
    """
    Orquesta el flujo completo de calidad para un DataFrame Silver.

    Injectable para tests:
        pipeline = QualityPipeline(policy=mock_policy, registry=mock_registry)

    Usage
    -----
    result = QualityPipeline().run(df, symbol="BTC/USDT", timeframe="1h",
                                   exchange="bybit", run_id=run_id)
    if not result.accepted:
        raise QualityError(f"Batch rechazado: score={result.score:.1f}")
    """

    def __init__(
        self,
        policy:   Optional[DataQualityPolicy] = None,
        registry: Optional[AnomalyRegistryPort] = None,
    ) -> None:
        self._policy   = policy   or default_policy
        self._registry = registry or default_registry

    def run(
        self,
        df:           pd.DataFrame,
        symbol:       str,
        timeframe:    str,
        exchange:     str,
        run_id:       Optional[str] = None,
        rows_removed: int = 0,
    ) -> QualityPipelineResult:
        """
        Ejecuta el pipeline de calidad completo.

        Parameters
        ----------
        df           : DataFrame Silver a evaluar
        symbol       : par de trading (e.g. "BTC/USDT")
        timeframe    : intervalo (e.g. "1h")
        exchange     : nombre del exchange (e.g. "bybit")
        run_id       : ID de correlación para lineage (None = no registrar)
        rows_removed : velas CORRUPT eliminadas upstream (para gap scan)
        """
        # 1. Validación de calidad
        checker = DataQualityChecker(
            timeframe    = timeframe,
            exchange     = exchange,
            rows_removed = rows_removed,
        )
        report = checker.check(df, symbol=symbol)
        result = self._policy.evaluate(report)

        # 2. Gap scan post-ingesta
        self._scan_and_emit_gaps(df, timeframe, symbol, exchange, rows_removed)

        # 3. Decisión de tier + logging
        tier = self._resolve_tier(result, report, symbol, timeframe, exchange)

        # 4. Lineage (fail-soft — nunca bloquea el pipeline)
        self._record_lineage(run_id, tier, result, df, symbol, timeframe, exchange)

        return QualityPipelineResult(df=df, report=report, policy=result, tier=tier)

    # ── Internos ──────────────────────────────────────────────────────────────

    def _scan_and_emit_gaps(
        self,
        df:           pd.DataFrame,
        timeframe:    str,
        symbol:       str,
        exchange:     str,
        rows_removed: int,
    ) -> None:
        """
        Escanea gaps temporales y emite métricas Prometheus.

        Descuenta rows_removed para no contar gaps pipeline-induced
        como gaps reales de fuente.
        """
        gaps_raw = scan_gaps(df, timeframe)
        gaps     = gaps_raw[rows_removed:] if rows_removed > 0 else gaps_raw
        if not gaps:
            return

        high   = sum(1 for g in gaps if g.severity == "high")
        medium = sum(1 for g in gaps if g.severity == "medium")
        low    = len(gaps) - high - medium
        log_fn = logger.warning if high > 0 else logger.info
        suffix = f" (excluded {rows_removed} pipeline-removed)" if rows_removed else ""

        log_fn(
            "Gap scan | total={} high={} medium={} low={}{} | {}/{} exchange={}",
            len(gaps), high, medium, low, suffix, symbol, timeframe, exchange,
        )
        for sev, cnt in (("high", high), ("medium", medium), ("low", low)):
            if cnt:
                QUALITY_GAPS_TOTAL.labels(
                    exchange=exchange, symbol=symbol,
                    timeframe=timeframe, severity=sev,
                ).inc(cnt)

    def _resolve_tier(
        self,
        result:    PolicyResult,
        report:    DataQualityReport,
        symbol:    str,
        timeframe: str,
        exchange:  str,
    ) -> DataTier:
        """Traduce QualityDecision → DataTier con logging apropiado."""
        if result.decision == QualityDecision.REJECT:
            PIPELINE_ERRORS.labels(
                exchange=exchange, error_type="quality_reject"
            ).inc()
            logger.warning(
                "QualityPipeline REJECT | {}/{} exchange={} score={:.1f} reasons={}",
                symbol, timeframe, exchange, result.score, result.reasons,
            )
            return DataTier.REJECTED

        if result.decision == QualityDecision.ACCEPT_WITH_FLAGS:
            logger.info(
                "QualityPipeline ACCEPT_WITH_FLAGS | {}/{} exchange={} score={:.1f}",
                symbol, timeframe, exchange, result.score,
            )
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
            return DataTier.FLAGGED

        logger.debug(
            "QualityPipeline ACCEPT | {}/{} exchange={} score={:.1f}",
            symbol, timeframe, exchange, result.score,
        )
        return DataTier.CLEAN

    def _record_lineage(
        self,
        run_id:    Optional[str],
        tier:      DataTier,
        result:    PolicyResult,
        df:        pd.DataFrame,
        symbol:    str,
        timeframe: str,
        exchange:  str,
    ) -> None:
        """
        Registra evento de lineage. Fail-soft: nunca propaga excepciones.

        REJECT registra rows_out=0 y status=FAILED.
        CLEAN/FLAGGED registran rows_out=len(df) y status=SUCCESS/PARTIAL.
        """
        if run_id is None:
            return

        if tier == DataTier.REJECTED:
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
            return

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


# ===========================================================================
# Singleton de módulo — injectable para tests
# ===========================================================================

default_quality_pipeline: QualityPipeline = QualityPipeline()
"""
Instancia compartida para uso en producción.
Injectable:
    pipeline = QualityPipeline(policy=p, registry=r)  # tests/custom
"""


__all__ = [
    "QualityPipelineResult",
    "QualityPipeline",
    "default_quality_pipeline",
    # Re-export para backward-compat
    "DataTier",
]
