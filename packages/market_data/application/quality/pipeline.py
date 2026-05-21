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
from market_data.ports.outbound.data_quality_checker import CheckerFactory
from market_data.application.quality.data_quality import native_checker_factory

# terceros
import pandas as pd
from loguru import logger

# dominio
from market_data.domain.entities import DataTier
from market_data.domain.events import LineageEvent, LineageStatus, PipelineLayer
from market_data.domain.value_objects.gap_utils import scan_gaps

# ports — DIP: pipeline depende de abstracciones, nunca de infrastructure concreta
from market_data.ports.outbound.lineage import LineageTrackerPort
from market_data.ports.outbound.metrics import NullQualityMetrics, QualityMetricsPort
from market_data.ports.outbound.quality import AnomalyRegistryPort

# quality internals
from market_data.domain.policies.data_quality_policy import (
    DataQualityPolicy,
    PolicyResult,
    QualityDecision,
    default_policy,
)

# DataQualityReport: type usado en QualityPipelineResult y _resolve_tier
# DataQualityChecker: NO importar aquí — pipeline usa self._checker_factory (DIP)
from market_data.application.quality.data_quality import DataQualityReport

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

    df: pd.DataFrame
    report: DataQualityReport
    policy: PolicyResult
    tier: DataTier

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
        policy: Optional[DataQualityPolicy] = None,
        registry: Optional[AnomalyRegistryPort] = None,
        metrics: Optional[QualityMetricsPort] = None,
        lineage_tracker: Optional[LineageTrackerPort] = None,
        checker_factory: Optional[CheckerFactory] = None,
    ) -> None:
        self._policy = policy or default_policy
        # AnomalyRegistryPort inyectado desde pipeline_factory (Composition Root).
        # NullAnomalyRegistry si no se provee — fail-soft sin acoplamiento a infra.
        self._registry = registry if registry is not None else _null_registry()
        self._metrics = metrics or NullQualityMetrics()
        self._lineage_tracker = lineage_tracker or _null_lineage_tracker()
        # DIP: checker inyectado — default = native (backward compat)
        self._checker_factory = checker_factory or native_checker_factory

    def run(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        exchange: str,
        run_id: Optional[str] = None,
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
        # DIP: checker inyectado por factory
        checker = self._checker_factory(timeframe, exchange, rows_removed)
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
        df: pd.DataFrame,
        timeframe: str,
        symbol: str,
        exchange: str,
        rows_removed: int,
    ) -> None:
        """
        Escanea gaps temporales y emite métricas Prometheus.

        Descuenta rows_removed para no contar gaps pipeline-induced
        como gaps reales de fuente.
        """
        gaps_raw = scan_gaps(df, timeframe)
        gaps = gaps_raw[rows_removed:] if rows_removed > 0 else gaps_raw
        if not gaps:
            return

        high = sum(1 for g in gaps if g.severity == "high")
        medium = sum(1 for g in gaps if g.severity == "medium")
        low = len(gaps) - high - medium
        log_fn = logger.warning if high > 0 else logger.info
        suffix = f" (excluded {rows_removed} pipeline-removed)" if rows_removed else ""

        log_fn(
            "Gap scan | total={} high={} medium={} low={}{} | {}/{} exchange={}",
            len(gaps),
            high,
            medium,
            low,
            suffix,
            symbol,
            timeframe,
            exchange,
        )
        for sev, cnt in (("high", high), ("medium", medium), ("low", low)):
            if cnt:
                self._metrics.quality_gaps_inc(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    severity=sev,
                    count=cnt,
                )

    def _resolve_tier(
        self,
        result: PolicyResult,
        report: DataQualityReport,
        symbol: str,
        timeframe: str,
        exchange: str,
    ) -> DataTier:
        """Traduce QualityDecision → DataTier con logging apropiado."""
        if result.decision == QualityDecision.REJECT:
            self._metrics.pipeline_errors_inc(
                exchange=exchange,
                error_type="quality_reject",
            )
            logger.warning(
                "QualityPipeline REJECT | {}/{} exchange={} score={:.1f} reasons={}",
                symbol,
                timeframe,
                exchange,
                result.score,
                result.reasons,
            )
            return DataTier.REJECTED

        if result.decision == QualityDecision.ACCEPT_WITH_FLAGS:
            logger.info(
                "QualityPipeline ACCEPT_WITH_FLAGS | {}/{} exchange={} score={:.1f}",
                symbol,
                timeframe,
                exchange,
                result.score,
            )
            for reason in result.reasons:
                if self._registry.is_new(exchange, symbol, timeframe, reason):
                    logger.info(
                        "Quality anomaly (first occurrence) | {}/{} exchange={} | {}",
                        symbol,
                        timeframe,
                        exchange,
                        reason,
                    )
                else:
                    logger.debug(
                        "Quality anomaly (recurring) | {}/{} exchange={} | {}",
                        symbol,
                        timeframe,
                        exchange,
                        reason,
                    )
            return DataTier.FLAGGED

        logger.debug(
            "QualityPipeline ACCEPT | {}/{} exchange={} score={:.1f}",
            symbol,
            timeframe,
            exchange,
            result.score,
        )
        return DataTier.CLEAN

    def _record_lineage(
        self,
        run_id: Optional[str],
        tier: DataTier,
        result: PolicyResult,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        exchange: str,
    ) -> None:
        """
        Registra evento de lineage. Fail-soft: nunca propaga excepciones.

        REJECT registra rows_out=0 y status=FAILED.
        CLEAN/FLAGGED registran rows_out=len(df) y status=SUCCESS/PARTIAL.
        """
        if run_id is None:
            return

        if tier == DataTier.REJECTED:
            self._lineage_tracker.record(
                LineageEvent(
                    run_id=run_id,
                    layer=PipelineLayer.SILVER,
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    rows_in=len(df),
                    rows_out=0,
                    status=LineageStatus.FAILED,
                    quality_score=result.score,
                    params={"reasons": result.reasons},
                )
            )
            return

        self._lineage_tracker.record(
            LineageEvent(
                run_id=run_id,
                layer=PipelineLayer.SILVER,
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
                rows_in=len(df),
                rows_out=len(df),
                status=(LineageStatus.PARTIAL if tier == DataTier.FLAGGED else LineageStatus.SUCCESS),
                quality_score=result.score,
                params={"tier": tier.value, "reasons": result.reasons},
            )
        )


# ===========================================================================
# Null object para LineageTrackerPort — SafeOps cuando no se inyecta tracker
# ===========================================================================


# ---------------------------------------------------------------------------
# Null Object — AnomalyRegistry (fail-soft, sin acoplamiento a infra)
# ---------------------------------------------------------------------------
# Mismo patrón que _NullLineageTracker: factoría explícita evita singleton
# mutable compartido entre instancias del pipeline (thread-safety).
# DIP: satisface AnomalyRegistryPort estructuralmente (duck typing / Protocol).
# ---------------------------------------------------------------------------


class _NullAnomalyRegistry:
    """Null Object para AnomalyRegistryPort — descarta silenciosamente.

    Implementa la interfaz mínima del Protocol AnomalyRegistryPort:
      is_new  — considera toda anomalía nueva (no persiste estado)
      stats   — retorna lista vacía
      wipe    — no-op
    DIP: satisface el Protocol estructuralmente (duck typing), sin herencia.
    """

    def is_new(self, *args: object, **kwargs: object) -> bool:  # noqa: ANN401
        """Null: toda anomalía es nueva — no persiste estado entre llamadas."""
        return True

    def stats(self) -> list:
        """Null: sin anomalías registradas."""
        return []

    def wipe(self) -> None:
        """Null: no-op."""
        pass


def _null_registry() -> _NullAnomalyRegistry:
    """Factoría del null object — evita singleton mutable compartido."""
    return _NullAnomalyRegistry()


class _NullLineageTracker:
    """
    Implementación vacía de LineageTrackerPort.

    Uso: cuando run_id=None o lineage_tracker no está configurado.
    No-op en todos los métodos — nunca propaga excepciones (SafeOps).
    """

    def record(self, event: object) -> None:
        pass

    def new_run_id(self) -> str:
        import uuid

        return str(uuid.uuid4())


def _null_lineage_tracker() -> _NullLineageTracker:
    """Factoría del null object — evita singleton mutable compartido."""
    return _NullLineageTracker()


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
