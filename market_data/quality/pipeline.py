from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum
from typing import FrozenSet, Optional, Tuple

import pandas as pd
from loguru import logger

from market_data.quality.validators.data_quality import DataQualityChecker, DataQualityReport
from market_data.processing.strategies.repair import scan_gaps
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
# Known Anomaly Registry
# ----------------------------------------------------------
# Cache thread-safe de anomalías ya reportadas.
# Evita repetir el mismo WARNING en cada ejecución incremental
# cuando el dato problemático ya está en Silver y no cambiará.
# Key: (exchange, symbol, timeframe, reason_prefix)
# ----------------------------------------------------------

_AnomalyKey = Tuple[str, str, str, str]

class _AnomalyRegistry:
    """Registro en memoria de anomalías ya logueadas en esta sesión."""

    def __init__(self) -> None:
        self._seen: set[_AnomalyKey] = set()
        self._lock = threading.Lock()

    def is_new(self, exchange: str, symbol: str, timeframe: str, reason: str) -> bool:
        """Retorna True si esta anomalía no había sido vista antes y la registra."""
        # Normalizar el reason a su prefijo (e.g. "price_outliers_mad" sin el conteo)
        reason_key = reason.split("(")[0].strip()
        key: _AnomalyKey = (exchange, symbol, timeframe, reason_key)
        with self._lock:
            if key in self._seen:
                return False
            self._seen.add(key)
            return True

    def clear(self) -> None:
        with self._lock:
            self._seen.clear()


_registry = _AnomalyRegistry()


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
        df:        pd.DataFrame,
        symbol:    str,
        timeframe: str,
        exchange:  str,
    ) -> QualityPipelineResult:
        checker = DataQualityChecker(timeframe=timeframe, exchange=exchange)
        report  = checker.check(df, symbol=symbol)
        result  = self._policy.evaluate(report)

        # Gap scan post-ingesta: detecta huecos temporales silenciosos.
        # Corre siempre, independiente del resultado de calidad.
        # Warning únicamente — no bloquea el pipeline (datos parciales > sin datos).
        _gaps = scan_gaps(df, timeframe)
        if _gaps:
            _high = sum(1 for g in _gaps if g.severity == "high")
            _lvl  = logger.warning if _high > 0 else logger.info
            _lvl(
                "Gap scan | total={} high={} | {}/{} exchange={}",
                len(_gaps), _high, symbol, timeframe, exchange,
            )

        if result.decision == QualityDecision.REJECT:
            tier = DataTier.REJECTED
            logger.warning(
                "QualityPipeline REJECT | {}/{} exchange={} score={:.1f} reasons={}",
                symbol, timeframe, exchange, result.score, result.reasons,
            )

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

        return QualityPipelineResult(df=df, report=report, policy=result, tier=tier)


default_quality_pipeline = QualityPipeline()
