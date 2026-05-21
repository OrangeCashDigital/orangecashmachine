# -*- coding: utf-8 -*-
"""
market_data/application/quality/data_quality.py
=================================================

DataQualityChecker — checks estadísticos sobre DataFrames OHLCV.

Responsabilidad
---------------
Única: dado un DataFrame Silver, detectar issues de calidad y retornar
un DataQualityReport con la lista de QualityIssue encontrados.
No decide qué hacer con los issues — eso es DataQualityPolicy (SRP).

Tipos de dominio
----------------
DataQualityReport y QualityIssue viven en domain/quality/types.py.
Este módulo solo contiene el checker (lógica pandas — application layer).

Principios: SRP · DIP · SSOT · KISS · Fail-Fast
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

if TYPE_CHECKING:
    from market_data.ports.outbound.data_quality_checker import DataQualityCheckerPort

import subprocess
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from loguru import logger

from market_data.domain.exceptions import DataQualityError  # noqa: F401
from market_data.domain.quality.types import DataQualityReport, QualityIssue
from market_data.domain.value_objects.timeframe import timeframe_to_ms


# ===========================================================================
# Helpers
# ===========================================================================

def _get_git_hash() -> str:
    """Retorna el git hash corto del HEAD. Fail-soft: retorna 'unknown'."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


# ===========================================================================
# Constantes — SSOT
# ===========================================================================

_TF_MS: Dict[str, int] = {
    tf: timeframe_to_ms(tf)
    for tf in ["1m", "5m", "15m", "1h", "4h", "1d"]
}

_MAD_THRESHOLD:        float = 3.5
_ZSCORE_WINDOW:        int   = 20
_ZSCORE_THRESHOLD:     float = 4.0
_GAP_TOLERANCE_FACTOR: float = 1.5
_CRITICAL_GAP_PCT:     float = 0.05

_FLATLINE_THRESHOLD_BY_TF: Dict[str, float] = {
    "1m":  0.0001, "5m":  0.0001, "15m": 0.0002,
    "1h":  0.0005, "4h":  0.001,  "1d":  0.002,
}
_FLATLINE_THRESHOLD_DEFAULT: float = 0.0001


# ===========================================================================
# Checker
# ===========================================================================

class DataQualityChecker:
    """
    Ejecuta todos los checks de calidad sobre un DataFrame OHLCV.

    Stateless entre runs — cada llamada a check() es independiente.

    Usage
    -----
    checker = DataQualityChecker(timeframe="1h", exchange="bybit")
    report  = checker.check(df, symbol="BTC/USDT")
    if report.has_critical_issues:
        raise DataQualityError(report.summary())
    """

    def __init__(self, timeframe: str, exchange: str = "unknown", rows_removed: int = 0) -> None:
        self._timeframe          = timeframe
        self._exchange           = exchange
        self._tf_ms              = _TF_MS.get(timeframe)
        self._flatline_threshold = _FLATLINE_THRESHOLD_BY_TF.get(timeframe, _FLATLINE_THRESHOLD_DEFAULT)
        self._rows_removed       = max(0, int(rows_removed))

    def check(self, df: pd.DataFrame, symbol: str) -> DataQualityReport:
        report = DataQualityReport(
            symbol=symbol, timeframe=self._timeframe, exchange=self._exchange,
            rows=len(df) if df is not None else 0,
            checked_at=datetime.now(timezone.utc).isoformat(),
            git_hash=_get_git_hash(),
        )
        if df is None or df.empty:
            report.issues.append(QualityIssue(
                check="empty_dataset", severity="critical",
                description="DataFrame vacío", affected_rows=0,
            ))
            return report

        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        self._check_future_timestamps(ts, report)
        self._check_gaps(ts, report)
        self._check_ohlc_inconsistencies(df, report)
        self._check_outliers_mad(df, report)
        self._check_outliers_rolling_zscore(df, report)
        self._check_flatlines(df, report)
        self._log_result(report, symbol, len(df))
        return report

    def _log_result(self, report: DataQualityReport, symbol: str, rows: int) -> None:
        if report.is_clean:
            logger.debug("Data quality OK | {}/{} exchange={} rows={}", symbol, self._timeframe, self._exchange, rows)
        else:
            level = "error" if report.has_critical_issues else "warning"
            getattr(logger, level)(
                "Data quality issues | {}/{} exchange={} warnings={} criticals={}",
                symbol, self._timeframe, self._exchange,
                len(report.warnings), len(report.criticals),
            )

    @staticmethod
    def _check_future_timestamps(ts: pd.Series, report: DataQualityReport) -> None:
        now = pd.Timestamp.now(tz="UTC")
        n   = int((ts > now).sum())
        if n > 0:
            report.issues.append(QualityIssue(
                check="future_timestamps", severity="critical",
                description=f"{n} timestamps en el futuro", affected_rows=n,
                details={"max_ts": str(ts.max()), "now": str(now)},
            ))

    def _check_gaps(self, ts: pd.Series, report: DataQualityReport) -> None:
        if self._tf_ms is None:
            return
        ts_s  = ts.sort_values().dropna()
        if len(ts_s) < 2:
            return
        diffs = ts_s.diff().dt.total_seconds() * 1000
        mask  = diffs > (self._tf_ms * _GAP_TOLERANCE_FACTOR)
        n     = max(0, int(mask.sum()) - self._rows_removed)
        if n == 0:
            return
        pct     = n / len(ts_s)
        missing = int((diffs[mask] / self._tf_ms).sum() - n)
        report.issues.append(QualityIssue(
            check="temporal_gaps",
            severity="critical" if pct >= _CRITICAL_GAP_PCT else "warning",
            description=f"{n} gaps ({missing} velas faltantes)", affected_rows=n,
            details={"gap_pct": round(pct*100,2), "missing_bars": missing,
                     "largest_gap_s": round(float(diffs[mask].max()/1000),1)},
        ))

    @staticmethod
    def _check_ohlc_inconsistencies(df: pd.DataFrame, report: DataQualityReport) -> None:
        if not {"high","low","open","close"}.issubset(df.columns):
            return
        h, lo, o, c = df["high"], df["low"], df["open"], df["close"]
        violations = {k: int(m.sum()) for k, m in {
            "high_lt_low": h<lo, "close_gt_high": c>h, "close_lt_low": c<lo,
            "open_gt_high": o>h, "open_lt_low": o<lo,
        }.items() if m.sum() > 0}
        if violations:
            report.issues.append(QualityIssue(
                check="ohlc_inconsistencies", severity="critical",
                description=f"Violaciones OHLC en {sum(violations.values())} filas",
                affected_rows=sum(violations.values()), details=violations,
            ))

    @staticmethod
    def _check_outliers_mad(df: pd.DataFrame, report: DataQualityReport) -> None:
        if "close" not in df.columns or len(df) < 5:
            return
        p = df["close"].dropna()
        if len(p) < 5:
            return
        window = min(100, max(10, len(p)//5))
        med = p.rolling(window, min_periods=10, center=True).median()
        mad = (p-med).abs().rolling(window, min_periods=10, center=True).median().replace(0, np.nan)
        mz  = (0.6745*(p-med).abs()/mad).fillna(0)
        n   = int((mz > _MAD_THRESHOLD).sum())
        if n > 0:
            report.issues.append(QualityIssue(
                check="price_outliers_mad", severity="warning",
                description=f"{n} outliers via MAD", affected_rows=n,
                details={"method":"rolling_MAD","window":window,
                         "threshold":_MAD_THRESHOLD,"max_zscore":round(float(mz.max()),2)},
            ))

    @staticmethod
    def _check_outliers_rolling_zscore(df: pd.DataFrame, report: DataQualityReport) -> None:
        if "close" not in df.columns or len(df) < _ZSCORE_WINDOW+1:
            return
        p = df["close"].dropna()
        if len(p) < _ZSCORE_WINDOW+1:
            return
        rm = p.rolling(_ZSCORE_WINDOW, min_periods=10).mean()
        rs = p.rolling(_ZSCORE_WINDOW, min_periods=10).std().replace(0, np.nan)
        z  = ((p-rm)/rs).abs()
        n  = int((z > _ZSCORE_THRESHOLD).sum())
        if n > 0:
            report.issues.append(QualityIssue(
                check="price_outliers_zscore", severity="warning",
                description=f"{n} spikes via rolling z-score (window={_ZSCORE_WINDOW})",
                affected_rows=n,
                details={"method":"rolling_zscore","window":_ZSCORE_WINDOW,
                         "threshold":_ZSCORE_THRESHOLD,
                         "max_zscore": round(float(z.max()),2) if not z.isna().all() else None},
            ))

    def _check_flatlines(self, df: pd.DataFrame, report: DataQualityReport) -> None:
        if not all(c in df.columns for c in ("high","low","close")):
            return
        n = int(((df["high"]-df["low"]) < df["close"]*self._flatline_threshold).sum())
        if n > 0:
            report.issues.append(QualityIssue(
                check="flatline_candles", severity="warning",
                description=f"{n} velas congeladas (high≈low)", affected_rows=n,
                details={"threshold_pct":self._flatline_threshold*100,
                         "timeframe":self._timeframe,"adaptive":True},
            ))



# ---------------------------------------------------------------------------
# Factory — producción de DataQualityChecker como DataQualityCheckerPort
# Movido desde ports/outbound/data_quality_checker.py (D-02: DIP).
# ports/ no puede instanciar clases concretas de application/.
# ---------------------------------------------------------------------------

def native_checker_factory(
    timeframe:    str,
    exchange:     str,
    rows_removed: int,
) -> "DataQualityCheckerPort":
    """
    Factory que produce el DataQualityChecker nativo como DataQualityCheckerPort.

    Fallback para entornos donde GE no está disponible, o como checker de
    referencia en tests de integración.

    SSOT: la implementación concreta (DataQualityChecker) y su factory
    viven juntos en application/quality/ — no en ports/.
    """
    from market_data.ports.outbound.data_quality_checker import DataQualityCheckerPort  # noqa: F401
    return DataQualityChecker(
        timeframe    = timeframe,
        exchange     = exchange,
        rows_removed = rows_removed,
    )


__all__ = [
    "DataQualityChecker",
    # Re-exports desde domain para backward-compat de consumidores directos
    "DataQualityReport",
    "QualityIssue",
    "DataQualityError",
]
