# -*- coding: utf-8 -*-
"""
market_data/application/quality/data_quality.py
=================================================

DataQualityChecker — checks estadísticos sobre DataFrames OHLCV.

Migración Fase 3 — pandas → polars
------------------------------------
Todo el procesamiento interno opera sobre pl.DataFrame nativo.
La firma pública check(df: pl.DataFrame) acepta polars directamente.
Los callers (QualityPipeline) ya reciben pl.DataFrame desde OHLCVTransformer.

Principios: SRP · DIP · SSOT · KISS · Fail-Fast
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from market_data.ports.outbound.data_quality_checker import DataQualityCheckerPort

import polars as pl
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
            capture_output=True,
            text=True,
            timeout=2,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


# ===========================================================================
# Constantes — SSOT
# ===========================================================================

_TF_MS: Dict[str, int] = {tf: timeframe_to_ms(tf) for tf in ["1m", "5m", "15m", "1h", "4h", "1d"]}

_MAD_THRESHOLD: float = 3.5
_ZSCORE_WINDOW: int = 20
_ZSCORE_THRESHOLD: float = 4.0
_GAP_TOLERANCE_FACTOR: float = 1.5
_CRITICAL_GAP_PCT: float = 0.05

_FLATLINE_THRESHOLD_BY_TF: Dict[str, float] = {
    "1m": 0.0001,
    "5m": 0.0001,
    "15m": 0.0002,
    "1h": 0.0005,
    "4h": 0.001,
    "1d": 0.002,
}
_FLATLINE_THRESHOLD_DEFAULT: float = 0.0001


# ===========================================================================
# Checker
# ===========================================================================


class DataQualityChecker:
    """
    Ejecuta todos los checks de calidad sobre un DataFrame OHLCV.

    Stateless entre runs — cada llamada a check() es independiente.
    Opera sobre pl.DataFrame nativo — sin conversiones internas.

    Usage
    -----
    checker = DataQualityChecker(timeframe="1h", exchange="bybit")
    report  = checker.check(df, symbol="BTC/USDT")
    if report.has_critical_issues:
        raise DataQualityError(report.summary())
    """

    def __init__(
        self,
        timeframe: str,
        exchange: str = "unknown",
        rows_removed: int = 0,
    ) -> None:
        self._timeframe = timeframe
        self._exchange = exchange
        self._tf_ms = _TF_MS.get(timeframe)
        self._flatline_threshold = _FLATLINE_THRESHOLD_BY_TF.get(timeframe, _FLATLINE_THRESHOLD_DEFAULT)
        self._rows_removed = max(0, int(rows_removed))

    def check(self, df: pl.DataFrame, symbol: str) -> DataQualityReport:
        report = DataQualityReport(
            symbol=symbol,
            timeframe=self._timeframe,
            exchange=self._exchange,
            rows=len(df) if df is not None else 0,
            checked_at=datetime.now(timezone.utc).isoformat(),
            git_hash=_get_git_hash(),
        )
        if df is None or df.is_empty():
            report.issues.append(
                QualityIssue(
                    check="empty_dataset",
                    severity="critical",
                    description="DataFrame vacío",
                    affected_rows=0,
                )
            )
            return report

        # Extraer serie timestamp una sola vez — reutilizada en múltiples checks
        ts = df["timestamp"]
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
            logger.debug(
                "Data quality OK | {}/{} exchange={} rows={}",
                symbol,
                self._timeframe,
                self._exchange,
                rows,
            )
        else:
            level = "error" if report.has_critical_issues else "warning"
            getattr(logger, level)(
                "Data quality issues | {}/{} exchange={} warnings={} criticals={}",
                symbol,
                self._timeframe,
                self._exchange,
                len(report.warnings),
                len(report.criticals),
            )

    @staticmethod
    def _check_future_timestamps(ts: pl.Series, report: DataQualityReport) -> None:
        now_us = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
        # ts es Datetime("us","UTC") — comparar en microsegundos epoch
        ts_us = ts.dt.timestamp("us")
        n = int((ts_us > now_us).sum())
        if n > 0:
            max_ts = ts.max()
            report.issues.append(
                QualityIssue(
                    check="future_timestamps",
                    severity="critical",
                    description=f"{n} timestamps en el futuro",
                    affected_rows=n,
                    details={"max_ts": str(max_ts), "now_us": now_us},
                )
            )

    def _check_gaps(self, ts: pl.Series, report: DataQualityReport) -> None:
        if self._tf_ms is None:
            return
        # Ordenar y calcular diferencias en ms
        ts_sorted = ts.sort().drop_nulls()
        if len(ts_sorted) < 2:
            return
        ts_ms = ts_sorted.dt.timestamp("ms")
        diffs_ms = ts_ms.diff().drop_nulls()
        threshold_ms = self._tf_ms * _GAP_TOLERANCE_FACTOR
        mask = diffs_ms > threshold_ms
        n = max(0, int(mask.sum()) - self._rows_removed)
        if n == 0:
            return
        pct = n / len(ts_sorted)
        # Velas faltantes estimadas: suma de (gap / tf) - 1 por gap
        missing = int(((diffs_ms.filter(mask) / self._tf_ms) - 1).sum())
        largest_gap_s = float(diffs_ms.filter(mask).max() or 0) / 1000
        report.issues.append(
            QualityIssue(
                check="temporal_gaps",
                severity="critical" if pct >= _CRITICAL_GAP_PCT else "warning",
                description=f"{n} gaps ({missing} velas faltantes)",
                affected_rows=n,
                details={
                    "gap_pct": round(pct * 100, 2),
                    "missing_bars": missing,
                    "largest_gap_s": round(largest_gap_s, 1),
                },
            )
        )

    @staticmethod
    def _check_ohlc_inconsistencies(df: pl.DataFrame, report: DataQualityReport) -> None:
        required = {"high", "low", "open", "close"}
        if not required.issubset(df.columns):
            return
        h, lo, o, c = df["high"], df["low"], df["open"], df["close"]
        violations = {
            k: int(mask.sum())
            for k, mask in {
                "high_lt_low": h < lo,
                "close_gt_high": c > h,
                "close_lt_low": c < lo,
                "open_gt_high": o > h,
                "open_lt_low": o < lo,
            }.items()
            if mask.sum() > 0
        }
        if violations:
            report.issues.append(
                QualityIssue(
                    check="ohlc_inconsistencies",
                    severity="critical",
                    description=f"Violaciones OHLC en {sum(violations.values())} filas",
                    affected_rows=sum(violations.values()),
                    details=violations,
                )
            )

    @staticmethod
    def _check_outliers_mad(df: pl.DataFrame, report: DataQualityReport) -> None:
        if "close" not in df.columns or len(df) < 5:
            return
        close = df["close"].drop_nulls()
        if len(close) < 5:
            return
        window = min(100, max(10, len(close) // 5))
        # Rolling median via map_elements — polars no tiene rolling_median nativo
        # en 1.x; usamos sort+quantile rolling aproximado con list.eval
        close_list = close.to_list()
        import statistics

        mz_vals = []
        for i in range(len(close_list)):
            lo_w = max(0, i - window // 2)
            hi_w = min(len(close_list), i + window // 2 + 1)
            window_vals = close_list[lo_w:hi_w]
            med = statistics.median(window_vals)
            abs_devs = [abs(v - med) for v in window_vals]
            mad = statistics.median(abs_devs) if abs_devs else 0
            if mad == 0:
                mz_vals.append(0.0)
            else:
                mz_vals.append(0.6745 * abs(close_list[i] - med) / mad)
        n = sum(1 for v in mz_vals if v > _MAD_THRESHOLD)
        if n > 0:
            report.issues.append(
                QualityIssue(
                    check="price_outliers_mad",
                    severity="warning",
                    description=f"{n} outliers via MAD",
                    affected_rows=n,
                    details={
                        "method": "rolling_MAD",
                        "window": window,
                        "threshold": _MAD_THRESHOLD,
                        "max_zscore": round(max(mz_vals), 2) if mz_vals else 0.0,
                    },
                )
            )

    @staticmethod
    def _check_outliers_rolling_zscore(df: pl.DataFrame, report: DataQualityReport) -> None:
        if "close" not in df.columns or len(df) < _ZSCORE_WINDOW + 1:
            return
        close = df["close"].drop_nulls()
        if len(close) < _ZSCORE_WINDOW + 1:
            return
        # Rolling mean y std con polars
        rm = close.rolling_mean(window_size=_ZSCORE_WINDOW, min_periods=10)
        rs = close.rolling_std(window_size=_ZSCORE_WINDOW, min_periods=10)
        # std=0 → null para evitar div-by-zero; luego fill_null(0)
        rs_safe = rs.map_elements(
            lambda x: None if (x is None or x == 0) else x,
            return_dtype=pl.Float64,
        )
        z = ((close - rm) / rs_safe).abs().fill_null(0.0)
        n = int((z > _ZSCORE_THRESHOLD).sum())
        if n > 0:
            max_z = float(z.max() or 0.0)
            report.issues.append(
                QualityIssue(
                    check="price_outliers_zscore",
                    severity="warning",
                    description=f"{n} spikes via rolling z-score (window={_ZSCORE_WINDOW})",
                    affected_rows=n,
                    details={
                        "method": "rolling_zscore",
                        "window": _ZSCORE_WINDOW,
                        "threshold": _ZSCORE_THRESHOLD,
                        "max_zscore": round(max_z, 2),
                    },
                )
            )

    def _check_flatlines(self, df: pl.DataFrame, report: DataQualityReport) -> None:
        if not all(c in df.columns for c in ("high", "low", "close")):
            return
        spread = df["high"] - df["low"]
        threshold = df["close"] * self._flatline_threshold
        n = int((spread < threshold).sum())
        if n > 0:
            report.issues.append(
                QualityIssue(
                    check="flatline_candles",
                    severity="warning",
                    description=f"{n} velas congeladas (high≈low)",
                    affected_rows=n,
                    details={
                        "threshold_pct": self._flatline_threshold * 100,
                        "timeframe": self._timeframe,
                        "adaptive": True,
                    },
                )
            )


# ---------------------------------------------------------------------------
# Factory — Composition Root la inyecta en QualityPipeline
# ---------------------------------------------------------------------------


def native_checker_factory(
    timeframe: str,
    exchange: str,
    rows_removed: int,
) -> "DataQualityCheckerPort":
    """
    Factory que produce el DataQualityChecker nativo como DataQualityCheckerPort.

    Fallback para entornos donde GE no está disponible.
    SSOT: implementación concreta y factory viven juntos en application/quality/.
    """
    return DataQualityChecker(
        timeframe=timeframe,
        exchange=exchange,
        rows_removed=rows_removed,
    )


__all__ = [
    "DataQualityChecker",
    "DataQualityReport",
    "QualityIssue",
    "DataQualityError",
]
