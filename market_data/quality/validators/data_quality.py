from __future__ import annotations

from core.config.lineage import get_git_hash as _get_git_hash
from dataclasses import dataclass, field
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from loguru import logger

from market_data.processing.utils.timeframe import timeframe_to_ms

_TF_MS = {tf: timeframe_to_ms(tf) for tf in ["1m", "5m", "15m", "1h", "4h", "1d"]}
_MAD_THRESHOLD              = 3.5
_ZSCORE_WINDOW              = 20
_ZSCORE_THRESHOLD           = 4.0
_GAP_TOLERANCE_FACTOR       = 1.5
_CRITICAL_GAP_PCT           = 0.05
_FLATLINE_THRESHOLD_BY_TF   = {
    "1m": 0.0001, "5m": 0.0001, "15m": 0.0002,
    "1h": 0.0005, "4h": 0.001,  "1d": 0.002,
}
_FLATLINE_THRESHOLD_DEFAULT = 0.0001


@dataclass
class QualityIssue:
    check: str
    severity: str
    description: str
    affected_rows: int = 0
    details: dict = field(default_factory=dict)


@dataclass
class DataQualityReport:
    symbol: str
    timeframe: str
    exchange: str
    rows: int
    checked_at: str
    git_hash: str
    issues: list[QualityIssue] = field(default_factory=list)

    @property
    def warnings(self):
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def criticals(self):
        return [i for i in self.issues if i.severity == "critical"]

    @property
    def has_critical_issues(self):
        return bool(self.criticals)

    @property
    def is_clean(self):
        return not self.issues

    @property
    def affected_rows_total(self):
        return sum(i.affected_rows for i in self.issues)

    def issues_by_check(self, name):
        return [i for i in self.issues if i.check == name]

    def summary(self):
        parts = [
            f"DataQualityReport | {self.symbol}/{self.timeframe} exchange={self.exchange}"
            f" rows={self.rows} warnings={len(self.warnings)}"
            f" criticals={len(self.criticals)} git={self.git_hash}"
        ]
        for i in self.issues:
            parts.append(f"  [{i.severity.upper()}] {i.check}: {i.description} (rows={i.affected_rows})")
        return chr(10).join(parts)

    def to_dict(self):
        return {
            "symbol":    self.symbol,
            "timeframe": self.timeframe,
            "exchange":  self.exchange,
            "rows":      self.rows,
            "checked_at": self.checked_at,
            "git_hash":  self.git_hash,
            "warnings":  len(self.warnings),
            "criticals": len(self.criticals),
            "is_clean":  self.is_clean,
            "issues": [
                {
                    "check":        i.check,
                    "severity":     i.severity,
                    "description":  i.description,
                    "affected_rows": i.affected_rows,
                    "details":      i.details,
                }
                for i in self.issues
            ],
        }


class DataQualityError(Exception):
    pass


class DataQualityChecker:
    def __init__(self, timeframe, exchange="unknown"):
        self._timeframe          = timeframe
        self._exchange           = exchange
        self._tf_ms              = _TF_MS.get(timeframe)
        self._flatline_threshold = _FLATLINE_THRESHOLD_BY_TF.get(timeframe, _FLATLINE_THRESHOLD_DEFAULT)

    def check(self, df, symbol):
        report = DataQualityReport(
            symbol=symbol,
            timeframe=self._timeframe,
            exchange=self._exchange,
            rows=len(df) if df is not None else 0,
            checked_at=datetime.now(timezone.utc).isoformat(),
            git_hash=_get_git_hash(),
        )
        if df is None or df.empty:
            report.issues.append(QualityIssue(
                check="empty_dataset",
                severity="critical",
                description="DataFrame vacio",
                affected_rows=0,
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
        """Emite el log de resultado. Separado de check() por SRP."""
        if report.is_clean:
            logger.debug(
                "Data quality OK | {}/{} exchange={} rows={}",
                symbol, self._timeframe, self._exchange, rows,
            )
        else:
            level = "error" if report.has_critical_issues else "warning"
            getattr(logger, level)(
                "Data quality issues | {}/{} exchange={} warnings={} criticals={}",
                symbol, self._timeframe, self._exchange,
                len(report.warnings), len(report.criticals),
            )

    @staticmethod
    def _check_future_timestamps(ts, report):
        now = pd.Timestamp.now(tz="UTC")
        n   = int((ts > now).sum())
        if n > 0:
            report.issues.append(QualityIssue(
                check="future_timestamps",
                severity="critical",
                description=f"{n} timestamps en el futuro",
                affected_rows=n,
                details={"max_ts": str(ts.max()), "now": str(now)},
            ))

    def _check_gaps(self, ts, report):
        if self._tf_ms is None:
            return
        ts_s = ts.sort_values().dropna()
        if len(ts_s) < 2:
            return
        diffs = ts_s.diff().dt.total_seconds() * 1000
        mask  = diffs > (self._tf_ms * _GAP_TOLERANCE_FACTOR)
        n     = int(mask.sum())
        if n == 0:
            return
        pct     = n / len(ts_s)
        sev     = "critical" if pct >= _CRITICAL_GAP_PCT else "warning"
        missing = int((diffs[mask] / self._tf_ms).sum() - n)
        report.issues.append(QualityIssue(
            check="temporal_gaps",
            severity=sev,
            description=f"{n} gaps ({missing} velas faltantes)",
            affected_rows=n,
            details={
                "gap_pct":      round(pct * 100, 2),
                "missing_bars": missing,
                "largest_gap_s": round(float(diffs[mask].max() / 1000), 1),
            },
        ))

    @staticmethod
    def _check_ohlc_inconsistencies(df, report):
        if not {"high", "low", "open", "close"}.issubset(df.columns):
            return
        h, lo, o, c = df["high"], df["low"], df["open"], df["close"]
        v = {
            k: int(m.sum())
            for k, m in {
                "high_lt_low":   h < lo,
                "close_gt_high": c > h,
                "close_lt_low":  c < lo,
                "open_gt_high":  o > h,
                "open_lt_low":   o < lo,
            }.items()
            if m.sum() > 0
        }
        if v:
            report.issues.append(QualityIssue(
                check="ohlc_inconsistencies",
                severity="critical",
                description=f"Violaciones OHLC en {sum(v.values())} filas",
                affected_rows=sum(v.values()),
                details=v,
            ))

    @staticmethod
    def _check_outliers_mad(df, report):
        if "close" not in df.columns or len(df) < 5:
            return
        p = df["close"].dropna()
        if len(p) < 5:
            return
        # Rolling MAD (window adaptativo) en lugar de MAD global.
        # El MAD global detecta movimientos históricos legítimos de mercado
        # (e.g. BTC $3k→$69k) como outliers — falsos positivos masivos.
        # Rolling MAD evalúa cada vela en su contexto local.
        window = min(100, max(10, len(p) // 5))
        med = p.rolling(window, min_periods=10, center=True).median()
        mad = (p - med).abs().rolling(window, min_periods=10, center=True).median()
        mad = mad.replace(0, np.nan)  # evitar división por cero en baja volatilidad
        mz  = (0.6745 * (p - med).abs() / mad).fillna(0)
        n   = int((mz > _MAD_THRESHOLD).sum())
        if n > 0:
            report.issues.append(QualityIssue(
                check="price_outliers_mad",
                severity="warning",
                description=f"{n} outliers via MAD",
                affected_rows=n,
                details={
                    "method":     "rolling_MAD",
                    "window":     window,
                    "threshold":  _MAD_THRESHOLD,
                    "max_zscore": round(float(mz.max()), 2),
                },
            ))

    @staticmethod
    def _check_outliers_rolling_zscore(df, report):
        if "close" not in df.columns or len(df) < _ZSCORE_WINDOW + 1:
            return
        p = df["close"].dropna()
        if len(p) < _ZSCORE_WINDOW + 1:
            return
        rm = p.rolling(_ZSCORE_WINDOW, min_periods=10).mean()
        rs = p.rolling(_ZSCORE_WINDOW, min_periods=10).std().replace(0, np.nan)
        z  = ((p - rm) / rs).abs()
        n  = int((z > _ZSCORE_THRESHOLD).sum())
        if n > 0:
            report.issues.append(QualityIssue(
                check="price_outliers_zscore",
                severity="warning",
                description=f"{n} spikes via rolling z-score (window={_ZSCORE_WINDOW})",
                affected_rows=n,
                details={
                    "method":     "rolling_zscore",
                    "window":     _ZSCORE_WINDOW,
                    "threshold":  _ZSCORE_THRESHOLD,
                    "max_zscore": round(float(z.max()), 2) if not z.isna().all() else None,
                },
            ))

    def _check_flatlines(self, df, report):
        if not all(c in df.columns for c in ("high", "low", "close")):
            return
        n = int(((df["high"] - df["low"]) < df["close"] * self._flatline_threshold).sum())
        if n > 0:
            report.issues.append(QualityIssue(
                check="flatline_candles",
                severity="warning",
                description=f"{n} velas congeladas (high~=low)",
                affected_rows=n,
                details={
                    "threshold_pct": self._flatline_threshold * 100,
                    "timeframe":     self._timeframe,
                    "adaptive":      True,
                },
            ))
