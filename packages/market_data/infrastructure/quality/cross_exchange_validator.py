from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
import pandas as pd
from loguru import logger

_PRICE_DIVERGENCE_PCT  = 0.005
_VOLUME_DIVERGENCE_PCT = 0.50
_MIN_OVERLAP_ROWS      = 10

@dataclass
class CrossExchangeIssue:
    check: str
    severity: str
    description: str
    affected_rows: int = 0
    details: dict = field(default_factory=dict)

@dataclass
class CrossExchangeReport:
    symbol: str
    timeframe: str
    exchange_a: str
    exchange_b: str
    overlap_rows: int
    checked_at: str
    issues: list[CrossExchangeIssue] = field(default_factory=list)

    @property
    def has_critical_issues(self) -> bool:
        return any(i.severity == "critical" for i in self.issues)

    @property
    def is_clean(self) -> bool:
        return not self.issues

    def summary(self) -> str:
        lines = [f"CrossExchangeReport | {self.symbol}/{self.timeframe} {self.exchange_a} vs {self.exchange_b} overlap={self.overlap_rows} issues={len(self.issues)}"]
        for i in self.issues:
            lines.append(f"  [{i.severity.upper()}] {i.check}: {i.description} (rows={i.affected_rows})")
        return chr(10).join(lines)

    def to_dict(self) -> dict:
        return {
            "symbol":       self.symbol,
            "timeframe":    self.timeframe,
            "exchange_a":   self.exchange_a,
            "exchange_b":   self.exchange_b,
            "overlap_rows": self.overlap_rows,
            "checked_at":   self.checked_at,
            "warnings":     sum(1 for i in self.issues if i.severity == "warning"),
            "criticals":    sum(1 for i in self.issues if i.severity == "critical"),
            "is_clean":     self.is_clean,
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

class CrossExchangeValidator:
    def validate(self, df_a: pd.DataFrame, df_b: pd.DataFrame, symbol: str, timeframe: str, exchange_a: str, exchange_b: str) -> CrossExchangeReport:
        report = CrossExchangeReport(
            symbol=symbol, timeframe=timeframe,
            exchange_a=exchange_a, exchange_b=exchange_b,
            overlap_rows=0,
            checked_at=datetime.now(timezone.utc).isoformat(),
        )
        ts_a = pd.to_datetime(df_a["timestamp"], utc=True)
        ts_b = pd.to_datetime(df_b["timestamp"], utc=True)
        common = set(ts_a.dt.floor("min")) & set(ts_b.dt.floor("min"))
        if len(common) < _MIN_OVERLAP_ROWS:
            report.issues.append(CrossExchangeIssue(
                check="insufficient_overlap",
                severity="warning",
                description=f"Solo {len(common)} timestamps comunes",
                affected_rows=len(common),
            ))
            return report
        merged = self._align_dataframes(df_a, df_b)
        report.overlap_rows = len(merged)
        self._check_price_divergence(merged, report)
        self._check_volume_divergence(merged, report)
        self._check_timestamp_gaps(ts_a, ts_b, report)
        self._log_result(report, symbol, timeframe, exchange_a, exchange_b)
        return report

    @staticmethod
    def _align_dataframes(df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame:
        """Alinea dos DataFrames por timestamp (floor min). Separado de validate() por SRP."""
        a = df_a.copy()
        a["_ts"] = pd.to_datetime(a["timestamp"], utc=True).dt.floor("min")
        b = df_b.copy()
        b["_ts"] = pd.to_datetime(b["timestamp"], utc=True).dt.floor("min")
        return a.merge(b, on="_ts", suffixes=("_a", "_b"))

    @staticmethod
    def _log_result(
        report: CrossExchangeReport,
        symbol: str,
        timeframe: str,
        exchange_a: str,
        exchange_b: str,
    ) -> None:
        """Emite el log de resultado. Separado de validate() por SRP."""
        if report.is_clean:
            logger.debug(
                "CrossExchange OK | {}/{} {} vs {} overlap={}",
                symbol, timeframe, exchange_a, exchange_b, report.overlap_rows,
            )
        else:
            level = "error" if report.has_critical_issues else "warning"
            getattr(logger, level)(
                "CrossExchange issues | {}/{} {} vs {} warnings={} criticals={}",
                symbol, timeframe, exchange_a, exchange_b,
                sum(1 for i in report.issues if i.severity == "warning"),
                sum(1 for i in report.issues if i.severity == "critical"),
            )

    @staticmethod
    def _check_price_divergence(merged: pd.DataFrame, report: CrossExchangeReport) -> None:
        mid_a = (merged["high_a"] + merged["low_a"]) / 2
        mid_b = (merged["high_b"] + merged["low_b"]) / 2
        avg   = (mid_a + mid_b) / 2
        div   = ((mid_a - mid_b).abs() / avg.replace(0, float("nan")))
        mask  = div > _PRICE_DIVERGENCE_PCT
        n     = int(mask.sum())
        if n > 0:
            sev = "critical" if n / len(merged) > 0.05 else "warning"
            report.issues.append(CrossExchangeIssue(
                check="price_divergence",
                severity=sev,
                description=f"{n} velas con divergencia de precio >{_PRICE_DIVERGENCE_PCT*100:.1f}%",
                affected_rows=n,
                details={
                    "max_div_pct":   round(float(div.max() * 100), 3),
                    "threshold_pct": _PRICE_DIVERGENCE_PCT * 100,
                },
            ))

    @staticmethod
    def _check_volume_divergence(merged: pd.DataFrame, report: CrossExchangeReport) -> None:
        vol_a = merged["volume_a"]
        vol_b = merged["volume_b"]
        avg   = (vol_a + vol_b) / 2
        div   = ((vol_a - vol_b).abs() / avg.replace(0, float("nan")))
        mask  = div > _VOLUME_DIVERGENCE_PCT
        n     = int(mask.sum())
        if n > 0:
            report.issues.append(CrossExchangeIssue(
                check="volume_divergence",
                severity="warning",
                description=f"{n} velas con divergencia de volumen >{_VOLUME_DIVERGENCE_PCT*100:.0f}%",
                affected_rows=n,
                details={"threshold_pct": _VOLUME_DIVERGENCE_PCT * 100},
            ))

    @staticmethod
    def _check_timestamp_gaps(ts_a: pd.Series, ts_b: pd.Series, report: CrossExchangeReport) -> None:
        only_a = len(set(ts_a.dt.floor("min")) - set(ts_b.dt.floor("min")))
        only_b = len(set(ts_b.dt.floor("min")) - set(ts_a.dt.floor("min")))
        if only_a > 0 or only_b > 0:
            report.issues.append(CrossExchangeIssue(
                check="timestamp_mismatch",
                severity="warning",
                description=f"Timestamps exclusivos: {only_a} solo en exchange_a, {only_b} solo en exchange_b",
                affected_rows=only_a + only_b,
                details={"only_in_a": only_a, "only_in_b": only_b},
            ))
