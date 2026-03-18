"""
data_quality.py
===============

Data Quality checks para OHLCV — más allá de la validación de schema.

Responsabilidad
---------------
Detectar anomalías de calidad que Pandera no puede validar porque
requieren contexto temporal o estadístico:

  • Gaps temporales: velas faltantes entre timestamps
  • Outliers de precio: spikes anómalos (>3σ o >factor configurable)
  • Timestamp drift: datos con timestamp > ahora (imposible en histórico)
  • Flatlines: velas con open=high=low=close (precio congelado)

Filosofía
---------
Los checks son NO BLOQUEANTES por defecto — loggean WARNING y
añaden metadata al resultado. El caller decide si tratar como error.
Esto es correcto en pipelines de ingestión donde queremos continuar
aunque haya datos imperfectos, pero registrar los problemas.

Uso
---
from market_data.batch.schemas.data_quality import DataQualityChecker

checker = DataQualityChecker(timeframe="1m", exchange="kucoin")
report  = checker.check(df, symbol="BTC/USDT")

if report.has_critical_issues:
    raise DataQualityError(report.summary())
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger


# ==========================================================
# Constants
# ==========================================================

# Timeframe → milliseconds (para calcular gaps esperados)
_TF_MS: Dict[str, int] = {
    "1m":  60_000,
    "5m":  300_000,
    "15m": 900_000,
    "1h":  3_600_000,
    "4h":  14_400_000,
    "1d":  86_400_000,
}

# Outlier: vela con rango high-low > N veces la mediana del rango
_OUTLIER_RANGE_FACTOR = 10.0

# Flatline: high - low < X% del precio → vela "congelada"
_FLATLINE_THRESHOLD_PCT = 0.0001  # 0.01%

# Gap crítico: más de este % de velas faltantes en el período
_CRITICAL_GAP_PCT = 0.05  # 5%


# ==========================================================
# Result Types
# ==========================================================

@dataclass
class QualityIssue:
    """Una anomalía de calidad encontrada en el dataset."""
    check:       str   # nombre del check
    severity:    str   # "warning" | "critical"
    description: str
    affected_rows: int = 0
    details:     Dict  = field(default_factory=dict)


@dataclass
class DataQualityReport:
    """
    Reporte completo de calidad de datos.

    Incluye metadata de trazabilidad (git_hash, exchange, timeframe)
    para que el reporte sea completamente reproducible.
    """
    symbol:      str
    timeframe:   str
    exchange:    str
    rows:        int
    checked_at:  str
    git_hash:    str
    issues:      List[QualityIssue] = field(default_factory=list)

    @property
    def warnings(self) -> List[QualityIssue]:
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def criticals(self) -> List[QualityIssue]:
        return [i for i in self.issues if i.severity == "critical"]

    @property
    def has_critical_issues(self) -> bool:
        return bool(self.criticals)

    @property
    def is_clean(self) -> bool:
        return not self.issues

    def summary(self) -> str:
        parts = [
            f"DataQualityReport | {self.symbol}/{self.timeframe} "
            f"exchange={self.exchange} rows={self.rows} "
            f"warnings={len(self.warnings)} criticals={len(self.criticals)} "
            f"git={self.git_hash}"
        ]
        for issue in self.issues:
            parts.append(
                f"  [{issue.severity.upper()}] {issue.check}: "
                f"{issue.description} (rows={issue.affected_rows})"
            )
        return "\n".join(parts)

    def to_dict(self) -> Dict:
        return {
            "symbol":      self.symbol,
            "timeframe":   self.timeframe,
            "exchange":    self.exchange,
            "rows":        self.rows,
            "checked_at":  self.checked_at,
            "git_hash":    self.git_hash,
            "warnings":    len(self.warnings),
            "criticals":   len(self.criticals),
            "is_clean":    self.is_clean,
            "issues": [
                {
                    "check":        i.check,
                    "severity":     i.severity,
                    "description":  i.description,
                    "affected_rows": i.affected_rows,
                }
                for i in self.issues
            ],
        }


# ==========================================================
# Exception
# ==========================================================

class DataQualityError(Exception):
    """Lanzada cuando hay issues críticos de calidad."""


# ==========================================================
# Checker
# ==========================================================

class DataQualityChecker:
    """
    Ejecuta checks de calidad sobre un DataFrame OHLCV.

    Uso
    ---
    checker = DataQualityChecker(timeframe="1m", exchange="kucoin")
    report  = checker.check(df, symbol="BTC/USDT")
    logger.info(report.summary())
    """

    def __init__(
        self,
        timeframe: str,
        exchange:  str = "unknown",
    ) -> None:
        self._timeframe = timeframe
        self._exchange  = exchange
        self._tf_ms     = _TF_MS.get(timeframe)

    def check(self, df: pd.DataFrame, symbol: str) -> DataQualityReport:
        """
        Ejecuta todos los checks y devuelve un DataQualityReport.

        No lanza excepción — el caller decide qué hacer con los issues.
        """
        report = DataQualityReport(
            symbol     = symbol,
            timeframe  = self._timeframe,
            exchange   = self._exchange,
            rows       = len(df),
            checked_at = datetime.now(timezone.utc).isoformat(),
            git_hash   = _get_git_hash(),
        )

        if df is None or df.empty:
            report.issues.append(QualityIssue(
                check="empty_dataset",
                severity="critical",
                description="DataFrame vacío",
                affected_rows=0,
            ))
            return report

        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

        # Ejecutar checks
        self._check_future_timestamps(ts, report)
        self._check_gaps(ts, report)
        self._check_outliers(df, report)
        self._check_flatlines(df, report)

        # Log resumen
        if report.is_clean:
            logger.debug(
                "Data quality OK | {}/{} exchange={} rows={}",
                symbol, self._timeframe, self._exchange, len(df),
            )
        else:
            level = "error" if report.has_critical_issues else "warning"
            getattr(logger, level)(
                "Data quality issues | {}/{} exchange={} warnings={} criticals={}",
                symbol, self._timeframe, self._exchange,
                len(report.warnings), len(report.criticals),
            )

        return report

    # ======================================================
    # Individual Checks
    # ======================================================

    @staticmethod
    def _check_future_timestamps(
        ts: pd.Series,
        report: DataQualityReport,
    ) -> None:
        """Detecta timestamps en el futuro (imposible en datos históricos)."""
        now = pd.Timestamp.now(tz="UTC")
        future_mask = ts > now
        n_future = future_mask.sum()

        if n_future > 0:
            report.issues.append(QualityIssue(
                check        = "future_timestamps",
                severity     = "critical",
                description  = f"Timestamps en el futuro detectados",
                affected_rows = int(n_future),
                details      = {"max_ts": str(ts.max()), "now": str(now)},
            ))

    def _check_gaps(
        self,
        ts: pd.Series,
        report: DataQualityReport,
    ) -> None:
        """
        Detecta gaps temporales mayores al esperado para el timeframe.

        Un gap es cuando la diferencia entre dos timestamps consecutivos
        es mayor que el período esperado (ej. > 1 minuto para 1m).
        """
        if self._tf_ms is None:
            return  # timeframe desconocido, no podemos calcular gaps

        ts_sorted = ts.sort_values().dropna()
        if len(ts_sorted) < 2:
            return

        diffs_ms = ts_sorted.diff().dt.total_seconds() * 1000
        # Gap = diferencia > 1.5x el período esperado (tolerancia del 50%)
        expected_ms = self._tf_ms
        gap_mask = diffs_ms > (expected_ms * 1.5)
        n_gaps = gap_mask.sum()

        if n_gaps == 0:
            return

        # ¿Es crítico? Más del 5% de velas faltantes
        total_expected = len(ts_sorted)
        gap_pct = n_gaps / total_expected

        severity = "critical" if gap_pct >= _CRITICAL_GAP_PCT else "warning"

        # Calcular velas totales faltantes (suma de todos los gaps)
        missing_bars = int(
            (diffs_ms[gap_mask] / expected_ms).sum() - n_gaps
        )

        report.issues.append(QualityIssue(
            check        = "temporal_gaps",
            severity     = severity,
            description  = f"{n_gaps} gaps detectados ({missing_bars} velas faltantes)",
            affected_rows = int(n_gaps),
            details      = {
                "gap_pct":       round(gap_pct * 100, 2),
                "missing_bars":  missing_bars,
                "largest_gap_s": round(float(diffs_ms[gap_mask].max() / 1000), 1),
            },
        ))

    @staticmethod
    def _check_outliers(
        df: pd.DataFrame,
        report: DataQualityReport,
    ) -> None:
        """
        Detecta velas con rangos de precio anómalos (spikes).

        Un outlier es una vela donde high-low > N * mediana(high-low).
        Esto captura flash crashes, errores de feed, etc.
        """
        if "high" not in df.columns or "low" not in df.columns:
            return

        ranges = df["high"] - df["low"]
        median_range = ranges.median()

        if median_range <= 0:
            return

        outlier_mask = ranges > (_OUTLIER_RANGE_FACTOR * median_range)
        n_outliers = outlier_mask.sum()

        if n_outliers > 0:
            report.issues.append(QualityIssue(
                check        = "price_outliers",
                severity     = "warning",
                description  = (
                    f"{n_outliers} velas con rango >{_OUTLIER_RANGE_FACTOR}x la mediana"
                ),
                affected_rows = int(n_outliers),
                details      = {
                    "median_range": round(float(median_range), 4),
                    "max_range":    round(float(ranges.max()), 4),
                    "threshold":    round(float(_OUTLIER_RANGE_FACTOR * median_range), 4),
                },
            ))

    @staticmethod
    def _check_flatlines(
        df: pd.DataFrame,
        report: DataQualityReport,
    ) -> None:
        """
        Detecta velas con precio completamente congelado (high ≈ low).

        En crypto, esto suele indicar datos sintéticos, errores de feed
        o períodos de mercado cerrado.
        """
        if not all(c in df.columns for c in ("high", "low", "close")):
            return

        # Flatline: rango < 0.01% del precio de cierre
        close_price = df["close"]
        ranges = df["high"] - df["low"]
        threshold = close_price * _FLATLINE_THRESHOLD_PCT

        flatline_mask = ranges < threshold
        n_flatlines = flatline_mask.sum()

        if n_flatlines > 0:
            report.issues.append(QualityIssue(
                check        = "flatline_candles",
                severity     = "warning",
                description  = f"{n_flatlines} velas con precio congelado (high≈low)",
                affected_rows = int(n_flatlines),
                details      = {
                    "threshold_pct": _FLATLINE_THRESHOLD_PCT * 100,
                },
            ))


# ==========================================================
# Helpers
# ==========================================================

def _get_git_hash() -> str:
    """Obtiene el hash corto del commit actual para trazabilidad."""
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
