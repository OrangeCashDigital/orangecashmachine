# -*- coding: utf-8 -*-
"""
market_data/domain/quality/types.py
=====================================

Value Objects de calidad de datos — tipos puros del dominio.

Responsabilidad
---------------
Definir DataQualityReport y QualityIssue como tipos de dominio.
Sin dependencias de pandas ni de infraestructura.

Consumidores legítimos
----------------------
  domain/policies/data_quality_policy.py  — evalúa el report
  ports/outbound/data_quality_checker.py  — contrato del checker
  application/quality/data_quality.py     — produce el report (checker)
  application/quality/pipeline.py         — consume el report

Principios: SRP · DIP · SSOT
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List


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


@dataclass
class QualityIssue:
    """
    Issue de calidad detectado en un DataFrame OHLCV.

    Value Object — inmutable por convención.

    Attributes
    ----------
    check         : identificador del check (e.g. "temporal_gaps")
    severity      : "warning" | "critical"
    description   : descripción legible del problema
    affected_rows : número de filas afectadas
    details       : dict con métricas auxiliares para debugging
    """
    check:         str
    severity:      str
    description:   str
    affected_rows: int  = 0
    details:       Dict = field(default_factory=dict)


@dataclass
class DataQualityReport:
    """
    Resultado completo de ejecutar DataQualityChecker sobre un DataFrame.

    Value Object — inmutable por convención post-construcción.

    Attributes
    ----------
    symbol     : par de trading evaluado
    timeframe  : resolución temporal
    exchange   : exchange de origen
    rows       : número de filas evaluadas
    checked_at : timestamp UTC de la ejecución
    git_hash   : hash de git del código que generó el report (trazabilidad)
    issues     : lista de QualityIssue detectados (vacía = datos limpios)
    """
    symbol:     str
    timeframe:  str
    exchange:   str
    rows:       int
    checked_at: str
    git_hash:   str
    issues:     List[QualityIssue] = field(default_factory=list)

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

    @property
    def affected_rows_total(self) -> int:
        return sum(i.affected_rows for i in self.issues)

    def issues_by_check(self, name: str) -> List[QualityIssue]:
        return [i for i in self.issues if i.check == name]

    def summary(self) -> str:
        parts = [
            f"DataQualityReport | {self.symbol}/{self.timeframe} "
            f"exchange={self.exchange} rows={self.rows} "
            f"warnings={len(self.warnings)} criticals={len(self.criticals)} "
            f"git={self.git_hash}"
        ]
        for i in self.issues:
            parts.append(
                f"  [{i.severity.upper()}] {i.check}: "
                f"{i.description} (rows={i.affected_rows})"
            )
        return "\n".join(parts)

    def to_dict(self) -> Dict:
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
                    "check":         i.check,
                    "severity":      i.severity,
                    "description":   i.description,
                    "affected_rows": i.affected_rows,
                    "details":       i.details,
                }
                for i in self.issues
            ],
        }


__all__ = ["QualityIssue", "DataQualityReport"]
