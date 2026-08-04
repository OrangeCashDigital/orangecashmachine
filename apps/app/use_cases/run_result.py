# -*- coding: utf-8 -*-
"""
app/use_cases/run_result.py
=============================

Resultado de un ciclo de trading — compartido por live y paper.

H12 (AUDIT-apps-2026-08-03): LiveRunResult y PaperRunResult eran dataclass
byte-idénticas duplicadas en execute_live.py y execute_paper.py. Un único
CycleRunResult elimina la duplicación sin cambiar el contrato con los CLIs.

Principios: DRY · SSOT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from trading.analytics.performance import PerformanceSummary
    from trading.engine import EngineResult

__all__ = ["CycleRunResult"]


@dataclass
class CycleRunResult:
    """Resultado completo de un ciclo de trading (live o paper).

    Usado por el CLI para determinar el exit code y el logging final.
    """

    success: bool
    error: Optional[str] = None
    engine_result: Optional["EngineResult"] = None
    performance: Optional["PerformanceSummary"] = None
    open_positions: Optional[dict] = None
    oms_summary: Optional[dict] = None

    @property
    def exit_code(self) -> int:
        return 0 if self.success else 1
