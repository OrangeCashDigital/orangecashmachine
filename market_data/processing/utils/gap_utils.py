"""
market_data/processing/utils/gap_utils.py
==========================================
Detección de huecos temporales en DataFrames OHLCV.

Extraído de repair.py para romper el import circular con quality/pipeline.py.
Ambos módulos importan desde processing/utils/ — sin dependencias cruzadas.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd

from domain.value_objects.timeframe import timeframe_to_ms


@dataclass(frozen=True)
class GapRange:
    start_ms: int
    end_ms:   int
    expected: int
    run_id:   str = ""  # lineage: run que detectó este gap (SSOT con run_registry)

    def __str__(self) -> str:
        start = pd.Timestamp(self.start_ms, unit="ms", tz="UTC").isoformat()
        end   = pd.Timestamp(self.end_ms,   unit="ms", tz="UTC").isoformat()
        return f"Gap[{start} → {end} expected={self.expected}]"

    @property
    def duration_ms(self) -> int:
        return self.end_ms - self.start_ms

    @property
    def severity(self) -> str:
        if self.expected <= 2:
            return "low"
        elif self.expected <= 10:
            return "medium"
        return "high"


def scan_gaps(df: pd.DataFrame, timeframe: str, tolerance: int = 0) -> List[GapRange]:
    """
    Detecta huecos temporales en un DataFrame OHLCV ordenado.
    Retorna lista de GapRange. Lista vacía = sin huecos.
    """
    if df is None or df.empty or len(df) < 2:
        return []

    tf_ms     = timeframe_to_ms(timeframe)
    threshold = tf_ms * (tolerance + 2)

    df_sorted = df.sort_values("timestamp").reset_index(drop=True)
    ts_col    = df_sorted["timestamp"]
    if hasattr(ts_col.dtype, "tz"):
        ts_ms = (ts_col.astype("int64") // 1_000_000).values
    else:
        ts_ms = ts_col.astype("int64").values

    gaps: List[GapRange] = []
    for i in range(len(ts_ms) - 1):
        delta = int(ts_ms[i + 1]) - int(ts_ms[i])
        if delta >= threshold:
            expected = delta // tf_ms - 1
            gaps.append(GapRange(
                start_ms=int(ts_ms[i]),
                end_ms=int(ts_ms[i + 1]),
                expected=int(expected),
            ))
    return gaps
