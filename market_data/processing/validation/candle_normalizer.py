"""
market_data/processing/validation/candle_normalizer.py
=======================================================

Transform Layer — raw CCXT candles → DataFrame Silver tipado.

Responsabilidad
---------------
Única: convertir ValidationResult[] a pd.DataFrame con schema Silver.
No valida, no escribe, no hace I/O — SRP estricto.

Contrato de entrada
-------------------
Lista de ValidationResult con label CLEAN o SUSPECT.
Velas CORRUPT nunca llegan aquí — el caller filtra antes de llamar.

Contrato de salida (columnas Silver)
--------------------------------------
  timestamp    : pd.Timestamp UTC (timezone-aware)
  open         : float64
  high         : float64
  low          : float64
  close        : float64
  volume       : float64
  quality_flag : str  ("clean" | "suspect")
  exchange     : str
  symbol       : str
  timeframe    : str

Principios
----------
SOLID/SRP  — solo transforma, no valida ni escribe
SSOT       — SILVER_DTYPE_MAP define tipos en un solo lugar
DRY        — tipos aplicados desde el mapa, no inline por columna
KISS       — sin lógica de negocio, solo coerción de tipos + enriquecimiento
"""
from __future__ import annotations

from typing import List

import pandas as pd

from market_data.processing.validation.candle_validator import (
    ValidationResult,
)


# ── Schema SSOT ──────────────────────────────────────────────────────────────

SILVER_DTYPE_MAP: dict = {
    "open":         "float64",
    "high":         "float64",
    "low":          "float64",
    "close":        "float64",
    "volume":       "float64",
    "quality_flag": "object",
    "exchange":     "object",
    "symbol":       "object",
    "timeframe":    "object",
}

_SILVER_COLUMNS = [
    "timestamp", "open", "high", "low", "close", "volume",
    "quality_flag", "exchange", "symbol", "timeframe",
]


# ── Normalizador ──────────────────────────────────────────────────────────────

class CandleNormalizer:
    """
    Convierte ValidationResult[] → pd.DataFrame normalizado para Silver.

    Usage
    -----
    normalizer = CandleNormalizer(exchange="bybit", symbol="BTC/USDT", timeframe="1m")
    df         = normalizer.normalize(validation_results)
    """

    def __init__(self, exchange: str, symbol: str, timeframe: str) -> None:
        self._exchange  = exchange
        self._symbol    = symbol
        self._timeframe = timeframe

    def normalize(self, results: List[ValidationResult]) -> pd.DataFrame:
        """
        Normaliza velas CLEAN y SUSPECT a DataFrame Silver.

        Velas CORRUPT se ignoran — el caller ya las separó y loggeó
        antes de llamar normalize().

        Returns
        -------
        pd.DataFrame con schema Silver. Puede estar vacío si todos
        los results eran CORRUPT.
        """
        accepted = [r for r in results if not r.is_corrupt]
        if not accepted:
            return self._empty_frame()

        rows = []
        for result in accepted:
            c = result.candle
            rows.append({
                "timestamp":    pd.Timestamp(int(c[0]), unit="ms", tz="UTC"),
                "open":         float(c[1]),
                "high":         float(c[2]),
                "low":          float(c[3]),
                "close":        float(c[4]),
                "volume":       float(c[5]),
                "quality_flag": result.label.value,
                "exchange":     self._exchange,
                "symbol":       self._symbol,
                "timeframe":    self._timeframe,
            })

        df = pd.DataFrame(rows)
        df = df.astype({k: v for k, v in SILVER_DTYPE_MAP.items() if k in df.columns})
        return df.sort_values("timestamp").reset_index(drop=True)

    def _empty_frame(self) -> pd.DataFrame:
        return pd.DataFrame(columns=_SILVER_COLUMNS)
