# -*- coding: utf-8 -*-
"""
market_data/application/use_cases/candle_normalizer.py
=======================================================

Transform Layer — ValidationResult[] → pl.DataFrame Silver tipado.

Migración Fase 3 — pandas → polars
------------------------------------
Produce pl.DataFrame nativo. El único lugar donde sigue existiendo
pd.DataFrame en la firma pública es el ACL de OHLCVTransformer.transform()
— este módulo ya no es parte de ese boundary.

Responsabilidad única (SRP)
---------------------------
Convertir ValidationResult[] a pl.DataFrame con schema Silver.
No valida, no escribe, no hace I/O.

Contrato de salida (columnas Silver)
--------------------------------------
  timestamp    : Datetime("us", "UTC")
  open         : Float64
  high         : Float64
  low          : Float64
  close        : Float64
  volume       : Float64
  quality_flag : Utf8  ("clean" | "suspect")
  exchange     : Utf8
  symbol       : Utf8
  timeframe    : Utf8

Principios: SOLID/SRP · SSOT · DRY · KISS
"""

from __future__ import annotations

from typing import List

import polars as pl

from market_data.domain.value_objects.candle_validator import ValidationResult

# ── Schema SSOT ──────────────────────────────────────────────────────────────

SILVER_SCHEMA: dict = {
    "timestamp": pl.Datetime("us", "UTC"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "quality_flag": pl.Utf8,
    "exchange": pl.Utf8,
    "symbol": pl.Utf8,
    "timeframe": pl.Utf8,
}

_SILVER_COLUMNS: list[str] = list(SILVER_SCHEMA.keys())


# ── Normalizador ──────────────────────────────────────────────────────────────


class CandleNormalizer:
    """
    Convierte ValidationResult[] → pl.DataFrame normalizado para Silver.

    Usage
    -----
    normalizer = CandleNormalizer(exchange="bybit", symbol="BTC/USDT", timeframe="1m")
    df         = normalizer.normalize(validation_results)
    """

    def __init__(self, exchange: str, symbol: str, timeframe: str) -> None:
        self._exchange = exchange
        self._symbol = symbol
        self._timeframe = timeframe

    def normalize(self, results: List[ValidationResult]) -> pl.DataFrame:
        """
        Normaliza velas CLEAN y SUSPECT a pl.DataFrame Silver.

        Velas CORRUPT se ignoran — el caller ya las separó y loggeó
        antes de llamar normalize().

        Returns
        -------
        pl.DataFrame con schema Silver. Puede estar vacío si todos
        los results eran CORRUPT.
        """
        accepted = [r for r in results if not r.is_corrupt]
        if not accepted:
            return self._empty_frame()

        # Construir columnas separadas — más eficiente que lista de dicts
        ts_ms_list: list[int] = []
        open_list: list[float] = []
        high_list: list[float] = []
        low_list: list[float] = []
        close_list: list[float] = []
        volume_list: list[float] = []
        flag_list: list[str] = []

        for result in accepted:
            c = result.candle
            ts_ms_list.append(int(c[0]))
            open_list.append(float(c[1]))
            high_list.append(float(c[2]))
            low_list.append(float(c[3]))
            close_list.append(float(c[4]))
            volume_list.append(float(c[5]))
            flag_list.append(result.label.value)

        n = len(accepted)
        df = pl.DataFrame(
            {
                "timestamp": pl.Series(ts_ms_list, dtype=pl.Int64)
                .cast(pl.Datetime("ms"))
                .dt.replace_time_zone("UTC")
                .dt.cast_time_unit("us"),
                "open": pl.Series(open_list, dtype=pl.Float64),
                "high": pl.Series(high_list, dtype=pl.Float64),
                "low": pl.Series(low_list, dtype=pl.Float64),
                "close": pl.Series(close_list, dtype=pl.Float64),
                "volume": pl.Series(volume_list, dtype=pl.Float64),
                "quality_flag": pl.Series(flag_list, dtype=pl.Utf8),
                "exchange": pl.Series([self._exchange] * n, dtype=pl.Utf8),
                "symbol": pl.Series([self._symbol] * n, dtype=pl.Utf8),
                "timeframe": pl.Series([self._timeframe] * n, dtype=pl.Utf8),
            }
        )

        return df.sort("timestamp")

    def _empty_frame(self) -> pl.DataFrame:
        return pl.DataFrame(schema=SILVER_SCHEMA)


__all__ = ["CandleNormalizer", "SILVER_SCHEMA"]
