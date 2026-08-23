# -*- coding: utf-8 -*-
"""
tests/features/test_feature_engineer_edge_cases.py
==================================================

Tests de casos límite del transformer Gold (GoldTransformer).

Migrado de FeatureEngineer (tombstone v2.0.0 → eliminado v3.0.0)
a GoldTransformer, que es el SSOT activo de features Gold.

Cobertura
---------
  - close=0 como numerador  → log_return NaN, nunca -inf
  - close=0 como denominador (prev_close) → log_return NaN, nunca +inf
  - rachas de ceros consecutivos → no inf, propagación NaN correcta
  - volume=0 → no inf en vwap
  - semántica NaN: no imputación silenciosa (responsabilidad del caller)
  - input normal → ninguna columna con ±inf

Principios
----------
SSOT  — GoldTransformer es la única implementación activa
SRP   — cada test verifica una sola propiedad
DRY   — helpers _make_df / _has_inf reutilizados en todos los tests
"""

from __future__ import annotations

import numpy as np
import polars as pl
from market_data.infrastructure.storage.gold.transformer import GoldTransformer

# ── constantes de prueba — SSOT local ────────────────────────────────────────

_SYMBOL = "BTC/USDT"
_TIMEFRAME = "1h"
_EXCHANGE = "bybit"


# ── helpers ───────────────────────────────────────────────────────────────────


def _make_df(n: int = 30, *, base_close: float = 100.0) -> pl.DataFrame:
    """
    DataFrame OHLCV sintético que simula salida Silver válida.

    - close siempre > 0 por defecto (abs + 1.0)
    - sin quality_flag — GoldTransformer lo tolera (is_suspect=False)
    - timestamp como float (GoldTransformer no requiere datetime en tests)
    """
    rng = np.random.default_rng(42)
    close = base_close + rng.standard_normal(n).cumsum()
    close = np.abs(close) + 1.0
    return pl.DataFrame(
        {
            "timestamp": np.arange(n, dtype=np.float64),
            "open": close * 0.99,
            "high": close * 1.01,
            "low": close * 0.98,
            "close": close,
            "volume": rng.uniform(100, 1_000, n),
        }
    )


def _transform(df: pl.DataFrame) -> pl.DataFrame:
    """Invoca GoldTransformer.transform con parámetros de prueba fijos."""
    return GoldTransformer.transform(
        df,
        symbol=_SYMBOL,
        timeframe=_TIMEFRAME,
        exchange=_EXCHANGE,
    )


def _has_inf(df: pl.DataFrame) -> bool:
    """True si alguna columna numérica contiene ±inf."""
    numeric_cols = [c for c, t in zip(df.columns, df.dtypes, strict=True) if t.is_numeric()]
    if not numeric_cols:
        return False
    inf_mask = df.select([pl.col(c).is_infinite().any() for c in numeric_cols])
    return bool(inf_mask.row(0) and any(inf_mask.row(0)))


# ── Tests: close == 0 (numerador) ────────────────────────────────────────────


class TestCloseZeroNumerator:
    """close[i] == 0 → log_return[i] debe ser null (inf convertido a null), nunca -inf."""

    def test_single_zero_close_produces_null(self):
        df = _make_df(30)
        df = df.with_columns(pl.when(pl.arange(0, 30) == 10).then(0.0).otherwise(pl.col("close")).alias("close"))
        result = _transform(df)
        assert result["log_return"].item(10) is None, "close=0 debe producir null en log_return (inf → null), no -inf"

    def test_single_zero_close_no_inf(self):
        df = _make_df(30)
        df = df.with_columns(pl.when(pl.arange(0, 30) == 5).then(0.0).otherwise(pl.col("close")).alias("close"))
        result = _transform(df)
        assert not _has_inf(result), "No debe haber ±inf tras close=0"


# ── Tests: close == 0 (denominador / prev_close) ─────────────────────────────


class TestCloseZeroDenominator:
    """prev_close == 0 → log_return[i+1] debe ser null (inf → null), nunca +inf."""

    def test_prev_close_zero_produces_null(self):
        df = _make_df(30)
        df = df.with_columns(pl.when(pl.arange(0, 30) == 7).then(0.0).otherwise(pl.col("close")).alias("close"))
        result = _transform(df)
        msg = "prev_close=0 debe producir null en log_return (inf → null), no +inf"
        assert result["log_return"].item(8) is None, msg

    def test_prev_close_zero_no_inf(self):
        df = _make_df(30)
        df = df.with_columns(pl.when(pl.arange(0, 30) == 7).then(0.0).otherwise(pl.col("close")).alias("close"))
        result = _transform(df)
        assert not _has_inf(result), "No debe haber +inf cuando prev_close=0"


# ── Tests: secuencia de ceros consecutivos ────────────────────────────────────


class TestZeroSequences:
    """Rachas de ceros no deben producir inf ni colapsar rolling stats."""

    def test_run_of_zeros_no_inf(self):
        df = _make_df(40)
        mask = (pl.arange(0, 40) >= 10) & (pl.arange(0, 40) <= 15)
        df = df.with_columns(pl.when(mask).then(0.0).otherwise(pl.col("close")).alias("close"))
        result = _transform(df)
        assert not _has_inf(result), "Racha de ceros no debe generar inf"

    def test_run_of_zeros_nan_propagates(self):
        """log_return debe ser NaN en la racha y justo después."""
        df = _make_df(40)
        mask = (pl.arange(0, 40) >= 10) & (pl.arange(0, 40) <= 15)
        df = df.with_columns(pl.when(mask).then(0.0).otherwise(pl.col("close")).alias("close"))
        result = _transform(df)
        nan_range = result["log_return"][10:17]
        assert nan_range.is_null().all(), "log_return debe ser NaN durante y justo después de la racha de ceros"

    def test_volatility_after_zero_run(self):
        """volatility_20 puede tener NaN en la racha pero nunca inf."""
        df = _make_df(60)
        mask = (pl.arange(0, 60) >= 5) & (pl.arange(0, 60) <= 10)
        df = df.with_columns(pl.when(mask).then(0.0).otherwise(pl.col("close")).alias("close"))
        result = _transform(df)
        assert not result["volatility_20"].is_infinite().any(), "volatility_20 no debe tener inf tras racha de ceros"

    def test_all_zeros_returns_nan_features(self):
        """DataFrame con todos los close=0 → features NaN, sin crash ni inf."""
        df = _make_df(30)
        df = df.with_columns(pl.lit(0.0).alias("close"))
        result = _transform(df)  # Fail-Soft: no debe lanzar
        assert not _has_inf(result), "Todo-cero no debe producir inf"


# ── Tests: volume == 0 ────────────────────────────────────────────────────────


class TestVolumeZero:
    """volume=0 no debe producir inf en vwap."""

    def test_zero_volume_no_inf_vwap(self):
        df = _make_df(30)
        mask = (pl.arange(0, 30) >= 5) & (pl.arange(0, 30) <= 10)
        df = df.with_columns(pl.when(mask).then(0.0).otherwise(pl.col("volume")).alias("volume"))
        result = _transform(df)
        assert not _has_inf(result), "volume=0 no debe producir inf en vwap"


# ── Tests: semántica NaN ──────────────────────────────────────────────────────


class TestNullSemantics:
    """
    null (desde ±inf) se propagan; GoldTransformer NO imputa.
    Política de imputación: responsabilidad del caller (QualityPipeline,
    estrategia). SSOT de política: caller.
    """

    def test_null_not_imputed(self):
        df = _make_df(30)
        df = df.with_columns(pl.when(pl.arange(0, 30) == 10).then(0.0).otherwise(pl.col("close")).alias("close"))
        result = _transform(df)
        msg = "null (desde inf) no debe ser imputado por GoldTransformer — SSOT: caller"
        assert result["log_return"].item(10) is None, msg

    def test_output_contains_no_inf_on_normal_input(self):
        df = _make_df(50)
        result = _transform(df)
        assert not _has_inf(result), "Input normal no debe producir inf en ninguna columna derivada"
