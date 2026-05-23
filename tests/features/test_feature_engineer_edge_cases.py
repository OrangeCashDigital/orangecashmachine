# -*- coding: utf-8 -*-
"""
tests/features/test_feature_engineer_edge_cases.py
====================================================

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

import math

import numpy as np
import pandas as pd
from market_data.infrastructure.storage.gold.transformer import GoldTransformer

# ── constantes de prueba — SSOT local ────────────────────────────────────────

_SYMBOL = "BTC/USDT"
_TIMEFRAME = "1h"
_EXCHANGE = "bybit"


# ── helpers ───────────────────────────────────────────────────────────────────


def _make_df(n: int = 30, *, base_close: float = 100.0) -> pd.DataFrame:
    """
    DataFrame OHLCV sintético que simula salida Silver válida.

    - close siempre > 0 por defecto (abs + 1.0)
    - sin quality_flag — GoldTransformer lo tolera (is_suspect=False)
    - timestamp como float (GoldTransformer no requiere datetime en tests)
    """
    rng = np.random.default_rng(42)
    close = base_close + rng.standard_normal(n).cumsum()
    close = np.abs(close) + 1.0
    return pd.DataFrame(
        {
            "timestamp": np.arange(n, dtype=float),
            "open": close * 0.99,
            "high": close * 1.01,
            "low": close * 0.98,
            "close": close,
            "volume": rng.uniform(100, 1_000, n),
        }
    )


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """Invoca GoldTransformer.transform con parámetros de prueba fijos."""
    result = GoldTransformer.transform(
        df,
        symbol=_SYMBOL,
        timeframe=_TIMEFRAME,
        exchange=_EXCHANGE,
    )
    # ACL out: GoldTransformer retorna pl.DataFrame; edge_cases usa pandas API
    return result.to_pandas() if hasattr(result, "to_pandas") else result


def _has_inf(df: pd.DataFrame) -> bool:
    """True si alguna columna numérica contiene ±inf."""
    return bool(np.isinf(df.select_dtypes(include="number")).any().any())


# ── Tests: close == 0 (numerador) ────────────────────────────────────────────


class TestCloseZeroNumerator:
    """close[i] == 0 → log_return[i] debe ser NaN, nunca -inf."""

    def test_single_zero_close_produces_nan(self):
        df = _make_df(30)
        df.loc[10, "close"] = 0.0
        result = _transform(df)
        assert math.isnan(result.loc[10, "log_return"]), "close=0 debe producir NaN en log_return, no -inf"

    def test_single_zero_close_no_inf(self):
        df = _make_df(30)
        df.loc[5, "close"] = 0.0
        result = _transform(df)
        assert not _has_inf(result), "No debe haber ±inf tras close=0"


# ── Tests: close == 0 (denominador / prev_close) ─────────────────────────────


class TestCloseZeroDenominator:
    """prev_close == 0 → log_return[i+1] debe ser NaN, nunca +inf."""

    def test_prev_close_zero_produces_nan(self):
        df = _make_df(30)
        df.loc[7, "close"] = 0.0  # prev_close del índice 8
        result = _transform(df)
        assert math.isnan(result.loc[8, "log_return"]), "prev_close=0 debe producir NaN en log_return, no +inf"

    def test_prev_close_zero_no_inf(self):
        df = _make_df(30)
        df.loc[7, "close"] = 0.0
        result = _transform(df)
        assert not _has_inf(result), "No debe haber +inf cuando prev_close=0"


# ── Tests: secuencia de ceros consecutivos ────────────────────────────────────


class TestZeroSequences:
    """Rachas de ceros no deben producir inf ni colapsar rolling stats."""

    def test_run_of_zeros_no_inf(self):
        df = _make_df(40)
        df.loc[10:15, "close"] = 0.0  # 6 ceros consecutivos
        result = _transform(df)
        assert not _has_inf(result), "Racha de ceros no debe generar inf"

    def test_run_of_zeros_nan_propagates(self):
        """log_return debe ser NaN en la racha y justo después."""
        df = _make_df(40)
        df.loc[10:15, "close"] = 0.0
        result = _transform(df)
        nan_range = result.loc[10:16, "log_return"]
        assert nan_range.isna().all(), "log_return debe ser NaN durante y justo después de la racha de ceros"

    def test_volatility_after_zero_run(self):
        """volatility_20 puede tener NaN en la racha pero nunca inf."""
        df = _make_df(60)
        df.loc[5:10, "close"] = 0.0
        result = _transform(df)
        assert not np.isinf(result["volatility_20"]).any(), "volatility_20 no debe tener inf tras racha de ceros"

    def test_all_zeros_returns_nan_features(self):
        """DataFrame con todos los close=0 → features NaN, sin crash ni inf."""
        df = _make_df(30)
        df["close"] = 0.0
        result = _transform(df)  # Fail-Soft: no debe lanzar
        assert not _has_inf(result), "Todo-cero no debe producir inf"


# ── Tests: volume == 0 ────────────────────────────────────────────────────────


class TestVolumeZero:
    """volume=0 no debe producir inf en vwap."""

    def test_zero_volume_no_inf_vwap(self):
        df = _make_df(30)
        df.loc[5:10, "volume"] = 0.0
        result = _transform(df)
        assert not _has_inf(result), "volume=0 no debe producir inf en vwap"


# ── Tests: semántica NaN ──────────────────────────────────────────────────────


class TestNaNSemantics:
    """
    NaN se propagan; GoldTransformer NO imputa.
    Política de imputación: responsabilidad del caller (QualityPipeline,
    estrategia). SSOT de política: caller.
    """

    def test_nan_not_imputed(self):
        df = _make_df(30)
        df.loc[10, "close"] = 0.0
        result = _transform(df)
        assert math.isnan(result.loc[10, "log_return"]), "NaN no debe ser imputado por GoldTransformer — SSOT: caller"

    def test_output_contains_no_inf_on_normal_input(self):
        df = _make_df(50)
        result = _transform(df)
        assert not _has_inf(result), "Input normal no debe producir inf en ninguna columna derivada"
