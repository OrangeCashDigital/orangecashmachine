"""
Tests de cobertura para casos límite de FeatureEngineer:
  - secuencias de ceros (close, volume, rango HL)
  - propagación de NaN (semántica definida)
  - ausencia de +inf / -inf tras compute()

Todos los tests usan -W error::RuntimeWarning implícitamente via pytest.ini / conftest.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from market_data.storage.gold.feature_engineer import FeatureEngineer

# ── fixtures ──────────────────────────────────────────────────────────────────

def _make_df(n: int = 30, *, base_close: float = 100.0) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    close = base_close + rng.standard_normal(n).cumsum()
    close = np.abs(close) + 1.0          # siempre > 0 por defecto
    return pd.DataFrame({
        "timestamp": np.arange(n, dtype=float),
        "open":   close * 0.99,
        "high":   close * 1.01,
        "low":    close * 0.98,
        "close":  close,
        "volume": rng.uniform(100, 1_000, n),
    })


@pytest.fixture
def fe() -> FeatureEngineer:
    return FeatureEngineer()


# ── helpers ───────────────────────────────────────────────────────────────────

def _has_inf(df: pd.DataFrame) -> bool:
    return bool(np.isinf(df.select_dtypes(include="number")).any().any())


def _nan_cols(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col].isna()


# ── Tests: close == 0 (numerador) ────────────────────────────────────────────

class TestCloseZeroNumerator:
    """close[i] == 0 → log_return[i] debe ser NaN, nunca -inf."""

    def test_single_zero_close_produces_nan(self, fe):
        df = _make_df(30)
        df.loc[10, "close"] = 0.0
        result = fe.compute(df)
        assert math.isnan(result.loc[10, "log_return"]), \
            "close=0 debe producir NaN en log_return"

    def test_single_zero_close_no_inf(self, fe):
        df = _make_df(30)
        df.loc[5, "close"] = 0.0
        result = fe.compute(df)
        assert not _has_inf(result), "No debe haber ±inf tras close=0"


# ── Tests: close == 0 (denominador / prev_close) ─────────────────────────────

class TestCloseZeroDenominator:
    """prev_close == 0 → log_return[i+1] debe ser NaN, nunca +inf."""

    def test_prev_close_zero_produces_nan(self, fe):
        df = _make_df(30)
        df.loc[7, "close"] = 0.0          # prev_close del índice 8
        result = fe.compute(df)
        # índice 8: log(close[8] / 0) → sin fix = +inf; con fix = NaN
        assert math.isnan(result.loc[8, "log_return"]), \
            "prev_close=0 debe producir NaN, no +inf"

    def test_prev_close_zero_no_inf(self, fe):
        df = _make_df(30)
        df.loc[7, "close"] = 0.0
        result = fe.compute(df)
        assert not _has_inf(result), "No debe haber +inf cuando prev_close=0"


# ── Tests: secuencia de ceros consecutivos ────────────────────────────────────

class TestZeroSequences:
    """Rachas de ceros no deben colapsar rolling stats ni producir inf."""

    def test_run_of_zeros_no_inf(self, fe):
        df = _make_df(40)
        df.loc[10:15, "close"] = 0.0      # 6 ceros consecutivos
        result = fe.compute(df)
        assert not _has_inf(result), "Racha de ceros no debe generar inf"

    def test_run_of_zeros_nan_propagates(self, fe):
        df = _make_df(40)
        df.loc[10:15, "close"] = 0.0
        result = fe.compute(df)
        # log_return en el rango y el siguiente deben ser NaN
        nan_range = result.loc[10:16, "log_return"]
        assert nan_range.isna().all(), \
            "log_return debe ser NaN durante y justo después de la racha de ceros"

    def test_volatility_after_zero_run(self, fe):
        """volatility_20 puede tener NaN en la racha pero no inf."""
        df = _make_df(60)
        df.loc[5:10, "close"] = 0.0
        result = fe.compute(df)
        vol = result["volatility_20"]
        assert not np.isinf(vol).any(), "volatility_20 no debe tener inf"

    def test_all_zeros_returns_nan_features(self, fe):
        """DataFrame con todos los close=0 → features NaN, no crash."""
        df = _make_df(30)
        df["close"] = 0.0
        result = fe.compute(df)         # no debe lanzar
        assert not _has_inf(result), "Todo-cero no debe producir inf"


# ── Tests: volume == 0 ────────────────────────────────────────────────────────

class TestVolumeZero:
    def test_zero_volume_no_inf_vwap(self, fe):
        df = _make_df(30)
        df.loc[5:10, "volume"] = 0.0
        result = fe.compute(df)
        assert not _has_inf(result), "volume=0 no debe producir inf en vwap"


# ── Tests: semántica NaN — no se imputan silenciosamente ─────────────────────

class TestNaNSemantics:
    """Los NaN se propagan; FeatureEngineer NO imputa (responsabilidad del caller)."""

    def test_nan_not_imputed(self, fe):
        df = _make_df(30)
        df.loc[10, "close"] = 0.0
        result = fe.compute(df)
        # Verificar que el NaN no fue rellenado hacia adelante
        assert math.isnan(result.loc[10, "log_return"]), \
            "NaN no debe ser imputado por FeatureEngineer (SSOT de NaN: caller)"

    def test_output_contains_no_inf_on_normal_input(self, fe):
        df = _make_df(50)
        result = fe.compute(df)
        assert not _has_inf(result), \
            "Input normal no debe producir inf en ninguna columna derivada"
