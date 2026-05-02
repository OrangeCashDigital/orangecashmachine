
# ── Auditoría global: RuntimeWarning → error ──────────────────────────────────
# Fail-fast ante operaciones numéricas inválidas (divide-by-zero, overflow…).
# Consistente con la práctica establecida de -W error::RuntimeWarning.
import warnings
warnings.filterwarnings("error", category=RuntimeWarning)
# ─────────────────────────────────────────────────────────────────────────────


import numpy as np
import pandas as pd
import pytest


def assert_no_inf_nan(df: pd.DataFrame, *, allow_nan: bool = True) -> None:
    """Helper de auditoría: verifica ausencia de inf (y opcionalmente NaN).

    Uso en tests:
        result = fe.compute(df)
        assert_no_inf_nan(result)               # permite NaN, prohíbe inf
        assert_no_inf_nan(result, allow_nan=False)  # prohíbe ambos
    """
    numeric = df.select_dtypes(include="number")
    inf_mask = np.isinf(numeric)
    if inf_mask.any().any():
        cols = numeric.columns[inf_mask.any()].tolist()
        pytest.fail(f"±inf detectado en columnas: {cols}")
    if not allow_nan:
        nan_mask = numeric.isna()
        if nan_mask.any().any():
            cols = numeric.columns[nan_mask.any()].tolist()
            pytest.fail(f"NaN detectado en columnas: {cols}")

