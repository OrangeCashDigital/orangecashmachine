# ── Auditoría numérica: RuntimeWarning → error (opt-in por marker) ──────────
# Usar: @pytest.mark.filterwarnings("error::RuntimeWarning") en tests nuevos
# o pasar -W error::RuntimeWarning en CLI para auditar toda la suite.
# ─────────────────────────────────────────────────────────────────────────────


import polars as pl
import pytest


def assert_no_inf_nan(df: pl.DataFrame, *, allow_nan: bool = True) -> None:
    """Helper de auditoría: verifica ausencia de inf (y opcionalmente NaN).

    Uso en tests:
        result = fe.compute(df)
        assert_no_inf_nan(result)               # permite NaN, prohíbe inf
        assert_no_inf_nan(result, allow_nan=False)  # prohíbe ambos
    """
    numeric_cols = [c for c, t in zip(df.columns, df.dtypes, strict=True) if t.is_numeric()]
    inf_mask = df.select([pl.col(c).is_infinite().any() for c in numeric_cols])
    if inf_mask.row(0) and any(inf_mask.row(0)):
        cols = [c for c, v in zip(numeric_cols, inf_mask.row(0), strict=True) if v]
        pytest.fail(f"±inf detectado en columnas: {cols}")
    if not allow_nan:
        nan_mask = df.select([pl.col(c).is_null().any() for c in numeric_cols])
        if nan_mask.row(0) and any(nan_mask.row(0)):
            cols = [c for c, v in zip(numeric_cols, nan_mask.row(0), strict=True) if v]
            pytest.fail(f"NaN detectado en columnas: {cols}")
