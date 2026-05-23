"""
market_data/gold/transformer.py
================================
Silver pl.DataFrame → features Gold pl.DataFrame.

Migración Fase 3 — pandas → polars
------------------------------------
Todo el procesamiento interno opera sobre pl.DataFrame nativo.
GoldTransformer.transform() acepta y retorna pl.DataFrame.

Los callers (GoldStorage.build, ResamplePipeline post-Gold) deben
pasar pl.DataFrame. Si algún caller upstream sigue en pandas, el
ACL de conversión es responsabilidad del caller, no de este transformer.

Invariantes del input (Silver)
------------------------------
- timestamp : Datetime("us", "UTC"), monotónico creciente
- open, high, low, close : Float64 > 0
- volume : Float64 >= 0
- quality_flag : Utf8 ("clean" | "suspect")

Invariantes del output (Gold)
------------------------------
- Todas las columnas Silver preservadas (no destructivo).
- Nuevas columnas: ver FEATURE_COLUMNS.
- null en primeras _ROLLING_WINDOW-1 filas de features rolling (diseño).
- ±inf → null al final (_sanitize).
- is_suspect : Bool derivado de quality_flag.

Principios
----------
- SSOT: única implementación activa de features Gold.
- SRP: cada _compute_* hace exactamente una feature.
- Fail-Soft: vacío in → vacío out, sin excepción.
- No destructivo: nunca muta el DataFrame de entrada.
"""

from __future__ import annotations

import pandas as pd
import polars as pl
from loguru import logger

from market_data.infrastructure.lineage.tracker import (
    LineageEvent,
    LineageStatus,
    PipelineLayer,
    lineage_tracker,
)

# ------------------------------------------------------------------
# Constantes — SSOT para toda la capa Gold
# ------------------------------------------------------------------

_ROLLING_WINDOW: int = 20
_MIN_PERIODS: int = 5

# Factores de anualización: periodos completos por año (crypto 24/7).
_ANNUALIZATION_MAP: dict[str, int] = {
    "1m": 525_600,
    "5m": 105_120,
    "15m": 35_040,
    "30m": 17_520,
    "1h": 8_760,
    "4h": 2_190,
    "8h": 1_095,
    "1d": 365,
    "1w": 52,
}
_DEFAULT_ANNUALIZATION: int = 365

FEATURE_COLUMNS: list[str] = [
    "log_return",
    "return_1",
    "volatility_20",
    "vwap",
    "high_low_spread",
    "volume_z",
    "price_range_pct",
    "body_pct",
    "is_suspect",
]

VERSION: str = "3.0.0"
# 3.0.0 — migración completa a polars nativo
#          semántica idéntica a 2.0.0 (VWAP rolling-20, min_periods=5)


class GoldTransformer:
    """
    Transforma un pl.DataFrame Silver en features Gold.
    """

    @classmethod
    def transform(
        cls,
        df: pl.DataFrame,
        symbol: str,
        timeframe: str,
        exchange: str,
        run_id: str | None = None,
    ) -> pl.DataFrame:
        """
        Pipeline principal Silver → Gold.

        Fail-Soft: vacío in → vacío out, nunca lanza excepción.
        """
        # ACL: aceptar pd.DataFrame de callers legacy — convertir una vez
        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)
        if df is None or df.is_empty():
            logger.warning(
                "GoldTransformer: empty input | {}/{} exchange={}",
                symbol,
                timeframe,
                exchange,
            )
            return pl.DataFrame()

        rows_in = len(df)
        gold = df.sort("timestamp")

        gold = cls._compute_log_return(gold)
        gold = cls._compute_return_1(gold)
        gold = cls._compute_volatility(gold, timeframe)
        gold = cls._compute_vwap(gold)
        gold = cls._compute_high_low_spread(gold)
        gold = cls._compute_volume_zscore(gold)
        gold = cls._compute_price_range(gold)
        gold = cls._compute_body_pct(gold)
        gold = cls._compute_is_suspect(gold)
        gold = cls._sanitize(gold)

        rows_out = len(gold)
        new_cols = [c for c in gold.columns if c not in df.columns]

        logger.info(
            "GoldTransformer | {}/{} exchange={} rows={} features={} v={}",
            symbol,
            timeframe,
            exchange,
            rows_out,
            new_cols,
            VERSION,
        )

        if run_id is not None:
            lineage_tracker.record(
                LineageEvent(
                    run_id=run_id,
                    layer=PipelineLayer.GOLD,
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    rows_in=rows_in,
                    rows_out=rows_out,
                    status=LineageStatus.SUCCESS,
                    params={
                        "rolling_window": _ROLLING_WINDOW,
                        "min_periods": _MIN_PERIODS,
                        "timeframe": timeframe,
                        "version": VERSION,
                    },
                )
            )

        return gold

    # ------------------------------------------------------------------
    # Feature computers — SRP: un método = una feature
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_log_return(df: pl.DataFrame) -> pl.DataFrame:
        """log(close_t / close_{t-1}). close=0 → null."""
        return df.with_columns(
            (
                pl.col("close").replace(0, None).log(base=2.718281828)
                - pl.col("close").replace(0, None).shift(1).log(base=2.718281828)
            ).alias("log_return")
        )

    @staticmethod
    def _compute_return_1(df: pl.DataFrame) -> pl.DataFrame:
        """Retorno simple período a período."""
        return df.with_columns((pl.col("close") / pl.col("close").shift(1) - 1.0).alias("return_1"))

    @staticmethod
    def _compute_volatility(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        """Volatilidad realizada rolling anualizada."""
        ann = _ANNUALIZATION_MAP.get(timeframe, _DEFAULT_ANNUALIZATION)
        return df.with_columns(
            (
                pl.col("log_return").rolling_std(window_size=_ROLLING_WINDOW, min_periods=_MIN_PERIODS) * (ann**0.5)
            ).alias("volatility_20")
        )

    @staticmethod
    def _compute_vwap(df: pl.DataFrame) -> pl.DataFrame:
        """VWAP rolling 20 periodos sobre typical price × volume."""
        return (
            df.with_columns(
                [
                    ((pl.col("high") + pl.col("low") + pl.col("close")) / 3.0).alias("_tp"),
                    pl.col("volume").replace(0, None).alias("_vol_safe"),
                ]
            )
            .with_columns(
                [
                    (pl.col("_tp") * pl.col("_vol_safe")).alias("_tpv"),
                ]
            )
            .with_columns(
                [
                    (
                        pl.col("_tpv").rolling_sum(window_size=_ROLLING_WINDOW, min_periods=_MIN_PERIODS)
                        / pl.col("_vol_safe").rolling_sum(window_size=_ROLLING_WINDOW, min_periods=_MIN_PERIODS)
                    ).alias("vwap"),
                ]
            )
            .drop(["_tp", "_vol_safe", "_tpv"])
        )

    @staticmethod
    def _compute_high_low_spread(df: pl.DataFrame) -> pl.DataFrame:
        """(high - low) / close — rango normalizado."""
        return df.with_columns(
            ((pl.col("high") - pl.col("low")) / pl.col("close").replace(0, None)).alias("high_low_spread")
        )

    @staticmethod
    def _compute_volume_zscore(df: pl.DataFrame) -> pl.DataFrame:
        """Z-score de volumen rolling. std=0 → null."""
        return df.with_columns(
            (
                (
                    pl.col("volume")
                    - pl.col("volume").rolling_mean(window_size=_ROLLING_WINDOW, min_periods=_MIN_PERIODS)
                )
                / pl.col("volume").rolling_std(window_size=_ROLLING_WINDOW, min_periods=_MIN_PERIODS).replace(0, None)
            ).alias("volume_z")
        )

    @staticmethod
    def _compute_price_range(df: pl.DataFrame) -> pl.DataFrame:
        """(high - low) / open — volatilidad intra-bar."""
        return df.with_columns(
            ((pl.col("high") - pl.col("low")) / pl.col("open").replace(0, None)).alias("price_range_pct")
        )

    @staticmethod
    def _compute_body_pct(df: pl.DataFrame) -> pl.DataFrame:
        """abs(close - open) / open — tamaño del cuerpo."""
        return df.with_columns(
            ((pl.col("close") - pl.col("open")).abs() / pl.col("open").replace(0, None)).alias("body_pct")
        )

    @staticmethod
    def _compute_is_suspect(df: pl.DataFrame) -> pl.DataFrame:
        """Bool derivado de quality_flag Silver."""
        if "quality_flag" in df.columns:
            return df.with_columns((pl.col("quality_flag") == "suspect").alias("is_suspect"))
        return df.with_columns(pl.lit(False).alias("is_suspect"))

    @staticmethod
    def _sanitize(df: pl.DataFrame) -> pl.DataFrame:
        """±inf → null en todas las columnas numéricas Float64."""
        float_cols = [c for c, t in zip(df.columns, df.dtypes) if t == pl.Float64]
        if not float_cols:
            return df
        inf_counts = {c: int(df[c].is_infinite().sum()) for c in float_cols}
        total_inf = sum(inf_counts.values())
        if total_inf:
            logger.warning("GoldTransformer._sanitize: ±inf→null | count={}", total_inf)
            return df.with_columns(
                [
                    pl.when(pl.col(c).is_infinite()).then(None).otherwise(pl.col(c)).alias(c)
                    for c in float_cols
                    if inf_counts[c] > 0
                ]
            )
        return df
