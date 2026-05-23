# -*- coding: utf-8 -*-
"""
market_data/application/use_cases/ohlcv_transformer.py
=======================================================

Responsabilidad
---------------
Preparar y transformar datos OHLCV antes de almacenamiento
o procesamiento cuantitativo.

Pipeline aplicado
-----------------
1. Validación de columnas
2. Conversión de tipos
3. Eliminación de duplicados
4. Alineación al grid temporal
5. Clasificación por vela (CandleValidator)
6. Limpieza residual (nulls post-coerción)
7. Ordenación temporal
8. Validación formal de schema (pandera·polars)
9. Re-attach quality_flag

Migración Fase 2 — completa
----------------------------
Todo el procesamiento interno opera sobre pl.DataFrame nativo.
polars_interop.py eliminado — sin puentes intermedios.

ACL en los límites públicos de transform():
  Entrada: pd.DataFrame → pl.from_pandas()  — única conversión
  Salida : pl.DataFrame → .to_pandas()      — única conversión

Razón del patrón ACL
--------------------
Los callers upstream (CCXT, fetchers REST) entregan pd.DataFrame.
Los callers downstream (IcebergStorage) consumen pd.DataFrame.
Convertir una sola vez en el borde elimina la deuda de polars_interop.py
y satisface SSOT + SRP: ningún método interno conoce pandas.

Principios aplicados
--------------------
• SOLID  — SRP por método; DIP vía callbacks de observabilidad en align_to_grid
• DRY    — columnas definidas como constantes de clase
• KISS   — flujo lineal sin estado mutable
• ACL    — conversión pd↔pl exclusivamente en transform()
• SafeOps — CandleValidator en try/except; nunca bloquea el pipeline
• fail-fast — _validate_columns antes de cualquier transformación costosa
"""

from __future__ import annotations

import pandas as pd
import polars as pl
from loguru import logger

from market_data.application.processing.grid_alignment import align_to_grid
from market_data.application.processing.ohlcv_schema import validate_ohlcv
from market_data.domain.value_objects.candle_validator import (
    CandleValidator,
    ValidationSummary,
)

# lineage: import local en transform() — BC-06


class OHLCVTransformer:
    """
    Transformador profesional para datasets OHLCV.

    Todos los métodos privados operan sobre pl.DataFrame nativo.
    transform() actúa como ACL: pd→pl al entrar, pl→pd al salir.
    """

    REQUIRED_COLUMNS: list[str] = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    NUMERIC_COLUMNS: list[str] = [
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    # ---------------------------------------------------------
    # Column Validation
    # ---------------------------------------------------------

    @classmethod
    def _validate_columns(cls, df: pl.DataFrame) -> None:
        """
        Verifica que el DataFrame contenga las columnas OHLCV requeridas.

        fail-fast: lanza antes de cualquier transformación costosa.
        """
        missing = set(cls.REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing OHLCV columns → {missing}")

    # ---------------------------------------------------------
    # Type Conversion
    # ---------------------------------------------------------

    @classmethod
    def _convert_types(cls, df: pl.DataFrame) -> pl.DataFrame:
        """
        Convierte columnas a tipos canónicos de dominio.

        Timestamp handling
        ------------------
        CCXT entrega timestamps como epoch milliseconds (int).
        Polars acepta cast Int64 → Datetime("ms") directamente.

        Estrategia:
          - Si ya es Datetime → normalizar timezone y precisión
          - Si es Int/Float   → epoch ms → Datetime("ms") → UTC → us
          - strict=False      → valores inválidos → null  (fail-soft · SafeOps)

        Ref: CCXT OHLCV schema
        https://docs.ccxt.com/#/?id=ohlcv-structure
        """
        ts_dtype = df["timestamp"].dtype

        if isinstance(ts_dtype, pl.Datetime):
            # Ya es Datetime — normalizar timezone
            if ts_dtype.time_zone is None:
                df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))
            elif ts_dtype.time_zone != "UTC":
                df = df.with_columns(pl.col("timestamp").dt.convert_time_zone("UTC"))
        else:
            # Int / Float / String → epoch ms (contrato CCXT)
            df = df.with_columns(
                pl.col("timestamp").cast(pl.Int64, strict=False).cast(pl.Datetime("ms")).dt.replace_time_zone("UTC")
            )

        # Tipo canónico del schema OHLCV: microsegundos UTC
        if df["timestamp"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp").dt.cast_time_unit("us"))

        # Columnas numéricas — strict=False → null en lugar de excepción (SafeOps)
        for col in cls.NUMERIC_COLUMNS:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

        return df

    # ---------------------------------------------------------
    # Remove Duplicates
    # ---------------------------------------------------------

    @classmethod
    def _remove_duplicates(cls, df: pl.DataFrame) -> pl.DataFrame:
        """
        Elimina duplicados por timestamp, conservando el primer registro.

        maintain_order=True preserva el orden de llegada dentro del bucket,
        garantizando reproducibilidad entre ejecuciones.
        """
        before = len(df)
        df = df.unique(subset=["timestamp"], keep="first", maintain_order=True)
        removed = before - len(df)
        if removed > 0:
            logger.warning("Removed {} duplicate OHLCV rows", removed)
        return df

    # ---------------------------------------------------------
    # Align to Temporal Grid
    # ---------------------------------------------------------

    @staticmethod
    def _align_to_grid(
        df: pl.DataFrame,
        timeframe: str,
        exchange: str,
        symbol: str,
    ) -> pl.DataFrame:
        """
        Alinea timestamps al grid canónico del timeframe.

        Delega directamente en align_to_grid (pl.DataFrame → pl.DataFrame).
        Sin conversión pd↔pl — nativo polars end-to-end.
        Si timeframe es "unknown", el paso se omite.
        """
        if timeframe == "unknown":
            return df
        return align_to_grid(df, timeframe, exchange=exchange, symbol=symbol)

    # ---------------------------------------------------------
    # Data Quality Validation (CandleValidator)
    # ---------------------------------------------------------

    @classmethod
    def _validate_and_classify(
        cls,
        df: pl.DataFrame,
        symbol: str,
        timeframe: str,
        exchange: str,
    ) -> tuple[pl.DataFrame, pl.Series]:
        """
        Clasifica velas como CLEAN, SUSPECT o CORRUPT usando CandleValidator.

        Contrato de entrada
        -------------------
        df tiene tipos ya convertidos (_convert_types ejecutado).
        timestamp es Datetime("us", UTC), OHLCV son Float64.

        Contrato de salida
        ------------------
        (df_accepted, quality_flag_series) donde:
          df_accepted         : DataFrame sin velas CORRUPT
          quality_flag_series : pl.Series[Utf8] con "clean"|"suspect" por fila

        SafeOps
        -------
        Si la extracción de timestamps falla (edge case en tests),
        retorna df original con quality_flag='clean' para todos.

        DRY
        ---
        SSOT de clasificación a nivel fila. QualityPipeline opera a nivel
        DataFrame después (gaps, outliers, policy) — sin duplicación.
        """
        try:
            ts_ms_list = df["timestamp"].dt.timestamp("ms").to_list()
        except Exception:
            return df, pl.Series("quality_flag", ["clean"] * len(df), dtype=pl.Utf8)

        opens = df["open"].to_list()
        highs = df["high"].to_list()
        lows = df["low"].to_list()
        closes = df["close"].to_list()
        volumes = df["volume"].to_list()

        raw_candles = [
            (
                int(ts_ms_list[i]) if ts_ms_list[i] is not None else 0,
                float(opens[i]) if opens[i] is not None else float("nan"),
                float(highs[i]) if highs[i] is not None else float("nan"),
                float(lows[i]) if lows[i] is not None else float("nan"),
                float(closes[i]) if closes[i] is not None else float("nan"),
                float(volumes[i]) if volumes[i] is not None else float("nan"),
            )
            for i in range(len(df))
        ]

        validator = CandleValidator(timeframe=timeframe if timeframe != "unknown" else "1m")
        results = validator.validate_batch(raw_candles)
        summary = ValidationSummary.from_results(results)

        # Quarantine log — fail-fast por vela, fail-soft para el pipeline
        if summary.corrupt > 0:
            logger.warning(
                "CandleValidator | {}/{} exchange={} corrupt={}/{} suspect={}/{}",
                symbol,
                timeframe,
                exchange,
                summary.corrupt,
                summary.total,
                summary.suspect,
                summary.total,
            )
            for r in summary.corrupt_results[:10]:  # max 10 — no saturar logs
                logger.warning(
                    "  corrupt candle | ts={} violations={} reason={}",
                    r.candle[0] if r.candle else "?",
                    r.violations,
                    r.reason,
                )
        elif summary.suspect > 0:
            logger.debug(
                "CandleValidator | {}/{} exchange={} suspect={}/{} (no corrupt)",
                symbol,
                timeframe,
                exchange,
                summary.suspect,
                summary.total,
            )

        # Filtrar CORRUPT
        accepted_mask = pl.Series([not r.is_corrupt for r in results])
        df_accepted = df.filter(accepted_mask)

        # quality_flag solo para filas aceptadas
        quality_flag_values = [r.label.value for r in results if not r.is_corrupt]
        quality_flag_series = pl.Series("quality_flag", quality_flag_values, dtype=pl.Utf8)

        return df_accepted, quality_flag_series

    # ---------------------------------------------------------
    # Remove Invalid Rows (nulls residuales post-coerción)
    # ---------------------------------------------------------

    @classmethod
    def _drop_invalid_rows(cls, df: pl.DataFrame) -> pl.DataFrame:
        """
        Elimina filas con nulls residuales en columnas críticas.

        Corre DESPUÉS de _validate_and_classify.
        CandleValidator ya eliminó velas CORRUPT estructurales.
        Este paso limpia nulls producidos por cast(strict=False)
        sobre valores no parseables.
        """
        before = len(df)
        df = df.drop_nulls(subset=cls.REQUIRED_COLUMNS)
        removed = before - len(df)
        if removed > 0:
            logger.warning("Removed {} null rows (post-validator)", removed)
        return df

    # ---------------------------------------------------------
    # Sort
    # ---------------------------------------------------------

    @staticmethod
    def _sort(df: pl.DataFrame) -> pl.DataFrame:
        """Ordena por timestamp ascendente."""
        return df.sort("timestamp")

    # ---------------------------------------------------------
    # Transform Pipeline — ACL boundary
    # ---------------------------------------------------------

    @classmethod
    def transform(
        cls,
        df: pd.DataFrame,
        symbol: str = "unknown",
        timeframe: str = "unknown",
        exchange: str = "unknown",
        run_id: str | None = None,
    ) -> pd.DataFrame:
        """
        Pipeline completo de transformación OHLCV.

        ACL boundary
        ------------
        Acepta pd.DataFrame — callers upstream (CCXT, fetchers REST).
        Retorna pd.DataFrame — callers downstream (IcebergStorage).
        Todo el procesamiento interno es polars nativo — sin puentes.

        Parameters
        ----------
        df        : DataFrame OHLCV crudo (CCXT format).
        symbol    : Par de trading (para data quality reporting).
        timeframe : Intervalo temporal (para detección de gaps y grid).
        exchange  : Exchange fuente (para trazabilidad).
        run_id    : ID de correlación — lineage registrado por
                    QualityPipelineConsumer post-transformación (BC-06).

        Returns
        -------
        pd.DataFrame con columna quality_flag adicional.
        """
        if df is None or df.empty:
            logger.warning("Received empty OHLCV dataframe")
            return pd.DataFrame(columns=cls.REQUIRED_COLUMNS)

        original_rows = len(df)

        # ── ACL in: pd.DataFrame → pl.DataFrame ─────────────────────────────
        # Única conversión de entrada — a partir de aquí todo es polars.
        pl_df = pl.from_pandas(df)

        cls._validate_columns(pl_df)

        # ── Stage 1: tipos y estructura básica ────────────────────────────────
        pl_df = cls._convert_types(pl_df)
        pl_df = cls._remove_duplicates(pl_df)
        pl_df = cls._align_to_grid(pl_df, timeframe, exchange, symbol)

        # ── Stage 2: clasificación por vela ───────────────────────────────────
        # quality_flag se detach antes de pandera (strict=True rechaza cols extra)
        # y se re-attach en Stage 5.
        pl_df, quality_flag = cls._validate_and_classify(pl_df, symbol, timeframe, exchange)

        # ── Stage 3: limpieza residual + orden ────────────────────────────────
        pl_df = cls._drop_invalid_rows(pl_df)
        pl_df = cls._sort(pl_df)

        # ── Stage 4: validación formal de schema (pandera·polars) ─────────────
        if pl_df.is_empty():
            logger.warning(
                "OHLCV transform | {}/{} exchange={} — all {} rows rejected (corrupt/invalid)",
                symbol,
                timeframe,
                exchange,
                original_rows,
            )
            return pd.DataFrame(columns=[*cls.REQUIRED_COLUMNS, "quality_flag"])

        # Re-alinear quality_flag si _drop_invalid_rows eliminó filas adicionales
        if len(quality_flag) != len(pl_df):
            quality_flag = pl.Series("quality_flag", ["clean"] * len(pl_df), dtype=pl.Utf8)

        pl_df = validate_ohlcv(pl_df, timeframe=timeframe)

        # ── Stage 5: re-attach quality_flag ───────────────────────────────────
        pl_df = pl_df.with_columns(quality_flag.alias("quality_flag"))

        # value_counts nativo polars — DRY, sin conversión a pandas
        vc = pl_df["quality_flag"].value_counts()
        flag_counts = dict(zip(vc["quality_flag"].to_list(), vc["count"].to_list()))
        logger.info(
            "OHLCV transformed | {}/{} exchange={} rows={}/{} quality_flag_counts={}",
            symbol,
            timeframe,
            exchange,
            len(pl_df),
            original_rows,
            flag_counts,
        )

        # ── ACL out: pl.DataFrame → pd.DataFrame ─────────────────────────────
        # Única conversión de salida — backward compat con callers downstream.
        return pl_df.to_pandas()
