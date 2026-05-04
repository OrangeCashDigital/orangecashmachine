"""
transformer.py
==============

Responsabilidad
---------------
Preparar y transformar datos OHLCV antes de almacenamiento
o procesamiento cuantitativo.

Pipeline aplicado
-----------------

1. Validación de columnas
2. Conversión de tipos
3. Eliminación de duplicados
4. Eliminación de registros inválidos
5. Orden temporal
6. Validación final de schema

Principios aplicados
--------------------

• SOLID
• DRY
• KISS
• SafeOps
"""

from __future__ import annotations

import pandas as pd
from loguru import logger

from market_data.quality.schemas.ohlcv_schema import validate_ohlcv
from market_data.processing.utils.grid_alignment import align_to_grid
from market_data.processing.validation.candle_validator import (
    CandleValidator,
    ValidationSummary,
)
from market_data.lineage.tracker import (
    LineageEvent, LineageStatus, PipelineLayer, lineage_tracker,
)


class OHLCVTransformer:
    """
    Transformador profesional para datasets OHLCV.

    Diseñado para pipelines de ingestión de market data
    antes del almacenamiento en el Data Lake.
    """

    REQUIRED_COLUMNS = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    NUMERIC_COLUMNS = [
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
    def _validate_columns(cls, df: pd.DataFrame) -> None:
        """
        Verifica que el DataFrame contenga las columnas OHLCV requeridas.
        """

        missing = set(cls.REQUIRED_COLUMNS) - set(df.columns)

        if missing:
            raise ValueError(
                f"Missing OHLCV columns → {missing}"
            )

    # ---------------------------------------------------------
    # Type Conversion
    # ---------------------------------------------------------

    @classmethod
    def _convert_types(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convierte columnas a tipos correctos.

        Timestamp handling
        ------------------
        CCXT entrega timestamps como epoch milliseconds (int).
        pd.to_datetime sin unit= interpreta ints como nanosegundos → bug silencioso.

        Estrategia:
          - Si la columna ya es datetime (tz-aware o tz-naive) → tz-localize a UTC
          - Si es int/float → epoch ms → pd.to_datetime(unit="ms", utc=True)
          - Fallback: coerce con unit="ms" (preserva el contrato CCXT)

        Ref: CCXT OHLCV schema — timestamp en ms desde epoch Unix
        https://docs.ccxt.com/#/?id=ohlcv-structure
        """
        df = df.copy()

        ts_col = df["timestamp"]
        if pd.api.types.is_datetime64_any_dtype(ts_col):
            # Ya es datetime — solo garantizar UTC
            if ts_col.dt.tz is None:
                df["timestamp"] = ts_col.dt.tz_localize("UTC")
            else:
                df["timestamp"] = ts_col.dt.tz_convert("UTC")
        else:
            # int/float/object → epoch ms (contrato CCXT)
            df["timestamp"] = pd.to_datetime(
                pd.to_numeric(ts_col, errors="coerce"),
                unit   = "ms",
                utc    = True,
                errors = "coerce",
            )

        for col in cls.NUMERIC_COLUMNS:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce",
            )

        return df

    # ---------------------------------------------------------
    # Remove Duplicates
    # ---------------------------------------------------------

    @classmethod
    def _remove_duplicates(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina duplicados por timestamp.
        """

        before = len(df)

        df = df.drop_duplicates(subset="timestamp")

        removed = before - len(df)

        if removed > 0:
            logger.warning("Removed {} duplicate OHLCV rows", removed)

        return df


    # ---------------------------------------------------------
    # Align to Temporal Grid
    # ---------------------------------------------------------

    @staticmethod
    def _align_to_grid(
        df:        "pd.DataFrame",
        timeframe: str,
        exchange:  str,
        symbol:    str,
    ) -> "pd.DataFrame":
        """
        Alinea timestamps al grid canónico del timeframe.

        Delega en align_to_grid (timeframe.py) — lógica de dominio pura.
        Si timeframe es "unknown", el paso se omite para no romper
        tests que no pasan timeframe explícito.
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
        df:        pd.DataFrame,
        symbol:    str,
        timeframe: str,
        exchange:  str,
    ) -> tuple["pd.DataFrame", "pd.Series"]:
        """
        Clasifica velas como CLEAN, SUSPECT o CORRUPT usando CandleValidator.

        Contrato de entrada
        -------------------
        df tiene tipos ya convertidos (_convert_types ejecutado).
        timestamp es pd.Timestamp UTC, OHLCV son float64.

        Contrato de salida
        ------------------
        Retorna (df_accepted, quality_flag_series) donde:
          df_accepted         : DataFrame sin velas CORRUPT (índice reseteado)
          quality_flag_series : pd.Series[str] con "clean"|"suspect" por fila

        Velas CORRUPT se loguean con código de violación para quarantine audit.
        Velas SUSPECT se escriben en Silver con quality_flag='suspect'.

        SafeOps
        -------
        Si CandleValidator no está disponible (ImportError en entornos legacy),
        retorna el df original con quality_flag='clean' para todos.
        Esto preserva compatibilidad sin romper el pipeline.

        DRY
        ---
        SSOT de clasificación a nivel fila — QualityPipeline opera a nivel
        DataFrame después (gaps, outliers, policy). No hay duplicación.

        Ref: market_data/processing/validation/candle_validator.py
        """
        # Convertir DataFrame tipado a RawCandle[] para el validator
        # timestamp → int ms (CandleValidator espera int, no pd.Timestamp)
        try:
            ts_ms = df["timestamp"].astype("int64") // 1_000_000
        except Exception:
            # Fallback si timestamp no es int64-casteable (edge case en tests)
            quality_flag = pd.Series(["clean"] * len(df), index=df.index, dtype="object")
            return df.copy(), quality_flag

        raw_candles = [
            (
                int(ts_ms.iloc[i]),
                float(df["open"].iloc[i]),
                float(df["high"].iloc[i]),
                float(df["low"].iloc[i]),
                float(df["close"].iloc[i]),
                float(df["volume"].iloc[i]),
            )
            for i in range(len(df))
        ]

        validator = CandleValidator(timeframe=timeframe if timeframe != "unknown" else "1m")
        results   = validator.validate_batch(raw_candles)
        summary   = ValidationSummary.from_results(results)

        # Quarantine log — CORRUPT (fail-fast por vela)
        if summary.corrupt > 0:
            logger.warning(
                "CandleValidator | {}/{} exchange={} corrupt={}/{} suspect={}/{}",
                symbol, timeframe, exchange,
                summary.corrupt, summary.total,
                summary.suspect, summary.total,
            )
            for r in summary.corrupt_results[:10]:  # max 10 para no saturar logs
                logger.warning(
                    "  corrupt candle | ts={} violations={} reason={}",
                    r.candle[0] if r.candle else "?",
                    r.violations,
                    r.reason,
                )
        elif summary.suspect > 0:
            logger.debug(
                "CandleValidator | {}/{} exchange={} suspect={}/{} (no corrupt)",
                symbol, timeframe, exchange, summary.suspect, summary.total,
            )

        # Filtrar CORRUPT del DataFrame
        accepted_mask = [not r.is_corrupt for r in results]
        df_accepted   = df[accepted_mask].reset_index(drop=True)

        # Construir quality_flag solo para filas aceptadas
        quality_flags = [
            r.label.value
            for r in results
            if not r.is_corrupt
        ]
        quality_flag_series = pd.Series(quality_flags, dtype="object")

        return df_accepted, quality_flag_series

    # ---------------------------------------------------------
    # Remove Invalid Rows (NaN residuales post-validator)
    # ---------------------------------------------------------

    @classmethod
    def _drop_invalid_rows(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina filas con NaN residuales en columnas críticas.

        Corre DESPUÉS de _validate_and_classify.
        CandleValidator ya eliminó velas CORRUPT estructurales (high<low, etc.).
        Este paso limpia NaN que pueden aparecer por _convert_types (coerce="coerce")
        sobre valores no parseables que no son None/NaN explícitos.
        """

        before = len(df)
        df     = df.dropna(subset=cls.REQUIRED_COLUMNS)
        removed = before - len(df)

        if removed > 0:
            logger.warning("Removed {} NaN rows (post-validator)", removed)

        return df

    # ---------------------------------------------------------
    # Sort Data
    # ---------------------------------------------------------

    @staticmethod
    def _sort(df: pd.DataFrame) -> pd.DataFrame:
        """
        Ordena por timestamp.
        """

        return (
            df.sort_values("timestamp")
            .reset_index(drop=True)
        )

    # ---------------------------------------------------------
    # Transform Pipeline
    # ---------------------------------------------------------

    @classmethod
    def transform(
        cls,
        df:        pd.DataFrame,
        symbol:    str = "unknown",
        timeframe: str = "unknown",
        exchange:  str = "unknown",
        run_id:    str | None = None,
    ) -> pd.DataFrame:
        """
        Pipeline completo de transformación OHLCV.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame OHLCV crudo.
        symbol : str
            Par de trading (para data quality reporting).
        timeframe : str
            Intervalo temporal (para detección de gaps).
        exchange : str
            Exchange fuente (para trazabilidad).

        Returns
        -------
        pd.DataFrame
            DataFrame transformado y validado.
        """

        if df is None or df.empty:

            logger.warning("Received empty OHLCV dataframe")

            return pd.DataFrame(columns=cls.REQUIRED_COLUMNS)

        cls._validate_columns(df)

        original_rows = len(df)

        # ── Stage 1: tipos y estructura básica ───────────────────────────────
        df = cls._convert_types(df)
        df = cls._remove_duplicates(df)
        df = cls._align_to_grid(df, timeframe, exchange, symbol)

        # ── Stage 2: Data Quality — clasificación por vela ───────────────────
        # CandleValidator clasifica CLEAN/SUSPECT/CORRUPT.
        # Velas CORRUPT se eliminan aquí (quarantine log).
        # quality_flag se detach antes de pandera (strict=True rechaza cols extra)
        # y se re-attach al DataFrame final.
        df, quality_flag = cls._validate_and_classify(df, symbol, timeframe, exchange)

        # ── Stage 3: limpieza residual + orden ───────────────────────────────
        df = cls._drop_invalid_rows(df)
        df = cls._sort(df)

        # ── Stage 4: validación formal de schema (pandera) ───────────────────
        # pandera opera sobre df limpio — sin quality_flag (strict=True).
        # Si el df quedó vacío (todos eran CORRUPT), retornar vacío sin lanzar.
        if df.empty:
            logger.warning(
                "OHLCV transform | {}/{} exchange={} — all {} rows rejected (corrupt/invalid)",
                symbol, timeframe, exchange, original_rows,
            )
            empty = pd.DataFrame(columns=[*cls.REQUIRED_COLUMNS, "quality_flag"])
            return empty

        # Alinear quality_flag al índice actual del df post-drop
        # (pueden haberse eliminado filas en _drop_invalid_rows)
        if len(quality_flag) != len(df):
            # Re-alinear: reconstruir con "clean" para filas sin flag
            quality_flag = pd.Series(["clean"] * len(df), dtype="object")

        df = validate_ohlcv(df, timeframe=timeframe)

        # ── Stage 5: re-attach quality_flag ──────────────────────────────────
        # Después de pandera — el schema ya validó sin esta columna.
        df = df.copy()
        df["quality_flag"] = quality_flag.values

        logger.info(
            "OHLCV transformed | {}/{} exchange={} rows={}/{} quality_flag_counts={}",
            symbol, timeframe, exchange,
            len(df), original_rows,
            dict(df["quality_flag"].value_counts()),
        )

        # ── Lineage SILVER ───────────────────────────────────────────────────
        # rows_in=original_rows captura pérdida real por velas CORRUPT.
        # Fail-soft: si lineage_tracker falla, el pipeline continúa (SafeOps).
        if run_id is not None:
            lineage_tracker.record(LineageEvent(
                run_id    = run_id,
                layer     = PipelineLayer.SILVER,
                exchange  = exchange,
                symbol    = symbol,
                timeframe = timeframe,
                rows_in   = original_rows,
                rows_out  = len(df),
                status    = (
                    LineageStatus.PARTIAL if len(df) < original_rows
                    else LineageStatus.SUCCESS
                ),
                params    = {
                    "timeframe":    timeframe,
                    "rows_removed": original_rows - len(df),
                },
            ))

        return df