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

from market_data.batch.schemas.ohlcv_schema import validate_ohlcv


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
        """

        df = df.copy()

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce",
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
            logger.warning(
                f"Removed {removed} duplicate OHLCV rows"
            )

        return df

    # ---------------------------------------------------------
    # Remove Invalid Rows
    # ---------------------------------------------------------

    @classmethod
    def _drop_invalid_rows(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina filas con NaN en columnas críticas.
        """

        before = len(df)

        df = df.dropna(subset=cls.REQUIRED_COLUMNS)

        removed = before - len(df)

        if removed > 0:
            logger.warning(
                f"Removed {removed} invalid OHLCV rows"
            )

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
    def transform(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeline completo de transformación OHLCV.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame OHLCV crudo.

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

        # Pipeline
        df = cls._convert_types(df)

        df = cls._remove_duplicates(df)

        df = cls._drop_invalid_rows(df)

        df = cls._sort(df)

        # Validación final
        df = validate_ohlcv(df)

        logger.info(
            f"OHLCV transformed → {original_rows} → {len(df)} rows"
        )

        return df