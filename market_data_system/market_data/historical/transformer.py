"""
transformer.py
==============

Responsabilidad:
Preparar y transformar los datos históricos OHLCV antes de almacenarlos.
Incluye validación de columnas, conversión de tipos y cálculo de métricas adicionales.

Este módulo es utilizado principalmente por:
- HistoricalFetcher
- Pipelines de datos históricos
"""

import pandas as pd
from loguru import logger


class OHLCVTransformer:
    """
    Clase encargada de procesar y transformar DataFrames OHLCV
    antes de su almacenamiento.
    """

    REQUIRED_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]

    @classmethod
    def validate(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida que el DataFrame contenga las columnas requeridas.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame a validar.

        Returns
        -------
        pd.DataFrame
            El mismo DataFrame si es válido.

        Raises
        ------
        ValueError
            Si faltan columnas requeridas.
        """
        missing_cols = set(cls.REQUIRED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"El DataFrame no contiene las columnas requeridas: {missing_cols}")
        return df

    @classmethod
    def convert_types(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Asegura que las columnas tengan los tipos correctos:
        - timestamp -> datetime
        - open, high, low, close, volume -> float

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame a transformar.

        Returns
        -------
        pd.DataFrame
            DataFrame con tipos corregidos.
        """
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    @classmethod
    def clean_duplicates(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina filas duplicadas basadas en el timestamp.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame a limpiar.

        Returns
        -------
        pd.DataFrame
            DataFrame sin duplicados.
        """
        df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
        return df

    @classmethod
    def transform(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Pipeline completa de transformación:
        1. Validación de columnas
        2. Conversión de tipos
        3. Eliminación de duplicados

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame OHLCV crudo.

        Returns
        -------
        pd.DataFrame
            DataFrame transformado listo para almacenamiento.
        """
        df = cls.validate(df)
        df = cls.convert_types(df)
        df = cls.clean_duplicates(df)
        logger.info(f"🔄 Transformación completada ({len(df)} filas)")
        return df