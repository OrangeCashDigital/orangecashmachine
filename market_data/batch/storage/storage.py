"""
historical_storage.py
=====================

Gestión profesional del almacenamiento OHLCV en Data Lake.

Características
---------------

• Organización por símbolo / timeframe / año / mes
• Escritura atómica (safe write)
• Append incremental eficiente
• Optimizado para datasets grandes
• Compatible con pipelines concurrentes
• Logging profesional

Principios aplicados
--------------------

• SOLID
• DRY
• KISS
• SafeOps
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, List

import pandas as pd
from loguru import logger


# ==========================================================
# Exceptions
# ==========================================================

class HistoricalStorageError(Exception):
    """Errores del storage layer."""


# ==========================================================
# Storage
# ==========================================================

class HistoricalStorage:
    """
    Storage profesional para datasets OHLCV en Parquet.

    Diseñado para:

    • pipelines de ingestión
    • datasets grandes
    • append incremental seguro
    """

    REQUIRED_COLUMNS: List[str] = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    # ------------------------------------------------------
    # Initialization
    # ------------------------------------------------------

    def __init__(self, base_path: Optional[str | Path] = None) -> None:

        self.base_path: Path = (
            Path(base_path)
            if base_path
            else Path(__file__).resolve().parents[3] / "data_lake" / "ohlcv"
        )

        self.base_path.mkdir(parents=True, exist_ok=True)

        logger.info(f"HistoricalStorage initialized → {self.base_path}")

    # ------------------------------------------------------
    # Utilities
    # ------------------------------------------------------

    @staticmethod
    def _safe_symbol(symbol: str) -> str:
        """Convierte BTC/USDT → BTC_USDT."""
        return symbol.replace("/", "_")

    def _symbol_timeframe_path(
        self,
        symbol: str,
        timeframe: str,
    ) -> Path:

        safe_symbol = self._safe_symbol(symbol)

        return self.base_path / safe_symbol / timeframe

    def _partition_folder(
        self,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
    ) -> Path:

        folder = (
            self._symbol_timeframe_path(symbol, timeframe)
            / str(year)
            / f"{month:02d}"
        )

        folder.mkdir(parents=True, exist_ok=True)

        return folder

    # ------------------------------------------------------
    # Validation
    # ------------------------------------------------------

    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        """
        Validación mínima antes de escribir.
        """

        if df is None or df.empty:
            raise HistoricalStorageError("Empty dataframe received")

        missing = set(self.REQUIRED_COLUMNS) - set(df.columns)

        if missing:
            raise HistoricalStorageError(
                f"Missing OHLCV columns: {missing}"
            )

    # ------------------------------------------------------
    # Save OHLCV
    # ------------------------------------------------------

    def save_ohlcv(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        mode: str = "append",
    ) -> None:

        if df is None or df.empty:
            logger.warning(f"No data to store → {symbol} {timeframe}")
            return

        self._validate_dataframe(df)

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)

        # --------------------------------------------------
        # Partition by year/month
        # --------------------------------------------------

        grouped = df.groupby(
            [df.timestamp.dt.year, df.timestamp.dt.month]
        )

        for (year, month), group in grouped:

            self._write_partition(
                group,
                symbol,
                timeframe,
                year,
                month,
                mode,
            )

    # ------------------------------------------------------
    # Partition Writer
    # ------------------------------------------------------

    def _write_partition(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
        mode: str,
    ) -> None:

        safe_symbol = self._safe_symbol(symbol)

        folder = self._partition_folder(
            symbol,
            timeframe,
            year,
            month,
        )

        file_path = folder / f"{safe_symbol}_{timeframe}.parquet"
        temp_path = file_path.with_suffix(".tmp")

        try:

            # --------------------------------------------------
            # Incremental append
            # --------------------------------------------------

            if file_path.exists() and mode == "append":

                existing_ts = pd.read_parquet(
                    file_path,
                    columns=["timestamp"],
                )

                last_ts = existing_ts["timestamp"].max()

                df = df[df["timestamp"] > last_ts]

                if df.empty:

                    logger.info(
                        f"No new rows → {symbol} {timeframe} {year}-{month:02d}"
                    )
                    return

                existing_full = pd.read_parquet(file_path)

                df = pd.concat(
                    [existing_full, df],
                    ignore_index=True,
                )

            # --------------------------------------------------
            # Cleanup
            # --------------------------------------------------

            df = (
                df
                .sort_values("timestamp")
                .drop_duplicates(subset="timestamp")
                .reset_index(drop=True)
            )

            # --------------------------------------------------
            # Atomic write
            # --------------------------------------------------

            df.to_parquet(
                temp_path,
                compression="snappy",
                index=False,
            )

            temp_path.replace(file_path)

            logger.success(
                f"Stored → {symbol} {timeframe} "
                f"{year}/{month:02d} rows={len(df)}"
            )

        except Exception as e:

            if temp_path.exists():
                temp_path.unlink(missing_ok=True)

            logger.error(
                f"Storage failure → {symbol} {timeframe} {year}-{month:02d}: {e}"
            )

            raise HistoricalStorageError(e)

    # ------------------------------------------------------
    # Last Timestamp
    # ------------------------------------------------------

    def get_last_timestamp(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:

        symbol_path = self._symbol_timeframe_path(symbol, timeframe)

        if not symbol_path.exists():
            return None

        safe_symbol = self._safe_symbol(symbol)

        files = list(
            symbol_path.rglob(f"{safe_symbol}_{timeframe}.parquet")
        )

        if not files:
            return None

        # usamos el archivo más reciente por fecha de modificación
        latest_file = max(
            files,
            key=lambda p: p.stat().st_mtime
        )

        try:

            df = pd.read_parquet(
                latest_file,
                columns=["timestamp"],
            )

            return pd.to_datetime(df["timestamp"].max())

        except Exception as e:

            logger.warning(
                f"Failed reading timestamp from {latest_file}: {e}"
            )

            return None