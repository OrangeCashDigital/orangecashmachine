"""
storage.py
==========

Gestión profesional del almacenamiento OHLCV en Data Lake.

Características
---------------
• particionado por símbolo / timeframe / año / mes
• escritura atómica segura
• append incremental con deduplicación
• lectura eficiente de timestamps

Principios
----------
SOLID   – SRP claro
DRY     – paths centralizados
KISS    – implementación simple
SafeOps – escrituras atómicas y limpieza segura
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Literal

import pandas as pd
from loguru import logger


# ==========================================================
# Constants
# ==========================================================

REQUIRED_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

_DEFAULT_DATA_LAKE: tuple[str, ...] = ("data_platform", "data_lake")

WriteMode = Literal["append", "overwrite"]


# ==========================================================
# Exceptions
# ==========================================================

class HistoricalStorageError(Exception):
    """Base error."""


class InvalidDataFrameError(HistoricalStorageError):
    """DataFrame inválido."""


class PartitionWriteError(HistoricalStorageError):
    """Fallo escribiendo partición."""


# ==========================================================
# HistoricalStorage
# ==========================================================

class HistoricalStorage:
    """
    Storage OHLCV basado en parquet particionado.

    Estructura:

    base/
        BTC_USDT/
            1h/
                2024/
                    01/
                        BTC_USDT_1h.parquet
    """

    def __init__(self, base_path: Optional[str | Path] = None) -> None:

        self._base_path: Path = _resolve_base_path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)

        logger.info("HistoricalStorage ready | {}", self._base_path)

    # ======================================================
    # Public API
    # ======================================================

    def save_ohlcv(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        mode: WriteMode = "append",
    ) -> None:

        _validate_dataframe(df)

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=False)

        partitions = df.groupby(
            [df["timestamp"].dt.year, df["timestamp"].dt.month],
            sort=True,
        )

        for (year, month), part in partitions:

            self._write_partition(
                df=part,
                symbol=symbol,
                timeframe=timeframe,
                year=int(year),
                month=int(month),
                mode=mode,
            )

    def get_last_timestamp(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:

        files = self._find_partition_files(symbol, timeframe)

        if not files:
            return None

        timestamps: List[pd.Timestamp] = []

        for f in files:

            ts = _read_max_timestamp(f)

            if ts is not None:
                timestamps.append(ts)

        if not timestamps:
            return None

        return max(timestamps)

    # ======================================================
    # Path helpers
    # ======================================================

    @staticmethod
    def _safe_symbol(symbol: str) -> str:
        return symbol.replace("/", "_")

    def _symbol_timeframe_dir(
        self,
        symbol: str,
        timeframe: str,
    ) -> Path:

        return self._base_path / self._safe_symbol(symbol) / timeframe

    def _partition_dir(
        self,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
    ) -> Path:

        path = (
            self._symbol_timeframe_dir(symbol, timeframe)
            / str(year)
            / f"{month:02d}"
        )

        path.mkdir(parents=True, exist_ok=True)

        return path

    def _partition_file(
        self,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
    ) -> Path:

        safe = self._safe_symbol(symbol)

        return (
            self._partition_dir(symbol, timeframe, year, month)
            / f"{safe}_{timeframe}.parquet"
        )

    def _find_partition_files(
        self,
        symbol: str,
        timeframe: str,
    ) -> List[Path]:

        root = self._symbol_timeframe_dir(symbol, timeframe)

        if not root.exists():
            return []

        safe = self._safe_symbol(symbol)

        pattern = f"{safe}_{timeframe}.parquet"

        return sorted(root.rglob(pattern))

    # ======================================================
    # Partition writer
    # ======================================================

    def _write_partition(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
        mode: WriteMode,
    ) -> None:

        file_path = self._partition_file(symbol, timeframe, year, month)

        temp_path = file_path.with_suffix(".tmp")

        try:

            if file_path.exists() and mode == "append":

                merged = _merge_with_existing(df, file_path)

                if merged is None:
                    logger.debug(
                        "No new rows | {} {} {}-{:02d}",
                        symbol,
                        timeframe,
                        year,
                        month,
                    )
                    return

                df = merged

            df = _clean_partition(df)

            df.to_parquet(
                temp_path,
                compression="snappy",
                index=False,
            )

            temp_path.replace(file_path)

            logger.info(
                "Partition saved | {} {} {}/{:02d} rows={}",
                symbol,
                timeframe,
                year,
                month,
                len(df),
            )

        except Exception as exc:

            _cleanup_temp(temp_path)

            raise PartitionWriteError(
                f"Failed writing partition {symbol}/{timeframe}/{year}/{month}"
            ) from exc


# ==========================================================
# Pure helpers
# ==========================================================

def _resolve_base_path(
    base_path: Optional[str | Path],
) -> Path:

    if base_path:
        return Path(base_path).resolve()

    return Path(__file__).resolve().parents[3].joinpath(*_DEFAULT_DATA_LAKE)


def _validate_dataframe(df: pd.DataFrame) -> None:

    if df is None or df.empty:
        raise InvalidDataFrameError("DataFrame vacío")

    missing = set(REQUIRED_COLUMNS) - set(df.columns)

    if missing:
        raise InvalidDataFrameError(
            f"Missing OHLCV columns: {sorted(missing)}"
        )


def _merge_with_existing(
    new_df: pd.DataFrame,
    file_path: Path,
) -> Optional[pd.DataFrame]:

    existing_ts = pd.read_parquet(file_path, columns=["timestamp"])

    last_ts = existing_ts["timestamp"].max()

    new_rows = new_df[new_df["timestamp"] > last_ts]

    if new_rows.empty:
        return None

    existing_full = pd.read_parquet(file_path)

    return pd.concat(
        [existing_full, new_rows],
        ignore_index=True,
    )


def _clean_partition(
    df: pd.DataFrame,
) -> pd.DataFrame:

    return (
        df.sort_values("timestamp")
        .drop_duplicates(subset="timestamp")
        .reset_index(drop=True)
    )


def _read_max_timestamp(
    file: Path,
) -> Optional[pd.Timestamp]:

    try:

        df = pd.read_parquet(file, columns=["timestamp"])

        return pd.to_datetime(df["timestamp"].max())

    except Exception as exc:

        logger.warning(
            "Timestamp read failed | {} | {}",
            file,
            exc,
        )

        return None


def _cleanup_temp(temp_path: Path) -> None:

    try:

        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    except Exception as exc:

        logger.warning(
            "Temp cleanup failed | {} | {}",
            temp_path,
            exc,
        )