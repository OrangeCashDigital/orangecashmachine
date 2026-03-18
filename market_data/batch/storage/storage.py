"""
storage.py
==========

Gestión profesional del almacenamiento OHLCV en Data Lake.

Características
---------------
• particionado por símbolo / timeframe / año / mes
• escritura atómica segura
• idempotencia real mediante merge + deduplicación
• tolerancia a overlap y correcciones de exchange
• lectura eficiente de timestamps

Principios
----------
SOLID   – SRP claro
DRY     – helpers reutilizables
KISS    – flujo simple y explícito
SafeOps – atomicidad + consistencia + cleanup seguro
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Literal

import pandas as pd
from loguru import logger


# ==========================================================
# Constants
# ==========================================================

REQUIRED_COLUMNS: tuple[str, ...] = (
    "timestamp", "open", "high", "low", "close", "volume"
)

_DEFAULT_DATA_LAKE: tuple[str, ...] = ("data_platform", "data_lake")

WriteMode = Literal["append", "overwrite"]


# ==========================================================
# Exceptions
# ==========================================================

class HistoricalStorageError(Exception):
    """Base error."""


class InvalidDataFrameError(HistoricalStorageError):
    """Invalid DataFrame."""


class PartitionWriteError(HistoricalStorageError):
    """Partition write failure."""


# ==========================================================
# Storage
# ==========================================================

class HistoricalStorage:
    """
    Storage OHLCV basado en parquet particionado.

    Garantías:
    ----------
    • Idempotente (puede reinsertar mismos datos sin duplicar)
    • Seguro ante overlap
    • Consistente ante correcciones de exchange
    """

    def __init__(self, base_path: Optional[str | Path] = None, exchange: Optional[str] = None) -> None:
        self._base_path: Path = _resolve_base_path(base_path)
        self._exchange: Optional[str] = exchange.lower() if exchange else None
        self._base_path.mkdir(parents=True, exist_ok=True)

        logger.info("HistoricalStorage ready | {} exchange={}", self._base_path, self._exchange or "shared")

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
        """
        Guarda OHLCV particionado por año/mes.

        SafeOps:
        • idempotente
        • tolera overlap
        • escritura atómica
        """
        _validate_dataframe(df)

        df = _normalize_dataframe(df)

        # 1m usa partición diaria para que cada merge sea barato (O(n) sobre ~1440 filas)
        # Timeframes >= 5m usan partición mensual (volumen manejable)
        use_daily = (timeframe == "1m")

        if use_daily:
            partitions = df.groupby(
                [df["timestamp"].dt.year, df["timestamp"].dt.month, df["timestamp"].dt.day],
                sort=True,
            )
            for (year, month, day), part in partitions:
                self._write_partition(
                    df=part,
                    symbol=symbol,
                    timeframe=timeframe,
                    year=int(year),
                    month=int(month),
                    mode=mode,
                    day=int(day),
                )
        else:
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
        """
        Obtiene el último timestamp disponible (O(n) en particiones).
        """
        files = self._find_partition_files(symbol, timeframe)

        if not files:
            return None

        timestamps: List[pd.Timestamp] = []

        for f in files:
            ts = _read_max_timestamp(f)
            if ts is not None:
                timestamps.append(ts)

        return max(timestamps) if timestamps else None

    # ======================================================
    # Path helpers
    # ======================================================

    @staticmethod
    def _safe_symbol(symbol: str) -> str:
        return symbol.replace("/", "_")

    def _symbol_timeframe_dir(self, symbol: str, timeframe: str) -> Path:
        if self._exchange:
            return self._base_path / "exchanges" / self._exchange / self._safe_symbol(symbol) / timeframe
        return self._base_path / self._safe_symbol(symbol) / timeframe

    def _partition_dir(
        self,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
        day: Optional[int] = None,
    ) -> Path:
        path = self._symbol_timeframe_dir(symbol, timeframe) / str(year) / f"{month:02d}"
        if day is not None:
            path = path / f"{day:02d}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _partition_file(
        self,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
        day: Optional[int] = None,
    ) -> Path:
        safe = self._safe_symbol(symbol)
        return self._partition_dir(symbol, timeframe, year, month, day) / f"{safe}_{timeframe}.parquet"

    def _find_partition_files(
        self,
        symbol: str,
        timeframe: str,
    ) -> List[Path]:
        root = self._symbol_timeframe_dir(symbol, timeframe)

        if not root.exists():
            return []

        pattern = f"{self._safe_symbol(symbol)}_{timeframe}.parquet"
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
        day: Optional[int] = None,
    ) -> None:
        """
        Escritura atómica con merge profesional.

        Secuencia:
        1. Leer partición existente (si existe y mode=append)
        2. Merge + dedup (last-write-wins) — tolera overlap y correcciones
        3. Escribir a .tmp
        4. atomic rename .tmp → .parquet
        5. Escribir metadata sidecar .meta.json

        Garantías:
        • Nunca corrompe datos si el proceso muere entre pasos 3 y 4
        • Idempotente: reinsertar los mismos datos no genera duplicados
        • last-write-wins: correcciones del exchange se aplican correctamente
        """
        file_path = self._partition_file(symbol, timeframe, year, month, day)
        temp_path = file_path.with_suffix(".tmp")
        meta_path = file_path.with_suffix(".meta.json")

        try:
            if file_path.exists() and mode == "append":
                df = _merge_full(df, file_path)

            df = _clean_partition(df)

            df.to_parquet(
                temp_path,
                compression="snappy",
                index=False,
            )

            temp_path.replace(file_path)

            # Metadata sidecar: permite auditoría sin leer el parquet completo
            _write_metadata(meta_path, df, symbol, timeframe)

            partition_label = f"{year}/{month:02d}/{day:02d}" if day is not None else f"{year}/{month:02d}"
            logger.info(
                "Partition saved | {} {} {} rows={} [{} → {}]",
                symbol, timeframe, partition_label, len(df),
                df["timestamp"].min().isoformat(),
                df["timestamp"].max().isoformat(),
            )

        except Exception as exc:
            _cleanup_temp(temp_path)
            raise PartitionWriteError(
                f"Failed writing partition {symbol}/{timeframe}/{year}/{month}"
            ) from exc


# ==========================================================
# Helpers (puros)
# ==========================================================

def _resolve_base_path(base_path: Optional[str | Path]) -> Path:
    if base_path:
        return Path(base_path).resolve()
    return Path(__file__).resolve().parents[3].joinpath(*_DEFAULT_DATA_LAKE)


def _validate_dataframe(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        raise InvalidDataFrameError("DataFrame vacío")

    missing = set(REQUIRED_COLUMNS) - set(df.columns)

    if missing:
        raise InvalidDataFrameError(f"Missing columns: {sorted(missing)}")


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    df = df.dropna(subset=["timestamp"])

    return df


def _merge_full(new_df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
    """
    Merge completo idempotente (PRO):
    • permite overlap
    • corrige datos existentes
    • elimina duplicados
    """
    existing = pd.read_parquet(file_path)
    existing["timestamp"] = pd.to_datetime(existing["timestamp"], utc=True)

    combined = pd.concat([existing, new_df], ignore_index=True)

    return (
        combined
        .sort_values("timestamp")
        .drop_duplicates(subset="timestamp", keep="last")
        .reset_index(drop=True)
    )


def _clean_partition(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.sort_values("timestamp")
        .drop_duplicates(subset="timestamp", keep="last")
        .reset_index(drop=True)
    )


def _read_max_timestamp(file: Path) -> Optional[pd.Timestamp]:
    try:
        df = pd.read_parquet(file, columns=["timestamp"])
        return pd.to_datetime(df["timestamp"].max(), utc=True)
    except Exception as exc:
        logger.warning("Timestamp read failed | {} | {}", file, exc)
        return None


def _cleanup_temp(temp_path: Path) -> None:
    try:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
    except Exception as exc:
        logger.warning("Temp cleanup failed | {} | {}", temp_path, exc)


def _write_metadata(meta_path: Path, df: pd.DataFrame, symbol: str, timeframe: str) -> None:
    """
    Escribe sidecar JSON con metadata de la partición.

    Permite auditoría rápida (min/max/rows/checksum) sin leer el parquet.
    No es crítico — fallo silencioso para no bloquear el pipeline.
    """
    try:
        ts_col = df["timestamp"]
        # Checksum ligero sobre timestamps: detecta corrupción o gaps
        ts_bytes = ts_col.astype("int64").values.tobytes()
        checksum = hashlib.md5(ts_bytes).hexdigest()

        meta: Dict = {
            "symbol":     symbol,
            "timeframe":  timeframe,
            "rows":       len(df),
            "min_ts":     ts_col.min().isoformat(),
            "max_ts":     ts_col.max().isoformat(),
            "checksum":   checksum,
            "written_at": datetime.now(timezone.utc).isoformat(),
        }

        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    except Exception as exc:
        logger.warning("Metadata write failed (non-critical) | {} | {}", meta_path, exc)