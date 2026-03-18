"""
silver_storage.py
=================

Capa Silver del Data Lakehouse.

Responsabilidad
---------------
Almacenar datos OHLCV limpios, deduplicados y validados.
Mantener un registro de versiones por dataset para reproducibilidad.

Diferencias con Bronze
----------------------
• Hace merge + dedup (last-write-wins)
• Normaliza schema y tipos
• Genera versiones trazables en _versions/
• Es determinista: misma entrada → misma salida

Estructura de partición
-----------------------
silver/ohlcv/
  exchange={exchange}/
    symbol={symbol}/
      timeframe={timeframe}/
        {year}/{month}[/{day}]/
          {symbol}_{tf}.parquet
          {symbol}_{tf}.meta.json
        _versions/
          v000001.json
          v000002.json
          latest.json

Versionado
----------
Cada escritura (save_ohlcv) genera un nuevo archivo en _versions/
que lista todas las particiones tocadas, con checksums y rangos.
latest.json apunta siempre a la versión más reciente.
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

_SILVER_SUBPATH = ("data_platform", "data_lake", "silver", "ohlcv")

WriteMode = Literal["append", "overwrite"]


# ==========================================================
# Exceptions
# ==========================================================

class SilverStorageError(Exception):
    """Base error."""

class InvalidDataFrameError(SilverStorageError):
    """Invalid DataFrame."""

class PartitionWriteError(SilverStorageError):
    """Partition write failure."""


# ==========================================================
# SilverStorage
# ==========================================================

class SilverStorage:
    """
    Storage OHLCV limpio con versionado para la capa Silver.

    Garantías
    ---------
    • Idempotente: reinsertar mismos datos no genera duplicados
    • Determinista: misma entrada → misma salida
    • Versionado: cada escritura genera una versión trazable
    • Reproducible: el loader puede pedir versión X o as_of=T

    Uso
    ---
    silver = SilverStorage(exchange="kucoin")
    silver.save_ohlcv(df, symbol="BTC/USDT", timeframe="1m", run_id=run_id)
    """

    def __init__(
        self,
        base_path: Optional[str | Path] = None,
        exchange: Optional[str] = None,
    ) -> None:
        self._base_path: Path = _resolve_base_path(base_path)
        self._exchange: Optional[str] = exchange.lower() if exchange else None
        self._base_path.mkdir(parents=True, exist_ok=True)
        logger.info(
            "SilverStorage ready | exchange={} path={}",
            self._exchange or "shared", self._base_path,
        )

    # ======================================================
    # Public API
    # ======================================================

    def save_ohlcv(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        mode: WriteMode = "append",
        run_id: Optional[str] = None,
    ) -> None:
        """
        Guarda OHLCV limpio y genera una nueva versión del dataset.

        Flujo
        -----
        1. Validar y normalizar DataFrame
        2. Particionar por año/mes (o día para 1m)
        3. Para cada partición: merge + dedup + escritura atómica
        4. Generar versión en _versions/

        Parameters
        ----------
        df : pd.DataFrame
            Datos OHLCV ya transformados.
        symbol : str
            Par de trading.
        timeframe : str
            Intervalo temporal.
        mode : "append" | "overwrite"
            append = merge con existente (recomendado).
            overwrite = reemplaza sin merge.
        run_id : str, optional
            ID del run de ingestión para correlación con bronze.
        """
        _validate_dataframe(df)
        df = _normalize_dataframe(df)

        partitions_written: List[Dict] = []

        use_daily = (timeframe == "1m")

        if use_daily:
            groups = df.groupby(
                [df["timestamp"].dt.year, df["timestamp"].dt.month, df["timestamp"].dt.day],
                sort=True,
            )
            for (year, month, day), part in groups:
                meta = self._write_partition(
                    df=part,
                    symbol=symbol,
                    timeframe=timeframe,
                    year=int(year),
                    month=int(month),
                    mode=mode,
                    day=int(day),
                )
                partitions_written.append(meta)
        else:
            groups = df.groupby(
                [df["timestamp"].dt.year, df["timestamp"].dt.month],
                sort=True,
            )
            for (year, month), part in groups:
                meta = self._write_partition(
                    df=part,
                    symbol=symbol,
                    timeframe=timeframe,
                    year=int(year),
                    month=int(month),
                    mode=mode,
                )
                partitions_written.append(meta)

        # Generar versión del dataset
        if partitions_written:
            self._write_version(symbol, timeframe, partitions_written, run_id)

    def get_last_timestamp(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        """Obtiene el último timestamp disponible leyendo metadata."""
        files = self._find_partition_files(symbol, timeframe)
        if not files:
            return None

        timestamps: List[pd.Timestamp] = []
        for f in files:
            ts = _read_max_timestamp(f)
            if ts is not None:
                timestamps.append(ts)

        return max(timestamps) if timestamps else None

    def get_version(self, symbol: str, timeframe: str, version: str = "latest") -> Optional[Dict]:
        """
        Obtiene el manifest de una versión específica del dataset.

        Parameters
        ----------
        version : str
            "latest" para la última versión, o "v000001" para una específica.

        Returns
        -------
        dict con la info de la versión, o None si no existe.
        """
        versions_dir = self._versions_dir(symbol, timeframe)
        if version == "latest":
            path = versions_dir / "latest.json"
        else:
            path = versions_dir / f"{version}.json"

        if not path.exists():
            return None

        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Version read failed | {} | {}", path, exc)
            return None

    # ======================================================
    # Path helpers
    # ======================================================

    @staticmethod
    def _safe_symbol(symbol: str) -> str:
        return symbol.replace("/", "_")

    def _dataset_root(self, symbol: str, timeframe: str) -> Path:
        """Raíz del dataset para este symbol/timeframe."""
        if self._exchange:
            return (
                self._base_path
                / f"exchange={self._exchange}"
                / f"symbol={self._safe_symbol(symbol)}"
                / f"timeframe={timeframe}"
            )
        return (
            self._base_path
            / f"symbol={self._safe_symbol(symbol)}"
            / f"timeframe={timeframe}"
        )

    def _versions_dir(self, symbol: str, timeframe: str) -> Path:
        path = self._dataset_root(symbol, timeframe) / "_versions"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _partition_dir(
        self,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
        day: Optional[int] = None,
    ) -> Path:
        path = self._dataset_root(symbol, timeframe) / str(year) / f"{month:02d}"
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

    def _find_partition_files(self, symbol: str, timeframe: str) -> List[Path]:
        root = self._dataset_root(symbol, timeframe)
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
    ) -> Dict:
        """
        Escritura atómica de una partición. Devuelve metadata de la partición.

        Secuencia:
        1. Merge con existente si mode=append
        2. Dedup (last-write-wins)
        3. Escribir a .tmp
        4. Atomic rename .tmp → .parquet
        5. Escribir .meta.json sidecar
        """
        file_path = self._partition_file(symbol, timeframe, year, month, day)
        temp_path = file_path.with_suffix(".tmp")
        meta_path = file_path.with_suffix(".meta.json")

        try:
            if file_path.exists() and mode == "append":
                df = _merge_full(df, file_path)

            df = _clean_partition(df)

            df.to_parquet(temp_path, compression="snappy", index=False)
            temp_path.replace(file_path)

            partition_meta = _write_partition_meta(meta_path, df, symbol, timeframe)

            label = f"{year}/{month:02d}/{day:02d}" if day is not None else f"{year}/{month:02d}"
            logger.info(
                "Partition saved | {} {} {} rows={} [{} → {}]",
                symbol, timeframe, label, len(df),
                df["timestamp"].min().isoformat(),
                df["timestamp"].max().isoformat(),
            )

            # Añadir path relativo para el manifest de versión
            partition_meta["path"] = str(file_path.relative_to(self._base_path))
            return partition_meta

        except Exception as exc:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            raise PartitionWriteError(
                f"Failed writing partition {symbol}/{timeframe}/{year}/{month}"
            ) from exc

    # ======================================================
    # Versioning
    # ======================================================

    def _write_version(
        self,
        symbol: str,
        timeframe: str,
        partitions: List[Dict],
        run_id: Optional[str],
    ) -> None:
        """
        Genera un nuevo archivo de versión en _versions/.

        Formato
        -------
        {
          "version": 42,
          "version_id": "v000042",
          "run_id": "20260318T043000-abc12345",
          "written_at": "2026-03-18T04:30:00Z",
          "symbol": "BTC/USDT",
          "timeframe": "1m",
          "exchange": "kucoin",
          "partitions": [
            {
              "path": "...",
              "rows": 1440,
              "min_ts": "...",
              "max_ts": "...",
              "checksum": "..."
            }
          ]
        }
        """
        versions_dir = self._versions_dir(symbol, timeframe)

        # Calcular número de versión secuencial
        existing = sorted(versions_dir.glob("v*.json"))
        version_num = len(existing) + 1
        version_id = f"v{version_num:06d}"

        version_data: Dict = {
            "version":    version_num,
            "version_id": version_id,
            "run_id":     run_id or "unknown",
            "written_at": datetime.now(timezone.utc).isoformat(),
            "symbol":     symbol,
            "timeframe":  timeframe,
            "exchange":   self._exchange or "shared",
            "layer":      "silver",
            "partitions": partitions,
        }

        # Escribir versión específica
        version_path = versions_dir / f"{version_id}.json"
        version_path.write_text(json.dumps(version_data, indent=2), encoding="utf-8")

        # Actualizar latest.json (siempre apunta a la última)
        latest_path = versions_dir / "latest.json"
        latest_path.write_text(json.dumps(version_data, indent=2), encoding="utf-8")

        logger.info(
            "Dataset version created | {}/{} {} exchange={} partitions={}",
            symbol, timeframe, version_id, self._exchange or "shared", len(partitions),
        )


# ==========================================================
# Helpers (puros)
# ==========================================================

def _resolve_base_path(base_path: Optional[str | Path]) -> Path:
    if base_path:
        return Path(base_path).resolve()
    return Path(__file__).resolve().parents[3].joinpath(*_SILVER_SUBPATH)


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


def _write_partition_meta(
    meta_path: Path,
    df: pd.DataFrame,
    symbol: str,
    timeframe: str,
) -> Dict:
    """Escribe sidecar .meta.json y devuelve el dict para el manifest de versión."""
    try:
        ts_col = df["timestamp"]
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
            "layer":      "silver",
        }

        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        return meta

    except Exception as exc:
        logger.warning("Partition meta write failed (non-critical) | {} | {}", meta_path, exc)
        return {"rows": len(df), "error": str(exc)}