"""
bronze_storage.py
=================

Capa Bronze del Data Lakehouse.

Responsabilidad
---------------
Almacenar datos OHLCV exactamente como llegan del exchange,
sin ninguna transformación, deduplicación ni corrección.

Principios
----------
• Append-only: nunca sobreescribe, nunca hace merge
• Inmutable: cada ingestión genera un archivo nuevo (part-{run_id})
• Forense: es la "caja negra" — si el exchange corrige datos,
  aquí se puede ver qué había antes
• Sin pérdida: duplicados, gaps y anomalías se preservan tal cual

Estructura de partición
-----------------------
bronze/ohlcv/
  exchange={exchange}/
    symbol={symbol}/
      timeframe={timeframe}/
        dt={YYYY-MM-DD}/
          part-{run_id}.parquet
          part-{run_id}.meta.json
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from loguru import logger

from core.config.paths import bronze_ohlcv_root
from data_platform.ohlcv_utils import safe_symbol as _safe_symbol_fn


# ==========================================================
# Constants
# ==========================================================

REQUIRED_COLUMNS: tuple[str, ...] = (
    "timestamp", "open", "high", "low", "close", "volume"
)

# _BRONZE_SUBPATH eliminado — path resuelto via core.config.paths


# ==========================================================
# Exceptions
# ==========================================================

class BronzeStorageError(Exception):
    """Base error."""

class BronzeWriteError(BronzeStorageError):
    """Fallo de escritura en bronze."""


# ==========================================================
# BronzeStorage
# ==========================================================

class BronzeStorage:
    """
    Writer append-only para la capa Bronze.

    Cada llamada a append() genera un nuevo archivo part-{run_id}.parquet
    en la partición correspondiente. Nunca lee ni modifica archivos
    existentes — solo escribe nuevos.

    Uso
    ---
    bronze = BronzeStorage(exchange="kucoin")
    bronze.append(df, symbol="BTC/USDT", timeframe="1m", run_id=run_id)
    """

    def __init__(
        self,
        base_path: Optional[str | Path] = None,
        exchange: Optional[str] = None,
    ) -> None:
        self._base_path: Path = _resolve_base_path(base_path)
        self._exchange: str = (exchange or "unknown").lower()
        self._base_path.mkdir(parents=True, exist_ok=True)
        logger.debug(
            "BronzeStorage ready | exchange={} path={}",
            self._exchange, self._base_path,
        )

    # ======================================================
    # Public API
    # ======================================================

    def append(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        run_id: Optional[str] = None,
    ) -> str:
        """
        Escribe df como un nuevo archivo part en bronze.

        El df se guarda exactamente como viene — sin dedup, sin merge,
        sin normalización (solo se añade ingestion_ts para trazabilidad).

        Parameters
        ----------
        df : pd.DataFrame
            Datos OHLCV tal como vienen del fetcher.
        symbol : str
            Par de trading, e.g. "BTC/USDT".
        timeframe : str
            Intervalo, e.g. "1m".
        run_id : str, optional
            ID del run de ingestión. Se genera uno si no se pasa.

        Returns
        -------
        str
            run_id usado (útil para correlacionar con silver/manifests).
        """
        _validate_dataframe(df)

        if run_id is None:
            run_id = _generate_run_id()

        # Añadir ingestion_ts para trazabilidad forense
        df = df.copy()
        df["ingestion_ts"] = datetime.now(timezone.utc).isoformat()

        # Normalizar timestamp solo para particionado (no modifica datos)
        ts_col = pd.to_datetime(df["timestamp"], unit="ms", utc=True, errors="coerce")
        ts_col = ts_col.fillna(pd.to_datetime(df["timestamp"], utc=True, errors="coerce"))

        # Agrupar por día para particionado
        dates = ts_col.dt.date.unique()

        for date in sorted(dates):
            mask = ts_col.dt.date == date
            part_df = df[mask].copy()
            if part_df.empty:
                continue
            self._write_part(part_df, symbol, timeframe, date, run_id)

        logger.debug(
            "Bronze append | exchange={} symbol={} timeframe={} run_id={} rows={}",
            self._exchange, symbol, timeframe, run_id, len(df),
        )
        return run_id

    # ======================================================
    # Path helpers
    # ======================================================

    @staticmethod
    def _safe_symbol(symbol: str) -> str:
        # Delega a data_platform.ohlcv_utils.safe_symbol — SSoT
        return _safe_symbol_fn(symbol)

    def _partition_dir(self, symbol: str, timeframe: str, date) -> Path:
        path = (
            self._base_path
            / f"exchange={self._exchange}"
            / f"symbol={self._safe_symbol(symbol)}"
            / f"timeframe={timeframe}"
            / f"dt={date}"
        )
        path.mkdir(parents=True, exist_ok=True)
        return path

    # ======================================================
    # Writer
    # ======================================================

    def _write_part(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        date,
        run_id: str,
    ) -> None:
        part_dir  = self._partition_dir(symbol, timeframe, date)
        part_file = part_dir / f"part-{run_id}.parquet"
        meta_file = part_dir / f"part-{run_id}.meta.json"
        temp_file = part_dir / f"part-{run_id}.tmp"

        if part_file.exists():
            raise BronzeWriteError(
                f"Duplicate run_id detected — {part_file.name} already exists"
            )

        try:
            df.to_parquet(temp_file, compression="snappy", index=False)
            temp_file.replace(part_file)
            _write_part_meta(meta_file, df, symbol, timeframe, run_id)

        except Exception as exc:
            if temp_file.exists():
                temp_file.unlink(missing_ok=True)
            raise BronzeWriteError(
                f"Failed writing bronze part {symbol}/{timeframe}/{date}"
            ) from exc


# ==========================================================
# Helpers
# ==========================================================

def _resolve_base_path(base_path: Optional[str | Path]) -> Path:
    """
    Resuelve el path base de Bronze.

    Orden de resolución:
    1. base_path explícito (argumento del constructor)
    2. core.config.paths.bronze_ohlcv_root() — lee storage.data_lake.path del YAML
       o OCM_DATA_LAKE_PATH si está seteada
    """
    if base_path:
        return Path(base_path).resolve()
    return bronze_ohlcv_root()


def _validate_dataframe(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        raise BronzeStorageError("DataFrame vacío")
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise BronzeStorageError(f"Missing columns: {sorted(missing)}")
    unexpected = set(df.columns) - set(REQUIRED_COLUMNS) - {"ingestion_ts"}
    if unexpected:
        logger.warning(
            "Bronze schema drift — columnas inesperadas serán preservadas | cols={}",
            sorted(unexpected),
        )


def _generate_run_id() -> str:
    """Genera un run_id único basado en timestamp + uuid corto."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"{ts}-{uid}"


def _write_part_meta(
    meta_path: Path,
    df: pd.DataFrame,
    symbol: str,
    timeframe: str,
    run_id: str,
) -> None:
    try:
        ts_col = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        ts_bytes = ts_col.dropna().astype("int64").values.tobytes()
        checksum = hashlib.md5(ts_bytes).hexdigest()

        meta: Dict = {
            "run_id":      run_id,
            "symbol":      symbol,
            "timeframe":   timeframe,
            "rows":        len(df),
            "min_ts":      str(ts_col.min()),
            "max_ts":      str(ts_col.max()),
            "checksum":    checksum,
            "written_at":  datetime.now(timezone.utc).isoformat(),
            "layer":       "bronze",
        }
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Bronze meta write failed (non-critical) | {} | {}", meta_path, exc)