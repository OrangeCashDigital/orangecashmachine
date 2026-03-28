"""
market_data_loader.py
=====================

MarketDataLoader — OrangeCashMachine Data Platform

Responsabilidad
---------------
Cargar datos OHLCV desde la capa Silver del Data Lakehouse,
con soporte de versiones para reproducibilidad total.

Filosofía de versionado
------------------------
• "latest"     → lee la última versión del dataset
• version="v000042" → lee exactamente esa versión (reproducible)
• as_of="2026-03-17T22:40:00Z" → lee la versión vigente en ese momento

Esto garantiza que cualquier backtest o modelo puede ser
reproducido exactamente, incluso meses después.

Diseñado para
-------------
• Research y backtesting reproducible
• Feature Engineering
• Machine Learning pipelines
• Auditoría de datos

Principios aplicados
--------------------
• SOLID  – responsabilidades separadas, dependencias inyectables
• DRY    – lógica de lectura centralizada
• KISS   – API simple: load_ohlcv(symbol, timeframe, version)
• SafeOps – errores explícitos, resultado tipado, pushdown filters
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from core.utils import silver_ohlcv_root
from data_platform.ohlcv_utils import safe_symbol, normalize_ohlcv_df


# ==========================================================
# Constants
# ==========================================================

OHLCV_COLUMNS: tuple[str, ...] = (
    "timestamp", "open", "high", "low", "close", "volume"
)



# ==========================================================
# Exceptions
# ==========================================================

class MarketDataLoaderError(Exception):
    """Error base del loader."""

class DataNotFoundError(MarketDataLoaderError):
    """No existen datos para el símbolo/timeframe/versión solicitado."""

class DataReadError(MarketDataLoaderError):
    """Ningún archivo Parquet pudo ser leído."""

class VersionNotFoundError(MarketDataLoaderError):
    """La versión solicitada no existe."""


# ==========================================================
# Result Types
# ==========================================================

@dataclass
class MultiSymbolResult:
    """Resultado tipado de una carga multi-símbolo."""
    data:   Dict[str, pd.DataFrame] = field(default_factory=dict)
    errors: Dict[str, str]          = field(default_factory=dict)

    @property
    def loaded(self) -> List[str]: return list(self.data.keys())

    @property
    def failed(self) -> List[str]: return list(self.errors.keys())

    @property
    def all_succeeded(self) -> bool: return not self.errors

    def log_summary(self) -> None:
        logger.info(
            "Multi-symbol load | loaded={} failed={}",
            len(self.loaded), len(self.failed),
        )
        for symbol, err in self.errors.items():
            logger.warning("  ✗ {} → {}", symbol, err)


# ==========================================================
# MarketDataLoader
# ==========================================================

class MarketDataLoader:
    """
    Carga DataFrames OHLCV desde la capa Silver del Data Lakehouse.

    Soporta carga versionada para reproducibilidad total:
    - load_ohlcv("BTC/USDT", "1h")                     → última versión
    - load_ohlcv("BTC/USDT", "1h", version="v000005")  → versión exacta
    - load_ohlcv("BTC/USDT", "1h", as_of="2026-03-17") → snapshot temporal

    Uso típico
    ----------
    loader = MarketDataLoader(exchange="kucoin")

    # Carga simple (última versión)
    df = loader.load_ohlcv("BTC/USDT", "1h")

    # Carga reproducible (versión exacta para backtesting)
    df = loader.load_ohlcv("BTC/USDT", "1h", version="v000003")

    # Snapshot temporal
    df = loader.load_ohlcv("BTC/USDT", "1h", as_of="2026-03-17T22:40:00Z")

    # Rango de fechas
    df = loader.load_ohlcv_range("BTC/USDT", "1h", start="2026-01-01")
    """

    def __init__(
        self,
        data_lake_path: Optional[str | Path] = None,
        exchange: Optional[str] = None,
    ) -> None:
        self._base_path = _resolve_base_path(data_lake_path)
        self._exchange  = exchange.lower() if exchange else None
        logger.info(
            "MarketDataLoader ready | path={} exchange={}",
            self._base_path, self._exchange or "any",
        )

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    def load_ohlcv(
        self,
        symbol:    str,
        timeframe: str,
        version:   str = "latest",
        as_of:     Optional[str] = None,
        columns:   Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Carga datos OHLCV completos usando el sistema de versiones.

        Parameters
        ----------
        symbol : str
            Par de trading, e.g. "BTC/USDT".
        timeframe : str
            Intervalo temporal, e.g. "1h".
        version : str
            "latest" (default) o versión específica como "v000003".
        as_of : str, optional
            ISO 8601 timestamp. Si se pasa, busca la versión vigente
            en ese momento (reproducibilidad temporal).
        columns : list[str], optional
            Subconjunto de columnas para reducir memoria.

        Returns
        -------
        pd.DataFrame
            DataFrame ordenado por timestamp, sin duplicados.

        Raises
        ------
        VersionNotFoundError
            Si la versión solicitada no existe.
        DataNotFoundError / DataReadError
            Si no hay datos o no pueden leerse.
        """
        # Resolver versión
        manifest = self._resolve_version(symbol, timeframe, version, as_of)

        if manifest:
            # Carga guiada por manifest (reproducible)
            files = self._files_from_manifest(manifest)
            logger.debug(
                "Loading from manifest | {}/{} {} version={} partitions={}",
                symbol, timeframe, self._exchange or "any",
                manifest.get("version_id", version), len(files),
            )
        else:
            # Fallback: carga directa del filesystem (compatibilidad)
            files = self._find_parquet_files(symbol, timeframe)
            logger.debug(
                "Loading from filesystem (no manifest) | {}/{} files={}",
                symbol, timeframe, len(files),
            )

        df = _read_parquet_files(files, columns=columns)
        df = normalize_ohlcv_df(df)

        logger.info(
            "OHLCV loaded | {}/{} exchange={} version={} rows={}",
            symbol, timeframe, self._exchange or "any",
            manifest.get("version_id", "filesystem") if manifest else "filesystem",
            len(df),
        )
        return df

    def load_ohlcv_range(
        self,
        symbol:     str,
        timeframe:  str,
        start_date: Optional[str] = None,
        end_date:   Optional[str] = None,
        version:    str = "latest",
        columns:    Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Carga OHLCV filtrando por rango de fechas.

        Combina versionado con pushdown de filtros para eficiencia.
        """
        start_ts = _parse_optional_timestamp(start_date, "start_date")
        end_ts   = _parse_optional_timestamp(end_date, "end_date")
        _validate_date_range(start_ts, end_ts)

        manifest = self._resolve_version(symbol, timeframe, version, None)

        if manifest:
            files = self._files_from_manifest(manifest)
        else:
            files = self._find_parquet_files(symbol, timeframe)

        filters = _build_parquet_filters(start_ts, end_ts)
        df = _read_parquet_files(files, columns=columns, filters=filters)
        df = normalize_ohlcv_df(df)

        logger.info(
            "OHLCV range loaded | {}/{} start={} end={} rows={}",
            symbol, timeframe, start_date, end_date, len(df),
        )
        return df

    def load_multiple_symbols(
        self,
        symbols:    List[str],
        timeframe:  str,
        start_date: Optional[str] = None,
        end_date:   Optional[str] = None,
        version:    str = "latest",
    ) -> MultiSymbolResult:
        """Carga varios símbolos con la misma versión."""
        result = MultiSymbolResult()
        for symbol in symbols:
            try:
                df = self.load_ohlcv_range(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                    version=version,
                )
                result.data[symbol] = df
            except MarketDataLoaderError as exc:
                result.errors[symbol] = str(exc)
                logger.warning("Symbol load failed | {} → {}", symbol, exc)
        result.log_summary()
        return result

    def list_versions(self, symbol: str, timeframe: str) -> List[str]:
        """Lista todas las versiones disponibles para un dataset."""
        versions_dir = self._versions_dir(symbol, timeframe)
        if not versions_dir.exists():
            return []
        return sorted(
            p.stem for p in versions_dir.glob("v*.json")
        )

    # ----------------------------------------------------------
    # Version resolution
    # ----------------------------------------------------------

    def _resolve_version(
        self,
        symbol: str,
        timeframe: str,
        version: str,
        as_of: Optional[str],
    ) -> Optional[Dict]:
        """
        Resuelve el manifest de versión correcto.

        Lógica
        ------
        1. Si as_of → busca la versión más reciente anterior a ese timestamp
        2. Si version="latest" → lee latest.json
        3. Si version="v000042" → lee ese archivo específico
        4. Si no existe el manifest → retorna None (fallback a filesystem)
        """
        versions_dir = self._versions_dir(symbol, timeframe)

        if not versions_dir.exists():
            return None

        if as_of is not None:
            return self._resolve_version_as_of(versions_dir, as_of)

        if version == "latest":
            path = versions_dir / "latest.json"
        else:
            path = versions_dir / f"{version}.json"

        if not path.exists():
            if version == "latest":
                return None  # Sin versiones aún, fallback OK
            raise VersionNotFoundError(
                f"Version '{version}' not found for {symbol}/{timeframe}. "
                f"Available: {self.list_versions(symbol, timeframe)}"
            )

        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Version manifest read failed | {} | {}", path, exc)
            return None

    def _resolve_version_as_of(
        self,
        versions_dir: Path,
        as_of: str,
    ) -> Optional[Dict]:
        """
        Busca la versión más reciente cuyo written_at <= as_of.

        Permite reconstruir el estado exacto del dataset en cualquier
        momento pasado (reproducibilidad temporal).
        """
        try:
            as_of_ts = pd.Timestamp(as_of, tz="UTC")
        except Exception:
            raise ValueError(f"Invalid as_of timestamp: '{as_of}'")

        candidates = []
        for path in sorted(versions_dir.glob("v*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                written_at = pd.Timestamp(data["written_at"])
                if written_at.tz is None:
                    written_at = written_at.tz_localize("UTC")
                if written_at <= as_of_ts:
                    candidates.append((written_at, data))
            except Exception:
                continue

        if not candidates:
            return None

        # La más reciente que sea <= as_of
        candidates.sort(key=lambda x: x[0])
        _, manifest = candidates[-1]
        logger.debug(
            "Resolved version as_of={} → {}",
            as_of, manifest.get("version_id"),
        )
        return manifest

    def _files_from_manifest(self, manifest: Dict) -> List[Path]:
        """Obtiene paths de particiones desde un manifest de versión."""
        files = []
        for partition in manifest.get("partitions", []):
            path_str = partition.get("path", "")
            if path_str:
                full_path = self._base_path / path_str
                if full_path.exists():
                    files.append(full_path)
                else:
                    logger.warning("Manifest partition not found | {}", full_path)
        return sorted(files)

    # ----------------------------------------------------------
    # Path helpers
    # ----------------------------------------------------------

    @staticmethod
    def _safe_symbol(symbol: str) -> str:
        return safe_symbol(symbol)

    def _dataset_root(self, symbol: str, timeframe: str) -> Path:
        if self._exchange:
            return (
                self._base_path
                / f"exchange={self._exchange}"
                / f"symbol={self._safe_symbol(symbol)}"
                / f"timeframe={timeframe}"
            )
        # Sin exchange: busca en todos los exchanges disponibles
        return self._base_path / f"symbol={self._safe_symbol(symbol)}" / f"timeframe={timeframe}"

    def _versions_dir(self, symbol: str, timeframe: str) -> Path:
        return self._dataset_root(symbol, timeframe) / "_versions"

    def _find_parquet_files(self, symbol: str, timeframe: str) -> List[Path]:
        """Fallback: busca archivos directamente en filesystem."""
        # Buscar en todos los exchanges si no se especificó uno
        if self._exchange:
            root = self._dataset_root(symbol, timeframe)
            if not root.exists():
                raise DataNotFoundError(
                    f"No data for '{symbol}/{timeframe}' exchange='{self._exchange}'"
                )
            pattern = f"{self._safe_symbol(symbol)}_{timeframe}.parquet"
            files = sorted(root.rglob(pattern))
        else:
            # Buscar en todos los exchanges
            files = []
            for exchange_dir in self._base_path.glob("exchange=*"):
                sym_tf = exchange_dir / f"symbol={self._safe_symbol(symbol)}" / f"timeframe={timeframe}"
                if sym_tf.exists():
                    pattern = f"{self._safe_symbol(symbol)}_{timeframe}.parquet"
                    files.extend(sorted(sym_tf.rglob(pattern)))
            files = sorted(set(files))

        if not files:
            raise DataNotFoundError(
                f"No Parquet files for '{symbol}/{timeframe}'"
            )
        return files


# ==========================================================
# Module-Level Private Helpers
# ==========================================================

def _resolve_base_path(data_lake_path: Optional[str | Path]) -> Path:
    """Resuelve el path base del Data Lake Silver. Usa silver_ohlcv_root() como default."""
    base = Path(data_lake_path).resolve() if data_lake_path else silver_ohlcv_root()
    if not base.exists():
        raise MarketDataLoaderError(
            f"Silver Data Lake not found → {base}\n"
            "Run the ingestion pipeline first."
        )
    return base


def _read_parquet_files(
    files:   List[Path],
    columns: Optional[List[str]] = None,
    filters: Optional[list]      = None,
) -> pd.DataFrame:
    dfs: List[pd.DataFrame] = []
    errors: List[str] = []

    for file in files:
        try:
            df = pd.read_parquet(file, columns=columns, filters=filters)
            if not df.empty:
                dfs.append(df)
        except Exception as exc:
            errors.append(f"{file.name}: {exc}")
            logger.warning("Parquet read failed | {} | {}", file, exc)

    if not dfs:
        raise DataReadError(
            f"All {len(files)} files failed. "
            f"First: {errors[0] if errors else 'unknown'}"
        )
    return pd.concat(dfs, ignore_index=True)


def _parse_optional_timestamp(value: Optional[str], param: str) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    try:
        return pd.Timestamp(value)
    except Exception:
        raise ValueError(f"Invalid date for '{param}': '{value}'")


def _validate_date_range(
    start: Optional[pd.Timestamp],
    end: Optional[pd.Timestamp],
) -> None:
    if start and end and start > end:
        raise ValueError(f"start_date ({start.date()}) must be before end_date ({end.date()})")


def _build_parquet_filters(
    start: Optional[pd.Timestamp],
    end: Optional[pd.Timestamp],
) -> Optional[list]:
    filters = []
    if start:
        filters.append(("timestamp", ">=", start))
    if end:
        filters.append(("timestamp", "<=", end))
    return filters if filters else None