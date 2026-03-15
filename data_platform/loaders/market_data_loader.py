"""
MarketDataLoader – OrangeCashMachine
====================================

Responsabilidad
---------------
Cargar datos de mercado desde el Data Lake en formato Parquet.

Diseñado para
-------------
• Research
• Feature Engineering
• Backtesting
• Machine Learning

Principios aplicados
--------------------
• SOLID  – responsabilidades separadas, dependencias inyectables
• DRY    – lógica de lectura Parquet centralizada en un método
• KISS   – sin abstracciones innecesarias
• SafeOps – filtrado con pushdown en lectura, errores explícitos,
            resultado tipado para carga multi-símbolo
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger


# ==========================================================
# Constants
# ==========================================================

# Columnas mínimas requeridas en todo archivo Parquet OHLCV
OHLCV_COLUMNS: tuple[str, ...] = (
    "timestamp", "open", "high", "low", "close", "volume"
)

# Ruta por defecto relativa al paquete data_platform
_DEFAULT_DATA_LAKE_SUBPATH: tuple[str, ...] = (
    "data_platform", "data_lake", "ohlcv"
)


# ==========================================================
# Exceptions
# ==========================================================

class MarketDataLoaderError(Exception):
    """
    Error base del MarketDataLoader.

    Separa errores de dominio (datos faltantes, paths inválidos)
    de errores genéricos de Python, facilitando el manejo
    selectivo por parte del caller.
    """


class DataNotFoundError(MarketDataLoaderError):
    """No existen archivos para el símbolo/timeframe solicitado."""


class DataReadError(MarketDataLoaderError):
    """Ningún archivo Parquet pudo ser leído correctamente."""


# ==========================================================
# Result Type for Multi-Symbol Load
# ==========================================================

@dataclass
class MultiSymbolResult:
    """
    Resultado tipado de una carga multi-símbolo.

    Permite al caller distinguir qué símbolos cargaron bien
    y cuáles fallaron, sin swallow silencioso de errores (SafeOps).
    """
    data:   Dict[str, pd.DataFrame] = field(default_factory=dict)
    errors: Dict[str, str]          = field(default_factory=dict)

    @property
    def loaded(self) -> List[str]:
        return list(self.data.keys())

    @property
    def failed(self) -> List[str]:
        return list(self.errors.keys())

    @property
    def all_succeeded(self) -> bool:
        return not self.errors

    def log_summary(self) -> None:
        logger.info(
            "Multi-symbol load summary | loaded={} failed={}",
            len(self.loaded), len(self.failed),
        )
        for symbol, err in self.errors.items():
            logger.warning("  ✗ {} → {}", symbol, err)


# ==========================================================
# MarketDataLoader
# ==========================================================

class MarketDataLoader:
    """
    Carga DataFrames OHLCV desde el Data Lake en formato Parquet.

    Características
    ---------------
    • Pushdown de filtros de fecha en lectura (no carga todo en memoria)
    • Detección automática del path del Data Lake
    • Inyección de dependencias para testing (data_lake_path)
    • Resultado tipado para carga multi-símbolo

    Uso típico
    ----------
    loader = MarketDataLoader()

    # Carga completa
    df = loader.load_ohlcv("BTC/USDT", "1h")

    # Filtrado eficiente por rango
    df = loader.load_ohlcv_range("BTC/USDT", "1h", start="2023-01-01")

    # Multi-símbolo
    result = loader.load_multiple_symbols(["BTC/USDT", "ETH/USDT"], "1h")
    """

    def __init__(self, data_lake_path: Optional[str | Path] = None) -> None:
        self._base_path = _resolve_base_path(data_lake_path)
        logger.info("MarketDataLoader ready | path={}", self._base_path)

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    def load_ohlcv(
        self,
        symbol:    str,
        timeframe: str,
        columns:   Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Carga todos los datos OHLCV disponibles para un par.

        Parameters
        ----------
        symbol : str
            Ejemplo: "BTC/USDT"
        timeframe : str
            Ejemplo: "1h", "5m"
        columns : list[str], optional
            Subconjunto de columnas a leer. Si es None, carga todas.
            Usar para optimizar memoria cuando solo se necesita, p.ej.,
            ["timestamp", "close"].

        Returns
        -------
        pd.DataFrame
            DataFrame ordenado por timestamp, sin duplicados de índice.

        Raises
        ------
        DataNotFoundError
            Si no existen archivos para el par solicitado.
        DataReadError
            Si ningún archivo Parquet pudo leerse.
        """
        files = self._find_parquet_files(symbol, timeframe)
        logger.debug(
            "Loading OHLCV | symbol={} timeframe={} files={}",
            symbol, timeframe, len(files),
        )

        df = _read_parquet_files(files, columns=columns)

        df = (
            df
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        logger.info(
            "OHLCV loaded | symbol={} timeframe={} rows={}",
            symbol, timeframe, len(df),
        )
        return df

    def load_ohlcv_range(
        self,
        symbol:     str,
        timeframe:  str,
        start_date: Optional[str] = None,
        end_date:   Optional[str] = None,
        columns:    Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Carga datos OHLCV filtrando por rango de fechas.

        El filtrado se aplica como pushdown de predicados en la lectura
        Parquet — no se carga el dataset completo en memoria.

        Parameters
        ----------
        symbol, timeframe : str
            Par e intervalo temporal.
        start_date : str, optional
            Fecha inicio ISO 8601, e.g. "2023-01-01".
        end_date : str, optional
            Fecha fin ISO 8601, e.g. "2023-12-31".
        columns : list[str], optional
            Subconjunto de columnas a leer.

        Returns
        -------
        pd.DataFrame
            DataFrame filtrado, ordenado por timestamp.

        Raises
        ------
        ValueError
            Si start_date o end_date tienen formato inválido.
        DataNotFoundError / DataReadError
            Igual que load_ohlcv.
        """
        start_ts = _parse_optional_timestamp(start_date, "start_date")
        end_ts   = _parse_optional_timestamp(end_date,   "end_date")

        _validate_date_range(start_ts, end_ts)

        files = self._find_parquet_files(symbol, timeframe)

        # Pushdown: construimos los filtros pyarrow/parquet
        filters = _build_parquet_filters(start_ts, end_ts)

        logger.debug(
            "Loading OHLCV range | symbol={} timeframe={} start={} end={} files={}",
            symbol, timeframe, start_date, end_date, len(files),
        )

        df = _read_parquet_files(files, columns=columns, filters=filters)

        df = (
            df
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        logger.info(
            "OHLCV range loaded | symbol={} timeframe={} start={} end={} rows={}",
            symbol, timeframe, start_date, end_date, len(df),
        )
        return df

    def load_multiple_symbols(
        self,
        symbols:   List[str],
        timeframe: str,
        start_date: Optional[str] = None,
        end_date:   Optional[str] = None,
    ) -> MultiSymbolResult:
        """
        Carga varios símbolos para el mismo timeframe.

        A diferencia del original, devuelve un MultiSymbolResult tipado
        en lugar de un dict anónimo. El caller sabe exactamente qué
        símbolos fallaron y por qué (SafeOps: sin swallow silencioso).

        Parameters
        ----------
        symbols : list[str]
            Lista de símbolos a cargar.
        timeframe : str
            Intervalo temporal común.
        start_date, end_date : str, optional
            Filtro de rango aplicado a todos los símbolos.

        Returns
        -------
        MultiSymbolResult
        """
        result = MultiSymbolResult()

        for symbol in symbols:
            try:
                df = self.load_ohlcv_range(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_date=start_date,
                    end_date=end_date,
                )
                result.data[symbol] = df
            except MarketDataLoaderError as exc:
                result.errors[symbol] = str(exc)
                logger.warning(
                    "Symbol load failed | symbol={} error={}", symbol, exc
                )

        result.log_summary()
        return result

    # ----------------------------------------------------------
    # Private Helpers
    # ----------------------------------------------------------

    @staticmethod
    def _safe_symbol(symbol: str) -> str:
        """Convierte 'BTC/USDT' → 'BTC_USDT' para uso en paths."""
        return symbol.replace("/", "_")

    def _get_symbol_dir(self, symbol: str) -> Path:
        """
        Devuelve el directorio del símbolo en el Data Lake.

        Raises
        ------
        DataNotFoundError
            Si el directorio no existe.
        """
        symbol_dir = self._base_path / self._safe_symbol(symbol)

        if not symbol_dir.exists():
            raise DataNotFoundError(
                f"No data directory found for symbol '{symbol}' "
                f"at {symbol_dir}"
            )
        return symbol_dir

    def _find_parquet_files(self, symbol: str, timeframe: str) -> List[Path]:
        """
        Localiza todos los archivos Parquet para un par.

        El patrón de búsqueda coincide con la convención de nombrado
        del HistoricalStorage: <SYMBOL>_<TIMEFRAME>.parquet

        Returns
        -------
        list[Path]
            Archivos ordenados por nombre (orden cronológico si el
            storage usa fechas en el path de partición).

        Raises
        ------
        DataNotFoundError
            Si no se encuentra ningún archivo.
        """
        symbol_dir  = self._get_symbol_dir(symbol)
        safe_symbol = self._safe_symbol(symbol)
        pattern     = f"{safe_symbol}_{timeframe}.parquet"

        files = sorted(symbol_dir.rglob(pattern))

        if not files:
            raise DataNotFoundError(
                f"No Parquet files found for '{symbol}' / '{timeframe}' "
                f"under {symbol_dir} (pattern: {pattern})"
            )

        logger.debug(
            "Parquet files found | symbol={} timeframe={} count={}",
            symbol, timeframe, len(files),
        )
        return files


# ==========================================================
# Module-Level Private Helpers
# ==========================================================
# Funciones puras sin estado: más fáciles de testear de forma
# independiente y reutilizables fuera de la clase.
# ==========================================================

def _resolve_base_path(data_lake_path: Optional[str | Path]) -> Path:
    """
    Resuelve y valida el path base del Data Lake.

    Raises
    ------
    MarketDataLoaderError
        Si el directorio no existe.
    """
    if data_lake_path:
        base = Path(data_lake_path).resolve()
    else:
        base = Path(__file__).resolve().parents[2].joinpath(
            *_DEFAULT_DATA_LAKE_SUBPATH
        )

    if not base.exists():
        raise MarketDataLoaderError(
            f"Data Lake directory not found → {base}\n"
            "Check that the pipeline has ingested data before loading."
        )
    return base


def _read_parquet_files(
    files:   List[Path],
    columns: Optional[List[str]] = None,
    filters: Optional[list]      = None,
) -> pd.DataFrame:
    """
    Lee y concatena una lista de archivos Parquet.

    Aplica column pruning y pushdown de filtros si se proporcionan,
    evitando cargar datos innecesarios en memoria.

    Parameters
    ----------
    files : list[Path]
        Archivos a leer.
    columns : list[str], optional
        Subconjunto de columnas (column pruning).
    filters : list, optional
        Filtros en formato pyarrow/pandas Parquet, e.g.:
        [("timestamp", ">=", start_ts), ("timestamp", "<=", end_ts)]

    Returns
    -------
    pd.DataFrame
        DataFrame concatenado. Puede estar vacío si los filtros
        excluyen todos los datos.

    Raises
    ------
    DataReadError
        Si ningún archivo pudo leerse.
    """
    dfs: List[pd.DataFrame] = []
    read_errors: List[str]  = []

    for file in files:
        try:
            df = pd.read_parquet(
                file,
                columns=columns,
                filters=filters,   # pushdown nativo de Parquet
            )
            if not df.empty:
                dfs.append(df)
        except Exception as exc:
            read_errors.append(f"{file.name}: {exc}")
            logger.warning("Failed reading Parquet file | file={} error={}", file, exc)

    if read_errors:
        logger.debug(
            "Read errors ({}/{}): {}",
            len(read_errors), len(files), read_errors,
        )

    if not dfs:
        raise DataReadError(
            f"All {len(files)} Parquet file(s) failed to load. "
            f"First error: {read_errors[0] if read_errors else 'unknown'}"
        )

    return pd.concat(dfs, ignore_index=True)


def _parse_optional_timestamp(
    value: Optional[str],
    param_name: str,
) -> Optional[pd.Timestamp]:
    """
    Parsea un string de fecha a pd.Timestamp, o devuelve None.

    Raises
    ------
    ValueError
        Si el formato no es parseable.
    """
    if value is None:
        return None

    try:
        return pd.Timestamp(value)
    except Exception:
        raise ValueError(
            f"Invalid date format for '{param_name}': '{value}'. "
            "Expected ISO 8601, e.g. '2023-01-01'."
        )


def _validate_date_range(
    start: Optional[pd.Timestamp],
    end:   Optional[pd.Timestamp],
) -> None:
    """
    Verifica que start <= end si ambos están presentes.

    Raises
    ------
    ValueError
        Si start es posterior a end.
    """
    if start and end and start > end:
        raise ValueError(
            f"start_date ({start.date()}) must be before "
            f"end_date ({end.date()})."
        )


def _build_parquet_filters(
    start: Optional[pd.Timestamp],
    end:   Optional[pd.Timestamp],
) -> Optional[list]:
    """
    Construye filtros en formato pyarrow para pushdown en lectura Parquet.

    El formato de lista de tuplas es el estándar que acepta
    pandas.read_parquet y pyarrow.parquet.read_table.

    Returns
    -------
    list of tuples, o None si no hay filtros.
    """
    filters: list = []

    if start:
        filters.append(("timestamp", ">=", start))
    if end:
        filters.append(("timestamp", "<=", end))

    return filters if filters else None
