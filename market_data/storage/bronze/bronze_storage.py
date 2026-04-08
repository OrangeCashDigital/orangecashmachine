"""
market_data/storage/bronze/bronze_storage.py
=============================================

Capa Bronze — append-only sobre Apache Iceberg.

Tabla : bronze.ohlcv
Particionado : exchange / market_type / symbol / timeframe / ts_month

Garantías
---------
• append() inserta filas sin dedup ni normalización de valores.
• ingestion_ts se añade como columna de trazabilidad forense.
• dry_run=True registra la intención sin escribir en Iceberg.
• BronzeWriteError encapsula cualquier fallo de escritura.

Migración desde filesystem
---------------------------
La versión anterior escribía part-*.parquet por run en directorios
particionados por dt=. Este módulo reemplaza esa lógica por un append
atómico en Iceberg: sin archivos individuales por run, sin meta.json
manuales — el versionado queda en snapshots de Iceberg.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pyarrow as pa
from loguru import logger

from market_data.storage.iceberg.catalog import ensure_bronze_table, get_catalog

# Columnas mínimas requeridas en el DataFrame de entrada
REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {"timestamp", "open", "high", "low", "close", "volume"}
)

# Orden canónico de columnas para la tabla Iceberg
_BRONZE_COLS = [
    "timestamp", "open", "high", "low", "close", "volume",
    "exchange", "market_type", "symbol", "timeframe", "ingestion_ts",
]


# =============================================================================
# Excepciones
# =============================================================================

class BronzeStorageError(Exception):
    """Error base de BronzeStorage."""


class BronzeWriteError(BronzeStorageError):
    """Fallo de escritura en Iceberg Bronze."""


# =============================================================================
# BronzeStorage
# =============================================================================

class BronzeStorage:
    """
    Writer append-only para la capa Bronze (Iceberg).

    Cada llamada a append() genera un nuevo snapshot en bronze.ohlcv
    con las filas del batch. Nunca lee ni modifica datos existentes.

    Uso
    ---
    bronze = BronzeStorage(exchange="kucoin", market_type="spot")
    bronze.append(df, symbol="BTC/USDT", timeframe="1m", run_id=run_id)
    """

    def __init__(
        self,
        exchange:    Optional[str] = None,
        market_type: Optional[str] = "spot",
        dry_run:     bool          = False,
        # base_path: ignorado — compat con código que usaba la API filesystem.
        # Bronze ya no usa el filesystem; este parámetro se acepta pero no
        # tiene efecto para no romper llamadas existentes durante la transición.
        base_path:   object        = None,
    ) -> None:
        self._exchange    = (exchange    or "unknown").lower()
        self._market_type = (market_type or "spot").lower()
        self._dry_run     = dry_run
        ensure_bronze_table()
        self._table = get_catalog().load_table("bronze.ohlcv")
        logger.debug(
            "BronzeStorage ready | exchange={} market_type={} dry_run={}",
            self._exchange, self._market_type, self._dry_run,
        )

    # =========================================================================
    # Public API
    # =========================================================================

    def append(
        self,
        df:        pd.DataFrame,
        symbol:    str,
        timeframe: str,
        run_id:    Optional[str] = None,
    ) -> str:
        """
        Inserta un batch OHLCV en bronze.ohlcv (Iceberg).

        Los datos se persisten exactamente como vienen — sin dedup,
        sin merge, sin normalización de valores. Solo se añade
        ingestion_ts para trazabilidad forense.

        Parameters
        ----------
        df        : DataFrame OHLCV. Debe tener las columnas REQUIRED_COLUMNS.
        symbol    : Par de trading, e.g. "BTC/USDT".
        timeframe : Intervalo, e.g. "1m".
        run_id    : Correlación con Silver/Gold. Se genera si no se pasa.

        Returns
        -------
        str
            run_id usado — correlaciona este batch con Silver y Gold.

        Raises
        ------
        BronzeStorageError : DataFrame vacío o columnas faltantes.
        BronzeWriteError   : Fallo al escribir en Iceberg.
        """
        _validate_dataframe(df)

        if run_id is None:
            run_id = _generate_run_id()

        if self._dry_run:
            logger.info(
                "[DRY RUN] BronzeStorage.append skipped | {}/{} exchange={} rows={}",
                symbol, timeframe, self._exchange, len(df),
            )
            return run_id

        prepared = _normalize_df(
            df,
            symbol      = symbol,
            timeframe   = timeframe,
            exchange    = self._exchange,
            market_type = self._market_type,
        )

        try:
            self._table.append(
                pa.Table.from_pandas(
                    prepared,
                    schema         = self._table.schema().as_arrow(),
                    preserve_index = False,
                )
            )
        except Exception as exc:
            raise BronzeWriteError(
                f"Bronze Iceberg append failed | {symbol}/{timeframe} | {exc}"
            ) from exc

        logger.debug(
            "Bronze append | exchange={} market_type={} symbol={} timeframe={}"
            " run_id={} rows={}",
            self._exchange, self._market_type, symbol, timeframe,
            run_id, len(prepared),
        )
        return run_id


# =============================================================================
# Helpers internos
# =============================================================================

def _normalize_df(
    df:          pd.DataFrame,
    symbol:      str,
    timeframe:   str,
    exchange:    str,
    market_type: str,
) -> pd.DataFrame:
    """
    Prepara el DataFrame para escritura en Iceberg:
    - Convierte timestamp a microsegundos UTC (pyiceberg 0.8 no soporta ns).
    - Inyecta columnas de partición e ingestion_ts.
    - Ordena por timestamp (sin dedup — eso es responsabilidad de Silver).
    """
    df = df.copy()
    df["timestamp"] = (
        pd.to_datetime(df["timestamp"], utc=True)
        .astype("datetime64[us, UTC]")
    )
    now_us = pd.Timestamp(datetime.now(timezone.utc)).floor("us")
    df["ingestion_ts"] = now_us.tz_localize(None)  # se reinterpreta como UTC al escribir
    df["ingestion_ts"] = (
        pd.to_datetime(df["ingestion_ts"], utc=True)
        .astype("datetime64[us, UTC]")
    )
    df["exchange"]     = exchange
    df["market_type"]  = market_type
    df["symbol"]       = symbol
    df["timeframe"]    = timeframe
    return df[_BRONZE_COLS].sort_values("timestamp").reset_index(drop=True)


def _validate_dataframe(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        raise BronzeStorageError("DataFrame vacío")
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise BronzeStorageError(f"Missing columns: {sorted(missing)}")


def _generate_run_id() -> str:
    ts  = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"{ts}-{uid}"
