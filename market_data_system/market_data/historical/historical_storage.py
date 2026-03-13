"""
historical_storage.py
====================

Responsabilidad:
Gestionar el almacenamiento de datos históricos OHLCV en formato Parquet
dentro del data lake de market_data_system.

Mejoras de esta versión profesional:
- Organización del data lake por símbolo/año/mes
- Append automático sin duplicados
- Escritura atómica (archivo temporal + rename)
- Logging profesional con loguru
- Compatible con millones de filas y múltiples meses en un mismo batch
- Consulta del último timestamp para descarga incremental
"""

from pathlib import Path
from typing import Optional
import pandas as pd
from loguru import logger


class HistoricalStorage:
    """
    Clase profesional para gestión de datos históricos OHLCV en Parquet.
    Data lake organizado por símbolo/año/mes.
    """

    REQUIRED_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]

    def __init__(self, base_path: str | Path = None) -> None:
        """
        Inicializa HistoricalStorage y asegura la carpeta base.

        Parameters
        ----------
        base_path : str | Path, optional
            Ruta base donde se almacenarán los archivos. Por defecto apunta a:
            `market_data_system/data_lake/ohlcv`.
        """
        self.base_path = Path(base_path) if base_path else Path(__file__).resolve().parents[3] / "data_lake" / "ohlcv"
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"📂 Carpeta de almacenamiento preparada en: {self.base_path}")

    def _get_folder_path(self, symbol: str, year: int, month: int) -> Path:
        """
        Construye la carpeta destino para un símbolo y mes específicos.

        Parameters
        ----------
        symbol : str
            Símbolo de trading (ej. 'BTC/USDT').
        year : int
            Año del batch de datos.
        month : int
            Mes del batch de datos.

        Returns
        -------
        Path
            Carpeta destino para almacenar el Parquet.
        """
        safe_symbol = symbol.replace("/", "_")
        folder_path = self.base_path / safe_symbol / str(year) / f"{month:02d}"
        folder_path.mkdir(parents=True, exist_ok=True)
        return folder_path

    def save_ohlcv(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        mode: str = "overwrite"
    ) -> None:
        """
        Guarda un DataFrame OHLCV en formato Parquet, automáticamente
        dividido por mes y año. Maneja append sin duplicados y escritura atómica.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame con columnas ['timestamp','open','high','low','close','volume'].
        symbol : str
            Símbolo de trading.
        timeframe : str
            Intervalo temporal (ej. '1m', '5m', '1h').
        mode : str
            'overwrite' o 'append'.
        """
        if df.empty:
            logger.warning(f"No hay datos para guardar para {symbol} ({timeframe})")
            return

        missing_cols = set(self.REQUIRED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise ValueError(f"El DataFrame no contiene las columnas requeridas: {missing_cols}")

        # Normalizar tipos
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        safe_symbol = symbol.replace("/", "_")

        # Agrupar por año/mes para manejar multi-mes
        for (year, month), group in df.groupby([df.timestamp.dt.year, df.timestamp.dt.month]):
            folder_path = self._get_folder_path(symbol, year, month)
            file_path = folder_path / f"{safe_symbol}_{timeframe}.parquet"
            temp_path = file_path.with_suffix(".tmp")

            try:
                # Append inteligente sin duplicados
                if file_path.exists() and mode == "append":
                    existing = pd.read_parquet(file_path)
                    group = pd.concat([existing, group]).drop_duplicates(subset=["timestamp"])

                group.sort_values("timestamp", inplace=True)

                # Escritura atómica: temporal → rename
                group.to_parquet(temp_path, compression="snappy", index=False)
                temp_path.replace(file_path)

                logger.success(f"💾 Guardado/Actualizado: {symbol} {timeframe} → {year}/{month:02d} ({len(group)} filas)")

            except Exception as e:
                if temp_path.exists():
                    temp_path.unlink()
                logger.error(f"Fallo crítico guardando {symbol} {timeframe} {year}/{month:02d}: {e}")
                raise

    def get_last_timestamp(self, symbol: str, timeframe: str) -> Optional[pd.Timestamp]:
        """
        Obtiene el último timestamp almacenado para un símbolo y timeframe.
        Permite descargas incrementales evitando redundancia.

        Parameters
        ----------
        symbol : str
            Símbolo de trading.
        timeframe : str
            Intervalo temporal.

        Returns
        -------
        Optional[pd.Timestamp]
            Último timestamp guardado, o None si no hay datos.
        """
        safe_symbol = symbol.replace("/", "_")
        base_symbol_path = self.base_path / safe_symbol

        if not base_symbol_path.exists():
            return None

        # Recorre todos los archivos Parquet del símbolo
        files = sorted(base_symbol_path.rglob(f"{safe_symbol}_{timeframe}.parquet"))
        if not files:
            return None

        last_ts = None
        for f in files:
            try:
                df = pd.read_parquet(f, columns=["timestamp"])
                file_max = df["timestamp"].max()
                if last_ts is None or file_max > last_ts:
                    last_ts = file_max
            except Exception as e:
                logger.warning(f"No se pudo leer {f}: {e}")
                continue

        return pd.to_datetime(last_ts) if last_ts is not None else None