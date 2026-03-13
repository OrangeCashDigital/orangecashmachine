# market_data_system/market_data/historical/historical_fetcher.py
"""
HistoricalFetcher Profesional – OrangeCashMachine
=================================================

Responsabilidad:
- Descargar datos históricos OHLCV desde un exchange usando ExchangeClient
- Generar DataFrames limpios para almacenamiento
- Soporte para descarga incremental y rate limits

Mejoras:
- Reintentos automáticos con backoff exponencial
- Throttling dinámico según rate limit del exchange
- Logging profesional
- Modular y extensible para múltiples símbolos/timeframes
"""

import time
from typing import Optional, List
import pandas as pd
from loguru import logger
from market_data.connectors.exchange_client import ExchangeClient
from market_data.historical.historical_storage import HistoricalStorage


class HistoricalFetcher:
    """Recolector profesional de datos OHLCV históricos."""

    DEFAULT_LIMIT = 500
    MAX_RETRIES = 5
    BACKOFF_BASE = 1.5

    def __init__(self, storage: Optional[HistoricalStorage] = None):
        """
        Inicializa el fetcher con cliente del exchange y storage opcional.

        Args:
            storage (Optional[HistoricalStorage]): Instancia de almacenamiento
                para descargas incrementales.
        """
        self.client = ExchangeClient().get_client()
        self.storage = storage
        self.rate_limit_s = getattr(self.client, "rateLimit", 1200) / 1000

    def _sleep_rate_limit(self):
        """Throttling dinámico según rate limit del exchange."""
        time.sleep(self.rate_limit_s)

    def _fetch_ohlcv_chunk(
        self, symbol: str, timeframe: str, since: int, limit: int
    ) -> List[List[float]]:
        """
        Descarga un chunk de OHLCV con reintentos y backoff exponencial.

        Args:
            symbol (str): Símbolo del exchange.
            timeframe (str): Intervalo temporal.
            since (int): Timestamp en ms de inicio.
            limit (int): Máximo de filas por request.

        Returns:
            List[List[float]]: Lista de OHLCV.
        """
        retries = 0
        while retries <= self.MAX_RETRIES:
            try:
                return self.client.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
            except Exception as e:
                retries += 1
                wait = self.BACKOFF_BASE ** retries
                logger.warning(
                    f"[Retry {retries}/{self.MAX_RETRIES}] Error descargando chunk "
                    f"({symbol}, {timeframe}): {e}. Esperando {wait:.1f}s..."
                )
                time.sleep(wait)

        logger.error(f"❌ Falló la descarga de OHLCV para {symbol} {timeframe} después de {self.MAX_RETRIES} reintentos")
        return []

    def download_data(
        self,
        symbol: str,
        timeframe: str,
        start_date: Optional[str] = None,
        limit: int = DEFAULT_LIMIT
    ) -> pd.DataFrame:
        """
        Descarga datos OHLCV históricos para un símbolo y timeframe.
        Respeta descarga incremental si `storage` está presente.

        Args:
            symbol (str): Símbolo del exchange (ej: "BTC/USDT").
            timeframe (str): Timeframe para OHLCV (ej: "1m", "1h").
            start_date (Optional[str]): Fecha inicial en ISO si no hay historial en storage.
            limit (int): Máximo de filas por request.

        Returns:
            pd.DataFrame: DataFrame limpio con columnas [timestamp, open, high, low, close, volume].
        """
        # Determinar timestamp de inicio
        since_ts: Optional[int] = None
        if self.storage:
            last_ts = self.storage.get_last_timestamp(symbol, timeframe)
            if last_ts:
                since_ts = int(last_ts.timestamp() * 1000)
                logger.info(f"📈 Descarga incremental: desde {last_ts} para {symbol} {timeframe}")

        if since_ts is None:
            if start_date is None:
                raise ValueError("Se requiere start_date si no hay historial en storage")
            since_ts = self.client.parse8601(start_date)
            logger.info(f"📈 Descargando desde {start_date} para {symbol} {timeframe}")

        all_ohlcv: List[List[float]] = []

        while True:
            chunk = self._fetch_ohlcv_chunk(symbol, timeframe, since=since_ts, limit=limit)
            if not chunk:
                break

            all_ohlcv.extend(chunk)
            since_ts = chunk[-1][0] + 1  # siguiente timestamp

            self._sleep_rate_limit()  # throttling dinámico

            if len(chunk) < limit:  # protección contra bucles infinitos
                break

        if not all_ohlcv:
            logger.warning(f"⚠️ No se descargaron datos para {symbol} {timeframe}")
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(all_ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.drop_duplicates(subset=["timestamp"], inplace=True)
        df.sort_values("timestamp", inplace=True)

        logger.success(f"✅ Descargadas {len(df)} filas para {symbol} {timeframe}")
        return df