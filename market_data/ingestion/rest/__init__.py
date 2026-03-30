"""
market_data/ingestion/rest
==========================
Fetchers REST para descarga histórica y polling de OHLCV.
"""
from market_data.ingestion.rest.ohlcv_fetcher import HistoricalFetcherAsync, DEFAULT_CHUNK_LIMIT

__all__ = ["HistoricalFetcherAsync", "DEFAULT_CHUNK_LIMIT"]
