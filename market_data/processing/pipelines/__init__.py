"""
market_data.processing.pipelines — public API.

Pipelines disponibles
---------------------
OHLCVPipeline        — candles OHLCV por timeframe        (implementado)
TradesPipeline       — tick data append-only              (esqueleto)
DerivativesPipeline  — funding_rate, open_interest, liq.  (esqueleto)
"""

from market_data.processing.pipelines.ohlcv_pipeline import OHLCVPipeline
from market_data.processing.pipelines.trades_pipeline import (
    TradesPipeline,
    TradesResult,
    TradesSummary,
)
from market_data.processing.pipelines.derivatives_pipeline import (
    DerivativesPipeline,
    DerivativesResult,
    DerivativesSummary,
    SUPPORTED_DERIVATIVE_DATASETS,
)

__all__ = [
    "OHLCVPipeline",
    "TradesPipeline",
    "TradesResult",
    "TradesSummary",
    "DerivativesPipeline",
    "DerivativesResult",
    "DerivativesSummary",
    "SUPPORTED_DERIVATIVE_DATASETS",
]
