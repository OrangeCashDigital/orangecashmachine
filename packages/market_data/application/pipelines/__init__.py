"""
market_data.application.pipelines — public API.

Pipelines disponibles
---------------------
OHLCVPipeline        — candles OHLCV por timeframe        (implementado)
TradesPipeline       — tick data append-only              (esqueleto)
DerivativesPipeline  — funding_rate, open_interest, liq.  (esqueleto)
"""

from market_data.application.pipelines.derivatives_pipeline import (
    SUPPORTED_DERIVATIVE_DATASETS,
    DerivativesPipeline,
    DerivativesResult,
    DerivativesSummary,
)
from market_data.application.pipelines.ohlcv_pipeline import OHLCVPipeline
from market_data.application.pipelines.trades_pipeline import (
    TradesPipeline,
    TradesResult,
    TradesSummary,
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
