"""
market_data/adapters/exchange/__init__.py
=========================================

Re-exports públicos del módulo exchange adapters.
"""

from market_data.adapters.exchange.ccxt_adapter import (
    CCXTAdapter,
    CircuitBreakerOpenError,
    ExchangeAdapterError,
    ExchangeConnectionError,
    RetryExhaustedError,
    UnsupportedExchangeError,
    AdaptiveThrottle,
    AdaptiveLimiter,
    get_or_create_throttle,
    get_or_create_limiter,
    get_throttle_state,
    get_limiter_state,
)
from market_data.adapters.exchange.errors import ExchangeCircuitOpenError

__all__ = [
    "CCXTAdapter",
    "ExchangeAdapterError",
    "UnsupportedExchangeError",
    "ExchangeConnectionError",
    "ExchangeCircuitOpenError",
    "CircuitBreakerOpenError",
    "RetryExhaustedError",
    "AdaptiveThrottle",
    "AdaptiveLimiter",
    "get_or_create_throttle",
    "get_or_create_limiter",
    "get_throttle_state",
    "get_limiter_state",
]
