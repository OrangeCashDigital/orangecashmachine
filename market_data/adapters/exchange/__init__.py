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
from market_data.adapters.exchange.resilience import (
    get_breaker_state,
    register_resilience_layer,
)

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
    "get_breaker_state",
    "register_resilience_layer",
]
