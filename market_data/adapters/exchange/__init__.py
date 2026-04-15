"""
market_data/adapters/exchange/__init__.py
=========================================

Re-exports públicos del módulo exchange adapters.
"""

# Cada símbolo importado desde su módulo propietario (SSOT).
# ccxt_adapter es un módulo interno — sus dependencias no se re-exportan desde él.
from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter
from market_data.adapters.exchange.errors import (
    ExchangeAdapterError,
    UnsupportedExchangeError,
    ExchangeConnectionError,
    ExchangeCircuitOpenError,
)
from market_data.adapters.exchange.resilience import (
    CircuitBreakerOpenError,
    RetryExhaustedError,
    get_breaker_state,
    register_resilience_layer,
)
from market_data.adapters.exchange.throttle import (
    AdaptiveThrottle,
    get_or_create_throttle,
    get_throttle_state,
)
from market_data.adapters.exchange.limiter import (
    AdaptiveLimiter,
    get_or_create_limiter,
    get_limiter_state,
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
