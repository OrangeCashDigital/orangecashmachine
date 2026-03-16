"""
orchestration/tasks/exchange_tasks.py
=====================================

Responsabilidad única
---------------------
Verificar conectividad con cada exchange y recopilar metadatos
operativos (latencia, rate limit, capacidades, fees) antes de
lanzar los pipelines de ingestión.

Principios
----------
SOLID
    SRP – solo valida conectividad y capacidades

DRY
    Helpers reutilizables para ccxt

KISS
    Un helper por responsabilidad

SafeOps
    Cierre garantizado del cliente
    logs estructurados
    errores explícitos
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from prefect import task, get_run_logger

from core.config.schema import ExchangeConfig, EXCHANGE_TASK_TIMEOUT


# ==============================================================
# Resultado del probe
# ==============================================================

@dataclass(slots=True)
class ExchangeProbe:
    """
    Metadatos operativos recopilados durante la validación del exchange.
    """

    exchange: str
    reachable: bool

    rate_limit_ms: int = 20
    max_concurrent: int = 10

    last_price: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    spread_pct: Optional[float] = None

    base_volume_24h: Optional[float] = None
    quote_volume_24h: Optional[float] = None

    fee_rate_maker: Optional[float] = None
    fee_rate_taker: Optional[float] = None

    server_time_ok: bool = True
    clock_drift_ms: Optional[int] = None

    latency_ms: Optional[int] = None

    supported_datasets: List[str] = field(default_factory=list)
    available_markets: List[str] = field(default_factory=list)

    warnings: List[str] = field(default_factory=list)

    def log_summary(self, log) -> None:
        """Resumen estructurado en logs."""
        log.info(
            "ExchangeProbe | exchange=%s reachable=%s latency=%sms "
            "last_price=%s spread=%s rate_limit=%sms max_concurrent=%s "
            "datasets=%s markets=%s clock_drift=%sms",
            self.exchange,
            self.reachable,
            self.latency_ms,
            self.last_price,
            self.spread_pct,
            self.rate_limit_ms,
            self.max_concurrent,
            self.supported_datasets,
            self.available_markets,
            self.clock_drift_ms,
        )

        for w in self.warnings:
            log.warning(
                "ExchangeProbe warning | exchange=%s msg=%s",
                self.exchange,
                w,
            )


# ==============================================================
# Helpers privados
# ==============================================================

_DATASET_CAPABILITY_MAP: Dict[str, str] = {
    "ohlcv": "fetchOHLCV",
    "trades": "fetchTrades",
    "orderbook": "fetchOrderBook",
    "funding_rate": "fetchFundingRates",
    "open_interest": "fetchOpenInterest",
    "liquidations": "fetchMyLiquidations",
    "mark_price": "fetchMarkOHLCV",
    "index_price": "fetchIndexOHLCV",
}

_MARKET_TYPE_KEYS: Tuple[str, ...] = (
    "spot",
    "margin",
    "swap",
    "future",
    "option",
)


def _build_ccxt_client(exchange_cfg: ExchangeConfig) -> Any:
    """Construye cliente ccxt async."""

    import ccxt.async_support as ccxt

    name = exchange_cfg.name.value

    if not name:
        raise ValueError("Exchange name cannot be empty")

    if not hasattr(ccxt, name):
        raise ImportError(
            f"Exchange '{name}' not supported by ccxt"
        )

    params: Dict[str, Any] = {
        "enableRateLimit": exchange_cfg.enableRateLimit,
        "options": {"defaultType": "spot"},
    }

    if exchange_cfg.has_credentials:
        params.update(exchange_cfg.ccxt_credentials())

    return getattr(ccxt, name)(params)


def _calculate_max_concurrent(rate_limit_ms: Optional[int]) -> int:
    """Calcula concurrencia óptima basada en rateLimit."""

    if not rate_limit_ms or rate_limit_ms <= 0:
        return 10

    return max(1, min(int(1000 / rate_limit_ms * 0.8), 20))


def _detect_supported_datasets(has: Dict[str, Any]) -> List[str]:
    """Detecta datasets soportados por el exchange."""
    return [
        dataset
        for dataset, method in _DATASET_CAPABILITY_MAP.items()
        if has.get(method) in (True, "emulated")
    ]


def _detect_available_markets(has: Dict[str, Any]) -> List[str]:
    """Detecta mercados disponibles."""
    return [m for m in _MARKET_TYPE_KEYS if has.get(m) is True]


async def _fetch_trading_fees(
    exchange: Any,
    symbol: str,
    log,
) -> Tuple[Optional[float], Optional[float]]:

    try:

        fee = await asyncio.wait_for(
            exchange.fetch_trading_fee(symbol),
            timeout=10,
        )

        return fee.get("maker"), fee.get("taker")

    except Exception as exc:

        log.debug(
            "Fee fetch skipped | reason=%s",
            exc,
        )

        return None, None


async def _check_clock_sync(
    exchange: Any,
    log,
) -> Tuple[bool, Optional[int]]:

    try:

        local_before = int(time.time() * 1000)

        server_time = await asyncio.wait_for(
            exchange.fetch_time(),
            timeout=5,
        )

        local_after = int(time.time() * 1000)

        drift_ms = abs(server_time - (local_before + local_after) // 2)

        return drift_ms < 5000, drift_ms

    except Exception as exc:

        log.debug(
            "Clock sync check skipped | reason=%s",
            exc,
        )

        return True, None


# ==============================================================
# Task principal
# ==============================================================

@task(
    name="validate_exchange_connection",
    retries=3,
    retry_delay_seconds=[10, 30, 60],
    timeout_seconds=EXCHANGE_TASK_TIMEOUT,
    description="Validates exchange connectivity and collects metadata.",
)
async def validate_exchange_connection(
    exchange_cfg: ExchangeConfig,
) -> ExchangeProbe:

    log = get_run_logger()

    name = exchange_cfg.name.value
    symbol = exchange_cfg.test_symbol

    if not symbol:
        raise ValueError("test_symbol must be defined in ExchangeConfig")

    log.info(
        "Validating exchange | name=%s symbol=%s credentials=%s",
        name,
        symbol,
        "yes" if exchange_cfg.has_credentials else "no",
    )

    try:
        import ccxt.async_support  # noqa
    except ImportError:
        log.error("ccxt not installed. Run: pip install ccxt")
        raise

    exchange = None

    try:

        exchange = _build_ccxt_client(exchange_cfg)

        t0 = time.monotonic()

        ticker = await asyncio.wait_for(
            exchange.fetch_ticker(symbol),
            timeout=EXCHANGE_TASK_TIMEOUT,
        )

        latency_ms = int((time.monotonic() - t0) * 1000)

        last = ticker.get("last")
        bid = ticker.get("bid")
        ask = ticker.get("ask")

        spread_pct = None
        if isinstance(bid, (int, float)) and isinstance(ask, (int, float)) and bid > 0:
            spread_pct = round((ask - bid) / bid * 100, 6)

        rate_limit_ms = exchange.rateLimit or 20

        has = exchange.has or {}

        maker_fee, taker_fee = await _fetch_trading_fees(
            exchange,
            symbol,
            log,
        )

        server_time_ok, clock_drift_ms = await _check_clock_sync(
            exchange,
            log,
        )

        warnings: List[str] = []

        if not server_time_ok:

            msg = (
                f"Clock drift {clock_drift_ms}ms exceeds 5000ms"
            )

            warnings.append(msg)

            log.warning(
                "Clock drift | name=%s drift=%sms",
                name,
                clock_drift_ms,
            )

        probe = ExchangeProbe(
            exchange=name,
            reachable=True,
            rate_limit_ms=rate_limit_ms,
            max_concurrent=_calculate_max_concurrent(rate_limit_ms),
            last_price=last,
            bid=bid,
            ask=ask,
            spread_pct=spread_pct,
            base_volume_24h=ticker.get("baseVolume"),
            quote_volume_24h=ticker.get("quoteVolume"),
            fee_rate_maker=maker_fee,
            fee_rate_taker=taker_fee,
            server_time_ok=server_time_ok,
            clock_drift_ms=clock_drift_ms,
            latency_ms=latency_ms,
            supported_datasets=_detect_supported_datasets(has),
            available_markets=_detect_available_markets(has),
            warnings=warnings,
        )

        probe.log_summary(log)

        return probe

    except asyncio.TimeoutError:

        log.error(
            "Validation timed out | name=%s timeout=%ss",
            name,
            EXCHANGE_TASK_TIMEOUT,
        )

        raise

    except Exception as exc:

        log.error(
            "Validation failed | name=%s error=%s",
            name,
            exc,
        )

        raise

    finally:

        if exchange is not None:

            try:
                await exchange.close()
            except Exception:
                pass