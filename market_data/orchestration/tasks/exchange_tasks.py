from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from prefect import task, get_run_logger

from core.config.schema import ExchangeConfig, EXCHANGE_TASK_TIMEOUT

# ==========================================================
# ExchangeProbe: Resultado de la validación
# ==========================================================

@dataclass(slots=True)
class ExchangeProbe:
    """Metadatos operativos recopilados durante la validación del exchange."""

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
        """Resumen estructurado en logs"""
        log.info(
            "ExchangeProbe | exchange=%s reachable=%s latency=%sms last_price=%s "
            "spread=%s rate_limit=%sms max_concurrent=%s datasets=%s markets=%s clock_drift=%sms",
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
            log.warning("ExchangeProbe warning | exchange=%s msg=%s", self.exchange, w)


# ==========================================================
# Helpers internos
# ==========================================================

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

_MARKET_TYPE_KEYS: Tuple[str, ...] = ("spot", "margin", "swap", "future", "option")


def _build_ccxt_client(cfg: ExchangeConfig) -> Any:
    """Construye cliente ccxt async con parámetros de configuración"""
    import ccxt.async_support as ccxt

    if not cfg.name.value:
        raise ValueError("Exchange name cannot be empty")
    if not hasattr(ccxt, cfg.name.value):
        raise ImportError(f"Exchange '{cfg.name.value}' not supported by ccxt")

    params = {"enableRateLimit": cfg.enableRateLimit, "options": {"defaultType": "spot"}}
    if cfg.has_credentials:
        params.update(cfg.ccxt_credentials())

    return getattr(ccxt, cfg.name.value)(params)


def _calculate_max_concurrent(rate_limit_ms: Optional[int]) -> int:
    """Calcula concurrencia óptima basada en rateLimit"""
    if not rate_limit_ms or rate_limit_ms <= 0:
        return 10
    return max(1, min(int(1000 / rate_limit_ms * 0.8), 20))


async def _detect_supported_datasets(exchange: Any, symbol: str, log) -> List[str]:
    """Detecta datasets operativos mediante prueba mínima y SafeOps"""
    supported: List[str] = []
    for dataset, method in _DATASET_CAPABILITY_MAP.items():
        has = getattr(exchange, 'has', {}) or {}
        if not has.get(method, False):
            continue
        try:
            coro = getattr(exchange, method)
            if dataset in ("ohlcv", "trades"):
                # Ejecuta solo una llamada mínima de prueba
                await asyncio.wait_for(coro(symbol, limit=1), timeout=5)
            supported.append(dataset)
        except Exception as e:
            log.debug("Dataset skipped | %s reason=%s", dataset, e)
    return supported


def _detect_available_markets(has: Dict[str, Any]) -> List[str]:
    """Detecta tipos de mercado disponibles"""
    return [m for m in _MARKET_TYPE_KEYS if has.get(m) is True]


async def _fetch_trading_fees(exchange: Any, symbol: str, log) -> Tuple[Optional[float], Optional[float]]:
    """Obtiene maker/taker fees de forma segura"""
    try:
        fee = await asyncio.wait_for(exchange.fetch_trading_fee(symbol), timeout=10)
        return fee.get("maker"), fee.get("taker")
    except Exception:
        try:
            market_info = exchange.markets.get(symbol, {})
            return market_info.get("maker"), market_info.get("taker")
        except Exception as e:
            log.debug("Fee fetch skipped | reason=%s", e)
            return None, None


async def _check_clock_sync(exchange: Any, log, max_drift_ms: int = 5000) -> Tuple[bool, Optional[int]]:
    """Verifica sincronización de reloj"""
    try:
        local_before = int(time.time() * 1000)
        server_time = await asyncio.wait_for(exchange.fetch_time(), timeout=5)
        local_after = int(time.time() * 1000)
        drift_ms = abs(server_time - (local_before + local_after) // 2)
        return drift_ms < max_drift_ms, drift_ms
    except Exception as e:
        log.debug("Clock sync check skipped | reason=%s", e)
        return True, None


def _select_test_symbol(exchange: Any, preferred: Optional[str] = None) -> str:
    """Selecciona un símbolo de prueba válido con fallback seguro"""
    symbols = getattr(exchange, "symbols", [])
    if not symbols:
        raise RuntimeError("No available symbols to test on exchange")
    if preferred and preferred in symbols:
        return preferred
    return symbols[0]


# ==========================================================
# Task principal
# ==========================================================

@task(
    name="validate_exchange_connection",
    retries=3,
    retry_delay_seconds=[10, 30, 60],
    timeout_seconds=EXCHANGE_TASK_TIMEOUT,
    description="Validates exchange connectivity and collects metadata.",
)
async def validate_exchange_connection(cfg: ExchangeConfig) -> ExchangeProbe:
    """
    Valida conectividad de un exchange y recopila metadata crítica.
    SafeOps:
      - Fallos parciales son loggeados
      - Timeout y errores críticos elevan excepción
    """
    log = get_run_logger()
    name = cfg.name.value
    if not name:
        raise ValueError("Exchange name must be defined")

    log.info("Validating exchange | name=%s credentials=%s", name, "yes" if cfg.has_credentials else "no")

    try:
        import ccxt.async_support  # noqa
    except ImportError:
        log.error("ccxt not installed. Run: pip install ccxt")
        raise

    exchange = None
    try:
        # Construye cliente y selecciona símbolo de prueba
        exchange = _build_ccxt_client(cfg)
        await asyncio.wait_for(exchange.load_markets(), timeout=EXCHANGE_TASK_TIMEOUT)
        symbol = _select_test_symbol(exchange, cfg.test_symbol)

        # Medir latencia y precios
        t0 = time.monotonic()
        ticker = await asyncio.wait_for(exchange.fetch_ticker(symbol), timeout=EXCHANGE_TASK_TIMEOUT)
        latency_ms = int((time.monotonic() - t0) * 1000)
        last, bid, ask = ticker.get("last"), ticker.get("bid"), ticker.get("ask")
        spread_pct = round((ask - bid) / bid * 100, 6) if bid and ask and bid > 0 else None
        rate_limit_ms = getattr(exchange, "rateLimit", 20)
        has = getattr(exchange, "has", {}) or {}

        # Fetch de fees y clock drift
        maker_fee, taker_fee = await _fetch_trading_fees(exchange, symbol, log)
        server_time_ok, clock_drift_ms = await _check_clock_sync(exchange, log)

        # Detectar datasets y mercados
        supported_datasets = await _detect_supported_datasets(exchange, symbol, log)
        available_markets = _detect_available_markets(has)

        warnings: List[str] = []
        if not server_time_ok:
            warnings.append(f"Clock drift {clock_drift_ms}ms exceeds {5000}ms")
            log.warning("Clock drift | name=%s drift=%sms", name, clock_drift_ms)

        # Construir ExchangeProbe
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
            supported_datasets=supported_datasets,
            available_markets=available_markets,
            warnings=warnings,
        )

        probe.log_summary(log)
        return probe

    except asyncio.TimeoutError:
        log.error("Validation timed out | name=%s timeout=%ss", name, EXCHANGE_TASK_TIMEOUT)
        raise
    except Exception as exc:
        log.error("Validation failed | name=%s error=%s", name, exc)
        raise
    finally:
        if exchange:
            try:
                await exchange.close()
            except Exception:
                log.debug("Failed to close exchange client | name=%s", name)