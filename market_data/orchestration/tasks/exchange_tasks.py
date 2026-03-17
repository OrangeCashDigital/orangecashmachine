"""
market_data/orchestration/tasks/exchange_tasks.py
==================================================

Prefect task de validación operativa de exchanges.

Responsabilidad
---------------
Validar conectividad y recopilar metadata operativa de un exchange
antes de lanzar pipelines. Produce un ExchangeProbe con toda la
información necesaria para que el orchestrator tome decisiones.

Este módulo NO descarga datos de mercado.
Eso es responsabilidad de fetchers y pipelines.

Principios
----------
SOLID  – SRP: solo validación pre-vuelo
KISS   – helpers pequeños y enfocados
DRY    – lógica centralizada, sin repetición
SafeOps – fallos parciales loggeados, cierre seguro via CCXTAdapter
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from prefect import task, get_run_logger

from core.config.schema import ExchangeConfig, EXCHANGE_TASK_TIMEOUT
from services.exchange.ccxt_adapter import CCXTAdapter

# ==========================================================
# Constants
# ==========================================================

_MAX_CLOCK_DRIFT_MS:  int = 5_000
_FEE_FETCH_TIMEOUT:   int = 10
_CLOCK_FETCH_TIMEOUT: int = 5
_DATASET_PROBE_TIMEOUT: int = 5

_DATASET_CAPABILITY_MAP: Dict[str, str] = {
    "ohlcv":        "fetchOHLCV",
    "trades":       "fetchTrades",
    "orderbook":    "fetchOrderBook",
    "funding_rate": "fetchFundingRates",
    "open_interest":"fetchOpenInterest",
    "liquidations": "fetchMyLiquidations",
    "mark_price":   "fetchMarkOHLCV",
    "index_price":  "fetchIndexOHLCV",
}

_MARKET_TYPE_KEYS: Tuple[str, ...] = (
    "spot", "margin", "swap", "future", "option"
)

# Datasets que requieren prueba real (no solo declaración en has{})
_PROBE_REQUIRED: frozenset[str] = frozenset({"ohlcv", "trades"})


# ==========================================================
# ExchangeProbe
# ==========================================================

@dataclass(slots=True)
class ExchangeProbe:
    """
    Snapshot operativo de un exchange tras validación.

    Usado por el orchestrator para decidir:
    - qué pipelines lanzar
    - con qué concurrencia
    - qué datasets están disponibles
    """

    exchange:         str
    reachable:        bool
    rate_limit_ms:    int            = 20
    max_concurrent:   int            = 10
    last_price:       Optional[float] = None
    bid:              Optional[float] = None
    ask:              Optional[float] = None
    spread_pct:       Optional[float] = None
    base_volume_24h:  Optional[float] = None
    quote_volume_24h: Optional[float] = None
    fee_rate_maker:   Optional[float] = None
    fee_rate_taker:   Optional[float] = None
    server_time_ok:   bool            = True
    clock_drift_ms:   Optional[int]   = None
    latency_ms:       Optional[int]   = None
    supported_datasets: List[str]    = field(default_factory=list)
    available_markets:  List[str]    = field(default_factory=list)
    warnings:           List[str]    = field(default_factory=list)

    def log_summary(self, log) -> None:
        """Emite resumen estructurado en una sola línea de log."""
        log.info(
            "ExchangeProbe | exchange=%s reachable=%s latency=%sms last_price=%s "
            "spread=%s rate_limit=%sms max_concurrent=%s datasets=%s markets=%s clock_drift=%sms",
            self.exchange, self.reachable, self.latency_ms, self.last_price,
            self.spread_pct, self.rate_limit_ms, self.max_concurrent,
            self.supported_datasets, self.available_markets, self.clock_drift_ms,
        )
        for w in self.warnings:
            log.warning("ExchangeProbe warning | exchange=%s msg=%s", self.exchange, w)


# ==========================================================
# Helpers internos (privados, sin estado)
# ==========================================================

def _calculate_max_concurrent(rate_limit_ms: Optional[int]) -> int:
    """
    Calcula concurrencia óptima basada en rateLimit del exchange.
    Fórmula: 80% de la capacidad teórica, acotada a [1, 20].
    """
    if not rate_limit_ms or rate_limit_ms <= 0:
        return 10
    return max(1, min(int(1000 / rate_limit_ms * 0.8), 20))


def _select_test_symbol(exchange: Any, preferred: Optional[str] = None) -> str:
    """
    Selecciona un símbolo de prueba válido con fallback al primero disponible.

    Raises RuntimeError si el exchange no tiene símbolos cargados
    (indica que load_markets() no fue llamado).
    """
    symbols: List[str] = getattr(exchange, "symbols", [])
    if not symbols:
        raise RuntimeError(
            "No symbols available — ensure load_markets() was called before validation."
        )
    if preferred and preferred in symbols:
        return preferred
    return symbols[0]


def _detect_available_markets(has: Dict[str, Any]) -> List[str]:
    """Detecta tipos de mercado activos según el dict has{} de ccxt."""
    return [m for m in _MARKET_TYPE_KEYS if has.get(m) is True]


async def _detect_supported_datasets(
    exchange: Any,
    symbol: str,
    log: Any,
) -> List[str]:
    """
    Detecta datasets operativos del exchange.

    Estrategia:
    - Datasets en _PROBE_REQUIRED: prueba real con llamada mínima (limit=1)
    - Resto: basta con que has{} declare soporte
    SafeOps: excepciones individuales se loggean y se omite el dataset.
    """
    has: Dict[str, Any] = getattr(exchange, "has", {}) or {}
    supported: List[str] = []

    for dataset, method in _DATASET_CAPABILITY_MAP.items():
        if not has.get(method, False):
            continue
        if dataset not in _PROBE_REQUIRED:
            supported.append(dataset)
            continue
        try:
            coro = getattr(exchange, method)
            await asyncio.wait_for(coro(symbol, limit=1), timeout=_DATASET_PROBE_TIMEOUT)
            supported.append(dataset)
        except Exception as exc:
            log.debug("Dataset skipped | dataset=%s reason=%s", dataset, exc)

    return supported


async def _fetch_trading_fees(
    exchange: Any,
    symbol: str,
    log: Any,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Obtiene maker/taker fees con doble fallback:
    1. fetch_trading_fee() (endpoint dedicado)
    2. markets dict (metadata estática ya cargada)
    SafeOps: nunca lanza excepción al caller.
    """
    try:
        fee = await asyncio.wait_for(
            exchange.fetch_trading_fee(symbol),
            timeout=_FEE_FETCH_TIMEOUT,
        )
        return fee.get("maker"), fee.get("taker")
    except Exception:
        pass

    try:
        market = (exchange.markets or {}).get(symbol, {})
        return market.get("maker"), market.get("taker")
    except Exception as exc:
        log.debug("Fee fetch skipped | reason=%s", exc)
        return None, None


async def _check_clock_sync(
    exchange: Any,
    log: Any,
    max_drift_ms: int = _MAX_CLOCK_DRIFT_MS,
) -> Tuple[bool, Optional[int]]:
    """
    Verifica sincronización del reloj del exchange vs reloj local.
    SafeOps: fallo en fetch_time() se considera aceptable (True, None).
    """
    try:
        local_before = int(time.time() * 1000)
        server_time  = await asyncio.wait_for(
            exchange.fetch_time(),
            timeout=_CLOCK_FETCH_TIMEOUT,
        )
        local_after = int(time.time() * 1000)
        drift_ms = abs(server_time - (local_before + local_after) // 2)
        return drift_ms < max_drift_ms, drift_ms
    except Exception as exc:
        log.debug("Clock sync skipped | reason=%s", exc)
        return True, None


# ==========================================================
# Prefect Task
# ==========================================================

@task(
    name="validate_exchange_connection",
    retries=3,
    retry_delay_seconds=[10, 30, 60],
    timeout_seconds=EXCHANGE_TASK_TIMEOUT,
    description="Validates exchange connectivity and collects operational metadata.",
)
async def validate_exchange_connection(cfg: ExchangeConfig) -> ExchangeProbe:
    """
    Valida conectividad de un exchange y construye un ExchangeProbe.

    Flujo
    -----
    1. Conectar via CCXTAdapter (retry + backoff encapsulados)
    2. Seleccionar símbolo de prueba
    3. Medir latencia y precios (ticker)
    4. Obtener fees y verificar clock drift
    5. Detectar datasets y mercados disponibles
    6. Construir y retornar ExchangeProbe

    SafeOps
    -------
    - Fallos parciales (fees, clock) son loggeados y no detienen la validación
    - Timeout y errores críticos propagan excepción (Prefect reintenta)
    - CCXTAdapter garantiza cierre seguro del cliente en finally
    """
    log  = get_run_logger()
    name = cfg.name.value

    if not name:
        raise ValueError("ExchangeConfig.name must be defined")

    log.info(
        "Validating exchange | name=%s credentials=%s",
        name,
        "yes" if cfg.has_credentials else "no",
    )

    adapter = CCXTAdapter(config=cfg)

    try:
        await adapter.connect()
        exchange = await adapter._get_client()

        symbol        = _select_test_symbol(exchange, cfg.test_symbol)
        has           = getattr(exchange, "has", {}) or {}
        rate_limit_ms = getattr(exchange, "rateLimit", 20)

        # --- Latencia y precios ---
        t0        = time.monotonic()
        ticker    = await asyncio.wait_for(
            exchange.fetch_ticker(symbol),
            timeout=EXCHANGE_TASK_TIMEOUT,
        )
        latency_ms = int((time.monotonic() - t0) * 1000)

        last, bid, ask = ticker.get("last"), ticker.get("bid"), ticker.get("ask")
        spread_pct = (
            round((ask - bid) / bid * 100, 6)
            if bid and ask and bid > 0
            else None
        )

        # --- Fees y clock (fallos parciales tolerados) ---
        maker_fee, taker_fee   = await _fetch_trading_fees(exchange, symbol, log)
        server_time_ok, drift  = await _check_clock_sync(exchange, log)

        # --- Capacidades ---
        supported_datasets = await _detect_supported_datasets(exchange, symbol, log)
        available_markets  = _detect_available_markets(has)

        # --- Warnings ---
        warnings: List[str] = []
        if not server_time_ok:
            msg = f"Clock drift {drift}ms exceeds {_MAX_CLOCK_DRIFT_MS}ms"
            warnings.append(msg)
            log.warning("Clock drift | name=%s drift=%sms", name, drift)

        probe = ExchangeProbe(
            exchange          = name,
            reachable         = True,
            rate_limit_ms     = rate_limit_ms,
            max_concurrent    = _calculate_max_concurrent(rate_limit_ms),
            last_price        = last,
            bid               = bid,
            ask               = ask,
            spread_pct        = spread_pct,
            base_volume_24h   = ticker.get("baseVolume"),
            quote_volume_24h  = ticker.get("quoteVolume"),
            fee_rate_maker    = maker_fee,
            fee_rate_taker    = taker_fee,
            server_time_ok    = server_time_ok,
            clock_drift_ms    = drift,
            latency_ms        = latency_ms,
            supported_datasets= supported_datasets,
            available_markets = available_markets,
            warnings          = warnings,
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
        await adapter.close()
