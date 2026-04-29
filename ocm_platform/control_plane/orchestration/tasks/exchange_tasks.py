"""
ocm_platform/control_plane/orchestration/tasks/exchange_tasks.py
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
SafeOps – fallos parciales loggeados, cierre seguro via finally
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from market_data.adapters.exchange import CCXTAdapter

from prefect import task, get_run_logger

from ocm_platform.config.schema import ExchangeConfig, EXCHANGE_TASK_TIMEOUT
from market_data.adapters.exchange import CCXTAdapter
from market_data.observability.metrics import record_exchange_probe_metrics


# ==========================================================
# Constants
# ==========================================================

_MAX_CLOCK_DRIFT_MS:    int = 5_000
_FEE_FETCH_TIMEOUT:     int = 10
_CLOCK_FETCH_TIMEOUT:   int = 5
_DATASET_PROBE_TIMEOUT: int = 15  # aumentado: 3s era insuficiente en exchanges lentos
_FETCH_TICKER_TIMEOUT: int = 10   # ticker independiente del timeout del task

_DATASET_CAPABILITY_MAP: Dict[str, str] = {
    "ohlcv":         "fetchOHLCV",
    "trades":        "fetchTrades",
    "orderbook":     "fetchOrderBook",
    "funding_rate":  "fetchFundingRates",
    "open_interest": "fetchOpenInterest",
    "liquidations":  "fetchMyLiquidations",
    "mark_price":    "fetchMarkOHLCV",
    "index_price":   "fetchIndexOHLCV",
}

_MARKET_TYPE_KEYS: Tuple[str, ...] = (
    "spot", "margin", "swap", "future", "option"
)


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

    exchange:           str
    reachable:          bool
    rate_limit_ms:      int             = 20
    max_concurrent:     int             = 10
    last_price:         Optional[float] = None
    bid:                Optional[float] = None
    ask:                Optional[float] = None
    spread_pct:         Optional[float] = None
    base_volume_24h:    Optional[float] = None
    quote_volume_24h:   Optional[float] = None
    fee_rate_maker:     Optional[float] = None
    fee_rate_taker:     Optional[float] = None
    server_time_ok:     bool            = True
    clock_drift_ms:     Optional[int]   = None
    latency_ms:         Optional[int]   = None
    supported_datasets: List[str]       = field(default_factory=list)
    available_markets:  List[str]       = field(default_factory=list)
    warnings:           List[str]       = field(default_factory=list)
    adapter:            "Optional[CCXTAdapter]" = field(default=None, repr=False)  # lifecycle: flow owner

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

def _calculate_max_concurrent(
    rate_limit_ms: Optional[int],
    latency_ms:    Optional[int] = None,
) -> int:
    """
    Calcula concurrencia óptima combinando rateLimit y latencia observada.

    Base: 80% de la capacidad teórica por rate limit, acotada a [1, 20].
    Penalización adaptiva: latencia alta reduce concurrencia para no saturar
    un exchange que ya está bajo presión.

    Thresholds
    ----------
    latency > 2000ms → 25% de la base  (exchange muy lento / degradado)
    latency > 1000ms → 50% de la base
    latency >  500ms → 75% de la base
    latency ≤  500ms → sin penalización
    """
    if not rate_limit_ms or rate_limit_ms <= 0:
        base = 10
    else:
        base = max(1, min(int(1000 / rate_limit_ms * 0.8), 20))

    if not latency_ms or latency_ms <= 0:
        return base

    if latency_ms > 2000:
        factor = 0.25
    elif latency_ms > 1000:
        factor = 0.5
    elif latency_ms > 500:
        factor = 0.75
    else:
        factor = 1.0

    return max(1, int(base * factor))


def _select_test_symbol(exchange: Any, preferred: Optional[str] = None) -> str:
    """
    Selecciona un símbolo de prueba válido con awareness del market_type cargado.

    Estrategia
    ----------
    1. Si preferred existe y pertenece al universo cargado → usarlo (camino feliz).
    2. Detectar market_type desde options.defaultType del exchange.
    3. Si es derivado → filtrar usando exchange.markets metadata real (no string parsing).
       exchange.markets[symbol]["type"] ∈ {"swap", "future", "option"} es la fuente
       canónica de CCXT — más robusta que heurísticas de formato de símbolo.
    4. Fallback final: primer símbolo disponible.

    Raises RuntimeError si el exchange no tiene símbolos cargados,
    lo que indica que load_markets() no fue llamado previamente.
    """
    symbols: List[str] = getattr(exchange, "symbols", [])
    if not symbols:
        raise RuntimeError(
            "No symbols available — ensure load_markets() was called before validation."
        )

    # 1. Preferred válido dentro del universo cargado
    if preferred and preferred in symbols:
        return preferred

    # 2. Detectar market_type desde opciones internas del cliente ccxt
    market_type: Optional[str] = (
        getattr(exchange, "options", {}) or {}
    ).get("defaultType")

    # 3. Para derivados: usar metadata real de exchange.markets en lugar de
    #    string parsing. exchange.markets es un dict {symbol: market_dict}
    #    donde market_dict["type"] es la fuente canónica de CCXT.
    if market_type in ("swap", "future", "option"):
        markets: Dict[str, Any] = getattr(exchange, "markets", {}) or {}
        candidates = [
            s for s, m in markets.items()
            if isinstance(m, dict) and m.get("type") in ("swap", "future", "option")
            and m.get("active", True)  # excluir mercados deslistados
        ]
        if candidates:
            return candidates[0]

    # 4. Fallback final seguro
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
    Detecta datasets soportados via has{} de ccxt (sin prueba real).

    Estrategia deliberada
    ---------------------
    Las pruebas reales causan timeouts impredecibles en exchanges con alta
    latencia. has{} es suficientemente confiable para decidir qué pipelines
    lanzar. Los pipelines mismos fallarán explícitamente si algo no funciona.

    SafeOps: nunca lanza excepción al caller.
    """
    has: Dict[str, Any] = getattr(exchange, "has", {}) or {}
    supported: List[str] = []

    for dataset, method in _DATASET_CAPABILITY_MAP.items():
        if has.get(method, False):
            supported.append(dataset)
            log.debug("Dataset disponible | dataset=%s method=%s", dataset, method)

    log.info("Datasets detectados | exchange=%s datasets=%s", exchange.id, supported)
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

    SafeOps: fallo en fetch_time() se considera aceptable — retorna (True, None)
    para no bloquear la validación por un endpoint no crítico.
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
    tags=["exchange", "validation"],
)
async def validate_exchange_connection(
    cfg: ExchangeConfig,
) -> Tuple[ExchangeProbe, CCXTAdapter]:
    """
    Valida conectividad de un exchange y construye un ExchangeProbe.

    Retorna (probe, adapter) — el adapter queda abierto para reutilización
    en pipelines downstream, evitando repetir load_markets().
    El caller (batch_flow) es responsable de cerrar el adapter.

    Flujo
    -----
    1. Conectar via CCXTAdapter
    2. Seleccionar símbolo de prueba
    3. Medir latencia y precios via ticker
    4. Obtener fees y verificar clock drift (fallos tolerados)
    5. Detectar datasets y mercados disponibles
    6. Emitir métricas Prometheus
    7. Retornar (ExchangeProbe, CCXTAdapter)

    SafeOps
    -------
    - Fallos parciales (fees, clock) loggeados sin interrumpir validación
    - Error crítico: adapter.close() garantizado en except antes de re-raise
    - Prefect reintenta automáticamente según retry_delay_seconds
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

    adapter = CCXTAdapter(config=cfg, default_type="spot")  # probe usa spot — pipelines futures crean su propio adapter

    try:
        await adapter.connect()
        exchange = await adapter._get_client()

        symbol        = _select_test_symbol(exchange, cfg.test_symbol)
        has           = getattr(exchange, "has", {}) or {}
        rate_limit_ms = getattr(exchange, "rateLimit", 20)

        # --- Latencia y precios ---
        t0 = time.monotonic()
        ticker = await asyncio.wait_for(
            exchange.fetch_ticker(symbol),
            timeout=_FETCH_TICKER_TIMEOUT,
        )
        latency_ms = int((time.monotonic() - t0) * 1000)

        last, bid, ask = ticker.get("last"), ticker.get("bid"), ticker.get("ask")
        spread_pct = (
            round((ask - bid) / bid * 100, 6)
            if bid and ask and bid > 0
            else None
        )

        # --- Fees, clock y datasets en paralelo (1 RTT efectivo) ---
        try:
            (maker_fee, taker_fee), (server_time_ok, drift), supported_datasets = (
                await asyncio.gather(
                    _fetch_trading_fees(exchange, symbol, log),
                    _check_clock_sync(exchange, log),
                    _detect_supported_datasets(exchange, symbol, log),
                    return_exceptions=False,
                )
            )
        except asyncio.TimeoutError:
            log.warning("Parallel probe timed out | name=%s — usando defaults", name)
            maker_fee, taker_fee  = None, None
            server_time_ok, drift = True, None
            supported_datasets    = []
        except Exception as exc:
            log.warning("Parallel probe failed | name=%s error=%s — usando defaults", name, exc)
            maker_fee, taker_fee  = None, None
            server_time_ok, drift = True, None
            supported_datasets    = []

        available_markets = _detect_available_markets(has)

        # --- Warnings ---
        warnings: List[str] = []
        if not server_time_ok:
            msg = f"Clock drift {drift}ms exceeds {_MAX_CLOCK_DRIFT_MS}ms"
            warnings.append(msg)
            log.warning("Clock drift | name=%s drift=%sms", name, drift)

        probe = ExchangeProbe(
            exchange           = name,
            reachable          = True,
            rate_limit_ms      = rate_limit_ms,
            max_concurrent     = _calculate_max_concurrent(rate_limit_ms, latency_ms),
            last_price         = last,
            bid                = bid,
            ask                = ask,
            spread_pct         = spread_pct,
            base_volume_24h    = ticker.get("baseVolume"),
            quote_volume_24h   = ticker.get("quoteVolume"),
            fee_rate_maker     = maker_fee,
            fee_rate_taker     = taker_fee,
            server_time_ok     = server_time_ok,
            clock_drift_ms     = drift,
            latency_ms         = latency_ms,
            supported_datasets = supported_datasets,
            available_markets  = available_markets,
            warnings           = warnings,
        )

        # --- Métricas Prometheus ---
        record_exchange_probe_metrics(probe)

        probe.log_summary(log)
        return probe, adapter   # adapter queda abierto — caller lo cierra

    except Exception as exc:
        # Cierre garantizado ante cualquier fallo — evita fugas de conexión
        await adapter.close()
        log.error("Validation failed | name=%s error=%s", name, exc)
        raise
