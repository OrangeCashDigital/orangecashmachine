# -*- coding: utf-8 -*-
"""
app/use_cases/rebalance.py
===========================

Use case: rebalancear el portfolio según target weights.

Responsabilidad
---------------
  1. Leer estado actual del portfolio (PortfolioService)
  2. Calcular ajustes necesarios (RebalanceService)
  3. Emitir las señales al OMS para ejecución
  4. Retornar resultado completo al CLI

Integración
-----------
  CLI → execute(args, targets)
    → PortfolioService.snapshot()
    → RebalanceService.rebalance(state, targets)
    → OMS.submit(signal) por cada RebalanceSignal

Nota sobre RebalanceSignal → Signal
------------------------------------
RebalanceSignal es un value object de portfolio/.
El OMS espera un SignalProtocol (ocm_platform/boundaries.py).
_RebalanceSignalAdapter hace el bridge sin acoplar los BCs.

Principios: SRP · DIP · DRY · SafeOps · Composition Root
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from portfolio.services.rebalance_service import RebalanceSignal


# ---------------------------------------------------------------------------
# Adapter — RebalanceSignal → SignalProtocol
# ---------------------------------------------------------------------------

class _RebalanceSignalAdapter:
    """
    Adapta RebalanceSignal al SignalProtocol que el OMS espera.

    DIP: el OMS no conoce RebalanceSignal.
    OCP: nuevos tipos de señal → nuevo adapter, OMS sin cambios.
    """

    def __init__(self, rs: RebalanceSignal, current_price: float) -> None:
        self._rs            = rs
        self._current_price = current_price

    # SignalProtocol campos
    @property
    def symbol(self) -> str:
        return self._rs.symbol

    @property
    def timeframe(self) -> str:
        return "rebalance"   # semántico — no es un timeframe de vela

    @property
    def signal(self) -> str:
        return self._rs.action   # "buy" | "sell"

    @property
    def price(self) -> float:
        return self._current_price

    @property
    def confidence(self) -> float:
        return 1.0   # rebalanceo es una decisión determinista, no probabilística

    @property
    def timestamp(self) -> datetime:
        return self._rs.generated_at

    @property
    def is_actionable(self) -> bool:
        return True   # RebalanceSignal siempre es accionable (ya filtrado por drift)


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class RebalanceResult:
    """Resultado completo de un ciclo de rebalanceo."""
    success:          bool
    error:            Optional[str]        = None
    signals_computed: int                  = 0
    orders_submitted: int                  = 0
    orders_filled:    int                  = 0
    orders_rejected:  int                  = 0
    signals:          list[RebalanceSignal] = field(default_factory=list)
    portfolio_before: Optional[object]     = None   # PortfolioState
    portfolio_after:  Optional[object]     = None   # PortfolioState

    @property
    def exit_code(self) -> int:
        return 0 if self.success else 1


# ---------------------------------------------------------------------------
# Use case
# ---------------------------------------------------------------------------

def execute(
    targets:     dict[str, float],
    capital_usd: float,
    exchange:    str,
    market_type: str   = "spot",
    trigger:     str   = "manual",
    dry_run:     bool  = False,
    redis_host:  str   = "localhost",
    redis_port:  int   = 6379,
    redis_db:    int   = 1,
) -> RebalanceResult:
    """
    Ejecuta un ciclo de rebalanceo de portfolio.

    Parameters
    ----------
    targets     : dict symbol → target_pct (e.g. {"BTC/USDT": 0.4, "ETH/USDT": 0.2})
    capital_usd : capital total del portfolio
    exchange    : exchange de referencia
    market_type : tipo de mercado
    trigger     : razón del rebalanceo ("scheduled" | "drift" | "manual")
    dry_run     : si True, calcula señales sin enviar al OMS
    redis_host/port/db : conexión Redis para PortfolioService

    Returns
    -------
    RebalanceResult con todo lo necesario para que el CLI reporte.
    SafeOps: nunca lanza.
    """
    from portfolio.services.portfolio_service import PortfolioService, InMemoryPositionStore
    from portfolio.services.rebalance_service import RebalanceService
    from portfolio.infra.redis_store import RedisPositionStore

    # Construir PortfolioService con Redis (o in-memory en dry-run)
    try:
        if dry_run:
            store = InMemoryPositionStore()
            logger.info("[DRY-RUN] PortfolioService con store in-memory")
        else:
            import redis as redis_lib
            redis_client = redis_lib.Redis(
                host             = redis_host,
                port             = redis_port,
                db               = redis_db,
                socket_timeout   = 3,
                decode_responses = False,
            )
            store = RedisPositionStore(redis_client=redis_client, exchange=exchange)

        portfolio = PortfolioService(
            capital_usd = capital_usd,
            store       = store,
            exchange    = exchange,
        )
    except Exception as exc:
        logger.error("Error construyendo PortfolioService | {}", exc)
        return RebalanceResult(success=False, error=str(exc))

    # Leer estado actual
    state_before = portfolio.snapshot()
    logger.info("Portfolio actual | {}", state_before)

    # Validar targets
    rebalance_svc = RebalanceService()
    valid, err = rebalance_svc.validate_targets(targets)
    if not valid:
        logger.error("Targets inválidos | {}", err)
        return RebalanceResult(success=False, error=f"invalid_targets:{err}")

    # Calcular señales
    signals = rebalance_svc.rebalance(state_before, targets, trigger=trigger)
    logger.info(
        "Señales de rebalanceo | count={} trigger={}",
        len(signals), trigger,
    )

    if not signals:
        logger.info("Portfolio dentro de tolerancia — sin ajustes necesarios.")
        return RebalanceResult(
            success          = True,
            signals_computed = 0,
            portfolio_before = state_before,
            portfolio_after  = state_before,
        )

    if dry_run:
        logger.info("[DRY-RUN] Señales calculadas (sin enviar al OMS):")
        for s in signals:
            logger.info("  {}", s)
        return RebalanceResult(
            success          = True,
            signals_computed = len(signals),
            signals          = signals,
            portfolio_before = state_before,
            portfolio_after  = state_before,
        )

    # Enviar señales al OMS
    # Nota: precio actual se obtiene del último precio conocido en Gold.
    # Para simplificar, usamos el entry_price de la posición existente
    # o 0.0 si es una posición nueva (BUY desde cero).
    # En producción: consultar el precio actual via GoldLoader o CCXT.
    try:
        from trading.risk.models import RiskConfig
        from trading.execution.oms import OMS
        from trading.execution.paper_executor import PaperExecutor
        from trading.risk.manager import RiskManager

        risk_manager = RiskManager(
            config      = RiskConfig(),
            capital_usd = capital_usd,
        )
        oms = OMS(
            risk_manager = risk_manager,
            executor     = PaperExecutor(),
        )

        submitted = 0
        filled    = 0
        rejected  = 0

        current_prices = {
            pos.symbol: pos.entry_price
            for pos in state_before.positions
        }

        for rs in signals:
            price  = current_prices.get(rs.symbol, 1.0)   # 1.0 si no hay posición previa
            signal = _RebalanceSignalAdapter(rs, current_price=price)
            order  = oms.submit(signal)

            if order is None:
                rejected += 1
            else:
                submitted += 1
                from trading.execution.order import OrderStatus
                if order.status == OrderStatus.FILLED:
                    filled += 1
                else:
                    rejected += 1

        state_after = portfolio.snapshot()
        return RebalanceResult(
            success          = True,
            signals_computed = len(signals),
            orders_submitted = submitted,
            orders_filled    = filled,
            orders_rejected  = rejected,
            signals          = signals,
            portfolio_before = state_before,
            portfolio_after  = state_after,
        )

    except Exception as exc:
        logger.error("Error enviando señales al OMS | {}", exc)
        return RebalanceResult(success=False, error=str(exc))
