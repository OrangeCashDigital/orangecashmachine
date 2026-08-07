# -*- coding: utf-8 -*-
"""
app/use_cases/execute_live.py
==============================

Use case: ejecutar un ciclo de live trading.

Responsabilidad
---------------
Ensamblar las dependencias para live trading y ejecutar un ciclo:
  GoldData -> TradingEngine(live) -> TradeTracker -> PortfolioService

Diferencias vs execute_paper.py
--------------------------------
  - LiveExecutor en lugar de PaperExecutor
  - PortfolioService inyectado (store decidido por PortfolioCompositionRoot)
  - guard obligatorio -- sin kill switch no hay live trading
  - risk_config obligatoria -- no defaults permisivos
  - Sin SyntheticDataSource -- siempre datos reales de Gold

SafeOps en live
---------------
- Fail-Fast en build: guard y risk_config obligatorios.
- Fail-Soft en execute: errores retornados en CycleRunResult, no lanzan.
- Toda orden enviada al exchange queda logueada con order_id.

H1 (AUDIT-apps-2026-08-03): las firmas reciben TradingConfig/RiskConfig
tipados (no argparse.Namespace). El borde CLI deriva max_order_usd y
min_order_usd vía model_copy — este use case no repite fórmulas ni getattr.

Principios: SRP - DIP - DRY - SafeOps - Composition Root
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from portfolio.services.portfolio_service import PortfolioService
    from trading.analytics.trade_tracker import TradeTracker
    from trading.engine import TradingEngine

    from ocm.config.schema import ExchangeConfig, RiskConfig, TradingConfig

from loguru import logger

from app.use_cases.run_result import CycleRunResult

# ---------------------------------------------------------------------------
# Resources -- recursos vivos abiertos por build_live_engine()
# ---------------------------------------------------------------------------


@dataclass
class LiveEngineResources:
    """
    Recursos construidos por build_live_engine() y su ciclo de vida.

    engine / portfolio son consumidos por execute(). Los demas campos son
    handles a recursos externos que deben cerrarse ordenadamente ante fin
    de ciclo, excepcion, o senal SIGINT/SIGTERM.

    exchange_client, kafka_producer y metrics_server son placeholders para
    cuando esos recursos existan de verdad -- hoy LiveExecutor es un stub
    sin conexion real al exchange (ver trading/execution/live_executor.py),
    no hay producer Kafka en el camino de live trading, y no hay
    metrics_server dedicado. Se declaran ahora para que shutdown() no deba
    reescribirse cuando aparezcan -- solo hay que poblarlos aqui.
    """

    engine: TradingEngine
    portfolio: PortfolioService
    tracker: "TradeTracker"
    redis_client: Any
    exchange_client: Optional[Any] = None
    kafka_producer: Optional[Any] = None
    metrics_server: Optional[Any] = None

    def shutdown(self) -> None:
        """
        Cierra todos los recursos abiertos.

        SafeOps: cada cierre esta aislado -- el fallo de uno no impide
        intentar cerrar el resto. Nunca lanza al caller.
        """
        for name, resource, method_name in (
            ("metrics_server", self.metrics_server, "stop"),
            ("kafka_producer", self.kafka_producer, "close"),
            ("exchange_client", self.exchange_client, "close"),
            ("redis_client", self.redis_client, "close"),
        ):
            if resource is None:
                continue
            try:
                getattr(resource, method_name)()
                logger.debug("shutdown: {} cerrado", name)
            except Exception as exc:
                logger.warning("shutdown: error cerrando {} | {}", name, exc)


# ---------------------------------------------------------------------------
# Builder -- ensamblaje de dependencias live
# ---------------------------------------------------------------------------


def build_live_engine(
    trading: "TradingConfig",
    risk: "RiskConfig",
    portfolio_service: PortfolioService,
    *,
    max_errors: int,
    min_confidence: float,
    exchange_config: Optional["ExchangeConfig"] = None,
) -> LiveEngineResources:
    """
    Ensambla TradingEngine(live) + PortfolioService vía Composition Root.

    Fail-Fast:
    - guard construido y validado antes de llamar a assemble_live()
    - risk explicita (AppConfig.risk derivada por el CLI) -- no defaults permisivos

    Parameters
    ----------
    trading : TradingConfig tipado — derivado por assemble_cli_config() en
        el borde CLI (AppConfig SSOT + flags CLI; ADR-0003).
    risk    : RiskConfig tipado — idem. max_order_usd/min_order_usd ya
        derivados en el CLI vía model_copy (H1/H4).
    portfolio_service : PortfolioService ya ensamblado por
        PortfolioCompositionRoot.assemble() (app/cli/live_hydra.py). El
        Composition Root del portfolio es dueno de su conexion Redis (SSOT:
        un unico dueno de la conexion por ejecucion); este builder NO abre
        ninguna conexion propia.
    max_errors : errores consecutivos antes de activar guard/halt.
    min_confidence : confianza mínima de señal (live: más restrictivo).
    exchange_config : ExchangeConfig (credenciales) para el transporte real
        hacia el exchange. Si es None, el motor arranca en modo PAPER
        (PaperTransport) — ignora honorario real. (ADR-0016)

    Returns
    -------
    LiveEngineResources
    """
    from trading.bootstrap.composition_root import TradingCompositionRoot

    from ocm.runtime.guard import ExecutionGuard

    # Fail-Fast: guard obligatorio en live -- sin kill switch no hay ejecucion
    guard = ExecutionGuard(max_errors=max_errors)

    from portfolio.services.rebalance_service import RebalanceService

    root = TradingCompositionRoot(
        trading=trading,
        risk=risk,
        portfolio=portfolio_service,
        guard=guard,
        rebalance_port=RebalanceService(),
    )
    runtime = root.assemble_live(
        min_confidence=min_confidence,
        exchange_config=exchange_config,
    )

    # redis_client siempre None: la conexion Redis pertenece al
    # PortfolioCompositionRoot del caller (portfolio_root.close()).
    return LiveEngineResources(
        engine=runtime.engine,
        portfolio=runtime.portfolio,
        tracker=runtime.tracker,
        redis_client=None,
    )


# ---------------------------------------------------------------------------
# Use case -- ejecutar ciclo completo
# ---------------------------------------------------------------------------


def execute(
    trading: "TradingConfig",
    risk: "RiskConfig",
    portfolio_service: PortfolioService,
    *,
    max_errors: int,
    min_confidence: float,
    exchange_config: Optional["ExchangeConfig"] = None,
) -> CycleRunResult:
    """
    Ejecuta un ciclo de live trading.

    SafeOps: nunca lanza -- errores retornados en CycleRunResult.

    H7: analytics (closed_trades + summarize) dentro del try -- un fallo en
    PerformanceEngine no pierde el resultado del ciclo.

    Returns
    -------
    CycleRunResult con todo lo necesario para que el CLI loguee y salga.
    """
    from trading.analytics.performance import PerformanceEngine

    try:
        resources = build_live_engine(
            trading,
            risk,
            portfolio_service=portfolio_service,
            max_errors=max_errors,
            min_confidence=min_confidence,
            exchange_config=exchange_config,
        )
    except Exception as exc:
        logger.error(
            "Error construyendo engine live | {} -- {}",
            type(exc).__name__,
            exc,
        )
        return CycleRunResult(success=False, error=str(exc))

    logger.info("Engine live listo | {}", resources.engine)
    logger.info("Portfolio | {}", resources.portfolio)

    try:
        engine_result = resources.engine.run_once()
    except Exception as exc:
        logger.error(
            "Error en run_once live | {} -- {}",
            type(exc).__name__,
            exc,
        )
        return CycleRunResult(success=False, error=str(exc))
    finally:
        resources.shutdown()

    try:
        trades = resources.tracker.closed_trades
        performance = PerformanceEngine.summarize(trades, capital_usd=trading.capital_usd) if trades else None
    except Exception as exc:
        logger.error(
            "Error calculando performance live | {} -- {}",
            type(exc).__name__,
            exc,
        )
        return CycleRunResult(success=False, error=str(exc))

    return CycleRunResult(
        success=True,
        engine_result=engine_result,
        performance=performance,
        open_positions=resources.tracker.open_positions,
        oms_summary=resources.engine.oms_summary,
    )
