# -*- coding: utf-8 -*-
"""
trading/bootstrap/composition_root.py
=======================================

TradingCompositionRoot — punto único de ensamblado del bounded context trading.

Interfaz SSOT (ADR-0003, enmendado 2026-08-03)
-----------------------------------------------
    TradingCompositionRoot.__init__(trading, risk, portfolio, guard=None)

    trading   : ocm.config.schema.TradingConfig   — estrategia / capital / exchange.
    risk      : ocm.config.schema.RiskConfig      — límites de riesgo (AppConfig.risk).
    portfolio : PortfolioService ya ensamblado    — inyectado (BC-43/ADR-0006).
    guard     : ExecutionGuard | None             — kill switch (obligatorio en live).

Historial
---------
v1 (bc21c52): extracción mecánica con imports a paths inexistentes
              (trading.application.*) — nunca pudo importarse.
v2 (WIP):     interfaz ``__init__(config: TradingConfig)`` — DESCARTADA por la
              auditoría 2026-08-03 (H3): contradice el ADR-0003 real. No es
              base a corregir; se parte de cero.
v3 (este):    reconstruida desde cero con la interfaz aprobada
              ``__init__(trading, risk, portfolio, guard=None)``.

Guardrails de la auditoría 2026-08-03
--------------------------------------
- NO se reconstruyen ``_build_position_store_*``/``_build_portfolio`` del
  forense: predatan ADR-0006/BC-43 y violarían BC-43 directamente. El único
  constructor legítimo de PositionStore es ``PortfolioCompositionRoot.assemble()``;
  aquí portfolio llega ya ensamblado (decisión D2).
- ``RedisFactory`` queda OBSOLETO: portfolio es el único dueño de Redis. No se
  recrea.
- El único import de market_data permitido (BC-50) vive en este módulo:
  ``build_gold_data_source()`` → GoldReader (lazy, SafeOps en tests/CI).

Principios: DIP . SRP . DRY . SSOT . SafeOps (fail-fast en live, fail-soft en paper)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from loguru import logger
from trading.risk.models import (
    DrawdownConfig,
    OrderLimits,
    PositionConfig,
    RiskConfig,
    StopLossConfig,
)

from ocm.runtime.guard import ExecutionGuard
from shared.contracts.boundaries import FeatureSource

if TYPE_CHECKING:
    from portfolio.services.portfolio_service import PortfolioService
    from trading.analytics.trade_tracker import TradeTracker
    from trading.engine import TradingEngine

    from ocm.config.schema import RiskConfig as AppRiskConfig
    from ocm.config.schema import TradingConfig

__all__ = ["TradingCompositionRoot", "TradingRuntime"]


# ---------------------------------------------------------------------------
# TradingRuntime
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TradingRuntime:
    """Resultado tipado de assemble_live()/assemble_paper().

    Campos recuperados por el forense de bytecode: engine, portfolio, tracker.

    - engine    : TradingEngine listo para ejecutar ciclos (run_once()).
    - portfolio : PortfolioService inyectado (misma instancia que recibió
                  el constructor — el root no crea estado propio).
    - tracker   : TradeTracker de ejecución (fills/órdenes), conectado vía
                  ``build_fill_sync``.
    """

    engine: "TradingEngine"
    portfolio: "PortfolioService"
    tracker: "TradeTracker"


# ---------------------------------------------------------------------------
# Mapeo de riesgo: AppConfig.risk -> trading.risk.models.RiskConfig
# ---------------------------------------------------------------------------


def _map_risk_config(app_risk: "AppRiskConfig") -> RiskConfig:
    """Traduce ocm.config.schema.RiskConfig -> trading.risk.models.RiskConfig.

    Ambos modelos comparten campos en position/stop_loss/drawdown/order —
    se copian 1:1. ``signal_filter.min_confidence`` NO existe en AppConfig.risk
    (gap real documentado): se resuelve en assemble_live()/assemble_paper()
    vía el parámetro opcional ``min_confidence`` del CLI, nunca inventando un
    valor aquí.
    """
    return RiskConfig(
        position=PositionConfig(
            max_position_pct=app_risk.position.max_position_pct,
            max_open_positions=app_risk.position.max_open_positions,
        ),
        stop_loss=StopLossConfig(
            enabled=app_risk.stop_loss.enabled,
            default_pct=app_risk.stop_loss.default_pct,
        ),
        drawdown=DrawdownConfig(
            max_daily_drawdown_pct=app_risk.drawdown.max_daily_drawdown_pct,
            max_total_drawdown_pct=app_risk.drawdown.max_total_drawdown_pct,
            halt_on_breach=app_risk.drawdown.halt_on_breach,
        ),
        order=OrderLimits(
            min_order_usd=app_risk.order.min_order_usd,
            max_order_usd=app_risk.order.max_order_usd,
        ),
    )


# ---------------------------------------------------------------------------
# Gold feature source — único contacto de trading con market_data (BC-50)
# ---------------------------------------------------------------------------


class _GoldFeatureSource:
    """Adapta GoldReader (market_data) al protocolo FeatureSource.

    Este módulo es el único de trading autorizado a importar market_data
    (BC-50, formaliza ADR-0004). El adapter vive aquí — no en trading/data/
    — para concentrar el acoplamiento en un archivo auditable.

    Reordena parámetros ((exchange, symbol, timeframe, market_type) ->
    (symbol, market_type, timeframe, exchange=)) y convierte errores de
    lectura en None (SafeOps: TradingEngine espera None/DataFrame vacío,
    no excepciones de datos). Imports lazy: sin catálogo Iceberg en CI.
    """

    def __init__(self, exchange: str) -> None:
        from market_data.adapters.outbound.storage.gold_reader import GoldReader

        self._reader = GoldReader(exchange=exchange)
        self._exchange = exchange
        self._log = logger.bind(component="GoldFeatureSource", exchange=exchange)

    def load_features(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        market_type: str = "spot",
        **kwargs: Any,
    ) -> Optional[Any]:
        from market_data.domain.exceptions import DataNotFoundError, DataReadError

        exch = exchange or self._exchange
        try:
            return self._reader.load_features(
                symbol=symbol,
                market_type=market_type,
                timeframe=timeframe,
                exchange=exch,
            )
        except (DataNotFoundError, DataReadError) as exc:
            self._log.warning(
                "Sin datos Gold | {}/{}/{}/{} | {}",
                exch,
                symbol,
                market_type,
                timeframe,
                type(exc).__name__,
            )
            return None
        except Exception as exc:
            self._log.error(
                "Error leyendo Gold | {}/{}/{}/{} | {} — {}",
                exch,
                symbol,
                market_type,
                timeframe,
                type(exc).__name__,
                exc,
            )
            return None


# ---------------------------------------------------------------------------
# TradingCompositionRoot
# ---------------------------------------------------------------------------


class TradingCompositionRoot:
    """Composition root de trading — ensambla el runtime, nunca lo ejecuta.

    Constructor angosto (ADR-0003): recibe los sub-configs sueltos, no
    AppConfig completo, para no forzar exchanges/pipeline dummy en callers
    puros (paper trading manual, scripts de investigación).

    Fail-Fast: trading y portfolio son obligatorios — sin config de negocio
    no hay estrategia/capital/exchange definidos, y sin portfolio inyectado
    el sync de posiciones (BC-43) no tiene dueño.
    """

    def __init__(
        self,
        trading: "TradingConfig",
        risk: Optional["AppRiskConfig"],
        portfolio: "PortfolioService",
        guard: Optional[ExecutionGuard] = None,
    ) -> None:
        if trading is None:
            raise ValueError(
                "TradingCompositionRoot: trading (TradingConfig) es obligatorio. "
                "Fail-Fast: sin config no hay estrategia, capital ni exchange definidos."
            )
        if portfolio is None:
            raise ValueError(
                "TradingCompositionRoot: portfolio (PortfolioService) es obligatorio. "
                "Inyectar el ya ensamblado por PortfolioCompositionRoot.assemble() "
                "(BC-43) — el root no construye stores propios."
            )
        self._trading = trading
        self._risk = risk
        self._portfolio = portfolio
        self._guard = guard

    # ------------------------------------------------------------------
    # Data source
    # ------------------------------------------------------------------

    def build_gold_data_source(self) -> FeatureSource:
        """Construye la FeatureSource real sobre GoldReader (Iceberg).

        Único punto de trading autorizado a importar market_data (BC-50).
        SafeOps: sin I/O en construcción — el catálogo Iceberg se abre en
        el primer load_features().
        """
        return _GoldFeatureSource(exchange=self._trading.exchange)

    # ------------------------------------------------------------------
    # Ensamblado
    # ------------------------------------------------------------------

    def assemble_live(self, *, min_confidence: Optional[float] = None) -> TradingRuntime:
        """Ensambla TradingEngine + TradeTracker para live trading.

        Fail-Fast: guard y risk son obligatorios — sin kill switch ni límites
        explícitos no hay live trading con capital real. Ensambla aquí mismo
        las dependencias internas (Strategy, RiskManager, LiveExecutor, OMS) —
        el root es el único punto de ensamblado (ADR-0003/ADR-0012).
        """
        from trading.analytics.trade_tracker import TradeTracker
        from trading.engine import TradingEngine
        from trading.execution.fill_sync import build_fill_sync
        from trading.execution.live_executor import LiveExecutor
        from trading.execution.oms import OMS
        from trading.risk.manager import RiskManager
        from trading.strategies.registry import StrategyRegistry

        if self._guard is None:
            raise ValueError(
                "assemble_live: guard es obligatorio en live trading. "
                "Sin ExecutionGuard no hay kill switch para capital real."
            )
        if self._risk is None:
            raise ValueError(
                "assemble_live: risk (AppConfig.risk) es obligatorio en live trading. "
                "Los defaults de RiskConfig no son apropiados para capital real."
            )

        tracker = TradeTracker(exchange=self._trading.exchange)

        risk_config = self._resolve_risk_config(min_confidence)
        strategy = StrategyRegistry.get(self._trading.strategy_name)(**self._trading.strategy_cfg)
        risk_manager = RiskManager(
            config=risk_config,
            capital_usd=self._trading.capital_usd,
        )
        executor = LiveExecutor(
            exchange=self._trading.exchange,
            market_type=self._trading.market_type,
        )
        oms = OMS(
            risk_manager=risk_manager,
            executor=executor,
            guard=self._guard,
            on_fill=build_fill_sync(tracker, self._portfolio),
            on_reject=None,
        )
        engine = TradingEngine(
            strategy=strategy,
            oms=oms,
            data_source=self.build_gold_data_source(),
            guard=self._guard,
            exchange=self._trading.exchange,
            market_type=self._trading.market_type,
        )
        return TradingRuntime(engine=engine, portfolio=self._portfolio, tracker=tracker)

    def assemble_paper(
        self,
        data_source: FeatureSource,
        *,
        min_confidence: Optional[float] = None,
    ) -> TradingRuntime:
        """Ensambla TradingEngine + TradeTracker para paper trading.

        Fail-Soft: sin risk explícito usa defaults de RiskConfig — paper no
        mueve capital real. ``data_source`` viene del caller (sintético en
        dry-run, real vía ``build_gold_data_source()`` si no). Ensambla aquí
        mismo las dependencias internas (Strategy, RiskManager, PaperExecutor,
        OMS) — el root es el único punto de ensamblado (ADR-0003/ADR-0012).
        """
        from trading.analytics.trade_tracker import TradeTracker
        from trading.engine import TradingEngine
        from trading.execution.fill_sync import build_fill_sync
        from trading.execution.oms import OMS
        from trading.execution.paper_executor import PaperExecutor
        from trading.risk.manager import RiskManager
        from trading.strategies.registry import StrategyRegistry

        if data_source is None:
            raise ValueError(
                "assemble_paper: data_source es obligatorio. Sintético en dry-run, build_gold_data_source() si no."
            )

        tracker = TradeTracker(exchange=self._trading.exchange)

        risk_config = self._resolve_risk_config(min_confidence)
        strategy = StrategyRegistry.get(self._trading.strategy_name)(**self._trading.strategy_cfg)
        risk_manager = RiskManager(
            config=risk_config,
            capital_usd=self._trading.capital_usd,
        )
        executor = PaperExecutor()
        oms = OMS(
            risk_manager=risk_manager,
            executor=executor,
            guard=self._guard,
            on_fill=build_fill_sync(tracker, self._portfolio),
            on_reject=None,
        )
        engine = TradingEngine(
            strategy=strategy,
            oms=oms,
            data_source=data_source,
            guard=self._guard,
            exchange=self._trading.exchange,
            market_type=self._trading.market_type,
        )
        return TradingRuntime(engine=engine, portfolio=self._portfolio, tracker=tracker)

    def assemble_rebalance(self, *, use_redis: bool = False) -> TradingRuntime:
        """Rebalance de posiciones — stub documentado (decisión D3).

        TODO(ADR-0011): decisión de delegación PENDIENTE — delegar en
        RebalanceService de portfolio vs. tracking propio de trading. Hasta
        resolverla (ver ADR-0011), este método falla explícitamente en vez de
        ensamblar un camino sin SSOT.

        El tracking real de posiciones y la capacidad de rebalance viven en
        el bounded context portfolio (RebalanceService), no en trading. La
        decisión de delegación sigue pendiente; hasta resolverla, este método
        falla explícitamente en vez de ensamblar un camino sin SSOT.
        """
        raise NotImplementedError(
            "assemble_rebalance: decisión de delegación pendiente (D3, auditoría "
            "2026-08-03; ver ADR-0011). El tracking de posiciones vive en portfolio "
            "(RebalanceService), no en trading."
        )

    # ------------------------------------------------------------------
    # Helpers privados
    # ------------------------------------------------------------------

    def _resolve_risk_config(self, min_confidence: Optional[float]) -> RiskConfig:
        """Resuelve el RiskConfig de dominio para el modo actual.

        Mapea AppConfig.risk si existe; si no, usa defaults (paper). Aplica
        el override de min_confidence del CLI sobre signal_filter — sin
        inventar un valor en _map_risk_config.
        """
        from trading.risk.models import SignalFilterConfig

        risk_config = _map_risk_config(self._risk) if self._risk is not None else RiskConfig()
        if min_confidence is not None:
            risk_config = risk_config.model_copy(
                update={"signal_filter": SignalFilterConfig(min_confidence=min_confidence)}
            )
        return risk_config

    def __repr__(self) -> str:
        return (
            f"TradingCompositionRoot(strategy={self._trading.strategy_name!r}, "
            f"exchange={self._trading.exchange!r}, portfolio={type(self._portfolio).__name__})"
        )
