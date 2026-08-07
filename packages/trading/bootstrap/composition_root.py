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
from trading.execution.transport import OrderState, OrderStatus, OrderTransport
from trading.risk.models import (
    DrawdownConfig,
    OrderLimits,
    PositionConfig,
    RiskConfig,
    StopLossConfig,
)

from ocm.runtime.guard import ExecutionGuard
from shared.contracts.boundaries import FeatureSource
from shared.kafka.provenance import require_promoted

if TYPE_CHECKING:
    from portfolio.services.portfolio_service import PortfolioService
    from trading.analytics.trade_tracker import TradeTracker
    from trading.engine import TradingEngine

    from ocm.config.schema import ExchangeConfig, TradingConfig
    from ocm.config.schema import RiskConfig as AppRiskConfig

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
# Transporte de órdenes — envío real al exchange (F3 / ADR-0016)
# Único contacto de trading con market_data (BC-50) además de GoldReader.
# ---------------------------------------------------------------------------


class _BybitTransport:
    """Adapta CCXTAdapter al port OrderTransport (submit + fetch_state).

    Vive aquí —no en trading/execution— para concentrar el acoplamiento a
    market_data en un solo archivo auditable (BC-50, mismo criterio que
    _GoldFeatureSource). CCXTAdapter es async; el motor de ejecución es sync,
    así que cada operación se resuelve con un loop limpio vía asyncio.run y
    se cierra el adapter inmediatamente (SafeOps: sin clientes colgados).
    """

    def __init__(self, exchange_config: "ExchangeConfig") -> None:
        from market_data.adapters.outbound.exchange.ccxt_adapter import CCXTAdapter

        self._factory = lambda: CCXTAdapter(config=exchange_config)
        self._exchange = exchange_config.name.value
        self._log = logger.bind(
            component="BybitTransport",
            exchange=self._exchange,
        )

    def submit(
        self,
        symbol: str,
        side: str,
        qty: float,
        *,
        client_order_id: str,
    ) -> "OrderState":
        try:
            raw = run_ccxt_async(
                self._factory,
                lambda a: a.create_order(
                    symbol,
                    side,
                    amount=qty,
                    order_type="market",
                    client_order_id=client_order_id,
                ),
            )
        except Exception as exc:
            return OrderState(status=OrderStatus.ERROR, error=f"{type(exc).__name__}: {exc}")
        return map_ccxt_order(raw)

    def fetch_state(self, exchange_order_id: str) -> "OrderState":
        try:
            raw = run_ccxt_async(self._factory, lambda a: a.fetch_order(exchange_order_id))
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            return OrderState(
                order_id=exchange_order_id,
                status=OrderStatus.ERROR,
                error=error,
            )
        return map_ccxt_order(raw)

    def close(self) -> None:
        return None

    def __repr__(self) -> str:
        return f"BybitTransport(exchange={self._exchange!r})"


def run_ccxt_async(factory, op):
    """Ejecuta una operación CCXT asíncrona con adapter efímero en loop limpio.

    CCXTAdapter es async; el motor de ejecución es sync. Cada operación abre
    un loop nuevo (asyncio.run), conecta, opera y cierra — sin clientes
    colgados entre llamadas (SafeOps).
    """
    import asyncio

    async def _work():
        adapter = factory()
        try:
            await adapter.connect()
            return await op(adapter)
        finally:
            try:
                await adapter.close()
            except Exception:
                pass

    return asyncio.run(_work())


def map_ccxt_order(raw: Any) -> "OrderState":
    """Mapea una orden cruda CCXT a OrderState de dominio."""
    ccxt_status = raw.get("status")  # 'open' | 'closed' | 'canceled' | 'rejected'
    status = OrderStatus.SUBMITTED
    if ccxt_status in ("closed", "filled"):
        status = OrderStatus.FILLED
    elif ccxt_status in ("canceled", "cancelled"):
        status = OrderStatus.CANCELLED
    elif ccxt_status in ("rejected", "expired"):
        status = OrderStatus.REJECTED
    return OrderState(
        order_id=raw.get("id"),
        status=status,
        filled_qty=raw.get("filled"),
        fill_price=raw.get("average"),
    )


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

    def assemble_live(
        self,
        *,
        min_confidence: Optional[float] = None,
        exchange_config: Optional["ExchangeConfig"] = None,
    ) -> TradingRuntime:
        """Ensambla TradingEngine + TradeTracker para live trading.

        Fail-Fast: guard y risk son obligatorios — sin kill switch ni límites
        explícitos no hay live trading con capital real. En modo live se exige
        ``exchange_config`` (credenciales) para construir el transporte real
        (ADR-0016: Bybit). Ensambla aquí mismo las dependencias internas
        (Strategy, RiskManager, LiveExecutor, OMS) — el root es el único punto
        de ensamblado (ADR-0003/ADR-0012).
        """
        from trading.analytics.trade_tracker import TradeTracker
        from trading.engine import TradingEngine
        from trading.execution.fill_sync import build_fill_sync
        from trading.execution.live_executor import LiveExecutor
        from trading.execution.oms import OMS
        from trading.execution.transport import PaperTransport
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
        if exchange_config is not None:
            transport: OrderTransport = _BybitTransport(exchange_config)
        else:
            # Paper: mismo flujo orden→fill→reconciliación, sin I/O (ADR-0016).
            transport = PaperTransport()
        # Guard B-23: fail-closed — Promotion Rule (ADR-0017 §14). Defensa en
        # profundidad: protege contra degradación futura de PROVIDENCE (p. ej.
        # un payload que pase a ASSUMED sin revalidación), no corrige un fallo
        # actual — hoy Orders/Fills ya están en DOMAIN (promovido). Va antes
        # de IS_STUB porque es un gate independiente y más estricto: ni se
        # instancia el executor si la procedencia no está satisfecha.
        require_promoted("OrderFilledPayload", "OrderRejectedPayload")

        executor = LiveExecutor(
            capital_usd=self._trading.capital_usd,
            transport=transport,
            exchange=self._trading.exchange,
            market_type=self._trading.market_type,
            guard=self._guard,
        )
        if executor.IS_STUB:
            raise RuntimeError(  # Guard R1 / B-01: fail-closed
                "assemble_live bloqueado: LiveExecutor es STUB (CCXT no activo). "
                "No se opera capital real con un executor simulado. "
                "F3 (B-12) reimplementa `_submit` y pone IS_STUB=False."
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
