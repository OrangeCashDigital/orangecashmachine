# -*- coding: utf-8 -*-
"""
trading/execution/paper_bot.py
================================

PaperBot — facade de conveniencia sobre TradingEngine para paper trading.

Responsabilidad unica (SRP)
---------------------------
Proveer una API simple de alto nivel para ejecutar un ciclo de paper trading
con una sola llamada. No contiene logica de negocio propia.

Toda la logica real vive en las capas correctas:
  - Carga de datos   : FeatureSource (inyectado)
  - Estrategia       : BaseStrategy  (inyectado)
  - Validacion risk  : RiskManager   (construido internamente via TradingEngine)
  - Ejecucion        : PaperExecutor (construido internamente via TradingEngine)
  - Ciclo completo   : TradingEngine (delegado en run_once)

Cuando usar PaperBot vs TradingEngine directamente
---------------------------------------------------
  PaperBot      : tests unitarios, scripts simples, exploracion interactiva.
  TradingEngine : produccion, Prefect tasks, integracion con TradeTracker.

Principios: SOLID (SRP, DIP, OCP) · KISS · DRY · SafeOps
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from loguru import logger

from core.boundaries import FeatureSource, SignalProtocol
from trading.engine import TradingEngine
from trading.execution.order import Order
from trading.risk.models import RiskConfig
from trading.strategies.base import BaseStrategy


# =============================================================================
# Value object — orden simulada observable
# =============================================================================

@dataclass
class PaperOrder:
    """
    Snapshot observable de una orden paper aceptada.

    Inmutable semanticamente — representa un fill simulado en el momento
    en que la orden fue aceptada por el OMS.

    Nota: wrappea Order del OMS para mantener compatibilidad de API
    con tests y callers existentes que usan PaperBot directamente.
    """
    symbol:    str
    side:      str
    price:     float
    size_pct:  float
    timestamp: datetime
    signal:    SignalProtocol
    reason:    str = ""

    def log(self) -> None:
        logger.info(
            "[PAPER] {} {} {} @ {:.2f} | size={:.1%} capital"
            " | confidence={:.0%} | {}",
            self.side.upper(),
            self.symbol,
            self.signal.timeframe,
            self.price,
            self.size_pct,
            self.signal.confidence,
            self.reason or getattr(self.signal, "metadata", {}).get("strategy", "signal"),
        )

    @classmethod
    def from_order(cls, order: Order) -> "PaperOrder":
        """
        Factory: construye PaperOrder desde Order del OMS.

        Punto unico de conversion OMS → API observable de PaperBot.
        """
        return cls(
            symbol    = order.symbol,
            side      = order.side.value,
            price     = order.fill_price,
            size_pct  = order.size_pct,
            timestamp = order.fill_timestamp,
            signal    = order.signal,
        )


# =============================================================================
# PaperBot — facade sobre TradingEngine
# =============================================================================

class PaperBot:
    """
    Facade de paper trading sobre TradingEngine.

    Responsabilidad unica: proveer una API simple que oculta el ensamblaje
    interno de TradingEngine + RiskManager + PaperExecutor + OMS.

    No contiene logica de negocio — todo se delega al engine.

    Parameters
    ----------
    strategy    : BaseStrategy  — estrategia de trading a ejecutar.
    data_source : FeatureSource — fuente de datos Gold (DIP).
    risk        : RiskConfig, optional — limites de riesgo (defaults seguros).
    exchange    : str — exchange de origen.
    market_type : str — tipo de mercado ("spot" | "linear" | "inverse").
    capital_usd : float — capital virtual para sizing (default: 10_000).
    """

    def __init__(
        self,
        strategy:    BaseStrategy,
        data_source: FeatureSource,
        risk:        Optional[RiskConfig] = None,
        exchange:    str   = "bybit",
        market_type: str   = "spot",
        capital_usd: float = 10_000.0,
    ) -> None:
        self.strategy    = strategy
        self.data_source = data_source
        self.risk        = risk or RiskConfig()
        self.exchange    = exchange
        self.market_type = market_type

        # Estado observable — append-only, nunca muta elementos existentes
        self._open_trades: list[PaperOrder] = []
        self._order_log:   list[PaperOrder] = []

        # Engine interno — toda la logica de negocio vive aqui
        # on_fill callback conecta OMS → estado observable de PaperBot
        self._engine = TradingEngine.build_paper(
            strategy_name = strategy.name,
            strategy_cfg  = self._strategy_cfg(strategy),
            data_source   = data_source,
            risk_config   = self.risk,
            capital_usd   = capital_usd,
            exchange      = exchange,
            market_type   = market_type,
            on_fill       = self._on_fill,
        )
        self._log = logger.bind(component="PaperBot", exchange=exchange)

    # =========================================================================
    # Public API
    # =========================================================================

    def run_once(self) -> list[PaperOrder]:
        """
        Ejecuta un ciclo completo delegando al TradingEngine.

        Retorna las ordenes generadas en este ciclo (subset de order_history).
        SafeOps: nunca lanza — errores se loguean y retornan lista vacia.
        """
        snapshot_before = len(self._order_log)
        try:
            result = self._engine.run_once()
            if result.skipped:
                self._log.warning("Ciclo skipped | reason={}", result.skip_reason)
        except Exception as exc:
            self._log.error("run_once error | {}", exc)
            return []

        # Ordenes generadas en este ciclo = delta del log desde antes del run
        return list(self._order_log[snapshot_before:])

    def close_trade(self, order: PaperOrder) -> None:
        """Marca una posicion como cerrada en el libro interno."""
        if order in self._open_trades:
            self._open_trades.remove(order)
            self._log.info(
                "CLOSE {} {} @ {:.4f} | trades_open={}",
                order.symbol, order.side, order.price,
                len(self._open_trades),
            )

    @property
    def order_history(self) -> list[PaperOrder]:
        """Historial completo de ordenes aceptadas. Append-only."""
        return list(self._order_log)

    @property
    def open_trades(self) -> list[PaperOrder]:
        """Posiciones abiertas (BUY sin CLOSE correspondiente)."""
        return list(self._open_trades)

    def summary(self) -> dict:
        """Estado observable. SafeOps: nunca lanza."""
        return {
            "total_signals_acted": len(self._order_log),
            "open_trades":         len(self._open_trades),
            "last_order":          self._order_log[-1].timestamp.isoformat()
                                   if self._order_log else None,
        }

    # =========================================================================
    # Private
    # =========================================================================


    def _evaluate_signal(self, signal) -> "Optional[PaperOrder]":
        """
        Evalúa una señal contra las reglas de riesgo sin ejecutar el ciclo.

        Delega a RiskManager.validate() via el engine interno — sin duplicar
        lógica de negocio (DIP · DRY · SRP).

        Usado por tests unitarios para verificar el filtro de riesgo
        directamente, sin pasar por run_once() ni cargar datos.

        Returns
        -------
        PaperOrder  : orden simulada si la señal es aprobada.
        None        : si es rechazada por cualquier regla de riesgo.

        SafeOps: nunca lanza — errores se convierten en None.
        """
        try:
            risk_manager = self._engine._oms._risk
            decision     = risk_manager.validate(signal)
            if decision.rejected:
                self._log.debug(
                    "_evaluate_signal rechazada | reason={}", decision.reason
                )
                return None
            from datetime import datetime, timezone
            return PaperOrder(
                symbol    = signal.symbol,
                side      = signal.signal,
                price     = signal.price,
                size_pct  = decision.size_pct,
                timestamp = getattr(signal, "timestamp", None)
                            or datetime.now(tz=timezone.utc),
                signal    = signal,
                reason    = decision.reason,
            )
        except Exception as exc:
            self._log.error("_evaluate_signal error | {}", exc)
            return None

    def _on_fill(self, order: Order) -> None:
        """
        Callback OMS → PaperBot.

        Llamado por TradingEngine/OMS cuando una orden es llenada.
        Convierte Order del OMS en PaperOrder observable y actualiza estado.

        SafeOps: nunca lanza — errores se loguean y descartan.
        """
        try:
            paper = PaperOrder.from_order(order)
            paper.log()
            self._open_trades.append(paper)
            self._order_log.append(paper)
        except Exception as exc:
            self._log.error("_on_fill error | order={} error={}", order.order_id, exc)

    @staticmethod
    def _strategy_cfg(strategy: BaseStrategy) -> dict:
        """
        Extrae la configuracion de la estrategia para TradingEngine.build_paper.

        Usa atributos estandar de BaseStrategy. Extensible via __dict__
        sin romper la interfaz. (OCP)
        """
        cfg: dict = {}
        for attr in ("symbol", "timeframe", "fast_period", "slow_period"):
            val = getattr(strategy, attr, None)
            if val is not None:
                cfg[attr] = val
        return cfg
