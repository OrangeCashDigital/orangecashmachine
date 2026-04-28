# -*- coding: utf-8 -*-
"""
trading/execution/paper_bot.py
================================

Bot de paper trading minimo.

Regla de oro: NUNCA ejecuta ordenes reales.
Lee senal -> valida contra limites de risk -> loguea la orden.

Diseno
------
GoldStorage se inyecta en el constructor (data_source). Esto permite
testear el bot sin Iceberg real -- basta pasar un mock. El acoplamiento
con la capa de storage queda en el punto de entrada (main/CLI), no aqui.

Uso
---
    from market_data.storage.gold.gold_storage import GoldStorage
    from trading.strategies.ema_crossover import EMACrossoverStrategy
    from trading.execution.paper_bot import PaperBot
    from trading.risk.models import RiskConfig

    strategy = EMACrossoverStrategy(symbol="BTC/USDT", timeframe="1h")
    bot      = PaperBot(strategy=strategy, data_source=GoldStorage())
    bot.run_once()
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from loguru import logger

# Protocolos canonicos — SSOT en core/boundaries.py.
# PaperBot depende de abstracciones, no de implementaciones concretas. (DIP)
from core.boundaries import FeatureSource, SignalProtocol
from trading.risk.models import RiskConfig
from trading.strategies.base import BaseStrategy


# =============================================================================
# Orden simulada
# =============================================================================

@dataclass
class PaperOrder:
    """Orden de paper trading -- solo se loguea, nunca se ejecuta."""
    symbol:    str
    side:      str
    price:     float
    size_pct:  float
    timestamp: datetime
    signal:    Signal
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
            self.reason or self.signal.metadata.get("strategy", "signal"),
        )


# =============================================================================
# Bot
# =============================================================================

class PaperBot:
    """
    Bot de paper trading.

    Consume senales de una estrategia, las valida contra limites de riesgo
    y las registra como ordenes simuladas. Nunca toca dinero real.

    Parameters
    ----------
    strategy    : BaseStrategy
    data_source : GoldDataSource
    risk        : RiskConfig, optional
    exchange    : str
    """

    def __init__(
        self,
        strategy:    BaseStrategy,
        data_source: FeatureSource,
        risk:        Optional[RiskConfig] = None,
        exchange:    str = "bybit",
        market_type: str = "spot",
    ) -> None:
        self.strategy     = strategy
        self.data_source  = data_source
        self.risk         = risk or RiskConfig()
        self.exchange     = exchange
        self.market_type  = market_type
        self._open_trades: list[PaperOrder] = []
        self._order_log:   list[PaperOrder] = []

    # =========================================================================
    # Public API
    # =========================================================================

    def run_once(self) -> list[PaperOrder]:
        """Ejecuta un ciclo: carga datos -> genera senales -> valida -> loguea."""
        strategy = self.strategy

        df = self.data_source.load_features(
            exchange    = self.exchange,
            symbol      = strategy.symbol,
            market_type = self.market_type,
            timeframe   = strategy.timeframe,
        )

        if df is None or (hasattr(df, "empty") and df.empty):
            logger.warning(
                "[PaperBot] Sin datos en Gold | exchange={} symbol={} timeframe={}",
                self.exchange, strategy.symbol, strategy.timeframe,
            )
            return []

        signals = strategy.generate_signals(df)

        if not signals:
            logger.debug(
                "[PaperBot] Sin senales | symbol={} timeframe={}",
                strategy.symbol, strategy.timeframe,
            )
            return []

        orders: list[PaperOrder] = []
        for signal in signals:
            order = self._evaluate_signal(signal)
            if order:
                order.log()
                self._open_trades.append(order)
                self._order_log.append(order)
                orders.append(order)

        return orders

    def close_trade(self, order: PaperOrder) -> None:
        """Marca una posicion como cerrada (solo actualiza el contador interno)."""
        if order in self._open_trades:
            self._open_trades.remove(order)
            logger.info(
                "[PAPER] CLOSE {} {} @ {} | trades_open={}",
                order.symbol, order.side, order.price,
                len(self._open_trades),
            )

    @property
    def order_history(self) -> list[PaperOrder]:
        return list(self._order_log)

    @property
    def open_trades(self) -> list[PaperOrder]:
        return list(self._open_trades)

    def summary(self) -> dict:
        return {
            "total_signals_acted": len(self._order_log),
            "open_trades":         len(self._open_trades),
            "last_order":          self._order_log[-1].timestamp.isoformat()
                                   if self._order_log else None,
        }

    # =========================================================================
    # Internal
    # =========================================================================

    def _evaluate_signal(self, signal: SignalProtocol) -> Optional[PaperOrder]:
        """
        Valida la senal contra los limites de riesgo.

        Orden de checks (Fail-Fast -- del mas barato al mas costoso):
          1. Senal accionable  (is_actionable)
          2. Confianza minima  (signal_filter.min_confidence)  -- inclusivo
          3. Limite de trades  (position.max_open_positions)
        """
        if not signal.is_actionable:
            return None

        if signal.confidence < self.risk.signal_filter.min_confidence:
            logger.debug(
                "[PaperBot] Rechazada -- confianza insuficiente"
                " | confidence={:.0%} min={:.0%}",
                signal.confidence, self.risk.signal_filter.min_confidence,
            )
            return None

        if len(self._open_trades) >= self.risk.position.max_open_positions:
            logger.warning(
                "[PaperBot] Rechazada -- maximo trades abiertos"
                " | open={} max={}",
                len(self._open_trades), self.risk.position.max_open_positions,
            )
            return None

        return PaperOrder(
            symbol    = signal.symbol,
            side      = signal.signal,
            price     = signal.price,
            size_pct  = self.risk.position.max_position_pct,
            timestamp = signal.timestamp,
            signal    = signal,
        )
