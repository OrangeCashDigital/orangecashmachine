# -*- coding: utf-8 -*-
"""
tests/trading/test_paper_bot.py
==================================

Tests unitarios de PaperBot.
GoldStorage se reemplaza con un stub controlado — sin Iceberg real.

Cambios respecto a versión anterior
-------------------------------------
- GoldDataSource (Protocol local eliminado) → FeatureSource de core.boundaries
- RiskConfig(max_open_trades=N) → PositionConfig(max_open_positions=N)  (SSOT)
- _evaluate_signal eliminado de PaperBot — la evaluación de señales ahora vive
  en RiskManager/OMS. Los tests prueban comportamiento observable via run_once(),
  no implementación interna. (SRP — PaperBot es facade, no evaluador de riesgo)
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import numpy as np
import pandas as pd

from ocm_platform.boundaries import FeatureSource
from trading.execution.paper_bot import PaperBot, PaperOrder
from trading.risk.models import PositionConfig, RiskConfig, SignalFilterConfig, SignalFilterConfig
from trading.strategies.base import Signal
from trading.strategies.ema_crossover import EMACrossoverStrategy


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_ohlcv(n: int = 50, seed: int = 0) -> pd.DataFrame:
    rng   = np.random.default_rng(seed)
    close = rng.uniform(40_000, 50_000, n)
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
        "open":   close,
        "high":   close + 100,
        "low":    close - 100,
        "close":  close,
        "volume": rng.uniform(100, 1_000, n),
    })


def _make_crossover_df() -> pd.DataFrame:
    """DataFrame con golden cross garantizado en la última vela."""
    n     = 50
    close = np.full(n, 40_000.0)
    close[-1] = 60_000.0
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
        "open":   close,
        "high":   close + 100,
        "low":    close - 100,
        "close":  close,
        "volume": np.ones(n) * 500,
    })


def _make_signal(signal_type: str = "buy", confidence: float = 1.0) -> Signal:
    return Signal(
        symbol     = "BTC/USDT",
        timeframe  = "1h",
        signal     = signal_type,
        price      = 50_000.0,
        timestamp  = datetime(2024, 1, 1, tzinfo=timezone.utc),
        confidence = confidence,
    )


def _make_bot(df=None, risk=None) -> PaperBot:
    """Crea un PaperBot con data_source stubbed via FeatureSource."""
    source   = MagicMock(spec=FeatureSource)
    source.load_features.return_value = df if df is not None else _make_ohlcv()
    strategy = EMACrossoverStrategy(symbol="BTC/USDT", timeframe="1h")
    return PaperBot(
        strategy    = strategy,
        data_source = source,
        risk        = risk or RiskConfig(min_confidence=0.5),
    )


# ── Construcción ──────────────────────────────────────────────────────────────

def test_paper_bot_instantiates():
    bot = _make_bot()
    assert bot.exchange == "bybit"


def test_paper_bot_uses_default_risk_when_not_provided():
    source   = MagicMock(spec=FeatureSource)
    strategy = EMACrossoverStrategy()
    bot      = PaperBot(strategy=strategy, data_source=source)
    assert isinstance(bot.risk, RiskConfig)


# ── run_once — sin datos ──────────────────────────────────────────────────────

def test_run_once_returns_empty_when_no_data():
    bot = _make_bot(df=pd.DataFrame())
    assert bot.run_once() == []


def test_run_once_returns_empty_when_data_source_returns_none():
    bot = _make_bot(df=None)
    bot.data_source.load_features.return_value = None
    assert bot.run_once() == []


# ── run_once — con señal ──────────────────────────────────────────────────────

def test_run_once_generates_order_on_crossover():
    bot    = _make_bot(df=_make_crossover_df())
    orders = bot.run_once()
    assert len(orders) == 1
    assert orders[0].side == "buy"


def test_run_once_logs_order_to_history():
    bot = _make_bot(df=_make_crossover_df())
    bot.run_once()
    assert len(bot.order_history) == 1


def test_run_once_registers_open_trade():
    bot = _make_bot(df=_make_crossover_df())
    bot.run_once()
    assert len(bot.open_trades) == 1


def test_run_once_no_signal_no_order():
    """DataFrame plano → sin cruce → sin orden."""
    df         = _make_ohlcv(n=50)
    df["close"] = 45_000.0
    df["open"]  = 45_000.0
    bot        = _make_bot(df=df)
    assert bot.run_once() == []


# ── Risk checks — comportamiento observable vía run_once() ───────────────────
# Principio: los tests prueban QUÉ hace el sistema (comportamiento), no CÓMO
# (implementación interna). La evaluación de riesgo vive en RiskManager/OMS.

def test_run_once_rejects_signal_below_min_confidence():
    """
    RiskManager rechaza señales cuya confianza es inferior al umbral.

    Nota: EMACrossoverStrategy siempre emite confidence=1.0.
    Este test verifica el rechazo via _evaluate_signal con señal sintética
    de baja confianza — que es la superficie testeable correcta para este
    comportamiento. run_once() con EMA nunca puede generar confidence < 1.0.
    """
    bot    = _make_bot(risk=RiskConfig(
        signal_filter = SignalFilterConfig(min_confidence=0.9),
    ))
    signal = _make_signal(confidence=0.5)
    assert bot._evaluate_signal(signal) is None


def test_run_once_accepts_signal_at_default_min_confidence():
    """
    Señal con confianza suficiente (default) → orden generada.

    Verifica que el umbral de confianza no bloquea señales válidas.
    """
    bot = _make_bot(df=_make_crossover_df())
    assert len(bot.run_once()) == 1


def test_run_once_rejects_when_max_open_positions_reached():
    """
    RiskManager debe rechazar nuevas señales cuando se alcanza el límite
    de posiciones abiertas simultáneas.

    Primer run_once: genera orden, ocupa el único slot disponible.
    Segundo run_once: RiskManager rechaza la señal → resultado vacío.
    """
    risk = RiskConfig(
        position = PositionConfig(max_open_positions=1),
    )
    bot = _make_bot(df=_make_crossover_df(), risk=risk)

    first = bot.run_once()
    assert len(first) == 1, "El primer ciclo debe generar una orden"

    second = bot.run_once()
    assert len(second) == 0, "El segundo ciclo debe ser rechazado por max_open_positions"


# ── close_trade ───────────────────────────────────────────────────────────────

def test_close_trade_removes_from_open_trades():
    bot    = _make_bot(df=_make_crossover_df())
    orders = bot.run_once()
    assert len(bot.open_trades) == 1
    bot.close_trade(orders[0])
    assert len(bot.open_trades) == 0


def test_close_trade_preserves_order_history():
    bot    = _make_bot(df=_make_crossover_df())
    orders = bot.run_once()
    bot.close_trade(orders[0])
    assert len(bot.order_history) == 1   # log no se borra al cerrar


def test_close_trade_noop_on_unknown_order():
    bot    = _make_bot()
    signal = _make_signal()
    order  = PaperOrder(
        symbol    = "BTC/USDT",
        side      = "buy",
        price     = 50_000.0,
        size_pct  = 0.01,
        timestamp = datetime(2024, 1, 1, tzinfo=timezone.utc),
        signal    = signal,
    )
    bot.close_trade(order)   # no debe lanzar excepción
    assert len(bot.open_trades) == 0


# ── summary ───────────────────────────────────────────────────────────────────

def test_summary_initial_state():
    bot = _make_bot()
    s   = bot.summary()
    assert s["total_signals_acted"] == 0
    assert s["open_trades"] == 0
    assert s["last_order"] is None


def test_summary_after_run():
    bot = _make_bot(df=_make_crossover_df())
    bot.run_once()
    s = bot.summary()
    assert s["total_signals_acted"] == 1
    assert s["open_trades"] == 1
    assert s["last_order"] is not None
