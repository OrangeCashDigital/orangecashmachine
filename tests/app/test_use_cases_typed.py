# -*- coding: utf-8 -*-
"""
tests/app/test_use_cases_typed.py
===================================

Tests de los use cases tipados de trading (H1/H4/H7, AUDIT-apps-2026-08-03)
y del puente CLI -> config tipada (assemble_cli_config).

Cubre:
  · assemble_cli_config()  — deriva TradingConfig/RiskConfig/RunParams desde
    AppConfig (SSOT) + flags CLI; min_order_usd desde config (H4);
    max_order_usd = capital × max_position_pct (H1/R4); el Namespace muere
    en el borde CLI (R5).
  · CycleRunResult          — contrato único del resultado de ciclo (H12).
  · execute_paper(dry_run)  — smoke end-to-end offline (datos sintéticos,
    sin red ni Iceberg).
  · H7                      — fallo de analytics (summarize) no escapa del
    contrato "nunca lanza"; execute_* devuelve CycleRunResult(success=False).
  · execute_live            — fail-soft cuando el build del engine falla.

Principios: hermetico (sin red/infra), una responsabilidad por test.
"""

from __future__ import annotations

from argparse import Namespace
from types import SimpleNamespace

import pytest
from portfolio.infra.memory_store import InMemoryPositionStore
from portfolio.services.portfolio_service import PortfolioService

from ocm.config.schema import (
    AppConfig,
    ExchangeConfig,
    MarketConfig,
    MarketsConfig,
    PipelineConfig,
    PortfolioConfig,
    RiskConfig,
    RiskOrderConfig,
    RiskPositionConfig,
    SupportedExchange,
    TradingConfig,
)


@pytest.fixture(autouse=True)
def _force_test_env(monkeypatch) -> None:
    """Fuerza entorno no-producción — ExchangeConfig valida credenciales."""
    monkeypatch.setenv("OCM_ENV", "test")


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _app_config(*, min_order_usd: float = 25.0, max_position_pct: float = 0.02) -> AppConfig:
    return AppConfig(
        exchanges=[
            ExchangeConfig(
                name=SupportedExchange.BYBIT,
                enabled=True,
                markets=MarketsConfig(
                    spot=MarketConfig(enabled=True, symbols=["BTC/USDT"]),
                ),
            )
        ],
        pipeline=PipelineConfig(),
        portfolio=PortfolioConfig(capital_usd=5_000.0, exchange="kucoin"),
        risk=RiskConfig(
            position=RiskPositionConfig(
                max_position_pct=max_position_pct,
                max_open_positions=4,
            ),
            order=RiskOrderConfig(min_order_usd=min_order_usd, max_order_usd=1_000.0),
        ),
    )


def _cli_args(**overrides) -> Namespace:
    base = {
        "strategy": "ema_crossover",
        "symbol": "ETH/USDT",
        "timeframe": "4h",
        "market_type": "swap",
        "fast": 12,
        "slow": 26,
        "min_confidence": 0.7,
    }
    base.update(overrides)
    return Namespace(**base)


def _trading_config(capital: float = 10_000.0) -> TradingConfig:
    return TradingConfig(
        strategy_name="ema_crossover",
        strategy_cfg={
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "fast_period": 9,
            "slow_period": 21,
        },
        capital_usd=capital,
        exchange="bybit",
        market_type="spot",
    )


def _risk_config() -> RiskConfig:
    return RiskConfig(
        position=RiskPositionConfig(max_position_pct=0.05, max_open_positions=3),
        order=RiskOrderConfig(min_order_usd=10.0, max_order_usd=500.0),
    )


def _portfolio_service(capital: float = 10_000.0) -> PortfolioService:
    return PortfolioService(
        capital_usd=capital,
        store=InMemoryPositionStore(),
        exchange="bybit",
    )


# ---------------------------------------------------------------------------
# assemble_cli_config — H1/H4/R4 (puente CLI -> config tipada)
# ---------------------------------------------------------------------------


def test_assemble_cli_config_derives_typed_trading_config() -> None:
    """TradingConfig se deriva desde AppConfig (SSOT) + flags CLI, sin Namespace."""
    from app.cli._bootstrap import assemble_cli_config

    app_cfg = _app_config()
    trading_cfg, _, params = assemble_cli_config(app_cfg, _cli_args(), capital=2_000.0, max_errors=5)

    assert trading_cfg.strategy_name == "ema_crossover"
    assert trading_cfg.strategy_cfg == {
        "symbol": "ETH/USDT",
        "timeframe": "4h",
        "fast_period": 12,
        "slow_period": 26,
    }
    assert trading_cfg.capital_usd == 2_000.0
    assert trading_cfg.exchange == "kucoin"  # SSOT: config.portfolio.exchange
    assert trading_cfg.market_type == "swap"
    assert params.min_confidence == 0.7
    assert params.max_errors == 5
    assert params.dry_run is False


def test_assemble_cli_config_min_order_usd_comes_from_config() -> None:
    """H4: min_order_usd respeta config.risk.order.min_order_usd (SSOT)."""
    from app.cli._bootstrap import assemble_cli_config

    app_cfg = _app_config(min_order_usd=37.5)
    _, risk_cfg, _ = assemble_cli_config(app_cfg, _cli_args(), capital=2_000.0)

    assert risk_cfg.order.min_order_usd == 37.5


def test_assemble_cli_config_max_order_usd_derived_once_in_cli() -> None:
    """H1/R4: max_order_usd = capital × max_position_pct, derivado en el borde CLI."""
    from app.cli._bootstrap import assemble_cli_config

    app_cfg = _app_config(max_position_pct=0.02)
    _, risk_cfg, _ = assemble_cli_config(app_cfg, _cli_args(), capital=2_000.0)

    assert risk_cfg.order.max_order_usd == pytest.approx(40.0)


def test_assemble_cli_config_dry_run_flag_forwards_to_run_params() -> None:
    """El flag dry_run (solo paper) llega a RunParams sin alterar la config."""
    from app.cli._bootstrap import assemble_cli_config

    app_cfg = _app_config()
    _, _, params = assemble_cli_config(app_cfg, _cli_args(), capital=5_000.0, dry_run=True)

    assert params.dry_run is True


# ---------------------------------------------------------------------------
# CycleRunResult — H12 (contrato único del resultado de ciclo)
# ---------------------------------------------------------------------------


def test_cycle_run_result_exit_code_maps_success() -> None:
    from app.use_cases.run_result import CycleRunResult

    assert CycleRunResult(success=True).exit_code == 0
    assert CycleRunResult(success=False, error="boom").exit_code == 1


# ---------------------------------------------------------------------------
# execute_paper — smoke end-to-end (dry-run, offline)
# ---------------------------------------------------------------------------


def test_execute_paper_dry_run_returns_success_cycle_result() -> None:
    """Dry-run corre el ciclo completo con datos sintéticos, sin red ni Iceberg."""
    from app.use_cases.execute_paper import execute

    result = execute(
        _trading_config(),
        _risk_config(),
        portfolio_service=_portfolio_service(),
        dry_run=True,
        min_confidence=0.8,
    )

    assert result.success is True
    assert result.error is None
    assert result.engine_result is not None
    assert result.engine_result.symbol == "BTC/USDT"


# ---------------------------------------------------------------------------
# H7 — analytics no escapa del contrato "nunca lanza"
# ---------------------------------------------------------------------------


def test_execute_paper_h7_analytics_failure_is_caught(monkeypatch) -> None:
    """Si summarize lanza, execute devuelve success=False en vez de propagar."""
    from app.use_cases import execute_paper
    from trading.analytics.performance import PerformanceEngine
    from trading.engine import EngineResult

    fake_runtime = SimpleNamespace(
        engine=SimpleNamespace(
            run_once=lambda: EngineResult(symbol="BTC/USDT", timeframe="1h"),
            oms_summary={"orders": 1},
        ),
        portfolio=SimpleNamespace(),
        tracker=SimpleNamespace(
            closed_trades=[SimpleNamespace(entry_price=100.0, exit_price=110.0)],
            open_positions={},
        ),
    )
    monkeypatch.setattr(execute_paper, "build_paper_engine", lambda *a, **k: fake_runtime)

    def _explode(trades, capital_usd=None, periods_per_year=252):
        raise RuntimeError("analytics exploded")

    monkeypatch.setattr(PerformanceEngine, "summarize", staticmethod(_explode))

    result = execute_paper.execute(
        _trading_config(),
        _risk_config(),
        portfolio_service=_portfolio_service(),
        dry_run=True,
        min_confidence=0.8,
    )

    assert result.success is False
    assert "analytics exploded" in result.error


def test_execute_live_fail_soft_when_engine_build_fails(monkeypatch) -> None:
    """Fail-Soft: si build_live_engine lanza, execute devuelve success=False."""
    from app.use_cases import execute_live

    def _explode(trading, risk, portfolio_service, *, max_errors, min_confidence):
        raise RuntimeError("guard misconfigured")

    monkeypatch.setattr(execute_live, "build_live_engine", _explode)

    result = execute_live.execute(
        _trading_config(),
        _risk_config(),
        portfolio_service=_portfolio_service(),
        max_errors=3,
        min_confidence=0.9,
    )

    assert result.success is False
    assert "guard misconfigured" in result.error
