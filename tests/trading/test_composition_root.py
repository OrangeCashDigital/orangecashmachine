# -*- coding: utf-8 -*-
"""
tests/trading/test_composition_root.py
=======================================

Tests unitarios de TradingCompositionRoot (Fase B, auditoría 2026-08-03).

Interfaz SSOT verificada (ADR-0003 enmendado):
    __init__(trading, risk, portfolio, guard=None)

Cobertura (paso 7 del forense §7):
    - Fail-Fast del constructor (trading/portfolio obligatorios).
    - assemble_live(): guard y risk obligatorios; retorna TradingRuntime
      (engine, portfolio, tracker). Portfolio es la instancia INYECTADA —
      el root NO construye stores propios (G1/BC-43).
    - assemble_paper(): data_source obligatorio; runtime ensamblado sin I/O.
    - assemble_rebalance(): stub NotImplementedError (decisión D3).
    - build_gold_data_source(): FeatureSource sin I/O en construcción.

Hermético: sin red, sin Iceberg (GoldReader se construye pero no se lee).
"""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest
from trading.analytics.trade_tracker import TradeTracker
from trading.bootstrap.composition_root import TradingCompositionRoot
from trading.engine import TradingEngine
from trading.strategies.base import Signal

from ocm.config.schema import (
    RiskConfig as AppRiskConfig,
)
from ocm.config.schema import (
    RiskDrawdownConfig,
    RiskOrderConfig,
    RiskPositionConfig,
    RiskStopLossConfig,
    TradingConfig,
)
from ocm.runtime.guard import ExecutionGuard
from shared.contracts.boundaries import FeatureSource

# ── Fakes ────────────────────────────────────────────────────────────────────


class _FakePortfolio:
    """Stub de PortfolioService — solo lo que build_fill_sync necesita."""

    def open_position(self, **kwargs) -> None:
        pass

    def close_position(self, order_id) -> None:
        pass


class _FakeDataSource:
    """FeatureSource de prueba — nunca toca Iceberg."""

    def load_features(self, exchange, symbol, timeframe, market_type="spot", **kwargs):
        return None


class _FakeDataSourceWithData:
    """FeatureSource que devuelve un DataFrame fijo — sin Iceberg."""

    def __init__(self, df) -> None:
        self._df = df

    def load_features(self, exchange, symbol, timeframe, market_type="spot", **kwargs):
        return self._df


# ── Helpers ──────────────────────────────────────────────────────────────────


def _trading_config(**overrides) -> TradingConfig:
    base = dict(
        strategy_name="ema_crossover",
        strategy_cfg={
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "fast_period": 9,
            "slow_period": 21,
        },
        capital_usd=10_000.0,
        exchange="bybit",
        market_type="spot",
    )
    base.update(overrides)
    return TradingConfig(**base)


def _risk_config() -> AppRiskConfig:
    return AppRiskConfig(
        position=RiskPositionConfig(max_position_pct=0.02, max_open_positions=2),
        order=RiskOrderConfig(min_order_usd=10.0, max_order_usd=500.0),
    )


def _build_root(
    *,
    guard=None,
    risk: AppRiskConfig | None = None,
    _risk_absent: bool = False,
) -> TradingCompositionRoot:
    return TradingCompositionRoot(
        trading=_trading_config(),
        risk=None if _risk_absent else (risk if risk is not None else _risk_config()),
        portfolio=_FakePortfolio(),
        guard=guard,
    )


# ── Constructor — Fail-Fast ──────────────────────────────────────────────────


def test_constructor_requires_trading() -> None:
    with pytest.raises(ValueError, match="trading"):
        TradingCompositionRoot(
            trading=None,  # type: ignore[arg-type]
            risk=_risk_config(),
            portfolio=_FakePortfolio(),
            guard=None,
        )


def test_constructor_requires_portfolio() -> None:
    with pytest.raises(ValueError, match="portfolio"):
        TradingCompositionRoot(
            trading=_trading_config(),
            risk=_risk_config(),
            portfolio=None,  # type: ignore[arg-type]
            guard=None,
        )


# ── assemble_live() ──────────────────────────────────────────────────────────


def test_assemble_live_requires_guard() -> None:
    with pytest.raises(ValueError, match="guard"):
        _build_root(guard=None).assemble_live()


def test_assemble_live_requires_risk() -> None:
    with pytest.raises(ValueError, match="risk"):
        _build_root(guard=ExecutionGuard(max_errors=3), _risk_absent=True).assemble_live()


def test_assemble_live_returns_runtime_with_injected_portfolio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """G1: el root usa el portfolio INYECTADO — no construye stores propios.

    Guard R1 fail-closed: con un executor real (IS_STUB=False), assemble_live
    procede y devuelve el runtime. El executor STUB por defecto aborta (ver
    test_assemble_live_blocks_while_executor_is_stub).
    """
    from trading.execution.live_executor import LiveExecutor

    monkeypatch.setattr(LiveExecutor, "IS_STUB", False)

    portfolio = _FakePortfolio()
    root = TradingCompositionRoot(
        trading=_trading_config(),
        risk=_risk_config(),
        portfolio=portfolio,
        guard=ExecutionGuard(max_errors=3),
    )

    runtime = root.assemble_live()

    assert isinstance(runtime.engine, TradingEngine)
    assert isinstance(runtime.tracker, TradeTracker)
    assert runtime.portfolio is portfolio  # BC-43: misma instancia inyectada


def test_assemble_live_prefers_real_transport_when_exchange_config() -> None:
    """F3/ADR-0016: con exchange_config, assemble_live usa transporte real.

    El transporte real (Bybit/CCXT) se construye desde la credencial. Sin
    ella, usa PaperTransport (modo paper del camino live).
    """
    import market_data  # noqa: F401  (disponible en CI)
    from trading.execution.live_executor import LiveExecutor

    portfolio = _FakePortfolio()

    # Sin credenciales reales no instanciamos ExchangeConfig completo; en
    # paper el transporte es simulado y assemble_live procede.
    root = TradingCompositionRoot(
        trading=_trading_config(),
        risk=_risk_config(),
        portfolio=portfolio,
        guard=ExecutionGuard(max_errors=3),
    )
    runtime = root.assemble_live()
    assert isinstance(runtime.engine, TradingEngine)
    assert isinstance(runtime.tracker, TradeTracker)
    assert runtime.portfolio is portfolio

    assert not LiveExecutor.IS_STUB, "LiveExecutor no debe ser STUB tras F3 (B-12)"


def test_assemble_live_ok_con_provenance_actual() -> None:
    """B-23: con PROVIDENCE actual (Orders/Fills en DOMAIN), assemble_live no bloquea.

    Camino feliz — confirma que el guard de la Promotion Rule (ADR-0017 §14)
    no interfiere con el estado presente del sistema. B-23 es defensa en
    profundidad para el futuro, no una corrección de un fallo actual.
    """
    portfolio = _FakePortfolio()
    root = TradingCompositionRoot(
        trading=_trading_config(),
        risk=_risk_config(),
        portfolio=portfolio,
        guard=ExecutionGuard(max_errors=3),
    )

    runtime = root.assemble_live()  # no debe lanzar

    assert isinstance(runtime.engine, TradingEngine)
    assert runtime.portfolio is portfolio


def test_assemble_live_bloquea_si_orders_fills_no_promovidos(monkeypatch) -> None:
    """B-23: fail-closed — degradar OrderFilledPayload a ASSUMED bloquea assemble_live.

    Simula un descuido futuro (schema pierde su categoría promovida sin
    revalidación) y confirma que el guard de la Promotion Rule (ADR-0017 §14)
    frena el ensamblaje del executor real antes de tocar IS_STUB, con un
    mensaje que identifica el payload culpable.
    """
    from shared.kafka import provenance

    monkeypatch.setitem(
        provenance.PROVIDENCE,
        "OrderFilledPayload",
        ("ASSUMED", "wired", "monkeypatch de test B-23 — degradación simulada"),
    )

    root = TradingCompositionRoot(
        trading=_trading_config(),
        risk=_risk_config(),
        portfolio=_FakePortfolio(),
        guard=ExecutionGuard(max_errors=3),
    )

    with pytest.raises(ValueError, match="OrderFilledPayload"):
        root.assemble_live()


# ── assemble_paper() ─────────────────────────────────────────────────────────


def test_assemble_paper_requires_data_source() -> None:
    with pytest.raises(ValueError, match="data_source"):
        _build_root(guard=None).assemble_paper(data_source=None)  # type: ignore[arg-type]


def test_assemble_paper_returns_runtime_without_guard() -> None:
    """Fail-soft: paper no exige guard ni risk explícito."""
    root = _build_root(guard=None, _risk_absent=True)
    runtime = root.assemble_paper(data_source=_FakeDataSource())

    assert isinstance(runtime.engine, TradingEngine)
    assert isinstance(runtime.tracker, TradeTracker)


# ── Comportamiento paper vía assemble_paper (port de PaperBot) ───────────────
# PaperBot fue eliminado (2026-08-03). Su cobertura de comportamiento se portó
# aquí probando el sistema real: root → assemble_paper() → TradingEngine.run_once().


def _make_crossover_df() -> pd.DataFrame:
    """DataFrame con golden cross garantizado en la última vela."""
    n = 50
    close = np.full(n, 40_000.0)
    close[-1] = 60_000.0
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h", tz="UTC"),
            "open": close,
            "high": close + 100,
            "low": close - 100,
            "close": close,
            "volume": np.ones(n) * 500,
        }
    )


def test_paper_mode_generates_order_on_crossover() -> None:
    """Cruce EMA → run_once genera exactamente 1 orden BUY (end-to-end)."""
    runtime = _build_root(guard=None).assemble_paper(data_source=_FakeDataSourceWithData(_make_crossover_df()))

    result = runtime.engine.run_once()

    assert len(result.orders) == 1
    assert result.orders[0].side.value == "buy"


def test_paper_mode_rejects_signal_below_min_confidence() -> None:
    """min_confidence del CLI → RiskManager rechaza señales de baja confianza."""
    runtime = _build_root(guard=None).assemble_paper(
        data_source=_FakeDataSourceWithData(_make_crossover_df()),
        min_confidence=0.9,
    )

    def _low_confidence(df):
        return [
            Signal(
                symbol="BTC/USDT",
                timeframe="1h",
                direction="buy",
                price=float(df.select("close").row(-1)[0]),
                timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
                confidence=0.5,
            )
        ]

    runtime.engine._strategy.generate_signals = _low_confidence

    result = runtime.engine.run_once()

    assert result.signals_generated == 1
    assert result.orders_rejected == 1
    assert result.orders == []


def test_paper_mode_rejects_when_max_open_positions_reached() -> None:
    """max_open_positions=1 → el segundo ciclo rechaza la nueva señal."""
    root = TradingCompositionRoot(
        trading=_trading_config(),
        risk=AppRiskConfig(
            position=RiskPositionConfig(max_position_pct=0.02, max_open_positions=1),
            order=RiskOrderConfig(min_order_usd=10.0, max_order_usd=500.0),
        ),
        portfolio=_FakePortfolio(),
        guard=None,
    )
    runtime = root.assemble_paper(data_source=_FakeDataSourceWithData(_make_crossover_df()))

    first = runtime.engine.run_once()
    assert len(first.orders) == 1, "El primer ciclo debe generar una orden"

    second = runtime.engine.run_once()
    assert second.orders == [], "El segundo ciclo debe ser rechazado por max_open_positions"


# ── assemble_rebalance() ─────────────────────────────────────────────────────


def test_assemble_rebalance_is_stubbed() -> None:
    """D3: stub documentado — el tracking real vive en portfolio."""
    with pytest.raises(NotImplementedError, match="portfolio"):
        _build_root(guard=None).assemble_rebalance()


# ── _map_risk_config / _resolve_risk_config — mapeo 1:1 ──────────────────────


def test_map_risk_config_is_1to1_no_field_lost() -> None:
    """Mapeo 1:1: AppConfig.risk → trading.risk.models.RiskConfig sin perder campos."""
    from trading.bootstrap.composition_root import _map_risk_config

    app_risk = AppRiskConfig(
        position=RiskPositionConfig(max_position_pct=0.03, max_open_positions=5),
        stop_loss=RiskStopLossConfig(enabled=False, default_pct=0.05),
        drawdown=RiskDrawdownConfig(
            max_daily_drawdown_pct=0.04,
            max_total_drawdown_pct=0.12,
            halt_on_breach=False,
        ),
        order=RiskOrderConfig(min_order_usd=25.0, max_order_usd=750.0),
    )

    mapped = _map_risk_config(app_risk)

    assert mapped.position.max_position_pct == 0.03
    assert mapped.position.max_open_positions == 5
    assert mapped.stop_loss.enabled is False
    assert mapped.stop_loss.default_pct == 0.05
    assert mapped.drawdown.max_daily_drawdown_pct == 0.04
    assert mapped.drawdown.max_total_drawdown_pct == 0.12
    assert mapped.drawdown.halt_on_breach is False
    assert mapped.order.min_order_usd == 25.0
    assert mapped.order.max_order_usd == 750.0


def test_resolve_risk_config_applies_min_confidence_only_via_override() -> None:
    """min_confidence se aplica SOLO vía override CLI; sin él, default del modelo."""

    root = _build_root(guard=None)  # risk = _risk_config() (max_position_pct=0.02)

    default = root._resolve_risk_config(None)
    assert default.signal_filter.min_confidence == 0.8  # default del modelo, no inventado
    assert default.position.max_position_pct == 0.02  # mapeo preservado

    overridden = root._resolve_risk_config(0.65)
    assert overridden.signal_filter.min_confidence == 0.65
    assert overridden.position.max_position_pct == 0.02


def test_resolve_risk_config_uses_pure_defaults_when_risk_absent() -> None:
    """Fail-soft paper: sin risk explícito → defaults puros de RiskConfig."""
    from trading.risk.models import RiskConfig as DomainRiskConfig

    root = _build_root(guard=None, _risk_absent=True)

    cfg = root._resolve_risk_config(None)

    assert cfg == DomainRiskConfig()


# ── build_gold_data_source() ─────────────────────────────────────────────────


def test_build_gold_data_source_is_a_feature_source_without_io() -> None:
    """Construcción sin I/O — el catálogo Iceberg se abre en el primer read."""
    root = _build_root(guard=None)
    data_source = root.build_gold_data_source()

    assert isinstance(data_source, FeatureSource)
