# -*- coding: utf-8 -*-
"""
tests/portfolio/test_rebalance_service.py
===========================================

Suite unitaria de RebalanceService — aislada del Composition Root.

Principios de test
------------------
Aislamiento total  : RebalanceService se instancia directo, sin
                      CompositionRoot, sin AppConfig, sin Redis.
Fail-Fast           : cada test verifica una sola responsabilidad.
DRY                 : fixtures centralizan construcción de estado/servicio.
Nomenclatura        : test_<método>_<condición>_<resultado_esperado>

Cobertura
---------
__init__ (Fail-Fast):
  - drift_threshold fuera de (0, 1) -> ValueError
  - min_delta_pct fuera de (0, 1) -> ValueError
  - min_delta_pct >= drift_threshold -> ValueError
  - combinación válida no lanza

rebalance:
  - delta por debajo de drift_threshold -> sin señal
  - delta por encima de drift_threshold -> señal buy/sell correcta
  - múltiples señales se ordenan por |delta| descendente
  - SafeOps: excepción interna no propaga, retorna []
  - min_delta_pct es inalcanzable vía API pública dada la invariante
    min_delta_pct < drift_threshold (documentado en commit f251aa9)

validate_targets:
  - dict vacío -> (False, "targets vacío")
  - valor no numérico -> (False, ...)
  - valor fuera de [0, 1] -> (False, ...)
  - suma > 1.0 -> (False, ...)
  - targets válidos -> (True, "")
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from portfolio.models.position import PortfolioState, PositionSnapshot
from portfolio.services.rebalance_service import RebalanceService, RebalanceSignal

# ── Constantes de test ───────────────────────────────────────────────────────
_CAPITAL_USD = 10_000.0
_SYMBOL_BTC = "BTC/USDT"
_SYMBOL_ETH = "ETH/USDT"


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def service() -> RebalanceService:
    """RebalanceService con thresholds default (drift=0.05, min_delta=0.01)."""
    return RebalanceService()


def _position(symbol: str, size_pct: float, order_id: str) -> PositionSnapshot:
    """Construye una PositionSnapshot válida mínima para tests."""
    return PositionSnapshot(
        symbol=symbol,
        exchange="bybit",
        side="long",
        quantity=1.0,
        avg_entry=50_000.0,
        size_pct=size_pct,
        entry_at=datetime.now(timezone.utc),
        order_id=order_id,
    )


# ══════════════════════════════════════════════════════════════════════════════
# __init__ — Fail-Fast
# ══════════════════════════════════════════════════════════════════════════════


class TestInitFailFast:
    @pytest.mark.parametrize("drift", [0.0, 1.0, -0.1, 1.5])
    def test_drift_threshold_fuera_de_rango_lanza(self, drift: float) -> None:
        with pytest.raises(ValueError, match="drift_threshold"):
            RebalanceService(drift_threshold=drift, min_delta_pct=0.01)

    @pytest.mark.parametrize("min_delta", [0.0, 1.0, -0.1, 1.5])
    def test_min_delta_pct_fuera_de_rango_lanza(self, min_delta: float) -> None:
        with pytest.raises(ValueError, match="min_delta_pct"):
            RebalanceService(drift_threshold=0.5, min_delta_pct=min_delta)

    def test_min_delta_pct_igual_a_drift_lanza(self) -> None:
        with pytest.raises(ValueError, match="debe ser <"):
            RebalanceService(drift_threshold=0.05, min_delta_pct=0.05)

    def test_min_delta_pct_mayor_a_drift_lanza(self) -> None:
        with pytest.raises(ValueError, match="debe ser <"):
            RebalanceService(drift_threshold=0.05, min_delta_pct=0.06)

    def test_combinacion_valida_no_lanza(self) -> None:
        RebalanceService(drift_threshold=0.05, min_delta_pct=0.01)


# ══════════════════════════════════════════════════════════════════════════════
# rebalance — comportamiento
# ══════════════════════════════════════════════════════════════════════════════


class TestRebalance:
    def test_delta_bajo_drift_threshold_no_genera_senal(self, service: RebalanceService) -> None:
        state = PortfolioState(positions=(), capital_usd=_CAPITAL_USD)
        signals = service.rebalance(state, targets={_SYMBOL_BTC: 0.04})
        assert signals == []

    def test_delta_positivo_sobre_drift_genera_senal_buy(self, service: RebalanceService) -> None:
        state = PortfolioState(positions=(), capital_usd=_CAPITAL_USD)
        signals = service.rebalance(state, targets={_SYMBOL_BTC: 0.20})
        assert len(signals) == 1
        assert isinstance(signals[0], RebalanceSignal)
        assert signals[0].action == "buy"
        assert signals[0].symbol == _SYMBOL_BTC
        assert signals[0].delta_pct == pytest.approx(0.20)

    def test_delta_negativo_sobre_drift_genera_senal_sell(self, service: RebalanceService) -> None:
        pos = _position(_SYMBOL_BTC, size_pct=0.30, order_id="o1")
        state = PortfolioState(positions=(pos,), capital_usd=_CAPITAL_USD)
        signals = service.rebalance(state, targets={_SYMBOL_BTC: 0.10})
        assert len(signals) == 1
        assert signals[0].action == "sell"
        assert signals[0].delta_pct == pytest.approx(0.20)

    def test_multiples_senales_ordenadas_por_delta_descendente(self, service: RebalanceService) -> None:
        pos = _position(_SYMBOL_BTC, size_pct=0.10, order_id="o1")
        state = PortfolioState(positions=(pos,), capital_usd=_CAPITAL_USD)
        signals = service.rebalance(
            state,
            targets={_SYMBOL_BTC: 0.40, _SYMBOL_ETH: 0.15},
        )
        assert len(signals) == 2
        assert signals[0].symbol == _SYMBOL_BTC  # delta=0.30, mayor
        assert signals[1].symbol == _SYMBOL_ETH  # delta=0.15, menor
        assert signals[0].delta_pct > signals[1].delta_pct

    def test_excepcion_interna_no_propaga_retorna_lista_vacia(self, service: RebalanceService) -> None:
        # SafeOps: un target no numérico rompe el cálculo interno de _compute
        # (resta entre float y str) -- rebalance() debe absorberlo, no lanzar.
        state = PortfolioState(positions=(), capital_usd=_CAPITAL_USD)
        signals = service.rebalance(state, targets={_SYMBOL_BTC: "no-numerico"})  # type: ignore[dict-item]
        assert signals == []


# ══════════════════════════════════════════════════════════════════════════════
# min_delta_pct — inalcanzable vía API pública (documentado en f251aa9)
# ══════════════════════════════════════════════════════════════════════════════


class TestMinDeltaPctInalcanzable:
    """
    RebalanceService.__init__ garantiza min_delta_pct < drift_threshold
    (Fail-Fast). Como consecuencia, en _compute() cualquier delta que
    supere drift_threshold supera automáticamente min_delta_pct -- el
    segundo filtro nunca descarta una señal que el primero ya dejó pasar.

    Este test no ejercita esa rama (es inalcanzable por diseño desde la
    API pública) -- en su lugar demuestra la invariante empíricamente:
    dos servicios con el mismo drift_threshold pero distinto min_delta_pct
    producen salidas idénticas para cualquier conjunto de targets.
    """

    def test_variar_min_delta_pct_no_cambia_las_senales_generadas(self) -> None:
        pos = _position(_SYMBOL_BTC, size_pct=0.10, order_id="o1")
        state = PortfolioState(positions=(pos,), capital_usd=_CAPITAL_USD)
        targets = {_SYMBOL_BTC: 0.40, _SYMBOL_ETH: 0.06}

        service_min_delta_bajo = RebalanceService(drift_threshold=0.05, min_delta_pct=0.01)
        service_min_delta_alto = RebalanceService(drift_threshold=0.05, min_delta_pct=0.049)

        signals_bajo = service_min_delta_bajo.rebalance(state, targets=targets)
        signals_alto = service_min_delta_alto.rebalance(state, targets=targets)

        assert [(s.symbol, s.action, s.delta_pct) for s in signals_bajo] == [
            (s.symbol, s.action, s.delta_pct) for s in signals_alto
        ]


# ══════════════════════════════════════════════════════════════════════════════
# validate_targets
# ══════════════════════════════════════════════════════════════════════════════


class TestValidateTargets:
    def test_dict_vacio_es_invalido(self, service: RebalanceService) -> None:
        valid, msg = service.validate_targets({})
        assert valid is False
        assert "vacío" in msg

    def test_valor_no_numerico_es_invalido(self, service: RebalanceService) -> None:
        valid, msg = service.validate_targets({_SYMBOL_BTC: "no-numerico"})  # type: ignore[dict-item]
        assert valid is False
        assert "numérico" in msg

    def test_valor_fuera_de_rango_es_invalido(self, service: RebalanceService) -> None:
        valid, msg = service.validate_targets({_SYMBOL_BTC: 1.5})
        assert valid is False
        assert "rango" in msg

    def test_suma_mayor_a_uno_es_invalida(self, service: RebalanceService) -> None:
        valid, msg = service.validate_targets({_SYMBOL_BTC: 0.6, _SYMBOL_ETH: 0.5})
        assert valid is False
        assert "sumar" in msg

    def test_targets_validos(self, service: RebalanceService) -> None:
        valid, msg = service.validate_targets({_SYMBOL_BTC: 0.4, _SYMBOL_ETH: 0.3})
        assert valid is True
        assert msg == ""
