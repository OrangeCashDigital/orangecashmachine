# -*- coding: utf-8 -*-
"""
tests/portfolio/test_portfolio_service.py
=============================================

Suite unitaria de PortfolioService — aislada del Composition Root.

Principios de test
------------------
Aislamiento total  : PortfolioService se instancia directo con
                      InMemoryPositionStore (o un store falso que
                      simula fallos) -- sin CompositionRoot, sin Redis.
Fail-Fast           : cada test verifica una sola responsabilidad.
DRY                 : fixtures centralizan construcción de servicio/store.
Nomenclatura        : test_<método>_<condición>_<resultado_esperado>

Cobertura
---------
__init__:
  - capital_usd <= 0 -> ValueError (Fail-Fast)

open_position / close_position (comportamiento normal):
  - open + close roundtrip retorna la posición con los campos correctos
  - close de order_id inexistente retorna None, no lanza

open_position / close_position (SafeOps):
  - side inválido (vocabulario OMS "buy" en vez de "long") no lanza,
    la posición no queda persistida
  - store.save() que lanza no propaga
  - store.get() que lanza no propaga, close_position retorna None

snapshot / state / open_count / total_exposure:
  - reflejan el estado real del store
  - store.all() que lanza no propaga -- snapshot cae a estado vacío,
    open_count/total_exposure caen a 0/0.0
"""

from __future__ import annotations

from typing import Optional

import pytest
from portfolio.infra.memory_store import InMemoryPositionStore
from portfolio.models.position import PositionSnapshot
from portfolio.services.portfolio_service import PortfolioService

# ── Constantes de test ───────────────────────────────────────────────────────
_CAPITAL_USD = 10_000.0
_EXCHANGE = "bybit"
_SYMBOL = "BTC/USDT"
_ENTRY_PRICE = 50_000.0
_SIZE_PCT = 0.10
_ORDER_ID = "order-1"
_QUANTITY = 1.0


# ── Fakes ────────────────────────────────────────────────────────────────────


class _RaisingStore:
    """PositionStore falso -- cada método lanza. Simula backend caído."""

    def save(self, position: PositionSnapshot) -> None:
        raise RuntimeError("store caído: save")

    def get(self, order_id: str) -> Optional[PositionSnapshot]:
        raise RuntimeError("store caído: get")

    def delete(self, order_id: str) -> None:
        raise RuntimeError("store caído: delete")

    def all(self) -> list[PositionSnapshot]:
        raise RuntimeError("store caído: all")

    def clear(self) -> None:
        raise RuntimeError("store caído: clear")


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def store() -> InMemoryPositionStore:
    return InMemoryPositionStore()


@pytest.fixture
def service(store: InMemoryPositionStore) -> PortfolioService:
    return PortfolioService(capital_usd=_CAPITAL_USD, store=store, exchange=_EXCHANGE)


@pytest.fixture
def service_con_store_caido() -> PortfolioService:
    return PortfolioService(capital_usd=_CAPITAL_USD, store=_RaisingStore(), exchange=_EXCHANGE)


# ══════════════════════════════════════════════════════════════════════════════
# __init__ — Fail-Fast
# ══════════════════════════════════════════════════════════════════════════════


class TestInitFailFast:
    @pytest.mark.parametrize("capital", [0.0, -1.0, -100.0])
    def test_capital_usd_no_positivo_lanza(self, capital: float, store: InMemoryPositionStore) -> None:
        with pytest.raises(ValueError, match="capital_usd"):
            PortfolioService(capital_usd=capital, store=store)


# ══════════════════════════════════════════════════════════════════════════════
# open_position / close_position — comportamiento normal
# ══════════════════════════════════════════════════════════════════════════════


class TestOpenClosePositionRoundtrip:
    def test_open_luego_close_retorna_posicion_con_campos_correctos(self, service: PortfolioService) -> None:
        service.open_position(
            order_id=_ORDER_ID,
            symbol=_SYMBOL,
            side="long",
            avg_entry=_ENTRY_PRICE,
            size_pct=_SIZE_PCT,
            quantity=_QUANTITY,
        )
        closed, remaining = service.close_position(_ORDER_ID)
        assert closed is not None
        assert closed.symbol == _SYMBOL
        assert closed.side == "long"
        assert closed.exchange == _EXCHANGE  # heredado del servicio, no del caller
        assert closed.avg_entry == _ENTRY_PRICE
        assert closed.quantity == _QUANTITY  # ADR-0025: la posición conoce su cantidad
        assert closed.size_pct == _SIZE_PCT
        assert remaining == 0.0  # cierre completo → resto 0

    def test_close_elimina_la_posicion_del_store(self, service: PortfolioService) -> None:
        service.open_position(
            order_id=_ORDER_ID,
            symbol=_SYMBOL,
            side="long",
            avg_entry=_ENTRY_PRICE,
            size_pct=_SIZE_PCT,
            quantity=_QUANTITY,
        )
        closed, _ = service.close_position(_ORDER_ID)
        assert closed is not None
        closed2, _ = service.close_position(_ORDER_ID)
        assert closed2 is None  # ya no existe

    def test_close_order_id_inexistente_retorna_none(self, service: PortfolioService) -> None:
        closed, remaining = service.close_position("no-existe")
        assert closed is None
        assert remaining == 0.0


# ══════════════════════════════════════════════════════════════════════════════
# open_position / close_position — ADR-0025 (F4a/F4b): WAC y cierres parciales
# ══════════════════════════════════════════════════════════════════════════════


class TestWacAccounting:
    def test_multi_entry_acumula_weighted_average_cost(self, service: PortfolioService) -> None:
        service.open_position(order_id="b1", symbol=_SYMBOL, side="long", avg_entry=100.0, size_pct=0.1, quantity=1.0)
        service.open_position(order_id="b2", symbol=_SYMBOL, side="long", avg_entry=110.0, size_pct=0.1, quantity=2.0)
        positions = service.snapshot().positions
        assert len(positions) == 1, "multi-entry se fusiona en una única posición (WAC)"
        pos = positions[0]
        assert pos.quantity == pytest.approx(3.0)
        assert pos.avg_entry == pytest.approx(320.0 / 3.0)  # 106.667
        assert pos.cost_basis == pytest.approx(320.0)
        assert pos.order_id == "b1", "la clave de la posición queda la pierna de apertura"

    def test_partial_close_reduce_quantity_y_preserva_avg(self, service: PortfolioService) -> None:
        service.open_position(order_id="b1", symbol=_SYMBOL, side="long", avg_entry=100.0, size_pct=0.1, quantity=1.0)
        closed, remaining = service.close_position("b1", quantity=0.4)
        assert closed is not None
        assert closed.quantity == pytest.approx(0.4)  # realized P&L = closed_qty × (exit − avg)
        assert closed.avg_entry == pytest.approx(100.0)
        positions = service.snapshot().positions
        assert len(positions) == 1, "cierre parcial mantiene la posición abierta"
        assert positions[0].quantity == pytest.approx(0.6)
        assert positions[0].avg_entry == pytest.approx(100.0)
        assert remaining == pytest.approx(0.6)

    def test_multi_entry_partial_close_deja_basis_coherente(self, service: PortfolioService) -> None:
        service.open_position(order_id="b1", symbol=_SYMBOL, side="long", avg_entry=100.0, size_pct=0.1, quantity=1.0)
        service.open_position(order_id="b2", symbol=_SYMBOL, side="long", avg_entry=110.0, size_pct=0.1, quantity=2.0)
        closed, remaining = service.close_position("b1", quantity=1.0)
        assert closed is not None
        # WAC = 106.667; realized = 1 × (exit − 106.667) → datos de la porción cerrada
        assert closed.avg_entry == pytest.approx(106.6666667)
        positions = service.snapshot().positions
        assert positions[0].quantity == pytest.approx(2.0)
        assert positions[0].avg_entry == pytest.approx(106.6666667)
        assert positions[0].cost_basis == pytest.approx(2.0 * (320.0 / 3.0))  # 213.333
        assert remaining == pytest.approx(2.0)

    def test_close_completo_con_qty_mayor_a_posicion_no_sobrepasa(self, service: PortfolioService) -> None:
        service.open_position(order_id="b1", symbol=_SYMBOL, side="long", avg_entry=100.0, size_pct=0.1, quantity=0.5)
        closed, remaining = service.close_position("b1", quantity=2.0)  # oversell
        assert closed is not None
        assert closed.quantity == pytest.approx(0.5)  # nunca cierra más de lo abierto
        assert remaining == 0.0
        assert service.open_count == 0


# ══════════════════════════════════════════════════════════════════════════════
# open_position / close_position — SafeOps
# ══════════════════════════════════════════════════════════════════════════════


class TestSafeOps:
    def test_open_position_con_side_invalido_no_lanza_ni_persiste(self, service: PortfolioService) -> None:
        # "buy" es vocabulario OMS, no el vocabulario de dominio ("long"/"short")
        # -- PositionSnapshot.__post_init__ rechaza el valor; open_position debe
        # absorber la excepción (SafeOps), no propagarla.
        service.open_position(
            order_id=_ORDER_ID,
            symbol=_SYMBOL,
            side="buy",
            avg_entry=_ENTRY_PRICE,
            size_pct=_SIZE_PCT,
            quantity=_QUANTITY,
        )
        assert service.open_count == 0  # la construcción falló antes de store.save()

    def test_open_position_sin_quantity_no_persiste(self, service: PortfolioService) -> None:
        # INV-01: sin cantidad ejecutada real no hay posición (no se inventa qty).
        service.open_position(
            order_id=_ORDER_ID,
            symbol=_SYMBOL,
            side="long",
            avg_entry=_ENTRY_PRICE,
            size_pct=_SIZE_PCT,
            quantity=None,  # type: ignore[arg-type]
        )
        assert service.open_count == 0

    def test_open_position_con_store_caido_no_lanza(self, service_con_store_caido: PortfolioService) -> None:
        service_con_store_caido.open_position(
            order_id=_ORDER_ID,
            symbol=_SYMBOL,
            side="long",
            avg_entry=_ENTRY_PRICE,
            size_pct=_SIZE_PCT,
            quantity=_QUANTITY,
        )  # no debe lanzar

    def test_close_position_con_store_caido_retorna_none_no_lanza(
        self, service_con_store_caido: PortfolioService
    ) -> None:
        closed, _ = service_con_store_caido.close_position(_ORDER_ID)
        assert closed is None


# ══════════════════════════════════════════════════════════════════════════════
# snapshot / state / open_count / total_exposure
# ══════════════════════════════════════════════════════════════════════════════


class TestConsulta:
    def test_snapshot_refleja_posiciones_y_capital(self, service: PortfolioService) -> None:
        service.open_position(
            order_id=_ORDER_ID,
            symbol=_SYMBOL,
            side="long",
            avg_entry=_ENTRY_PRICE,
            size_pct=_SIZE_PCT,
            quantity=_QUANTITY,
        )
        snap = service.snapshot()
        assert snap.open_count == 1
        assert snap.capital_usd == _CAPITAL_USD
        assert snap.total_exposure == pytest.approx(_SIZE_PCT)

    def test_open_count_y_total_exposure_reflejan_store(self, service: PortfolioService) -> None:
        service.open_position(
            order_id="o1", symbol=_SYMBOL, side="long", avg_entry=_ENTRY_PRICE, size_pct=0.10, quantity=1.0
        )
        service.open_position(
            order_id="o2", symbol="ETH/USDT", side="short", avg_entry=3_000.0, size_pct=0.05, quantity=2.0
        )
        assert service.open_count == 2
        assert service.total_exposure == pytest.approx(0.15)

    def test_state_retorna_dict_con_campos_esperados(self, service: PortfolioService) -> None:
        result = service.state()
        assert result["capital_usd"] == _CAPITAL_USD
        assert result["open_positions"] == 0
        assert result["is_flat"] is True

    def test_snapshot_con_store_caido_retorna_estado_vacio(self, service_con_store_caido: PortfolioService) -> None:
        snap = service_con_store_caido.snapshot()
        assert snap.open_count == 0
        assert snap.capital_usd == _CAPITAL_USD

    def test_open_count_con_store_caido_retorna_cero(self, service_con_store_caido: PortfolioService) -> None:
        assert service_con_store_caido.open_count == 0

    def test_total_exposure_con_store_caido_retorna_cero(self, service_con_store_caido: PortfolioService) -> None:
        assert service_con_store_caido.total_exposure == 0.0
