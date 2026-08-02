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
            order_id=_ORDER_ID, symbol=_SYMBOL, side="long", entry_price=_ENTRY_PRICE, size_pct=_SIZE_PCT
        )
        position = service.close_position(_ORDER_ID)
        assert position is not None
        assert position.symbol == _SYMBOL
        assert position.side == "long"
        assert position.exchange == _EXCHANGE  # heredado del servicio, no del caller
        assert position.entry_price == _ENTRY_PRICE
        assert position.size_pct == _SIZE_PCT

    def test_close_elimina_la_posicion_del_store(self, service: PortfolioService) -> None:
        service.open_position(
            order_id=_ORDER_ID, symbol=_SYMBOL, side="long", entry_price=_ENTRY_PRICE, size_pct=_SIZE_PCT
        )
        service.close_position(_ORDER_ID)
        assert service.close_position(_ORDER_ID) is None  # ya no existe

    def test_close_order_id_inexistente_retorna_none(self, service: PortfolioService) -> None:
        assert service.close_position("no-existe") is None


# ══════════════════════════════════════════════════════════════════════════════
# open_position / close_position — SafeOps
# ══════════════════════════════════════════════════════════════════════════════


class TestSafeOps:
    def test_open_position_con_side_invalido_no_lanza_ni_persiste(self, service: PortfolioService) -> None:
        # "buy" es vocabulario OMS, no el vocabulario de dominio ("long"/"short")
        # -- PositionSnapshot.__post_init__ rechaza el valor; open_position debe
        # absorber la excepción (SafeOps), no propagarla.
        service.open_position(
            order_id=_ORDER_ID, symbol=_SYMBOL, side="buy", entry_price=_ENTRY_PRICE, size_pct=_SIZE_PCT
        )
        assert service.open_count == 0  # la construcción falló antes de store.save()

    def test_open_position_con_store_caido_no_lanza(self, service_con_store_caido: PortfolioService) -> None:
        service_con_store_caido.open_position(
            order_id=_ORDER_ID, symbol=_SYMBOL, side="long", entry_price=_ENTRY_PRICE, size_pct=_SIZE_PCT
        )  # no debe lanzar

    def test_close_position_con_store_caido_retorna_none_no_lanza(
        self, service_con_store_caido: PortfolioService
    ) -> None:
        assert service_con_store_caido.close_position(_ORDER_ID) is None


# ══════════════════════════════════════════════════════════════════════════════
# snapshot / state / open_count / total_exposure
# ══════════════════════════════════════════════════════════════════════════════


class TestConsulta:
    def test_snapshot_refleja_posiciones_y_capital(self, service: PortfolioService) -> None:
        service.open_position(
            order_id=_ORDER_ID, symbol=_SYMBOL, side="long", entry_price=_ENTRY_PRICE, size_pct=_SIZE_PCT
        )
        snap = service.snapshot()
        assert snap.open_count == 1
        assert snap.capital_usd == _CAPITAL_USD
        assert snap.total_exposure == pytest.approx(_SIZE_PCT)

    def test_open_count_y_total_exposure_reflejan_store(self, service: PortfolioService) -> None:
        service.open_position(order_id="o1", symbol=_SYMBOL, side="long", entry_price=_ENTRY_PRICE, size_pct=0.10)
        service.open_position(order_id="o2", symbol="ETH/USDT", side="short", entry_price=3_000.0, size_pct=0.05)
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
