# -*- coding: utf-8 -*-
"""
tests/portfolio/test_composition_root.py
==========================================

Suite de humo para CompositionRoot (bootstrap del bounded context portfolio).

Principios de test
------------------
Aislamiento total : AppConfig se simula con SimpleNamespace — sin Hydra,
                     sin pipeline L1-L5. Redis se simula con MagicMock —
                     sin conexión de red real.
Fail-Fast          : cada test verifica una sola responsabilidad.
DRY                : fixtures centralizan la construcción de config falsa.
Nomenclatura       : test_<método>_<condición>_<resultado_esperado>

Cobertura
---------
assemble (Redis deshabilitado):
  - usa InMemoryPositionStore
  - portfolio_service queda con capital_usd/exchange correctos
  - capital_usd_override tiene prioridad sobre config.portfolio.capital_usd
  - redis_client queda en None (nada que cerrar)

assemble (Redis habilitado):
  - usa RedisPositionStore
  - redis_client queda referenciado en el CompositionRoot (no solo en el store)

close:
  - no-op si redis_client es None (InMemoryPositionStore)
  - invoca exactamente una vez redis_client.close() cuando existe
  - no propaga excepción si redis_client.close() lanza (fail-soft)

assemble — Fail-Fast:
  - config=None lanza ValueError
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from portfolio.bootstrap.composition_root import CompositionRoot

# ── Constantes de test ───────────────────────────────────────────────────────
_CAPITAL_USD = 5_000.0
_EXCHANGE = "bybit"
_DRIFT = 0.05
_MIN_DELTA = 0.01
_TTL_DAYS = 7


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _make_config(*, redis_enabled: bool) -> SimpleNamespace:
    """AppConfig mínimo — solo los atributos que CompositionRoot lee."""
    return SimpleNamespace(
        integrations=SimpleNamespace(
            redis=SimpleNamespace(
                enabled=redis_enabled,
                host="localhost",
                port=6379,
                db=0,
                password=None,
                socket_timeout=5,
            )
        ),
        portfolio=SimpleNamespace(
            capital_usd=_CAPITAL_USD,
            exchange=_EXCHANGE,
            rebalance_drift_threshold=_DRIFT,
            rebalance_min_delta_pct=_MIN_DELTA,
            position_ttl_days=_TTL_DAYS,
        ),
    )


@pytest.fixture
def config_redis_disabled() -> SimpleNamespace:
    return _make_config(redis_enabled=False)


@pytest.fixture
def config_redis_enabled() -> SimpleNamespace:
    return _make_config(redis_enabled=True)


@pytest.fixture
def fake_redis_client() -> MagicMock:
    """Cliente Redis simulado — nunca toca la red."""
    return MagicMock(name="redis_client")


@pytest.fixture
def root_with_redis(
    config_redis_enabled: SimpleNamespace, fake_redis_client: MagicMock, monkeypatch
) -> CompositionRoot:
    """CompositionRoot ensamblado con Redis 'habilitado' pero mockeado."""
    monkeypatch.setattr(
        "portfolio.infra.redis_factory.build_redis_client",
        lambda **kwargs: fake_redis_client,
    )
    return CompositionRoot.assemble(config_redis_enabled)


# ══════════════════════════════════════════════════════════════════════════════
# assemble — Redis deshabilitado
# ══════════════════════════════════════════════════════════════════════════════


class TestAssembleRedisDeshabilitado:
    def test_usa_inmemory_position_store(self, config_redis_disabled: SimpleNamespace) -> None:
        """Wiring (BC-43): redis.enabled=False -> InMemoryPositionStore.

        Se accede a ``_store`` deliberadamente: este test valida la decision
        de wiring del Composition Root, no el comportamiento publico de
        PortfolioService (que mantiene el store privado por diseno).
        """
        from portfolio.infra.memory_store import InMemoryPositionStore

        root = CompositionRoot.assemble(config_redis_disabled)
        assert isinstance(root.portfolio_service._store, InMemoryPositionStore)

    def test_redis_client_queda_en_none(self, config_redis_disabled: SimpleNamespace) -> None:
        root = CompositionRoot.assemble(config_redis_disabled)
        assert root.redis_client is None

    def test_portfolio_service_usa_capital_y_exchange_de_config(self, config_redis_disabled: SimpleNamespace) -> None:
        """Capital y exchange se verifican via API publica (state(), open_position()).

        PortfolioService no expone capital_usd/exchange como atributos publicos
        (encapsulamiento intencional, ver docstring de la clase) -- se observan
        a traves de su comportamiento, no de su estado interno.
        """
        root = CompositionRoot.assemble(config_redis_disabled)
        assert root.portfolio_service.state()["capital_usd"] == _CAPITAL_USD

        # side usa el vocabulario de posicion del dominio ("long"/"short"),
        # no el vocabulario de orden OMS ("buy"/"sell") -- ver
        # PositionSnapshot.__post_init__ en portfolio/models/position.py.
        root.portfolio_service.open_position(
            order_id="probe-exchange",
            symbol="BTC/USDT",
            side="long",
            avg_entry=50_000.0,
            size_pct=0.1,
            quantity=1.0,
        )
        position, _ = root.portfolio_service.close_position("probe-exchange")
        assert position is not None
        # exchange no es publico (self._exchange); se verifica ya arriba,
        # de forma indirecta, via el exchange heredado por la posicion
        # abierta -- ese es el comportamiento observable real.
        assert position.exchange == "bybit"

    def test_capital_usd_override_tiene_prioridad(self, config_redis_disabled: SimpleNamespace) -> None:
        root = CompositionRoot.assemble(config_redis_disabled, capital_usd_override=999.0)
        assert root.portfolio_service.state()["capital_usd"] == 999.0

    def test_rebalance_service_construido_con_thresholds_de_config(
        self, config_redis_disabled: SimpleNamespace
    ) -> None:
        """drift_threshold es privado (Fail-Fast validado en __init__).

        Se verifica indirectamente: un delta justo debajo del umbral no
        genera senal; uno por encima si. Esto prueba el contrato de
        comportamiento en vez de un detalle de implementacion.
        """
        from portfolio.models.position import PortfolioState

        root = CompositionRoot.assemble(config_redis_disabled)
        state = PortfolioState(positions=(), capital_usd=_CAPITAL_USD)

        sin_senal = root.rebalance_service.rebalance(state, targets={"BTC/USDT": _DRIFT - 0.001})
        assert sin_senal == []

        con_senal = root.rebalance_service.rebalance(state, targets={"BTC/USDT": _DRIFT + 0.001})
        assert len(con_senal) == 1
        assert con_senal[0].symbol == "BTC/USDT"

        # min_delta_pct no se prueba aqui: RebalanceService valida en
        # __init__ que min_delta_pct < drift_threshold (Fail-Fast), por lo
        # que cualquier delta que supere drift_threshold automaticamente
        # supera min_delta_pct -- el filtro es inalcanzable desde la API
        # publica rebalance() con esta configuracion. Ademas, min_delta_pct
        # no es un atributo publico (encapsulamiento intencional, mismo
        # criterio que drift_threshold). Su comportamiento aislado
        # corresponde a un test unitario de RebalanceService, no a este
        # test de wiring del Composition Root.


# ══════════════════════════════════════════════════════════════════════════════
# assemble — Redis habilitado (mockeado)
# ══════════════════════════════════════════════════════════════════════════════


class TestAssembleRedisHabilitado:
    def test_usa_redis_position_store(self, root_with_redis: CompositionRoot) -> None:
        """Wiring (BC-43): redis.enabled=True -> RedisPositionStore."""
        from portfolio.infra.redis_store import RedisPositionStore

        assert isinstance(root_with_redis.portfolio_service._store, RedisPositionStore)

    def test_redis_client_queda_referenciado_en_composition_root(
        self, root_with_redis: CompositionRoot, fake_redis_client: MagicMock
    ) -> None:
        # Regresión del hallazgo original: antes de este campo, redis_client
        # se perdía dentro del store y CompositionRoot no tenía forma de cerrarlo.
        assert root_with_redis.redis_client is fake_redis_client


# ══════════════════════════════════════════════════════════════════════════════
# assemble — Fail-Fast
# ══════════════════════════════════════════════════════════════════════════════


class TestAssembleFailFast:
    def test_config_none_lanza_value_error(self) -> None:
        with pytest.raises(ValueError):
            CompositionRoot.assemble(None)


# ══════════════════════════════════════════════════════════════════════════════
# close
# ══════════════════════════════════════════════════════════════════════════════


class TestClose:
    def test_close_es_noop_sin_redis_client(self, config_redis_disabled: SimpleNamespace) -> None:
        root = CompositionRoot.assemble(config_redis_disabled)
        root.close()  # no debe lanzar ni requerir nada más

    def test_close_invoca_redis_client_close_una_vez(
        self, root_with_redis: CompositionRoot, fake_redis_client: MagicMock
    ) -> None:
        root_with_redis.close()
        fake_redis_client.close.assert_called_once()

    def test_close_no_propaga_si_redis_client_close_lanza(
        self, root_with_redis: CompositionRoot, fake_redis_client: MagicMock
    ) -> None:
        fake_redis_client.close.side_effect = RuntimeError("conexión ya caída")
        root_with_redis.close()  # SafeOps: no debe propagar
