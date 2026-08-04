# -*- coding: utf-8 -*-
"""
tests/app/test_cli_entrypoint.py
==================================

Tests herméticos de ``app.cli.entrypoint.run`` — el runner por defecto del
CLI Hydra (``uv run ocm``).

Cubre el fix de Fase A (auditoría 2026-08-03): ``PipelineOrchestrator`` debe
recibir su factory desde ``CompositionRoot.assemble()`` y nunca construirse
sin factory. Antes del fix, ``run()`` lanzaba ``PipelineBuildError`` en
runtime (verificado por ejecución).

Sin red ni infraestructura: ``CompositionRoot.assemble`` se monkeypatchea y
el pipeline real se reemplaza por un fake.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ocm.config.schema import (
    AppConfig,
    ExchangeConfig,
    MarketConfig,
    MarketsConfig,
    PipelineConfig,
    SupportedExchange,
)
from ocm.runtime.context import RuntimeContext
from ocm.runtime.run_config import RunConfig


@pytest.fixture(autouse=True)
def _force_test_env(monkeypatch) -> None:
    """Fuerza entorno no-producción — ExchangeConfig valida credenciales."""
    monkeypatch.setenv("OCM_ENV", "test")


def _build_context(*, with_symbols: bool) -> RuntimeContext:
    app_config = AppConfig(
        exchanges=[
            ExchangeConfig(
                name=SupportedExchange.BYBIT,
                enabled=True,
                markets=MarketsConfig(
                    spot=MarketConfig(
                        enabled=with_symbols,
                        symbols=["BTC/USDT"] if with_symbols else [],
                    )
                ),
            )
        ],
        pipeline=PipelineConfig(),
    )
    return RuntimeContext(
        app_config=app_config,
        run_config=RunConfig(
            env="test",
            debug=False,
            validate_only=False,
            run_id="test-run",
            config_path=None,
            pushgateway="",
        ),
        started_at=datetime.now(timezone.utc),
    )


class _FakePipeline:
    async def run(self, mode):
        return {"mode": mode}


class _FakeFactory:
    def build(self, request):
        return _FakePipeline()


class _StubRoot:
    factory = _FakeFactory()


def _patch_assemble(monkeypatch, calls: list) -> None:
    def _fake_assemble(cls, config):
        calls.append(config)
        return _StubRoot()

    monkeypatch.setattr(
        "market_data.infrastructure.bootstrap.composition_root.CompositionRoot.assemble",
        classmethod(_fake_assemble),
    )


def test_run_returns_1_without_symbols_and_uses_composition_root(monkeypatch) -> None:
    """Sin símbolos no hay corridas → exit 1, pero assemble se usa igualmente."""
    from app.cli.entrypoint import run

    ctx = _build_context(with_symbols=False)
    calls: list = []
    _patch_assemble(monkeypatch, calls)

    assert run(ctx) == 1
    assert calls == [ctx.app_config]


def test_run_returns_0_with_symbols_and_injects_factory(monkeypatch) -> None:
    """Con símbolos, el factory del Composition Root se inyecta al orquestador."""
    from app.cli.entrypoint import run

    ctx = _build_context(with_symbols=True)
    calls: list = []
    _patch_assemble(monkeypatch, calls)

    assert run(ctx) == 0
    assert calls == [ctx.app_config]
