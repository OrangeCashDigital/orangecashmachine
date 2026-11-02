"""Tests del contrato de liberación de PipelineOrchestrator.

Sin infraestructura: factory y pipeline son fakes. Cubre que `release()`
se invoque en éxito, en fallo de ejecución y en fallo de construcción —
este último es el que dejaba el CCXTAdapter ya instanciado sin cerrar.
"""

from __future__ import annotations

from typing import Any

import pytest
from market_data.application.use_cases.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineRequest,
)
from market_data.domain.exceptions import PipelineBuildError, PipelineExecutionError


class _FakePipeline:
    def __init__(self, error: BaseException | None = None) -> None:
        self._error = error

    async def run(self, mode: str) -> str:
        if self._error is not None:
            raise self._error
        return "summary"


class _FakeFactory:
    def __init__(
        self,
        pipeline: _FakePipeline | None = None,
        build_error: BaseException | None = None,
    ) -> None:
        self._pipeline = pipeline or _FakePipeline()
        self._build_error = build_error
        self.release_calls = 0

    def build(self, request: Any) -> _FakePipeline:
        if self._build_error is not None:
            raise self._build_error
        return self._pipeline

    async def release(self) -> None:
        self.release_calls += 1


def _request() -> PipelineRequest:
    return PipelineRequest(
        exchange="bybit",
        market_type="spot",
        pipeline="ohlcv",
        symbols=["BTC/USDT"],
    )


async def test_release_called_after_successful_run() -> None:
    factory = _FakeFactory()

    result = await PipelineOrchestrator(factory=factory).run(_request())

    assert result == "summary"
    assert factory.release_calls == 1


async def test_release_called_when_pipeline_raises() -> None:
    factory = _FakeFactory(pipeline=_FakePipeline(RuntimeError("boom")))

    with pytest.raises(PipelineExecutionError):
        await PipelineOrchestrator(factory=factory).run(_request())

    assert factory.release_calls == 1


async def test_release_called_when_build_fails() -> None:
    factory = _FakeFactory(build_error=PipelineBuildError("sin pipeline"))

    with pytest.raises(PipelineBuildError):
        await PipelineOrchestrator(factory=factory).run(_request())

    assert factory.release_calls == 1


async def test_missing_factory_does_not_attempt_release() -> None:
    with pytest.raises(PipelineBuildError):
        await PipelineOrchestrator().run(_request())
