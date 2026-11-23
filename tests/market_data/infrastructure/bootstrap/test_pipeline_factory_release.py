"""Tests de ConcretePipelineFactory.release() — cierre de adaptadores.

Sin infraestructura: los adaptadores son fakes. `release()` debe cerrar
todo lo registrado, ser idempotente y no propagar fallos de cierre.
"""

from __future__ import annotations

from market_data.infrastructure.bootstrap.pipeline_factory import (
    ConcretePipelineFactory,
)


class _FakeAdapter:
    def __init__(self, error: BaseException | None = None) -> None:
        self.close_calls = 0
        self._error = error

    async def close(self) -> None:
        self.close_calls += 1
        if self._error is not None:
            raise self._error


def _factory() -> ConcretePipelineFactory:
    factory = object.__new__(ConcretePipelineFactory)
    factory._owned = []
    return factory


async def test_release_closes_every_owned_adapter() -> None:
    first, second = _FakeAdapter(), _FakeAdapter()
    factory = _factory()
    factory._owned = [first, second]

    await factory.release()

    assert (first.close_calls, second.close_calls) == (1, 1)
    assert factory._owned == []


async def test_release_is_idempotent() -> None:
    adapter = _FakeAdapter()
    factory = _factory()
    factory._owned = [adapter]

    await factory.release()
    await factory.release()

    assert adapter.close_calls == 1


async def test_release_isolates_a_failing_adapter() -> None:
    broken = _FakeAdapter(error=RuntimeError("session already closed"))
    healthy = _FakeAdapter()
    factory = _factory()
    factory._owned = [broken, healthy]

    await factory.release()

    assert (broken.close_calls, healthy.close_calls) == (1, 1)
    assert factory._owned == []


def test_own_registers_the_adapter() -> None:
    adapter = _FakeAdapter()
    factory = _factory()

    assert factory._own(adapter) is adapter
    assert factory._owned == [adapter]


async def test_kafka_publisher_registers_and_closes_producer(monkeypatch) -> None:
    """El productor creado por el publisher queda bajo ownership de la factory."""
    from importlib import import_module
    from types import SimpleNamespace

    producer_module = import_module("market_data.infrastructure.kafka.producer")
    publisher_module = import_module("market_data.infrastructure.kafka.ohlcv_publisher")

    class FakeProducer:
        def __init__(self) -> None:
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    fake_producer = FakeProducer()
    fake_publisher = object()

    factory = _factory()
    factory._cfg = SimpleNamespace(integrations=SimpleNamespace(kafka=SimpleNamespace(enabled=True)))

    monkeypatch.setattr(
        producer_module.KafkaProducerAdapter,
        "from_env",
        classmethod(lambda cls: fake_producer),
    )
    monkeypatch.setattr(
        publisher_module,
        "KafkaOHLCVPublisher",
        lambda *, producer: fake_publisher if producer is fake_producer else None,
    )

    publisher = factory._build_kafka_publisher()

    assert publisher is fake_publisher
    assert factory._owned == [fake_producer]

    await factory.release()

    assert fake_producer.close_calls == 1
    assert factory._owned == []
