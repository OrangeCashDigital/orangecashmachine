# -*- coding: utf-8 -*-
"""
tests/ocm/observability/test_tracing.py
========================================

B-17/H-18: OpenTelemetry + request-id propagado en consumers.

Cubre el contrato de ``ocm/observability/tracing.py`` v0.2.0:
- ``TracingRuntime``: fail-soft, idempotente, disabled por defecto.
- ``init_tracing``: no-op con config ausente.
- ``trace_event``: span con atributos + request-id propagado a loguru.
- ``trace_consumer_event``: request-id derivado de ``event.event_id``.
- G11 (trazabilidad activa): propagación de contexto end-to-end.

Los tests usan ``InMemorySpanExporter`` — nunca tocan red (fail-safe en CI).

Nota OTel: el provider global solo puede setearse UNA vez por proceso
("Overriding of current TracerProvider is not allowed"). Por eso el provider
in-memory se instala en un fixture de módulo compartido y se limpia entre
tests con ``exporter.clear()``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from loguru import logger as _log
from opentelemetry import trace as _trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from ocm.observability import (
    get_tracing_runtime,
    init_tracing,
    request_id,
    trace_consumer_event,
    trace_event,
)
from ocm.observability.tracing import TracingRuntime


@dataclass(frozen=True)
class _FakeEvent:
    """Evento mínimo con event_id — compatible con DomainEvent (duck-typing)."""

    event_id: str = field(default_factory=lambda: "evt-0001")
    batch: object = field(default_factory=object)


class _FakeOTLPExporter:
    """Exporter OTel con el contrato mínimo (export/shutdown) para tests."""

    def export(self, spans) -> None:  # type: ignore[no-untyped-def]
        return None

    def shutdown(self) -> None:
        return None

    def force_flush(self, timeout_millis: int = 0) -> bool:
        return True


@pytest.fixture(scope="module", autouse=True)
def _inmemory_provider():
    """Instala el TracerProvider global con InMemorySpanExporter (una sola vez)."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    _trace.set_tracer_provider(provider)
    yield exporter
    provider.shutdown()


@pytest.fixture(autouse=True)
def _clear_spans(_inmemory_provider):
    """Limpia los spans exportados entre tests — aislamiento sin re-seteo."""
    _inmemory_provider.clear()
    yield
    _inmemory_provider.clear()


@pytest.fixture(autouse=True)
def _reset_tracing_globals():
    """Limpia el singleton de runtime entre tests para evitar acoplamiento."""
    yield
    from ocm.observability import tracing as _tracing_module

    _tracing_module._runtime = None
    _tracing_module._STARTED = False
    _tracing_module._ACTIVE_TRACER_PROVIDER = None


# ---------------------------------------------------------------------------
# TracingRuntime — fail-soft, idempotente, disabled
# ---------------------------------------------------------------------------


class _FakeTracingCfg:
    enabled: bool = False
    endpoint: str | None = None
    service_name: str = "orangecashmachine"
    sample_ratio: float = 1.0


def test_runtime_disabled_by_default() -> None:
    runtime = TracingRuntime.from_config(_FakeTracingCfg())
    assert runtime.enabled is False
    assert runtime.start() is False
    assert runtime.started is False


def test_runtime_start_validate_only_skips() -> None:
    cfg = _FakeTracingCfg()
    cfg.enabled = True
    runtime = TracingRuntime.from_config(cfg)
    assert runtime.start(validate_only=True) is False
    assert runtime.started is False


def test_runtime_start_with_endpoint_is_idempotent(monkeypatch) -> None:
    cfg = _FakeTracingCfg()
    cfg.enabled = True
    cfg.endpoint = "http://localhost:4318"

    monkeypatch.setattr(
        "ocm.observability.tracing.OTLPSpanExporter",
        lambda *a, **kw: _FakeOTLPExporter(),
    )

    runtime = TracingRuntime.from_config(cfg)
    assert runtime.start() is True
    assert runtime.start() is True  # idempotente: segundo start no falla
    assert runtime.started is True


def test_runtime_start_fail_soft_on_exporter_error(monkeypatch) -> None:
    """Fail-soft: un exporter que falla no propaga excepción — retorna False."""
    cfg = _FakeTracingCfg()
    cfg.enabled = True
    cfg.endpoint = "http://localhost:4318"

    def _boom(*args, **kwargs) -> None:
        raise RuntimeError("exporter init failed")

    monkeypatch.setattr("ocm.observability.tracing.OTLPSpanExporter", _boom)

    runtime = TracingRuntime.from_config(cfg)
    assert runtime.start() is False
    assert runtime.started is False


def test_init_tracing_noop_without_observability_config() -> None:
    class _FakeCfgNoObs:
        observability = None

    runtime = init_tracing(_FakeCfgNoObs())
    assert isinstance(runtime, TracingRuntime)
    assert runtime.enabled is False


def test_init_tracing_with_disabled_config() -> None:
    class _FakeObs:
        tracing = _FakeTracingCfg()

    class _FakeCfg:
        observability = _FakeObs()

    runtime = init_tracing(_FakeCfg())
    assert isinstance(runtime, TracingRuntime)
    assert runtime.enabled is False


# ---------------------------------------------------------------------------
# trace_event — span + request-id en logs
# ---------------------------------------------------------------------------


def test_trace_event_sets_span_attrs_and_request_id(_inmemory_provider) -> None:
    with trace_event(
        "pipeline.process",
        request_id="req-123",
        exchange="binance",
        symbol="BTC/USDT",
    ):
        assert request_id() == "req-123"

    assert request_id() == "-"  # fuera del contexto → default

    spans = _inmemory_provider.get_finished_spans()
    assert len(spans) == 1
    span = spans[0]
    assert span.name == "pipeline.process"
    attrs = dict(span.attributes)
    assert attrs["request.id"] == "req-123"
    assert attrs["exchange"] == "binance"
    assert attrs["symbol"] == "BTC/USDT"


def test_trace_event_propagates_request_id_to_loguru() -> None:
    captured: list[str] = []
    sink_id = _log.add(lambda m: captured.append(m.record["extra"].get("request_id", "")))
    try:
        with trace_event("span", request_id="req-logs-1"):
            _log.bind(component="test").info("inside")
        _log.bind(component="test").info("outside")
    finally:
        _log.remove(sink_id)

    assert captured == ["req-logs-1", ""]  # dentro sí, fuera no


def test_trace_event_without_request_id_does_not_set_attr(_inmemory_provider) -> None:
    with trace_event("span.no.request"):
        pass
    spans = _inmemory_provider.get_finished_spans()
    assert "request.id" not in spans[0].attributes


# ---------------------------------------------------------------------------
# trace_consumer_event — request-id derivado de event.event_id (G11)
# ---------------------------------------------------------------------------


def test_trace_consumer_event_derives_request_id_from_event(_inmemory_provider) -> None:
    with trace_consumer_event("QualityPipelineConsumer.handle", _FakeEvent(event_id="evt-abc")):
        assert request_id() == "evt-abc"

    spans = _inmemory_provider.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "QualityPipelineConsumer.handle"
    assert spans[0].attributes["request.id"] == "evt-abc"


def test_trace_consumer_event_event_without_id_still_works() -> None:
    class _NoId:
        pass

    with trace_consumer_event("Consumer.handle", _NoId()):
        assert request_id() == "-"


# ---------------------------------------------------------------------------
# G11 — trazabilidad activa: propagación de contexto end-to-end
# ---------------------------------------------------------------------------

# Re-export para el test G11 (protección contra renames accidentales).
G11_SPAN_ATTR = "request.id"


def test_g11_context_propagation_end_to_end(_inmemory_provider) -> None:
    """G11: el request-id del evento se propaga a loguru y al span OTel.

    Usa la infraestructura pública (BaseConsumer._traced_handle) para
    verificar que el event_id llega como request-id al contexto de logs
    y como atributo ``request.id`` al span exportado.
    """
    captured: list[str] = []
    sink_id = _log.add(lambda m: captured.append(m.record["extra"].get("request_id", "")))

    try:
        from market_data.application.consumers.base import BaseConsumer

        # Fuerza el path real: start() registra _traced_handle en el bus.
        class _DummyConsumer(BaseConsumer):
            event_type = _FakeEvent

            def handle(self, event) -> None:  # type: ignore[override]
                _log.bind(component="consumer").info("handled")
                assert request_id() == event.event_id

        # Protocolo EventBusPort mínimo
        class _Bus:
            def __init__(self) -> None:
                self.handlers: dict = {}

            def subscribe(self, etype, handler) -> None:
                self.handlers[etype] = handler

            def unsubscribe(self, etype, handler) -> None:  # pragma: no cover
                self.handlers.pop(etype, None)

        bus = _Bus()
        consumer = _DummyConsumer(bus)  # type: ignore[arg-type]  # duck-typing del port
        consumer.start()

        event = _FakeEvent(event_id="g11-evt-001")
        bus.handlers[_FakeEvent](event)
    finally:
        _log.remove(sink_id)

    spans = _inmemory_provider.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "_DummyConsumer.handle"
    assert spans[0].attributes[G11_SPAN_ATTR] == "g11-evt-001"
    assert captured == ["g11-evt-001"]  # log dentro del span lleva el request-id


def test_g11_span_carries_correlation_attributes(_inmemory_provider) -> None:
    """G11: los atributos de correlación del evento se fijan en el span."""

    @dataclass(frozen=True)
    class _Batch:
        exchange: str = "binance"
        symbol: str = "BTC/USDT"
        timeframe: str = "1m"

    @dataclass(frozen=True)
    class _RichEvent:
        event_id: str = "rich-001"
        batch: _Batch = field(default_factory=_Batch)

    with trace_consumer_event("RichConsumer.handle", _RichEvent()):
        pass

    attrs = dict(_inmemory_provider.get_finished_spans()[0].attributes)
    assert attrs["request.id"] == "rich-001"


def test_get_tracing_runtime_returns_current() -> None:
    class _FakeObs:
        tracing = _FakeTracingCfg()

    class _FakeCfg:
        observability = _FakeObs()

    init_tracing(_FakeCfg())
    assert isinstance(get_tracing_runtime(), TracingRuntime)
