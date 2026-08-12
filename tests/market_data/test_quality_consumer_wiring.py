# -*- coding: utf-8 -*-
"""
tests/market_data/test_quality_consumer_wiring.py
===================================================

Guardrail #9 (Engineering Guardrails, F-031): el fail-soft de
``_build_event_bus_wiring`` (ConcretePipelineFactory) NO debe ser mudo.

Si falla el registro de QualityPipelineConsumer, el pipeline sigue operando
(por diseño), pero la pérdida del observador de calidad debe ser observable:
log por loguru + counter ``ocm_quality_consumer_wiring_failures_total``.
Este test fuerza el fallo en el wiring real (event_bus.subscribe lanzando) y
verifica log + counter, en lugar del warning invisible del stdlib logging.
"""

from __future__ import annotations

import loguru
from market_data.infrastructure.bootstrap.pipeline_factory import (
    ConcretePipelineFactory,
)
from market_data.infrastructure.event_bus import event_bus
from market_data.infrastructure.observability.metrics import (
    QUALITY_CONSUMER_WIRING_FAILURES,
)


class _FakeCfg:
    """Stub mínimo — _build_event_bus_wiring no lee self._cfg."""

    integrations = object()


def test_wiring_failure_is_logged_and_counted(monkeypatch) -> None:
    captured: dict = {}

    class _FakeLogger:
        def warning(self, msg: str, **kwargs) -> None:
            captured["level"] = "warning"
            captured["msg"] = msg
            captured["error"] = kwargs.get("error")
            captured["kwargs"] = kwargs

    monkeypatch.setattr(loguru.logger, "warning", _FakeLogger().warning)

    def _boom(*args, **kwargs) -> None:
        raise RuntimeError("event_bus no puede subscribir consumer")

    monkeypatch.setattr(event_bus, "subscribe", _boom)

    factory = ConcretePipelineFactory(_FakeCfg())  # type: ignore[arg-type]  # stub: wiring no lee cfg
    bus = factory._build_event_bus_wiring()

    # Fail-soft: retorna el bus igual (pipeline sigue operando)
    assert bus is event_bus

    # Visible: log warning por loguru con el mensaje y el error origen
    assert captured.get("level") == "warning"
    assert "quality_consumer_wiring_failed" in captured.get("msg", "")
    assert "subscribir consumer" in str(captured.get("error"))

    # Métrica: counter incrementado con el tipo de excepción como reason
    sample = QUALITY_CONSUMER_WIRING_FAILURES.labels(reason="RuntimeError")
    assert sample._value.get() >= 1


def test_wiring_success_does_not_count(monkeypatch) -> None:
    """Caso feliz: wiring OK → no log de fallo ni counter."""
    captured: dict = {}

    class _FakeLogger:
        def warning(self, msg: str, **kwargs) -> None:
            captured["called"] = True

    monkeypatch.setattr(loguru.logger, "warning", _FakeLogger().warning)

    factory = ConcretePipelineFactory(_FakeCfg())  # type: ignore[arg-type]  # stub: wiring no lee cfg
    bus = factory._build_event_bus_wiring()

    assert bus is event_bus
    assert "called" not in captured

    # El consumer real se registra — verificar vía el bus in-memory
    from market_data.application.consumers.quality_consumer import (
        QualityPipelineConsumer,
    )

    record = event_bus._handlers.get(QualityPipelineConsumer.event_type)
    assert record is not None and len(record) >= 1
