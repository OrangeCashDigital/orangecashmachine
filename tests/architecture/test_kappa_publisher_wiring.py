# -*- coding: utf-8 -*-
"""
tests/architecture/test_kappa_publisher_wiring.py
===================================================

Guardrail #5 (tests negativos de seguridad) para F-031 / B-46.

Contexto
--------
OHLCVPipeline publicaba silenciosamente a NullOHLCVPublisher() sin que
ConcretePipelineFactory pudiera inyectar un publisher Kafka real —
publish_chunk() retornaba True sin publicar nada, y el cursor avanzaba
como si el dato hubiera llegado al topic (éxito silencioso con datos
perdidos). Ver docs/plans/backlog-priorizado-2026-08-08.md (F-031).

Este archivo era referenciado desde comentarios en pipeline_factory.py
y ohlcv_pipeline.py como si ya existiera y cubriera el caso en CI —
no existía. Este archivo cierra esa referencia muerta.

Principio (Engineering Guardrails #5): un hallazgo de auditoría se
convierte en test, no solo en corrección puntual. Si mañana alguien
borra el guard fail-fast de OHLCVPipeline.__init__, este test debe
fallar inmediatamente.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from market_data.application.pipelines.ohlcv_pipeline import OHLCVPipeline
from market_data.ports.outbound.publisher_port import (
    NullOHLCVPublisher,
    OHLCVPublisherPort,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _minimal_pipeline_kwargs(**overrides: object) -> dict:
    """
    Kwargs mínimos válidos para instanciar OHLCVPipeline en tests.

    Todas las dependencias de infraestructura son MagicMock — OHLCVPipeline
    no valida su tipo estructural en __init__, solo que no sean None
    (fail-fast de presencia, no de conformidad de Protocol). El guard bajo
    prueba (publisher/environment) es independiente de esos mocks.
    """
    kwargs: dict = dict(
        symbols=["BTC/USDT"],
        timeframes=["1h"],
        start_date="2024-01-01",
        exchange_client=MagicMock(name="exchange_client"),
        fetcher=MagicMock(name="fetcher"),
        metrics=MagicMock(name="metrics"),
        quality=MagicMock(name="quality"),
        repair_metrics=MagicMock(name="repair_metrics"),
        chunk_converter=MagicMock(name="chunk_converter"),
        cursor_store=MagicMock(name="cursor_store"),
        publisher=MagicMock(spec=OHLCVPublisherPort, name="publisher"),
        environment="development",
    )
    kwargs.update(overrides)
    return kwargs


# ══════════════════════════════════════════════════════════════════════════════
# F-031 — NullOHLCVPublisher PROHIBIDO en producción
# ══════════════════════════════════════════════════════════════════════════════


class TestNullPublisherForbiddenInProduction:
    """
    Guard central de F-031: OHLCVPipeline debe rechazar NullOHLCVPublisher
    cuando environment == 'production'. Sin este guard, publish_chunk()
    retorna True sin publicar — éxito silencioso con pérdida de datos.
    """

    def test_null_publisher_en_produccion_lanza_runtime_error(self) -> None:
        kwargs = _minimal_pipeline_kwargs(
            publisher=NullOHLCVPublisher(),
            environment="production",
        )
        with pytest.raises(RuntimeError, match="NullOHLCVPublisher"):
            OHLCVPipeline(**kwargs)

    def test_null_publisher_en_produccion_mayusculas_tambien_lanza(self) -> None:
        # environment se normaliza con .strip().lower() en el guard —
        # verificar que "PRODUCTION"/" Production " no bypassean la regla.
        kwargs = _minimal_pipeline_kwargs(
            publisher=NullOHLCVPublisher(),
            environment="  PRODUCTION  ",
        )
        with pytest.raises(RuntimeError, match="NullOHLCVPublisher"):
            OHLCVPipeline(**kwargs)

    @pytest.mark.parametrize("environment", ["development", "paper", "staging", "test"])
    def test_null_publisher_fuera_de_produccion_no_lanza(self, environment: str) -> None:
        # Modo degradado explícito y documentado — permitido fuera de producción.
        kwargs = _minimal_pipeline_kwargs(
            publisher=NullOHLCVPublisher(),
            environment=environment,
        )
        pipeline = OHLCVPipeline(**kwargs)
        assert pipeline is not None

    def test_publisher_real_en_produccion_no_lanza(self) -> None:
        kwargs = _minimal_pipeline_kwargs(
            publisher=MagicMock(spec=OHLCVPublisherPort),
            environment="production",
        )
        pipeline = OHLCVPipeline(**kwargs)
        assert pipeline is not None


# ══════════════════════════════════════════════════════════════════════════════
# publisher / chunk_converter obligatorios (sin default implícito)
# ══════════════════════════════════════════════════════════════════════════════


class TestPublisherAndChunkConverterMandatory:
    """
    Regresión: antes de F-031, OHLCVPipeline construía NullOHLCVPublisher()
    internamente si no se inyectaba publisher, y chunk_converter tenía
    default=None que fallaba tarde (en get_chunk_converter(), no en __init__).
    Ambos deben ser obligatorios y fallar temprano (fail-fast).
    """

    def test_publisher_none_lanza_type_error(self) -> None:
        kwargs = _minimal_pipeline_kwargs(publisher=None)
        with pytest.raises(TypeError, match="publisher.*obligatorio"):
            OHLCVPipeline(**kwargs)

    def test_chunk_converter_none_lanza_type_error(self) -> None:
        kwargs = _minimal_pipeline_kwargs(chunk_converter=None)
        with pytest.raises(TypeError, match="chunk_converter.*obligatorio"):
            OHLCVPipeline(**kwargs)
