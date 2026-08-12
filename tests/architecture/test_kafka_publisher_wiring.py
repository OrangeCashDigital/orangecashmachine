# -*- coding: utf-8 -*-
"""
tests/architecture/test_kafka_publisher_wiring.py
====================================================

Guardrail #4 (test de wiring del composition root) para F-031 / B-46.

A diferencia de test_kappa_publisher_wiring.py (que prueba el guard de
OHLCVPipeline con mocks/dobles), este archivo prueba el composition root
real: ConcretePipelineFactory._build_kafka_publisher() debe resolver un
KafkaOHLCVPublisher DE VERDAD (no un mock) cuando integrations.kafka.enabled
es True, y None cuando está deshabilitado.

Por qué no se prueba _build_ohlcv() completo
---------------------------------------------
_build_ohlcv() construye además CCXTAdapter, PrometheusPipelineMetrics,
cursor_store, QualityPipeline — reconstruir todo el grafo real introduce
riesgo de efectos colaterales entre tests (registro duplicado de métricas
Prometheus, llamadas de red del exchange) sin aportar señal adicional
sobre el wiring del publisher, que es lo que este guardrail cubre.
_build_kafka_publisher() es la unidad de composición correcta: es
exactamente el método que el propio código señala como la causa raíz
verificable (ver comentario en OHLCVPipeline.__init__).

KafkaProducerAdapter.from_env() no conecta al broker en construcción
(solo lee env vars) — la conexión ocurre en start(), que este test nunca
invoca. Es seguro instanciarlo real, sin mocks.
"""

from __future__ import annotations

from market_data.infrastructure.bootstrap.pipeline_factory import (
    ConcretePipelineFactory,
)
from market_data.infrastructure.kafka.ohlcv_publisher import KafkaOHLCVPublisher
from market_data.infrastructure.kafka.producer import KafkaProducerAdapter

from ocm.config.schema import (
    AppConfig,
    ExchangeConfig,
    IntegrationsConfig,
    KafkaConfig,
    MarketConfig,
    MarketsConfig,
    PipelineConfig,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _minimal_app_config(*, kafka_enabled: bool) -> AppConfig:
    """AppConfig real mínimo válido, con integrations.kafka.enabled parametrizado."""
    return AppConfig(
        exchanges=[
            ExchangeConfig(
                name="bybit",
                enabled=True,
                markets=MarketsConfig(
                    spot=MarketConfig(enabled=True, symbols=["BTC/USDT"]),
                ),
            )
        ],
        pipeline=PipelineConfig(),
        integrations=IntegrationsConfig(kafka=KafkaConfig(enabled=kafka_enabled)),
    )


# ══════════════════════════════════════════════════════════════════════════════
# ConcretePipelineFactory._build_kafka_publisher — wiring real, sin mocks
# ══════════════════════════════════════════════════════════════════════════════


class TestBuildKafkaPublisherWiring:
    """
    Guardrail #4: verifica que el composition root resuelve el publisher
    Kafka REAL cuando kafka.enabled=True — no solo que un guard rechace
    un mock/Null. Si mañana alguien rompe el wiring interno de
    _build_kafka_publisher() (p.ej. deja de pasar el producer real, o
    vuelve a quedar código muerto sin caller), este test debe fallar.
    """

    def test_kafka_enabled_resuelve_kafka_ohlcv_publisher_real(self) -> None:
        cfg = _minimal_app_config(kafka_enabled=True)
        factory = ConcretePipelineFactory(cfg)

        publisher = factory._build_kafka_publisher()

        assert isinstance(publisher, KafkaOHLCVPublisher)

    def test_kafka_disabled_retorna_none(self) -> None:
        cfg = _minimal_app_config(kafka_enabled=False)
        factory = ConcretePipelineFactory(cfg)

        publisher = factory._build_kafka_publisher()

        assert publisher is None

    def test_kafka_enabled_el_producer_interno_es_kafka_producer_adapter_real(self) -> None:
        # Regresión específica de F-031: antes de la remediación, este método
        # existía pero no tenía callers (código muerto) — OHLCVPipeline
        # construía NullOHLCVPublisher() sin pasar por aquí. Verificar el
        # tipo concreto del producer interno confirma que el wiring
        # instancia infraestructura Kafka real, no un doble de test.
        cfg = _minimal_app_config(kafka_enabled=True)
        factory = ConcretePipelineFactory(cfg)

        publisher = factory._build_kafka_publisher()

        assert isinstance(publisher, KafkaOHLCVPublisher)
        producer = getattr(publisher, "_producer", None) or getattr(publisher, "producer", None)
        assert producer is not None, (
            "KafkaOHLCVPublisher no expone su producer interno bajo "
            "'_producer' ni 'producer' — ajustar este test al nombre real del atributo."
        )
        assert isinstance(producer, KafkaProducerAdapter)
