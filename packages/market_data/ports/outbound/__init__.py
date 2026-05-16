# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/
============================

Driven (secondary) ports — interfaces que el dominio y la aplicación
usan para llamar a servicios externos.

Importar siempre desde el submódulo específico, nunca desde aquí:

    from market_data.ports.outbound.kafka_producer import KafkaProducerPort
    from market_data.ports.outbound.storage        import OHLCVStorage
    from market_data.ports.outbound.lineage        import LineagePort
    from market_data.ports.outbound.metrics        import MetricsPort, NullMetrics

Este __init__.py está intencionalmente vacío de re-exports para evitar
imports circulares y cascadas de inicialización.

Principios: KISS · fail-fast en imports · no side-effects al cargar el paquete.
"""
