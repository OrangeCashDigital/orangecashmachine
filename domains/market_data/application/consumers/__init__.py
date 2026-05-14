# -*- coding: utf-8 -*-
"""
market_data/application/consumers/
=====================================

Consumers del application layer — handlers de domain events.

Cada consumer suscribe a un tipo de DomainEvent en el EventBus
y ejecuta un pipeline de negocio en respuesta.

Exports
-------
BaseConsumer           : ABC con ciclo de vida start/stop (OCP · DIP)
QualityPipelineConsumer: OHLCVBatchReceived → quality check → lineage

Principios: SRP · DIP · OCP · Fail-soft
"""
from market_data.application.consumers.base             import BaseConsumer             # noqa: F401
from market_data.application.consumers.quality_consumer import QualityPipelineConsumer  # noqa: F401

__all__ = [
    "BaseConsumer",
    "QualityPipelineConsumer",
]
