# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/
============================

Driven (secondary) ports — interfaces que el dominio y la aplicación
usan para llamar a servicios externos.

Importar siempre desde el submódulo específico:
  from market_data.ports.outbound.kafka_producer import KafkaProducerPort
  from market_data.ports.outbound.storage import OHLCVStorage

Este __init__.py NO re-exporta para evitar imports circulares.

Puertos disponibles
-------------------
kafka_producer  — KafkaProducerPort + constantes TOPIC_*
kafka_consumer  — KafkaConsumerPort + KafkaMessage
storage         — OHLCVStorage (Bronze / Silver / Gold)
exchange        — ExchangePort (CCXT adapter contract)
observability   — MetricsPort · LineagePort · GitHashPort
quality         — AnomalyRegistryPort
event_bus       — EventBusPort (DomainEvent bus in-process)
"""
