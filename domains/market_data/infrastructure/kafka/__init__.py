# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/
==================================

Implementaciones concretas Kafka para OCM.

Módulos
-------
payloads   — EventPayload + OHLCVBar (wire format SSOT)
metrics    — KafkaMetrics + timer (observabilidad Prometheus)
dedup      — SeenFilter + CompositeSeenFilter (idempotencia)
serializer — serialize / deserialize / make_routing_key
producer   — KafkaProducerAdapter (implementa KafkaProducerPort)
consumer   — KafkaConsumerAdapter (implementa KafkaConsumerPort)
bronze_writer — KafkaBronzeWriter (stream processor Kappa)

Arquitectura
------------
Exchanges / Nodes / APIs
        ↓
  Kafka (source of truth)
        ↓
  Stream processors (KafkaBronzeWriter → Iceberg Bronze)
        ↓
  Silver / Gold (Dagster jobs)
        ↓
  Research / ML / Execution

Kafka es el único event backbone.
Redis queda como cache de estado (cursores, circuit breakers) — no como
event log ni streaming transport.

Principios: DIP · SRP · SafeOps · Kappa · at-least-once · SSOT
"""
