# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/
==================================

Implementaciones concretas Kafka para OCM.

Módulos
-------
payloads      — EventPayload + OHLCVBar (wire format SSOT)
metrics       — KafkaMetrics + timer (observabilidad Prometheus)
dedup         — SeenFilter + CompositeSeenFilter (idempotencia L1+L2)
serializer    — serialize / deserialize / make_routing_key
producer      — KafkaProducerAdapter  (implementa KafkaProducerPort)
consumer      — KafkaConsumerAdapter  (implementa KafkaConsumerPort)
bronze_writer — KafkaBronzeWriter     (stream processor Kappa)

Arquitectura
------------
  Exchanges / Nodes / APIs
          ↓
    Kafka (único event backbone — source of truth)
          ↓
    KafkaBronzeWriter (ohlcv.raw → Iceberg Bronze)
          ↓
    Silver / Gold — Dagster jobs (materialización batch)
          ↓
    Research / ML / Execution

Roles explícitos
----------------
  Kafka  → event log durable · replay · DLQ · source of truth
  Redis  → state cache · cursores · circuit breakers (NO event transport)
  Dagster→ orchestration · asset materialization · schedules

Tópicos canónicos (SSOT en ports/outbound/kafka_producer.py)
------------------------------------------------------------
  ohlcv.raw       — velas crudas CCXT → Bronze
  ohlcv.validated — post quality-gate → Silver
  ohlcv.features  — features Gold computados
  dlq.ohlcv       — Dead Letter Queue

Principios: DIP · SRP · SafeOps · Kappa · at-least-once · SSOT
"""
