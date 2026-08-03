# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/
==================================

Implementaciones concretas Kafka para OCM.

Módulos
-------
metrics       — KafkaMetrics + timer (observabilidad Prometheus)
dedup         — SeenFilter + CompositeSeenFilter (idempotencia L1+L2)
producer      — KafkaProducerAdapter  (implementa KafkaProducerPort)
consumer      — KafkaConsumerAdapter  (implementa KafkaConsumerPort)
bronze_writer — KafkaBronzeWriter     (stream processor Kappa)
ohlcv_publisher — OHLCVPublisherPort  (publica EventPayload a ohlcv.raw)

Wire format SSOT: shared.kafka (schemas/ + serializer + topics).
Ningún payload ni topic se define aquí — se importan de shared.

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

Tópicos canónicos (SSOT en shared.kafka.topics)
------------------------------------------------
  ohlcv.raw       — velas crudas CCXT → Bronze
  ohlcv.validated — post quality-gate → Silver
  ohlcv.features  — features Gold computados
  ocm.dlq         — Dead Letter Queue global

Principios: DIP · SRP · SafeOps · Kappa · at-least-once · SSOT
"""
