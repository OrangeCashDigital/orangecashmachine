# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/
==================================

Adaptadores concretos de Kafka para OCM.

Módulos
-------
producer  — KafkaProducerAdapter  (implementa KafkaProducerPort)
consumer  — KafkaConsumerAdapter  (implementa KafkaConsumerPort)
topics    — KafkaTopicAdmin       (crea tópicos al arranque)
serializer — funciones de serialización EventPayload ↔ bytes
"""
