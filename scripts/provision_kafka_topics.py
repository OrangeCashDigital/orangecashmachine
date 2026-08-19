#!/usr/bin/env python3
"""
Provisiona explícitamente todos los topics declarados en shared.kafka.topics.ALL_TOPICS.

Motivo (auditoría Kafka 2026-08-18):
KAFKA_AUTO_CREATE_TOPICS_ENABLE se desactivó en docker-compose.yml. Antes, un typo
en un nombre de topic creaba silenciosamente un topic fantasma en vez de fallar
(violación de fail-fast). Ahora la creación es explícita y deriva del SSOT
(shared/kafka/topics.py), no de una lista duplicada en este script (DRY).

Uso:
    python scripts/provision_kafka_topics.py
    python scripts/provision_kafka_topics.py --bootstrap-server localhost:9093

Idempotente (SafeOps): topics ya existentes se omiten, no se trata como error.
Fail-fast: errores de conexión u otros fallos reales sí abortan con exit code 1.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from shared.kafka.topics import ALL_TOPICS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("provision_kafka_topics")

DEFAULT_PARTITIONS = 3
DEFAULT_REPLICATION_FACTOR = 1


async def provision(bootstrap_server: str, num_partitions: int, replication_factor: int) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_server)
    await admin.start()
    created, skipped = 0, 0
    try:
        for topic in ALL_TOPICS:
            new_topic = NewTopic(
                name=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            try:
                await admin.create_topics([new_topic])
                created += 1
                logger.info("Creado: %s", topic)
            except TopicAlreadyExistsError:
                skipped += 1
                logger.debug("Ya existe, se omite: %s", topic)
        logger.info(
            "Provisioning completo. Creados=%d, ya existentes=%d, total=%d",
            created,
            skipped,
            len(ALL_TOPICS),
        )
    finally:
        await admin.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9093",
        help="Broker Kafka (default: localhost:9093, listener EXTERNAL desde host).",
    )
    parser.add_argument("--partitions", type=int, default=DEFAULT_PARTITIONS)
    parser.add_argument("--replication-factor", type=int, default=DEFAULT_REPLICATION_FACTOR)
    args = parser.parse_args()

    logger.info(
        "Provisionando %d topics en %s (partitions=%d, replication_factor=%d)",
        len(ALL_TOPICS),
        args.bootstrap_server,
        args.partitions,
        args.replication_factor,
    )
    try:
        asyncio.run(provision(args.bootstrap_server, args.partitions, args.replication_factor))
    except Exception:
        logger.exception("Fallo al provisionar topics.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
