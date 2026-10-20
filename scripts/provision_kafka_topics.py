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

Clasificación por topic (contrato aiokafka 0.14.0 — ver tests/kafka/test_provision_topics.py):
    create_topics() NO lanza TopicAlreadyExistsError; devuelve la
    CreateTopicsResponse con un resultado por topic en `topic_errors`
    (error_code 0=creado, 36=ya existe; resto=fallo del broker).
    Categorías: created · already_exists · failed · indeterminate.

Exit codes:
    0 — todos los topics clasificados como created o already_exists.
    1 — hubo failed/indeterminados, o un error de conexión/permisos
        (fail-fast: la excepción se propaga y aborta el resto).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any, Literal

from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from shared.kafka.topics import ALL_TOPICS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("provision_kafka_topics")

DEFAULT_PARTITIONS = 3
DEFAULT_REPLICATION_FACTOR = 1

# Códigos de error de broker relevantes (Kafka protocol / aiokafka errno).
_TOPIC_CREATED = 0
_TOPIC_ALREADY_EXISTS = 36  # TOPIC_ALREADY_EXISTS

Category = Literal["created", "already_exists", "failed", "indeterminate"]


def _entry_field(entry: Any, key: str, index: int) -> Any:
    """Lee un campo de una entrada de topic_errors (dict o tupla posicional)."""
    if isinstance(entry, dict):
        return entry.get(key)
    try:
        return entry[index]
    except (IndexError, TypeError):
        return None


def classify_topic_creation(topic: str, response: Any) -> tuple[Category, str]:
    """
    Clasifica el resultado de create_topics() para UN topic a partir de la
    CreateTopicsResponse devuelta por aiokafka.

    No hace suposiciones sobre la versión del protocolo: lee topic_errors
    y tolera entradas dict (to_object()) o tupla (decode posicional).

    Returns:
        (categoria, detalle) con categoria en
        created | already_exists | failed | indeterminate.
        created solo cuando el broker confirma error_code=0 para ESTE topic.
    """
    try:
        obj = response.to_object()
    except Exception as exc:  # noqa: BLE001 — una respuesta ilegible es indeterminada, no un éxito
        return "indeterminate", f"respuesta no decodificable: {exc}"

    errors = obj.get("topic_errors") if isinstance(obj, dict) else None
    if not isinstance(errors, list):
        return "indeterminate", "respuesta sin topic_errors"

    for entry in errors:
        if _entry_field(entry, "topic", 0) != topic:
            continue
        code = _entry_field(entry, "error_code", 1)
        message = _entry_field(entry, "error_message", 2)
        if not isinstance(code, int):
            return "indeterminate", f"error_code no numérico: {code!r}"
        if code == _TOPIC_CREATED:
            return "created", "el broker confirmó la creación"
        if code == _TOPIC_ALREADY_EXISTS:
            return "already_exists", "el topic ya existía en el broker"
        detail = f"error_code={code}"
        if message:
            detail += f" ({message})"
        return "failed", detail

    return "indeterminate", "topic ausente de topic_errors en la respuesta"


async def provision(bootstrap_server: str, num_partitions: int, replication_factor: int) -> dict[str, int]:
    """
    Crea los topics de ALL_TOPICS y clasifica cada resultado.

    Race-free: la clasificación se apoya EXCLUSIVAMENTE en el error_code
    que el broker devuelve por topic en la respuesta de create_topics
    (no hay list_topics previo, susceptible a carreras).

    Returns:
        Contadores {created, already_exists, failed, indeterminate}.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_server)
    await admin.start()
    counts: dict[str, int] = {
        "created": 0,
        "already_exists": 0,
        "failed": 0,
        "indeterminate": 0,
    }
    try:
        for topic in ALL_TOPICS:
            new_topic = NewTopic(
                name=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            # Sin try/except aquí: errores de conexión/timeout se propagan y
            # abortan con exit 1 (fail-fast original preservado).
            response = await admin.create_topics([new_topic])
            category, detail = classify_topic_creation(topic, response)
            counts[category] += 1
            if category == "created":
                logger.info("Creado: %s", topic)
            elif category == "already_exists":
                logger.info("Ya existía: %s (%s)", topic, detail)
            elif category == "failed":
                logger.error("Falló: %s (%s)", topic, detail)
            else:
                logger.warning("Indeterminado: %s (%s)", topic, detail)
        logger.info(
            "Provisioning completo. creados=%d, ya_existían=%d, fallidos=%d, indeterminados=%d, total=%d",
            counts["created"],
            counts["already_exists"],
            counts["failed"],
            counts["indeterminate"],
            len(ALL_TOPICS),
        )
    finally:
        await admin.close()
    return counts


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
        counts = asyncio.run(provision(args.bootstrap_server, args.partitions, args.replication_factor))
    except Exception:
        logger.exception("Fallo al provisionar topics.")
        return 1
    if counts["failed"] or counts["indeterminate"]:
        logger.error(
            "Provisioning con resultados no verificables (fallidos=%d, indeterminados=%d) — exit 1.",
            counts["failed"],
            counts["indeterminate"],
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
