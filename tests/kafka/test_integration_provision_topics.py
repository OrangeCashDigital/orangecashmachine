# -*- coding: utf-8 -*-
"""
tests/kafka/test_integration_provision_topics.py
================================================

Integración contra Kafka real del contrato de clasificación de
`scripts/provision_kafka_topics.py` (Kafka L1).

Cierra la carencia detectada en Kafka L1: los tests de
`tests/kafka/test_provision_topics.py` validan la lógica con un admin falso
(stubs); este archivo ejerce el **broker real** y verifica que la respuesta
de `AIOKafkaAdminClient.create_topics()` —no una excepción— es la fuente de
verdad para clasificar `created` / `already_exists`.

Qué se prueba (comportamiento real, sin mocks de topic_errors):
  1. Topic nuevo        → el broker responde error_code 0  → ``created``.
  2. Topic existente    → el broker responde error_code 36 → ``already_exists``.
  3. Recursos/errores   → el admin se cierra (operación posterior falla con
                          KafkaError) y el topic de prueba se elimina; el cierre
                          y la limpieza se verifican contra el broker real.

Aislamiento (requisitos B3/B5):
  - Se genera un topic ÚNICO por ejecución a partir del harness SSOT
    (``_unique_group``) → nunca colisiona con topics productivos ni entre runs.
  - El test administra EXCLUSIVAMENTE el topic que él mismo creó; jamás toca
    ``ALL_TOPICS`` ni ningún topic preexistente.
  - ``provision()`` NO se invoca: operaría sobre los 25 topics de producción
    (``ALL_TOPICS``). El test llama directamente a ``classify_topic_creation()``
    contra la respuesta real del broker para el topic aislado.

Conexión (requisito B2):
  - Usa ``BROKER`` de ``tests/kafka/conftest.py`` (SSOT del harness de
    integración). El job de CI levanta un broker efímero con el listener
    EXTERNAL en ese mismo endpoint. NO hay fallback a un broker compartido.

Cómo correr (requiere Kafka real en el endpoint de ``BROKER``):
    pytest tests/kafka/test_integration_provision_topics.py -v -s -m integration --no-cov

Contrato: tests/kafka/CONTRACT.md
Harness:  tests/kafka/conftest.py
Principios: Fail-Fast · SafeOps · SSOT · R-01..R-04
"""

from __future__ import annotations

import asyncio
import logging
import time

import pytest
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError

from scripts.provision_kafka_topics import classify_topic_creation
from tests.kafka.conftest import BROKER, _unique_group

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuración local del test
# ---------------------------------------------------------------------------

_ADMIN_TIMEOUT_MS: int = 15_000
"""Timeout del broker para creación/borrado (evita esperas arbitrarias)."""

_DELETE_POLL_TIMEOUT_S: float = 15.0
"""Máximo a esperar a que el borrado del topic sea visible en el broker."""

_DELETE_POLL_INTERVAL_S: float = 0.5
"""Intervalo entre lecturas de list_topics() durante la verificación de borrado."""


def _test_topic() -> str:
    """
    Nombre de topic único por ejecución.

    Reutiliza ``_unique_group`` (harness SSOT, R-01) para garantizar unicidad
    entre runs y evitar cualquier colisión con topics productivos. Prefijo
    inequívoco para que el topic sea atribuible a esta prueba.
    """
    return _unique_group("ocm-it-provision")


async def _create_and_classify(admin: AIOKafkaAdminClient, topic: str) -> tuple[str, str]:
    """Crea UN topic y clasifica la respuesta REAL del broker (sin mocks)."""
    response = await admin.create_topics(
        [NewTopic(name=topic, num_partitions=1, replication_factor=1)],
        timeout_ms=_ADMIN_TIMEOUT_MS,
    )
    return classify_topic_creation(topic, response)


async def _topic_absent(admin: AIOKafkaAdminClient, topic: str) -> bool:
    """True si el broker ya no lista el topic (consulta real, sin asumir posición)."""
    return topic not in await admin.list_topics()


async def _delete_and_confirm(admin: AIOKafkaAdminClient, topic: str) -> bool:
    """
    Elimina el topic y confirma su desaparición con el broker real.

    Sondea (sin sleep fijo ciego) hasta que el topic desaparece o vence el
    timeout. NUNCA oculta un fallo: devuelve False si el topic persiste.

    Returns:
        True si el topic ya no existe; False si sigue presente (limpieza fallida).
    """
    deadline = time.monotonic() + _DELETE_POLL_TIMEOUT_S
    while time.monotonic() < deadline:
        if await _topic_absent(admin, topic):
            return True
        await admin.delete_topics([topic], timeout_ms=_ADMIN_TIMEOUT_MS)
        await asyncio.sleep(_DELETE_POLL_INTERVAL_S)
    return await _topic_absent(admin, topic)


# ---------------------------------------------------------------------------
# Casos 1 + 2 + 3 — topic nuevo, topic existente, cierre y limpieza
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.integration
async def test_provision_real_broker_created_then_already_exists() -> None:
    """
    Comportamiento real del broker para un topic nuevo y su repetición.

    Invariantes (requisitos B4.1/B4.2/B6):
      - 1.ª creación  → ``created``       (el broker confirma error_code 0).
      - 2.ª creación  → ``already_exists`` (el broker confirma error_code 36),
        sin crear un duplicado ni reportar un segundo ``created``.

    Garantía de limpieza (B5): el topic de prueba se elimina con ``finally`` y el
    borrado se verifica contra el broker real. Un fallo de limpieza se propaga
    (no se oculta): el test falla de forma visible en CI.
    """
    topic = _test_topic()
    admin = AIOKafkaAdminClient(bootstrap_servers=BROKER)
    await admin.start()

    created_cleaned = False
    try:
        # --- Aislamiento: el topic no debe preexistir (nombre único por run) ---
        assert await _topic_absent(admin, topic), (
            f"El topic de prueba {topic!r} ya existía: el aislamiento del test está roto."
        )

        # --- Caso 1: topic nuevo → created (respuesta real del broker) ---
        cat_new, detail_new = await _create_and_classify(admin, topic)
        assert cat_new == "created", (
            f"Topic nuevo {topic!r}: el broker real no lo clasificó como 'created' sino {cat_new!r} ({detail_new})."
        )
        assert not await _topic_absent(admin, topic), f"El broker no reporta {topic!r} tras crearlo."

        # --- Caso 2: repetir la MISMA creación → already_exists (bug fix L1) ---
        cat_again, detail_again = await _create_and_classify(admin, topic)
        assert cat_again == "already_exists", (
            f"Topic existente {topic!r}: el broker real no lo clasificó como "
            f"'already_exists' sino {cat_again!r} ({detail_again}). "
            "Regresión del bug: aiokafka 0.14.0 NO lanza TopicAlreadyExistsError."
        )

        # --- Caso 3a (B4.3): cerrar el admin y comprobar el cierre real ---
        await admin.close()
        with pytest.raises(KafkaError):
            # Operación posterior al cierre: debe fallar (no silenciarse).
            await admin.list_topics()

        # Reabrir un cliente para la limpieza (el anterior quedó cerrado).
        admin = AIOKafkaAdminClient(bootstrap_servers=BROKER)
        await admin.start()
        # --- Caso 3b (B5): eliminar EXCLUSIVAMENTE el topic creado aquí ---
        created_cleaned = await _delete_and_confirm(admin, topic)
    finally:
        await admin.close()

    # Se afirma FUERA del bloque de limpieza para no enmascarar el resultado real.
    assert created_cleaned, (
        f"Limpieza incompleta: el topic de prueba {topic!r} seguía presente en el "
        "broker tras el borrado. Fallo visible, no oculto."
    )
