# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/_base.py
==============================

BasePayload — raíz común de todos los wire payloads Kafka de OCM.

Responsabilidad
---------------
Centralizar los campos de envelope que todo payload debe tener:
event_id, schema_version, occurred_at.

Separación dominio / wire
--------------------------
BasePayload NO es un DomainEvent. Es el sobre (envelope) de transporte.
La jerarquía es:

    DomainEvent (shared/types/)   ← objeto de dominio interno
         │
         │  serializa a
         ▼
    BasePayload (shared/kafka/schemas/)  ← contrato wire Kafka

Los bounded contexts crean DomainEvents; los publishers los convierten
a Payloads; los consumers deserializan Payloads y reconstruyen DomainEvents.

Versionado
----------
SCHEMA_VERSION es el contrato de compatibilidad de cada payload concreto.
El campo event_version en el payload wire permite detectar incompatibilidades
en el consumer antes de deserializar el body completo.

Política de compatibilidad:
  - Additive changes (nuevo campo con default) → mismo SCHEMA_VERSION
  - Breaking changes (rename, remove, type change) → bump SCHEMA_VERSION

Literales cross-wire
--------------------
Los literales compartidos (SignalDirection, OrderSide, PositionSide,
DataSource, DATASOURCE_*) viven en shared.enums (raíz del kernel) y se
re-exportan aquí por compatibilidad. BC-45 exige que shared.types y
shared.contracts importen desde shared.enums, nunca desde este módulo.

Principios: SSOT · DDD · KISS · Fail-Fast
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict

# =============================================================================
# Literales cross-wire — re-exportados desde shared.enums (SSOT real)
# =============================================================================
# BC-45 exige que shared.types y shared.contracts importen desde shared.enums,
# nunca desde este módulo. _base.py re-exporta por compatibilidad con los 9
# schemas y el serializer, que siguen importando solo de aquí (BC-33).
from shared.enums import (
    _VALID_SIGNAL_DIRECTIONS,
    _VALID_SOURCES,
    DATASOURCE_BACKFILL,
    DATASOURCE_LIVE,
    DATASOURCE_REPLAY,
    DataSource,
    OrderSide,
    PositionSide,
    SignalDirection,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_uuid() -> str:
    return str(uuid.uuid4())


# =============================================================================
# SchemaVersionError — fail-fast en deserialización (SSOT)
# =============================================================================


class SchemaVersionError(ValueError):
    """
    Versión de schema wire incompatible durante from_dict().

    Fail-fast: un consumer no puede procesar un payload de versión
    desconocida sin riesgo de corrupción silenciosa.
    """


@dataclass(frozen=True)
class KappaSourceMixin:
    """
    Campos y helpers Kappa (live | backfill | replay) para payloads con source.

    SSOT del campo `source` y de las reglas de filtrado por origen:
    consumers usan is_live/is_backfill/is_replay, nunca comparan el string.
    """

    source: DataSource = DATASOURCE_LIVE

    @property
    def is_live(self) -> bool:
        return self.source == DATASOURCE_LIVE

    @property
    def is_backfill(self) -> bool:
        return self.source == DATASOURCE_BACKFILL

    @property
    def is_replay(self) -> bool:
        return self.source == DATASOURCE_REPLAY


@dataclass(frozen=True)
class BasePayload:
    """
    Envelope común para todos los wire payloads Kafka.

    Campos
    ------
    event_id      : UUID v4 — idempotencia y deduplicación downstream
    occurred_at   : ISO-8601 UTC del momento de creación del payload

    Versionado
    ----------
    SCHEMA_VERSION (ClassVar) es el SSOT de versión de cada payload concreto.
    to_dict() lo emite como "event_version"; from_dict() valida contra él.
    Las subclases SOBRESCRIBEN SCHEMA_VERSION si su versión ≠ 1:

        class MyPayload(BasePayload):
            SCHEMA_VERSION: ClassVar[int] = 2
    """

    SCHEMA_VERSION: ClassVar[int] = 1

    event_id: str = field(default_factory=_new_uuid)
    occurred_at: str = field(default_factory=_utc_now)

    def to_dict(self) -> Dict[str, Any]:
        """Base dict con campos de envelope. Subclases extienden."""
        return {
            "event_id": self.event_id,
            "event_version": type(self).SCHEMA_VERSION,
            "occurred_at": self.occurred_at,
        }


__all__ = [
    "BasePayload",
    "SchemaVersionError",
    "KappaSourceMixin",
    "SignalDirection",
    "OrderSide",
    "PositionSide",
    "DataSource",
    "DATASOURCE_LIVE",
    "DATASOURCE_BACKFILL",
    "DATASOURCE_REPLAY",
    "_VALID_SOURCES",
    "_VALID_SIGNAL_DIRECTIONS",
]
