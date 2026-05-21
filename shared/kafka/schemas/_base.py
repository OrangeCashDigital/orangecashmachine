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

Principios: SSOT · DDD · KISS · Fail-Fast
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_uuid() -> str:
    return str(uuid.uuid4())


@dataclass(frozen=True)
class BasePayload:
    """
    Envelope común para todos los wire payloads Kafka.

    Campos
    ------
    event_id      : UUID v4 — idempotencia y deduplicación downstream
    event_version : versión del schema — detecta incompatibilidad en consumer
    occurred_at   : ISO-8601 UTC del momento de creación del payload

    Subclases deben declarar su propia constante SCHEMA_VERSION:
        class MyPayload(BasePayload):
            SCHEMA_VERSION: ClassVar[int] = 1
    """

    event_id: str = field(default_factory=_new_uuid)
    event_version: int = 1
    occurred_at: str = field(default_factory=_utc_now)

    def to_dict(self) -> Dict[str, Any]:
        """Base dict con campos de envelope. Subclases extienden."""
        return {
            "event_id": self.event_id,
            "event_version": self.event_version,
            "occurred_at": self.occurred_at,
        }


__all__ = ["BasePayload"]
