from __future__ import annotations

"""
infra/state/redis_stream.py
============================

Primitivas Redis Streams para OCM event-driven pipeline.

Responsabilidad
---------------
Publicar (XADD) y consumir (XREADGROUP) eventos en Redis Streams.
NO contiene lógica de negocio ni conoce EventPayload directamente —
trabaja con dicts planos (wire format).

Clases
------
RedisStreamPublisher  — escribe eventos al stream (XADD)
RedisStreamConsumer   — lee eventos con consumer groups (XREADGROUP + XACK)

Diseño
------
- Ambas clases reciben un redis.Redis ya construido (DI).
  No crean la conexión — eso es responsabilidad de factories.py.
- SafeOps: publish() y consume() nunca lanzan al caller.
  Retornan None/[] con warning en caso de error.
- Consumer groups: at-least-once delivery con XACK explícito.
  El caller decide cuándo hacer ACK (después de procesar, no antes).

Stream key
----------
  ocm:events:{stream_name}
  Ejemplo: ocm:events:ohlcv

Consumer group
--------------
  ocm-group-{stream_name}
  Ejemplo: ocm-group-ohlcv

Principios: SRP · DI · SafeOps · at-least-once
"""

import time
from typing import Any, Dict, List, Optional, Tuple

import redis as redis_lib
from loguru import logger


# --------------------------------------------------
# Constants
# --------------------------------------------------

_STREAM_PREFIX:  str = "ocm:events:"
_GROUP_PREFIX:   str = "ocm-group-"
_BLOCK_MS:       int = 2_000   # tiempo máximo bloqueando en XREADGROUP
_MAX_ENTRIES:    int = 10_000  # MAXLEN del stream (trimming aproximado)
_DEFAULT_STREAM: str = "ohlcv"


def _stream_key(stream_name: str) -> str:
    return f"{_STREAM_PREFIX}{stream_name}"


def _group_name(stream_name: str) -> str:
    return f"{_GROUP_PREFIX}{stream_name}"


# --------------------------------------------------
# Publisher
# --------------------------------------------------

class RedisStreamPublisher:
    """
    Publica eventos en un Redis Stream via XADD.

    Parámetros
    ----------
    client      : redis.Redis ya conectado (inyectado por factory).
    stream_name : nombre lógico del stream (sin prefijo).
    maxlen      : MAXLEN aproximado del stream — evita crecimiento ilimitado.

    Uso
    ---
        pub = RedisStreamPublisher(client=redis_client, stream_name="ohlcv")
        event_id = pub.publish({"event_version": "1", "exchange": "bybit", ...})
    """

    def __init__(
        self,
        client:      redis_lib.Redis,
        stream_name: str = _DEFAULT_STREAM,
        maxlen:      int = _MAX_ENTRIES,
    ) -> None:
        self._client  = client
        self._key     = _stream_key(stream_name)
        self._maxlen  = maxlen
        self._log     = logger.bind(
            component   = "RedisStreamPublisher",
            stream      = stream_name,
            stream_key  = self._key,
        )

    def publish(self, fields: Dict[str, Any]) -> Optional[str]:
        """
        Publica un evento en el stream.

        Convierte todos los valores a str — Redis Streams solo acepta
        strings en el wire format.

        Parámetros
        ----------
        fields : dict con los campos del evento (EventPayload.to_dict()).

        Retorna
        -------
        str  — Redis stream entry ID (ej: "1712345678901-0")
        None — si falla (SafeOps)
        """
        try:
            wire = {str(k): str(v) for k, v in fields.items()}
            entry_id = self._client.xadd(
                self._key,
                wire,
                maxlen=self._maxlen,
                approximate=True,
            )
            self._log.bind(entry_id=entry_id).debug("event_published")
            return entry_id
        except Exception as exc:
            self._log.bind(error=str(exc)).warning("publish_failed — event dropped")
            return None

    def stream_length(self) -> int:
        """Retorna el número de entradas en el stream. SafeOps: retorna 0 si falla."""
        try:
            return self._client.xlen(self._key)
        except Exception:
            return 0


# --------------------------------------------------
# Consumer
# --------------------------------------------------

class RedisStreamConsumer:
    """
    Consume eventos de un Redis Stream via XREADGROUP.

    Usa consumer groups para at-least-once delivery:
    - Cada mensaje debe confirmarse con ack() después de procesarlo.
    - Si el consumer muere antes del ack, el mensaje queda pendiente
      y puede reclamarse con claim_pending().

    Parámetros
    ----------
    client        : redis.Redis ya conectado (inyectado por factory).
    stream_name   : nombre lógico del stream.
    consumer_name : nombre de este consumer dentro del grupo.
    batch_size    : máximo de mensajes a leer por llamada a consume().

    Uso
    ---
        consumer = RedisStreamConsumer(client, "ohlcv", "worker-1")
        consumer.ensure_group()
        for entry_id, fields in consumer.consume():
            process(fields)
            consumer.ack(entry_id)
    """

    def __init__(
        self,
        client:        redis_lib.Redis,
        stream_name:   str = _DEFAULT_STREAM,
        consumer_name: str = "worker-1",
        batch_size:    int = 10,
    ) -> None:
        self._client        = client
        self._key           = _stream_key(stream_name)
        self._group         = _group_name(stream_name)
        self._consumer      = consumer_name
        self._batch_size    = batch_size
        self._log           = logger.bind(
            component     = "RedisStreamConsumer",
            stream        = stream_name,
            stream_key    = self._key,
            group         = self._group,
            consumer      = consumer_name,
        )

    def ensure_group(self) -> bool:
        """
        Crea el consumer group si no existe. Idempotente.

        Usa '$' como ID de inicio — solo consume mensajes nuevos
        después de la creación del grupo.

        Retorna True si el grupo ya existe o fue creado. False si falla.
        """
        try:
            self._client.xgroup_create(
                self._key, self._group, id="$", mkstream=True
            )
            self._log.info("consumer_group_created")
            return True
        except redis_lib.exceptions.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                self._log.debug("consumer_group_already_exists")
                return True
            self._log.bind(error=str(exc)).warning("ensure_group_failed")
            return False
        except Exception as exc:
            self._log.bind(error=str(exc)).warning("ensure_group_failed")
            return False

    def consume(self) -> List[Tuple[str, Dict[str, str]]]:
        """
        Lee hasta batch_size mensajes del stream.

        Bloquea hasta _BLOCK_MS ms si no hay mensajes nuevos.
        Retorna lista de (entry_id, fields). Lista vacía si no hay mensajes
        o si falla (SafeOps).

        El caller es responsable de llamar ack(entry_id) después de procesar.
        """
        try:
            results = self._client.xreadgroup(
                groupname   = self._group,
                consumername= self._consumer,
                streams     = {self._key: ">"},
                count       = self._batch_size,
                block       = _BLOCK_MS,
            )
            if not results:
                return []

            messages: List[Tuple[str, Dict[str, str]]] = []
            for _stream_key_resp, entries in results:
                for entry_id, fields in entries:
                    messages.append((entry_id, fields))

            if messages:
                self._log.bind(count=len(messages)).debug("messages_consumed")
            return messages

        except Exception as exc:
            self._log.bind(error=str(exc)).warning("consume_failed")
            return []

    def ack(self, entry_id: str) -> bool:
        """
        Confirma el procesamiento de un mensaje (XACK).

        Debe llamarse DESPUÉS de procesar exitosamente el mensaje.
        Si falla, el mensaje queda en PEL (pending entry list) y
        puede reclamarse con claim_pending().

        SafeOps: retorna False si falla, nunca lanza.
        """
        try:
            self._client.xack(self._key, self._group, entry_id)
            self._log.bind(entry_id=entry_id).debug("message_acked")
            return True
        except Exception as exc:
            self._log.bind(entry_id=entry_id, error=str(exc)).warning("ack_failed")
            return False

    def pending_count(self) -> int:
        """Retorna el número de mensajes pendientes (no acked). SafeOps: 0 si falla."""
        try:
            info = self._client.xpending(self._key, self._group)
            return info.get("pending", 0) if isinstance(info, dict) else 0
        except Exception:
            return 0

    def claim_pending(
        self,
        min_idle_ms:   int = 60_000,
        batch_size:    int = 10,
    ) -> List[Tuple[str, Dict[str, str]]]:
        """
        Reclama mensajes pendientes (PEL) idle más de min_idle_ms.

        Usa XAUTOCLAIM (Redis 7+) — atómico y eficiente.
        El caller procesa y hace ACK igual que con consume().

        Parámetros
        ----------
        min_idle_ms : tiempo mínimo idle en ms antes de reclamar.
        batch_size  : máximo de mensajes a reclamar por llamada.

        Retorna lista de (entry_id, fields). Lista vacía si no hay
        pendientes o si falla (SafeOps).

        Self-healing: permite recuperar mensajes de workers caídos
        sin intervención manual.
        """
        try:
            result = self._client.xautoclaim(
                name         = self._key,
                groupname    = self._group,
                consumername = self._consumer,
                min_idle_time= min_idle_ms,
                start_id     = "0-0",
                count        = batch_size,
            )
            # xautoclaim retorna (next_start_id, entries, deleted_ids)
            # entries es lista de (entry_id, fields)
            entries = result[1] if result else []
            if entries:
                self._log.bind(count=len(entries)).info("pending_messages_claimed")
            return [(eid, fields) for eid, fields in entries if fields]
        except Exception as exc:
            self._log.bind(error=str(exc)).warning("claim_pending_failed")
            return []

    def send_to_dlq(
        self,
        fields:             Dict[str, Any],
        reason:             str = "unknown",
        original_entry_id:  str = "",
    ) -> Optional[str]:
        """
        Publica un mensaje fallado al Dead Letter Stream.

        DLQ key: ocm:events:dead-letter
        Agrega campos de diagnóstico: dlq_reason, dlq_source_stream,
        dlq_original_entry_id, dlq_consumer.

        SafeOps: retorna None si falla, nunca lanza.
        """
        _DLQ_KEY    = "ocm:events:dead-letter"
        _DLQ_MAXLEN = 5_000

        try:
            wire = {str(k): str(v) for k, v in fields.items()}
            wire.update({
                "dlq_reason":           reason,
                "dlq_source_stream":    self._key,
                "dlq_original_entry_id": original_entry_id,
                "dlq_consumer":         self._consumer,
            })
            entry_id = self._client.xadd(
                _DLQ_KEY,
                wire,
                maxlen      = _DLQ_MAXLEN,
                approximate = True,
            )
            self._log.bind(
                dlq_entry_id       = entry_id,
                original_entry_id  = original_entry_id,
                reason             = reason,
            ).error("message_sent_to_dlq")
            return entry_id
        except Exception as exc:
            self._log.bind(error=str(exc)).warning("dlq_send_failed")
            return None
