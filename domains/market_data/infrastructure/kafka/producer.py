# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/producer.py
=============================================

KafkaProducerAdapter — implementación concreta de KafkaProducerPort.

Responsabilidad
---------------
Publicar EventPayload a Kafka usando aiokafka.AIOKafkaProducer.
Implementa KafkaProducerPort — los adapters inbound solo conocen
el port, nunca esta clase concreta (DIP).

Ciclo de vida
-------------
    producer = KafkaProducerAdapter.from_env()
    await producer.start()          # conectar al broker
    ...
    ok = await producer.send_async(TOPIC_OHLCV_RAW, value, key)
    ...
    await producer.flush()          # vaciar buffer antes de shutdown
    await producer.close()          # cerrar conexión

Resiliencia
-----------
- send_async() es fail-soft: captura excepciones y retorna False
- Reintentos internos de aiokafka (configurables via constructor)
- Circuit breaker externo recomendado en el caller si se necesita
  degradación más agresiva

Observabilidad
--------------
- Logs estructurados con loguru en cada operación crítica
- Métricas: kafka_messages_sent_total{topic, exchange}
  (via StreamMetrics — reutiliza infraestructura existente)

Principios: DIP · SRP · SafeOps · Resiliencia · KISS · OCP
"""
from __future__ import annotations

import os
from typing import Optional

from loguru import logger

from market_data.ports.outbound.kafka_producer import KafkaProducerPort  # noqa: F401 — DIP


class KafkaProducerAdapter:
    """
    Adaptador concreto de KafkaProducerPort usando aiokafka.

    Parámetros
    ----------
    bootstrap_servers : str
        Host:puerto del broker. Múltiples separados por coma.
        Ejemplo: "kafka:9092" (Docker interno) o "localhost:9093" (host).
    client_id         : str
        Identificador del producer en el broker — visible en kafka-ui.
    compression_type  : str
        Compresión de mensajes: "lz4" (default), "gzip", "snappy", None.
    acks              : str | int
        Confirmación de escritura:
          "all" → espera ISR completo (más seguro, más lento)
          1     → líder confirmó (balance)
          0     → fire-and-forget (más rápido, puede perder mensajes)
    max_batch_size    : int
        Tamaño máximo del batch en bytes (default 16KB).
    linger_ms         : int
        Tiempo de espera para acumular mensajes en el batch (ms).
        0 = sin espera (latencia mínima), >0 = mayor throughput.
    """

    def __init__(
        self,
        bootstrap_servers: str  = "kafka:9092",
        client_id:         str  = "ocm-market-data-producer",
        compression_type:  str  = "lz4",
        acks:              object = "all",
        max_batch_size:    int  = 16_384,
        linger_ms:         int  = 5,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._client_id         = client_id
        self._compression_type  = compression_type
        self._acks              = acks
        self._max_batch_size    = max_batch_size
        self._linger_ms         = linger_ms
        self._producer          = None   # lazy — creado en start()
        self._started           = False
        self._log               = logger.bind(
            component  = "KafkaProducerAdapter",
            broker     = bootstrap_servers,
            client_id  = client_id,
        )

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "KafkaProducerAdapter":
        """
        Construye el adapter desde variables de entorno.

        Variables
        ---------
        KAFKA_BOOTSTRAP_SERVERS : broker host:port (default: kafka:9092)
        KAFKA_CLIENT_ID         : client id       (default: ocm-market-data-producer)
        KAFKA_COMPRESSION_TYPE  : lz4|gzip|snappy (default: lz4)
        KAFKA_ACKS              : all|1|0          (default: all)
        KAFKA_LINGER_MS         : int ms           (default: 5)
        """
        return cls(
            bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            client_id         = os.environ.get("KAFKA_CLIENT_ID",         "ocm-market-data-producer"),
            compression_type  = os.environ.get("KAFKA_COMPRESSION_TYPE",  "lz4"),
            acks              = os.environ.get("KAFKA_ACKS",               "all"),
            linger_ms         = int(os.environ.get("KAFKA_LINGER_MS",     "5")),
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Conecta al broker y arranca el producer interno.

        Fail-Fast: lanza si no puede conectar (el caller decide si reintentar).
        Idempotente: segunda llamada es no-op.
        """
        if self._started:
            return
        from aiokafka import AIOKafkaProducer
        self._producer = AIOKafkaProducer(
            bootstrap_servers = self._bootstrap_servers,
            client_id         = self._client_id,
            compression_type  = self._compression_type,
            acks              = self._acks,
            max_batch_size    = self._max_batch_size,
            linger_ms         = self._linger_ms,
            # Reintentos internos ante fallos transitorios de red
            request_timeout_ms = 30_000,
            retry_backoff_ms   = 500,
        )
        await self._producer.start()
        self._started = True
        self._log.info("kafka_producer_started")

    async def close(self) -> None:
        """Cierra la conexión limpiamente. Idempotente. SafeOps."""
        if not self._started or self._producer is None:
            return
        try:
            await self._producer.stop()
            self._started = False
            self._log.info("kafka_producer_closed")
        except Exception as exc:
            self._log.warning("kafka_producer_close_error", error=str(exc))

    # ------------------------------------------------------------------
    # KafkaProducerPort — implementación del contrato
    # ------------------------------------------------------------------

    async def send_async(
        self,
        topic:   str,
        value:   bytes,
        key:     Optional[bytes] = None,
        headers: Optional[dict]  = None,
    ) -> bool:
        """
        Publica un mensaje a Kafka.

        SafeOps: captura cualquier excepción y retorna False.
        El caller (ohlcv_fetcher) nunca recibe una excepción de este método.
        """
        if not self._started or self._producer is None:
            self._log.warning("send_async_skipped — producer not started", topic=topic)
            return False
        try:
            # Convertir headers dict → lista de tuplas (formato aiokafka)
            kafka_headers = (
                [(k, v.encode() if isinstance(v, str) else v)
                 for k, v in headers.items()]
                if headers else []
            )
            await self._producer.send_and_wait(
                topic,
                value   = value,
                key     = key,
                headers = kafka_headers,
            )
            self._log.bind(topic=topic).debug("kafka_message_sent")
            return True
        except Exception as exc:
            self._log.bind(
                topic = topic,
                error = str(exc),
            ).warning("kafka_send_failed")
            return False

    async def flush(self) -> None:
        """Vacía el buffer interno. SafeOps."""
        if not self._started or self._producer is None:
            return
        try:
            await self._producer.flush()
            self._log.debug("kafka_producer_flushed")
        except Exception as exc:
            self._log.warning("kafka_flush_error", error=str(exc))


__all__ = ["KafkaProducerAdapter"]
