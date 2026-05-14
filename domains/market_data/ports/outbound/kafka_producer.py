# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/kafka_producer.py
=============================================

Puerto OUTBOUND: contrato de publicación a Kafka.

Topics canónicos — SSOT
-----------------------
Cada topic tiene un consumer group designado y una retención recomendada.
Modificar un topic aquí es la única forma válida de renombrarlo en el sistema.

  ohlcv.raw         → KafkaBronzeWriter (ocm-bronze-writer)  — retención 7d
  ohlcv.validated   → FeatureConsumer   (ocm-features)       — retención 7d
  ohlcv.features    → StrategyConsumer  (ocm-strategy)       — retención 7d
  signals.raw       → RiskGateConsumer  (ocm-risk-gate)      — retención 24h
  signals.approved  → ExecutionConsumer (ocm-execution)      — retención 24h
  orders.filled     → PortfolioConsumer (ocm-portfolio)      — retención 30d
  dlq.ohlcv         → (manual review)                        — retención 30d

Consumer groups — SSOT
-----------------------
Constantes GROUP_* usadas en KafkaConsumerAdapter factories.
Un solo lugar donde renombrar un grupo afecta a todos los consumers.

Routing key — SSOT
-------------------
"{exchange}:{symbol}:{timeframe}" — orden garantizado por par/timeframe.

Principios: DIP · SSOT · ISP · OCP · SafeOps
"""
from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Topics — SSOT
# ---------------------------------------------------------------------------

TOPIC_OHLCV_RAW:        str = "ohlcv.raw"
TOPIC_OHLCV_VALIDATED:  str = "ohlcv.validated"
TOPIC_OHLCV_FEATURES:   str = "ohlcv.features"
TOPIC_SIGNALS_RAW:      str = "signals.raw"
TOPIC_SIGNALS_APPROVED: str = "signals.approved"
TOPIC_ORDERS_FILLED:    str = "orders.filled"
TOPIC_POSITIONS:        str = "positions.updated"
TOPIC_DLQ:              str = "dlq.ohlcv"

# ---------------------------------------------------------------------------
# Consumer groups — SSOT
# ---------------------------------------------------------------------------

GROUP_BRONZE_WRITER: str = "ocm-bronze-writer"
GROUP_FEATURES:      str = "ocm-features"
GROUP_STRATEGY:      str = "ocm-strategy"
GROUP_RISK_GATE:     str = "ocm-risk-gate"
GROUP_EXECUTION:     str = "ocm-execution"
GROUP_PORTFOLIO:     str = "ocm-portfolio"

# ---------------------------------------------------------------------------
# Kafka header keys — SSOT
# ---------------------------------------------------------------------------

HEADER_SOURCE:  str = "x-ocm-source"    # "live" | "backfill" | "replay"
HEADER_VERSION: str = "x-ocm-version"   # str(PAYLOAD_SCHEMA_VERSION)
HEADER_RUN_ID:  str = "x-ocm-run-id"    # correlación con LineageTracker


# ---------------------------------------------------------------------------
# KafkaProducerPort — Protocol (DIP)
# ---------------------------------------------------------------------------

@runtime_checkable
class KafkaProducerPort(Protocol):
    """
    Contrato async de publicación a Kafka.

    Implementado por: KafkaProducerAdapter
    Usado por: BackfillProducer, CryptofeedAdapter, QualityGateConsumer

    SafeOps: send_async() DEBE retornar False en lugar de lanzar.
    """

    async def send_async(
        self,
        topic:   str,
        value:   bytes,
        key:     Optional[bytes] = None,
        headers: Optional[dict]  = None,
    ) -> bool: ...

    async def flush(self) -> None: ...

    async def close(self) -> None: ...


__all__ = [
    # Topics
    "TOPIC_OHLCV_RAW",
    "TOPIC_OHLCV_VALIDATED",
    "TOPIC_OHLCV_FEATURES",
    "TOPIC_SIGNALS_RAW",
    "TOPIC_SIGNALS_APPROVED",
    "TOPIC_ORDERS_FILLED",
    "TOPIC_POSITIONS",
    "TOPIC_DLQ",
    # Consumer groups
    "GROUP_BRONZE_WRITER",
    "GROUP_FEATURES",
    "GROUP_STRATEGY",
    "GROUP_RISK_GATE",
    "GROUP_EXECUTION",
    "GROUP_PORTFOLIO",
    # Headers
    "HEADER_SOURCE",
    "HEADER_VERSION",
    "HEADER_RUN_ID",
    # Protocol
    "KafkaProducerPort",
]
