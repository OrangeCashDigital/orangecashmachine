# -*- coding: utf-8 -*-
"""
shared/kafka/topics.py
=======================

SSOT global de topics, consumer groups y headers Kafka de OCM.

Migración desde market_data/infrastructure/kafka/topics.py
----------------------------------------------------------
Este módulo es el NUEVO hogar. El original en market_data/ será
deprecado — pasará a re-exportar desde aquí para compatibilidad
durante la transición, luego será eliminado.

Por qué en shared/ y no en market_data/
-----------------------------------------
topics.py define TOPIC_ORDERS_FILLED, TOPIC_SIGNALS_APPROVED, etc.
Esos topics pertenecen a trading/ y portfolio/, no a market_data/.
Un módulo de market_data que define contratos de otros bounded contexts
viola SRP y BC-10. shared/ es el único lugar correcto.

Topología Kappa de OCM
-----------------------

  Exchange REST/WS
       │
       ▼
  [ohlcv.raw]          — candles crudas (backfill + live)
       │
       ├──► BronzeWriter ──────────────────► Iceberg Bronze
       │
       └──► QualityGate ──► [ohlcv.validated]
                                  │
                            FeatureEngine ──► [ohlcv.features]
                                                    │
                                            StrategyConsumer
                                                    │
                                            [signals.raw]
                                                    │
                                            RiskGate
                                            │               │
                               [signals.approved]  [signals.rejected]
                                            │
                                    ExecutionConsumer → OMS
                                            │
                                    [orders.filled] / [orders.rejected]
                                            │
                                    PortfolioConsumer → TradeTracker
                                            │
                               [positions.opened] / [positions.closed]

  Cualquier topic → [*.dlq] si el mensaje no es procesable.

Principios: SSOT · SRP · KISS
"""
from __future__ import annotations

# =============================================================================
# Topics — nombres canónicos
# =============================================================================

# ── OHLCV pipeline ───────────────────────────────────────────────────────────

TOPIC_OHLCV_RAW:       str = "ohlcv.raw"
"""Candles crudas. Fuente: OHLCVPipeline. Consumidores: BronzeWriter, QualityGate."""

TOPIC_OHLCV_VALIDATED: str = "ohlcv.validated"
"""Candles validadas por QualityGate. Consumidores: FeatureEngine."""

TOPIC_OHLCV_FEATURES:  str = "ohlcv.features"
"""Candles + features técnicas. Solo source=live. Consumidores: StrategyConsumer."""

# ── Señales ───────────────────────────────────────────────────────────────────

TOPIC_SIGNALS_RAW:      str = "signals.raw"
"""Señales crudas de estrategias. Consumidores: RiskGate."""

TOPIC_SIGNALS_APPROVED: str = "signals.approved"
"""Señales aprobadas por RiskGate. Consumidores: ExecutionConsumer → OMS."""

TOPIC_SIGNALS_REJECTED: str = "signals.rejected"
"""Señales rechazadas por RiskGate. Para auditoría y alertas."""

# ── Órdenes ───────────────────────────────────────────────────────────────────

TOPIC_ORDERS_FILLED:   str = "orders.filled"
"""Órdenes ejecutadas. Fuente: OMS. Consumidores: PortfolioConsumer."""

TOPIC_ORDERS_REJECTED: str = "orders.rejected"
"""Órdenes rechazadas por executor o exchange. Para auditoría."""

# ── Posiciones ────────────────────────────────────────────────────────────────

TOPIC_POSITIONS_OPENED: str = "positions.opened"
"""Posición abierta. Fuente: PortfolioConsumer. Consumidores: risk/, observability."""

TOPIC_POSITIONS_CLOSED: str = "positions.closed"
"""Posición cerrada con PnL. Consumidores: analytics, research."""

# ── Dead Letter Queue ─────────────────────────────────────────────────────────

TOPIC_DLQ: str = "ocm.dlq"
"""Dead Letter Queue global — mensajes no procesables de cualquier topic."""

# =============================================================================
# Consumer Groups — SSOT de group_id por rol
# =============================================================================

GROUP_BRONZE_WRITER: str = "ocm-bronze-writer"
GROUP_QUALITY_GATE:  str = "ocm-quality-gate"
GROUP_FEATURES:      str = "ocm-feature-engine"
GROUP_STRATEGY:      str = "ocm-strategy-consumer"
GROUP_RISK_GATE:     str = "ocm-risk-gate"
GROUP_EXECUTION:     str = "ocm-execution"
GROUP_PORTFOLIO:     str = "ocm-portfolio"

# =============================================================================
# Headers — metadatos del mensaje (x-ocm-*)
# =============================================================================

HEADER_SOURCE:  str = "x-ocm-source"
"""Origen: 'backfill' | 'live' | 'replay'. Filtrar sin deserializar body."""

HEADER_VERSION: str = "x-ocm-schema-version"
"""Versión del schema payload. Detecta incompatibilidad en consumer."""

HEADER_RUN_ID:  str = "x-ocm-run-id"
"""Correlación con el run que generó el mensaje. Para lineage y auditoría."""

HEADER_DOMAIN:  str = "x-ocm-domain"
"""Dominio del payload: 'ohlcv' | 'signal' | 'order' | 'position'.
Permite routing sin deserializar en consumers multi-topic."""

# =============================================================================
# Retention & compaction policy — documentación operacional
# =============================================================================
#
# Aplicar via Kafka admin o docker-compose environment:
#
#   ohlcv.raw          delete   7d     — serie temporal, no compactar
#   ohlcv.validated    delete   3d
#   ohlcv.features     delete   1d
#   ohlcv.dlq          delete   30d    — replay manual
#   signals.raw        delete   1d
#   signals.approved   delete   1d
#   signals.rejected   delete   7d     — auditoría
#   orders.filled      delete   30d    — auditoría financiera
#   orders.rejected    delete   30d
#   positions.opened   delete   30d
#   positions.closed   delete   90d    — base analytics PnL
#   ocm.dlq            delete   30d
#
# NO usar compaction en ningún topic — todos los mensajes son relevantes.

# =============================================================================
# Routing keys canónicas — documentación
# =============================================================================
#
# ohlcv.*    → "{exchange}:{symbol}:{timeframe}"  (orden por par+tf)
# signals.*  → "{exchange}:{symbol}"              (orden por símbolo)
# orders.*   → "{exchange}:{symbol}"              (orden por símbolo)
# positions.*→ "{exchange}:{symbol}"              (orden por símbolo)

# =============================================================================
# Validación Fail-Fast en import
# =============================================================================

_ALL_TOPICS = [
    TOPIC_OHLCV_RAW, TOPIC_OHLCV_VALIDATED, TOPIC_OHLCV_FEATURES,
    TOPIC_SIGNALS_RAW, TOPIC_SIGNALS_APPROVED, TOPIC_SIGNALS_REJECTED,
    TOPIC_ORDERS_FILLED, TOPIC_ORDERS_REJECTED,
    TOPIC_POSITIONS_OPENED, TOPIC_POSITIONS_CLOSED,
    TOPIC_DLQ,
]
assert len(_ALL_TOPICS) == len(set(_ALL_TOPICS)), (
    "shared/kafka/topics.py: topics con string duplicado — colisión de nombres"
)

_ALL_GROUPS = [
    GROUP_BRONZE_WRITER, GROUP_QUALITY_GATE, GROUP_FEATURES,
    GROUP_STRATEGY, GROUP_RISK_GATE, GROUP_EXECUTION, GROUP_PORTFOLIO,
]
assert len(_ALL_GROUPS) == len(set(_ALL_GROUPS)), (
    "shared/kafka/topics.py: consumer groups con string duplicado"
)

del _ALL_TOPICS, _ALL_GROUPS


__all__ = [
    # Topics OHLCV
    "TOPIC_OHLCV_RAW", "TOPIC_OHLCV_VALIDATED", "TOPIC_OHLCV_FEATURES",
    # Topics señales
    "TOPIC_SIGNALS_RAW", "TOPIC_SIGNALS_APPROVED", "TOPIC_SIGNALS_REJECTED",
    # Topics órdenes
    "TOPIC_ORDERS_FILLED", "TOPIC_ORDERS_REJECTED",
    # Topics posiciones
    "TOPIC_POSITIONS_OPENED", "TOPIC_POSITIONS_CLOSED",
    # DLQ
    "TOPIC_DLQ",
    # Consumer groups
    "GROUP_BRONZE_WRITER", "GROUP_QUALITY_GATE", "GROUP_FEATURES",
    "GROUP_STRATEGY", "GROUP_RISK_GATE", "GROUP_EXECUTION", "GROUP_PORTFOLIO",
    # Headers
    "HEADER_SOURCE", "HEADER_VERSION", "HEADER_RUN_ID", "HEADER_DOMAIN",
]
