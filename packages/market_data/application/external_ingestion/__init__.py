"""
market_data/application/external_ingestion
==========================================

Capacidad external_ingestion de ADR-0014: adquisición periódica /
histórica no-streaming de fuentes externas, su normalización a evento
canónico y su publicación al log operacional Kafka.
"""

from __future__ import annotations

from market_data.application.external_ingestion.orchestrator import (
    ExternalIngestionOrchestrator,
    ExternalSourceRuntime,
)

__all__ = [
    "ExternalIngestionOrchestrator",
    "ExternalSourceRuntime",
]
