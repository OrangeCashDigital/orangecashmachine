"""Port de métricas del publisher de gaps (adapters no importan infrastructure)."""
from __future__ import annotations

from typing import Protocol


class GapPublisherMetricsPort(Protocol):
    def event_published(self, exchange: str = "unknown") -> None: ...

    def event_failed(
        self, exchange: str = "unknown", reason: str = "unknown"
    ) -> None: ...


class NullGapPublisherMetrics:
    def event_published(self, exchange: str = "unknown") -> None:
        return None

    def event_failed(
        self, exchange: str = "unknown", reason: str = "unknown"
    ) -> None:
        return None
