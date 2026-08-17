"""
market_data/application/external_ingestion/orchestrator.py
==========================================================

ExternalIngestionOrchestrator — lifecycle manager de la adquisición
no-streaming (capacidad external_ingestion, ADR-0014).

Responsabilidades
------------------
  • Un Task asyncio por fuente habilitada (polling loop).
  • Scheduling por intervalo (schedule_every_s).
  • Retries con backoff exponencial + jitter ante fallos de fuente.
  • Rate limiting por fuente (mínimo intervalo entre polls).
  • Cadena por ciclo: fetch (PollingSourcePort) → normalize (normalizers)
    → publish (ExternalEventPublisherPort).
  • Checkpoint en memoria del último timestamp procesado por fuente.
  • Lifecycle de fuentes: UNA instancia por source_id (reutilizada entre
    ciclos y reintentos) y close() de cada instancia en shutdown — evita
    fugas de recursos (p.ej. ClientSession aiohttp) en ejecución larga.
  • Shutdown graceful vía stop_event (mismo patrón que FeedOrchestrator).

DIP: depende de puertos por constructor, nunca de adapters concretos.
El scheduler es un mecanismo de ejecución — no se adueña del dominio.
"""

from __future__ import annotations

import asyncio
import random
import signal
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from loguru import logger

from market_data.application.external_ingestion.normalizers import normalize
from market_data.domain.events.external_events import ExternalMetricEvent
from market_data.ports.inbound.external.errors import ExternalRateLimitError
from market_data.ports.inbound.external.polling import (
    PollingRequest,
    PollingSourcePort,
)
from market_data.ports.outbound.external_publisher import (
    ExternalEventPublisherPort,
)
from market_data.ports.outbound.metrics import ExternalMetricsPort, NullExternalMetrics

__all__ = [
    "ExternalSourceRuntime",
    "ExternalIngestionOrchestrator",
]


@dataclass(frozen=True, slots=True)
class ExternalSourceRuntime:
    """Configuración runtime de una fuente para el orquestador.

    Poblada desde ocm.config.schema.ExternalSourceConfig en el wiring
    (composition root) — el orquestador no conoce Hydra.
    """

    source_id: str
    metric: str
    topic: str
    enabled: bool = False
    symbols: tuple[str, ...] = ()
    schedule_every_s: int = 300
    rate_limit_per_minute: int = 60
    max_attempts: int = 3
    backoff_factor: float = 2.0
    backoff_cap_s: float = 60.0

    @property
    def min_poll_interval_s(self) -> float:
        """Mínimo intervalo entre polls impuesto por el rate limit."""
        if self.rate_limit_per_minute <= 0:
            return 0.0
        return 60.0 / self.rate_limit_per_minute

    def next_backoff_s(self, attempt: int) -> float:
        """Backoff exponencial con jitter para el intento `attempt` (1-based).

        El jitter se clampa al cap: el retraso nunca excede backoff_cap_s.
        """
        base = min(self.backoff_cap_s, self.backoff_factor ** (attempt - 1))
        with_jitter = base + random.uniform(0.0, base * 0.25)
        return min(self.backoff_cap_s, with_jitter)


class ExternalIngestionOrchestrator:
    """Gestiona el lifecycle completo de las fuentes externas no-streaming.

    Uso
    ----
        cfg = ExternalSourceRuntime(source_id="coinglass", metric="funding_rate", ...)
        orch = ExternalIngestionOrchestrator([cfg], get_source, publisher)
        asyncio.run(orch.run())   # bloquea hasta SIGINT/SIGTERM
    """

    def __init__(
        self,
        sources: list[ExternalSourceRuntime],
        get_source: Callable[[str], PollingSourcePort],
        publisher: ExternalEventPublisherPort,
        metrics: ExternalMetricsPort | None = None,
    ) -> None:
        self._sources = [s for s in sources if s.enabled]
        self._get_source = get_source
        self._publisher = publisher
        self._metrics: ExternalMetricsPort = metrics or NullExternalMetrics()
        self._stop_event = asyncio.Event()
        self._last_processed: dict[str, int] = {}
        self._source_instances: dict[str, PollingSourcePort] = {}

    @property
    def last_processed(self) -> dict[str, int]:
        """Checkpoints in-memory: source_id → último timestamp_ms procesado."""
        return dict(self._last_processed)

    def _source_for(self, source_id: str) -> PollingSourcePort:
        """Devuelve la instancia única de una fuente, creándola la 1ª vez.

        El orquestador es dueño del lifecycle: una fuente por source_id,
        reutilizada entre ciclos y reintentos. Se cierra en shutdown.
        """
        if source_id not in self._source_instances:
            self._source_instances[source_id] = self._get_source(source_id)
        return self._source_instances[source_id]

    async def run(self) -> None:
        """Inicia los polling loops de las fuentes y bloquea hasta shutdown."""
        self._install_signal_handlers()

        if not self._sources:
            logger.info("[external-ingestion] sin fuentes habilitadas — no se inicia nada")
            return

        tasks = [asyncio.create_task(self._poll_loop(cfg), name=f"external-{cfg.source_id}") for cfg in self._sources]
        logger.info(
            "[external-ingestion] started | sources={}",
            [c.source_id for c in self._sources],
        )

        await self._stop_event.wait()

        logger.info("[external-ingestion] shutdown initiated...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for source in self._source_instances.values():
            await source.close()
        logger.info("[external-ingestion] shutdown complete.")

    # ── polling loop ───────────────────────────────────────────────────────────

    async def _poll_loop(self, cfg: ExternalSourceRuntime) -> None:
        """Loop periódico de una fuente: fetch → normalize → publish."""
        log = logger.bind(source_id=cfg.source_id, metric=cfg.metric)

        while not self._stop_event.is_set():
            cycle_start = asyncio.get_running_loop().time()
            self._metrics.cycles_total_inc(cfg.source_id, cfg.metric)
            try:
                events = await self._run_cycle(cfg, self._source_for(cfg.source_id))
                for event in events:
                    self._last_processed[cfg.source_id] = max(
                        self._last_processed.get(cfg.source_id, 0),
                        event.timestamp_ms,
                    )
                log.info(
                    "cycle_complete events={} checkpoint_ms={}",
                    len(events),
                    self._last_processed.get(cfg.source_id),
                )
            except asyncio.CancelledError:
                raise
            except ExternalRateLimitError as exc:
                log.warning("rate_limit_hit — backoff largo | {}", exc)
                self._metrics.errors_total_inc(cfg.source_id, cfg.metric, "rate_limit")
                await self._sleep_until_stop(cfg.backoff_cap_s)
            except Exception as exc:  # noqa: BLE001 — error de fuente, backoff
                log.warning("source_error | {}", exc)
                self._metrics.errors_total_inc(cfg.source_id, cfg.metric, "transient")
                await self._retry_cycle(cfg, log)

            elapsed = asyncio.get_running_loop().time() - cycle_start
            await self._sleep_until_stop(self._next_wait_s(cfg, elapsed))

    async def _run_cycle(
        self,
        cfg: ExternalSourceRuntime,
        source: PollingSourcePort,
    ) -> list[ExternalMetricEvent]:
        """Un ciclo completo de adquisición + normalización + publicación."""
        cycle_start = asyncio.get_running_loop().time()
        request = PollingRequest(metric=cfg.metric, symbols=list(cfg.symbols) or None)
        result = await source.fetch(request)
        self._metrics.fetches_total_inc(cfg.source_id, cfg.metric)
        fetched_at_ms = int(result.fetched_at.timestamp() * 1000)
        events = normalize(
            source.source_id,
            cfg.metric,
            result.payload,
            symbols=cfg.symbols,
            fetched_at_ms=fetched_at_ms,
        )
        for event in events:
            await self._publisher.publish(cfg.topic, event)
        self._metrics.events_published_inc(cfg.source_id, cfg.metric, count=len(events))
        self._metrics.cycle_duration_observe(
            cfg.source_id,
            cfg.metric,
            duration_ms=int((asyncio.get_running_loop().time() - cycle_start) * 1000),
        )
        return events

    async def _retry_cycle(
        self,
        cfg: ExternalSourceRuntime,
        log: Any,  # loguru binder
    ) -> None:
        """Reintenta el ciclo con backoff; agotados los intentos, pasa de ciclo."""
        for attempt in range(1, cfg.max_attempts + 1):
            if self._stop_event.is_set():
                return
            await self._sleep_until_stop(cfg.next_backoff_s(attempt))
            try:
                events = await self._run_cycle(cfg, self._source_for(cfg.source_id))
                log.bind(attempt=attempt).info("retry_success events={}", len(events))
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — error de fuente en retry
                log.bind(attempt=attempt).warning("retry_failed | {}", exc)
        log.error("cycle_failed — reintentos agotados")

    # ── helpers ───────────────────────────────────────────────────────────────

    def _next_wait_s(self, cfg: ExternalSourceRuntime, elapsed_s: float) -> float:
        """Espera hasta el próximo ciclo respetando schedule y rate limit."""
        schedule_wait = max(0.0, float(cfg.schedule_every_s) - elapsed_s)
        rate_wait = max(0.0, cfg.min_poll_interval_s - elapsed_s)
        return max(schedule_wait, rate_wait)

    async def _sleep_until_stop(self, seconds: float) -> None:
        """sleep interrumpible por shutdown."""
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return

    def _install_signal_handlers(self) -> None:
        """Registra SIGINT/SIGTERM → stop (el orquestador es dueño de señales)."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._stop_event.set)
