# -*- coding: utf-8 -*-
"""
market_data/processing/pipelines/derivatives_pipeline.py
=========================================================

Pipeline de ingestion de derivados.

Dominio
-------
Métricas de mercado de derivados con schema variable por tipo:
  funding_rate  — tasa de financiación periódica (cada 8h típico)
  open_interest — contratos abiertos agregados (snapshot por intervalo)

Liquidaciones pendientes: requieren endpoint distinto no disponible
en todos los exchanges via CCXT unified.

Diferencias vs OHLCVPipeline
------------------------------
- Schema variable : cada métrica tiene columnas propias
- Sin timeframe   : resolución depende del endpoint
- Sin repair      : gaps se rellenan en modo incremental

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Literal, Optional

from market_data.processing.pipelines._worker_pool import run_worker_pool
from market_data.ports.input.pipeline_trigger import PipelineTriggerPort

from loguru import logger

from market_data.adapters.exchange import CCXTAdapter
from market_data.storage.silver.derivatives_storage import DerivativesStorage
from market_data.adapters.inbound.rest.derivatives_fetcher import (
    FundingRateFetcher,
    OpenInterestFetcher,
)

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

DerivativesPipelineMode = Literal["incremental", "backfill"]

SUPPORTED_DERIVATIVE_DATASETS: frozenset[str] = frozenset(
    {"funding_rate", "open_interest"}
)

# ---------------------------------------------------------------------------
# Result / Summary
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class DerivativesResult:
    """Resultado de ingestion para un par (dataset, símbolo)."""
    dataset:     str
    symbol:      str
    success:     bool          = False
    rows:        int           = 0
    error:       Optional[str] = None
    duration_ms: int           = 0

    @property
    def skipped(self) -> bool:
        return self.success and self.rows == 0


@dataclass
class DerivativesSummary:
    """Resumen agregado de un run de DerivativesPipeline."""
    results:     List[DerivativesResult] = field(default_factory=list)
    duration_ms: int                     = 0
    mode:        str                     = "incremental"

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.success and r.error is not None)

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.skipped)

    @property
    def total_rows(self) -> int:
        return sum(r.rows for r in self.results)

    @property
    def status(self) -> str:
        if self.failed == 0:
            return "ok"
        if self.succeeded > 0:
            return "partial"
        return "failed"

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------



class DerivativesPipeline(PipelineTriggerPort):
    """
    Pipeline de ingestion de derivados.

    Ejecuta un fetcher por dataset × símbolo en paralelo controlado.
    Cada par (dataset, symbol) es una unidad de trabajo independiente.

    SafeOps
    -------
    Constructor valida datasets desconocidos (fail-fast).
    Errores por par son capturados — nunca abortan el pipeline.
    """

    def __init__(
        self,
        symbols:         List[str],
        datasets:        List[str],
        exchange_client: CCXTAdapter,
        market_type:     str  = "swap",
        dry_run:         bool = False,
        max_concurrency: int  = 4,
    ) -> None:
        if not symbols:
            raise ValueError("DerivativesPipeline: symbols no puede estar vacío")
        if not datasets:
            raise ValueError("DerivativesPipeline: datasets no puede estar vacío")
        if exchange_client is None:
            raise ValueError("DerivativesPipeline: exchange_client es obligatorio")

        unknown = set(datasets) - SUPPORTED_DERIVATIVE_DATASETS
        if unknown:
            raise ValueError(
                f"DerivativesPipeline: datasets no soportados: {sorted(unknown)}. "
                f"Soportados: {sorted(SUPPORTED_DERIVATIVE_DATASETS)}"
            )

        self.symbols:         List[str] = symbols
        self.datasets:        List[str] = list(datasets)
        self.market_type:     str       = market_type.lower()
        self.dry_run:         bool      = dry_run
        self.max_concurrency: int       = max_concurrency
        self._exchange_id:    str       = getattr(
            exchange_client, "_exchange_id", "unknown"
        )
        self._log = logger.bind(
            exchange=self._exchange_id, pipeline="derivatives",
        )

        # Catalog — inyectado o resuelto desde entorno (SafeOps)
        _catalog = getattr(exchange_client, '_catalog', None)
        if _catalog is None and not dry_run:
            from market_data.storage.iceberg.catalog import get_catalog
            _catalog = get_catalog()
        self._fetchers: dict[str, object] = {}
        if "funding_rate" in datasets:
            self._fetchers["funding_rate"] = FundingRateFetcher(
                exchange_client = exchange_client,
                storage         = DerivativesStorage(
                    dataset     = "funding_rate",
                    exchange    = self._exchange_id,
                    market_type = self.market_type,
                    catalog     = _catalog,
                    dry_run     = dry_run,
                ),
                market_type     = self.market_type,
                dry_run         = dry_run,
            )
        if "open_interest" in datasets:
            self._fetchers["open_interest"] = OpenInterestFetcher(
                exchange_client = exchange_client,
                storage         = DerivativesStorage(
                    dataset     = "open_interest",
                    exchange    = self._exchange_id,
                    market_type = self.market_type,
                    catalog     = _catalog,
                    dry_run     = dry_run,
                ),
                market_type     = self.market_type,
                dry_run         = dry_run,
            )

    async def run(
        self, mode: DerivativesPipelineMode = "incremental"
    ) -> DerivativesSummary:
        """
        Ejecuta la ingestion de derivados para todos los pares (dataset × símbolo).

        SafeOps: errores por par son capturados y logueados — el pipeline
        continúa con los demás pares.
        """
        pairs = [(ds, sym) for ds in self.datasets for sym in self.symbols]

        self._log.info(
            "DerivativesPipeline start | mode={} datasets={}"
            " symbols={} pairs={} concurrency={}",
            mode, self.datasets, len(self.symbols),
            len(pairs), self.max_concurrency,
        )

        pipeline_start = time.monotonic()
        results        = await self._run_worker_pool(pairs)
        duration_ms    = int((time.monotonic() - pipeline_start) * 1000)

        summary = DerivativesSummary(
            results     = results,
            duration_ms = duration_ms,
            mode        = mode,
        )

        self._log.info(
            "DerivativesPipeline done | ok={} failed={} skipped={}"
            " total_rows={} duration_ms={}",
            summary.succeeded, summary.failed,
            summary.skipped, summary.total_rows, duration_ms,
        )
        return summary

    async def _run_worker_pool(
        self, pairs: List[tuple[str, str]]
    ) -> List[DerivativesResult]:
        """Delega al worker pool genérico — sin lógica de concurrencia local."""
        total = len(pairs)
        items = [(idx, ds, sym) for idx, (ds, sym) in enumerate(pairs, 1)]

        async def _execute(item) -> DerivativesResult:
            idx, dataset, symbol = item
            return await self._fetch_pair(dataset, symbol, idx, total)

        results, _ = await run_worker_pool(
            items           = items,
            execute_fn      = _execute,
            max_concurrency = self.max_concurrency,
            exchange_id     = self._exchange_id,
            log             = self._log,
        )
        return results

    async def _fetch_pair(
        self, dataset: str, symbol: str, idx: int, total: int
    ) -> DerivativesResult:
        """Fetcha un par (dataset, símbolo) con captura de errores."""
        start   = time.monotonic()
        fetcher = self._fetchers.get(dataset)
        if fetcher is None:
            return DerivativesResult(
                dataset     = dataset,
                symbol      = symbol,
                success     = False,
                error       = f"No fetcher for dataset={dataset!r}",
                duration_ms = 0,
            )
        try:
            rows = await fetcher.fetch_symbol(symbol)
            duration_ms = int((time.monotonic() - start) * 1000)
            self._log.info(
                "  [{}/{}] {}/{} | rows={} duration={}ms",
                idx, total, dataset, symbol, rows, duration_ms,
            )
            return DerivativesResult(
                dataset     = dataset,
                symbol      = symbol,
                success     = True,
                rows        = rows,
                duration_ms = duration_ms,
            )
        except Exception as exc:
            duration_ms = int((time.monotonic() - start) * 1000)
            self._log.error(
                "  [{}/{}] {}/{} FAILED | err={} duration={}ms",
                idx, total, dataset, symbol, exc, duration_ms,
            )
            return DerivativesResult(
                dataset     = dataset,
                symbol      = symbol,
                success     = False,
                error       = str(exc),
                duration_ms = duration_ms,
            )

    def __repr__(self) -> str:
        return (
            f"DerivativesPipeline("
            f"exchange={self._exchange_id!r}, "
            f"symbols={len(self.symbols)}, "
            f"datasets={self.datasets}, "
            f"dry_run={self.dry_run})"
        )
