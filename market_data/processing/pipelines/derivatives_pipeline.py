from __future__ import annotations

"""
market_data/processing/pipelines/derivatives_pipeline.py
=========================================================

Pipeline de ingestion de derivados.

Dominio
-------
Métricas de mercado de derivados con schema variable por tipo:
  funding_rate  — tasa de financiación periódica (cada 8h típico)
  open_interest — contratos abiertos agregados (snapshot por intervalo)
  liquidations  — posiciones liquidadas forzosamente (event-driven)

Diferencias vs OHLCVPipeline
------------------------------
- Schema variable : cada métrica tiene columnas propias
- Sin timeframe   : resolución depende del endpoint
- Fuentes mixtas  : APIs del exchange + proveedores externos (Coinglass)
- Storage         : particionado por métrica, no por símbolo×timeframe
- Sin repair      : gaps se rellenan en modo incremental

Estado: esqueleto funcional. run() lanza NotImplementedError
hasta que los fetchers por métrica estén implementados.

Principios: SOLID · KISS · DRY · SafeOps
"""

from dataclasses import dataclass, field
from typing import List, Literal, Optional

from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

DerivativesPipelineMode = Literal["incremental", "backfill"]

#: Datasets soportados — fuente de verdad para validación en constructor.
SUPPORTED_DERIVATIVE_DATASETS: frozenset[str] = frozenset(
    {"funding_rate", "open_interest", "liquidations"}
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
        """True si no hay error pero tampoco rows nuevos."""
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
        """Estado agregado: ok | partial | failed."""
        if self.failed == 0:
            return "ok"
        if self.succeeded > 0:
            return "partial"
        return "failed"

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class DerivativesPipeline:
    """
    Pipeline de ingestion de derivados.

    Uso previsto::

        pipeline = DerivativesPipeline(
            symbols         = ["BTC/USDT"],
            datasets        = ["funding_rate", "open_interest"],
            exchange_client = adapter,
            dry_run         = False,
        )
        summary = await pipeline.run(mode="incremental")

    Invariantes de construcción
    ---------------------------
    - symbols         : lista no vacía
    - datasets        : subconjunto no vacío de SUPPORTED_DERIVATIVE_DATASETS
    - exchange_client : instancia CCXTAdapter (no None)

    SafeOps
    -------
    Constructor valida datasets desconocidos (fail-fast con mensaje claro).
    run() lanza NotImplementedError explícito — nunca falla silenciosamente.
    """

    def __init__(
        self,
        symbols:         List[str],
        datasets:        List[str],
        exchange_client: CCXTAdapter,
        dry_run:         bool = False,
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

        self.symbols:      List[str] = symbols
        self.datasets:     List[str] = list(datasets)
        self.dry_run:      bool      = dry_run
        self._exchange_id: str       = getattr(
            exchange_client, "_exchange_id", "unknown"
        )

    async def run(
        self, mode: DerivativesPipelineMode = "incremental"
    ) -> DerivativesSummary:
        """
        Ejecuta la ingestion de derivados.

        Raises
        ------
        NotImplementedError
            Hasta que los fetchers por métrica estén implementados.
            Deshabilitar con 'datasets.<metric>: false' en settings.yaml.
        """
        raise NotImplementedError(
            f"DerivativesPipeline.run() no implementado | "
            f"exchange={self._exchange_id} datasets={self.datasets} mode={mode}. "
            "Implementar fetchers por métrica antes de habilitar. "
            "Deshabilitar con 'datasets.<metric>: false' en settings.yaml."
        )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"DerivativesPipeline("
            f"exchange={self._exchange_id!r}, "
            f"symbols={len(self.symbols)}, "
            f"datasets={self.datasets}, "
            f"dry_run={self.dry_run})"
        )
