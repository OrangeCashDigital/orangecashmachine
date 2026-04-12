from __future__ import annotations

"""
market_data/processing/pipelines/trades_pipeline.py
====================================================

Pipeline de ingestion de trades (tick data).

Dominio
-------
Transacciones individuales — sin timeframe, append-only.
Schema: timestamp, price, amount, side, trade_id.

Diferencias vs OHLCVPipeline
------------------------------
- Sin timeframe  : cursor por símbolo, no por símbolo×timeframe
- Volumen masivo : paginación agresiva, storage append-only
- Sin repair     : trades son inmutables por definición

Estado: esqueleto funcional. run() lanza NotImplementedError
hasta que HistoricalTradesFetcher esté implementado.

Principios: SOLID · KISS · DRY · SafeOps
"""

from dataclasses import dataclass, field
from typing import List, Literal, Optional

from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

TradesPipelineMode = Literal["incremental", "backfill"]

# ---------------------------------------------------------------------------
# Result / Summary
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class TradesResult:
    """Resultado de ingestion para un símbolo."""

    symbol:      str
    success:     bool          = False
    rows:        int           = 0
    error:       Optional[str] = None
    duration_ms: int           = 0

    @property
    def skipped(self) -> bool:
        """True si no hay error pero tampoco rows nuevos (datos al día)."""
        return self.success and self.rows == 0


@dataclass
class TradesSummary:
    """Resumen agregado de un run de TradesPipeline."""

    results:     List[TradesResult] = field(default_factory=list)
    duration_ms: int                = 0
    mode:        str                = "incremental"

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


class TradesPipeline:
    """
    Pipeline de ingestion de trades (tick data).

    Uso previsto::

        pipeline = TradesPipeline(
            symbols         = ["BTC/USDT"],
            exchange_client = adapter,
            market_type     = "spot",
            dry_run         = False,
        )
        summary = await pipeline.run(mode="incremental")

    Invariantes de construcción
    ---------------------------
    - symbols         : lista no vacía
    - exchange_client : instancia CCXTAdapter (no None)
    - max_concurrency : >= 1

    SafeOps
    -------
    Constructor valida en tiempo de construcción (fail-fast).
    run() lanza NotImplementedError explícito — nunca falla silenciosamente.
    """

    def __init__(
        self,
        symbols:         List[str],
        exchange_client: CCXTAdapter,
        market_type:     str  = "spot",
        dry_run:         bool = False,
        max_concurrency: int  = 4,
    ) -> None:
        if not symbols:
            raise ValueError("TradesPipeline: symbols no puede estar vacío")
        if exchange_client is None:
            raise ValueError("TradesPipeline: exchange_client es obligatorio")
        if max_concurrency < 1:
            raise ValueError("TradesPipeline: max_concurrency debe ser >= 1")

        self.symbols:         List[str] = symbols
        self.market_type:     str       = market_type.lower()
        self.dry_run:         bool      = dry_run
        self.max_concurrency: int       = max_concurrency
        self._exchange_id:    str       = getattr(
            exchange_client, "_exchange_id", "unknown"
        )

    async def run(self, mode: TradesPipelineMode = "incremental") -> TradesSummary:
        """
        Ejecuta la ingestion de trades.

        Raises
        ------
        NotImplementedError
            Hasta que HistoricalTradesFetcher esté implementado.
            Deshabilitar con 'datasets.trades: false' en settings.yaml.
        """
        raise NotImplementedError(
            f"TradesPipeline.run() no implementado | "
            f"exchange={self._exchange_id} mode={mode}. "
            "Implementar HistoricalTradesFetcher antes de habilitar. "
            "Deshabilitar con 'datasets.trades: false' en settings.yaml."
        )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"TradesPipeline("
            f"exchange={self._exchange_id!r}, "
            f"symbols={len(self.symbols)}, "
            f"market_type={self.market_type!r}, "
            f"dry_run={self.dry_run})"
        )
