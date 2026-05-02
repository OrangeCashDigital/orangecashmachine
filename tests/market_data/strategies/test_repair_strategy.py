"""
tests/market_data/strategies/test_repair_strategy.py
=====================================================

Suite completa de RepairStrategy — cero dependencias externas.

Principios de test
------------------
Aislamiento total   : stubs inline, sin Redis, sin exchange real, sin Iceberg.
Fail-Fast           : cada test verifica una sola responsabilidad.
DRY                 : _make_ctx() y _make_df() centralizan construcción.
SSOT                : GapRange importado desde gap_utils (misma fuente que prod).
Nomenclatura        : test_<método>_<condición>_<resultado_esperado>

Cobertura objetivo
------------------
execute_pair:
  - skip si Silver vacío
  - skip si sin gaps
  - healed_count correcto con gaps sanables
  - partial_count correcto con fill_ratio < 0.95
  - gap demasiado grande → skip (guardrail _MAX_HEALABLE_GAP_CANDLES)
  - gap irrecuperable conocido → skip (GapRegistry)
  - error en gather → contado pero no propaga
  - CancelledError propagado

_read_silver:
  - retorna None si storage lanza excepción (SafeOps)
  - retorna columnas filtradas si columns_only

_heal_gap:
  - NoDataAvailableError si exchange retorna vacío
  - NoDataAvailableError si df vacío tras filtro
  - retorna (False, 0, 0.0) si calidad rechaza
  - retorna (True, rows, fill_ratio) en camino feliz
  - marca irrecuperable en GapRegistry si NoDataAvailableError
  - ChunkFetchError → (False, 0, 0.0) sin marcar irrecuperable
  - CancelledError propagado en _heal_gap
"""
from __future__ import annotations

import asyncio
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from market_data.processing.strategies.repair import RepairStrategy
from market_data.processing.strategies.base import (
    PipelineContext,
)
from market_data.processing.exceptions import (
    ChunkFetchError,
)
from market_data.processing.utils.gap_utils import GapRange


# ══════════════════════════════════════════════════════════════════════════════
# Stubs — sin dependencias externas
# ══════════════════════════════════════════════════════════════════════════════

class _StorageStub:
    """OHLCVStorage stub — comportamiento configurable por test."""

    def __init__(
        self,
        df:            Optional[pd.DataFrame] = None,
        raise_on_load: bool                   = False,
    ) -> None:
        self._df            = df
        self._raise_on_load = raise_on_load
        self.saved:         list[pd.DataFrame] = []

    def load_ohlcv(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        if self._raise_on_load:
            raise RuntimeError("storage unavailable")
        return self._df

    def save_ohlcv(self, df: pd.DataFrame, symbol: str, timeframe: str) -> None:
        self.saved.append(df)


class _FetcherStub:
    """HistoricalFetcherAsync stub — retorna raw OHLCV configurable."""

    def __init__(self, chunks: list[list] | None = None) -> None:
        # chunks es una lista de listas — cada llamada consume el siguiente chunk.
        # Si es None → retorna lista vacía siempre (exchange sin datos).
        self._chunks      = list(chunks or [])
        self._call_count  = 0

    async def fetch_chunk(
        self,
        symbol:    str,
        timeframe: str,
        since:     Optional[int],
        limit:     int,
        end_ms:    Optional[int] = None,
    ) -> list:
        if not self._chunks:
            return []
        chunk = self._chunks[0]
        self._chunks = self._chunks[1:]
        self._call_count += 1
        return chunk


class _QualityStub:
    """QualityPipeline stub — acepta o rechaza según configuración."""

    def __init__(self, accept: bool = True) -> None:
        self._accept = accept

    def run(
        self,
        df:       pd.DataFrame,
        symbol:   str,
        timeframe: str,
        exchange: str,
    ):
        result     = MagicMock()
        result.accepted = self._accept
        result.score    = 1.0 if self._accept else 0.1
        result.df       = df
        return result


class _GapRegistryStub:
    """GapRegistryPort stub — controla is_irrecoverable y registra mark_healed."""

    def __init__(self, irrecoverable_starts: set[int] | None = None) -> None:
        self._irrecoverable = set(irrecoverable_starts or [])
        self.healed_calls:  list[dict] = []

    def is_irrecoverable(
        self,
        exchange:  str,
        symbol:    str,
        timeframe: str,
        start_ms:  int,
    ) -> bool:
        return start_ms in self._irrecoverable

    def mark_healed(self, **kwargs) -> None:
        self.healed_calls.append(kwargs)


# ══════════════════════════════════════════════════════════════════════════════
# Factories — DRY, SSOT de construcción
# ══════════════════════════════════════════════════════════════════════════════

_TF = "1h"
_TF_MS = 3_600_000   # 1h en ms
_EXCHANGE = "bybit"
_SYMBOL   = "BTC/USDT"


def _make_df(n_rows: int = 10, start_ms: int = 0) -> pd.DataFrame:
    """DataFrame OHLCV mínimo con timestamps válidos y contiguos."""
    timestamps = [
        pd.Timestamp(start_ms + i * _TF_MS, unit="ms", tz="UTC")
        for i in range(n_rows)
    ]
    return pd.DataFrame({
        "timestamp": timestamps,
        "open":  [100.0] * n_rows,
        "high":  [110.0] * n_rows,
        "low":   [90.0]  * n_rows,
        "close": [105.0] * n_rows,
        "volume":[1000.0] * n_rows,
    })


class _SentinelType:
    pass
_SENTINEL = _SentinelType()

def _make_ctx(
    df:              Optional[pd.DataFrame] = None,
    raise_on_load:   bool                   = False,
    chunks:          list[list] | None      = None,
    quality_accept:  bool                   = True,
    irrecoverable:   set[int] | None        = None,
    gap_registry:    object | None          = _SENTINEL,
) -> PipelineContext:
    """
    Construye un PipelineContext completamente falso — sin infra real.
    gap_registry=_SENTINEL → usa _GapRegistryStub() por defecto.
    gap_registry=None       → simula modo degradado (SafeOps).
    """
    if gap_registry is _SENTINEL:
        gap_registry = _GapRegistryStub(irrecoverable_starts=irrecoverable)

    return PipelineContext(
        fetcher      = _FetcherStub(chunks=chunks),
        storage      = _StorageStub(df=df, raise_on_load=raise_on_load),
        bronze       = MagicMock(),
        cursor       = MagicMock(),
        quality      = _QualityStub(accept=quality_accept),
        exchange_id  = _EXCHANGE,
        market_type  = "spot",
        start_date   = "2021-01-01",
        gap_registry = gap_registry,
    )


# Sentinel para distinguir "no pasado" de None

def _make_gap(
    start_ms: int,
    expected: int,
    tf_ms:    int = _TF_MS,
) -> GapRange:
    """GapRange mínimo — end_ms calculado desde expected."""
    end_ms = start_ms + (expected + 1) * tf_ms
    return GapRange(start_ms=start_ms, end_ms=end_ms, expected=expected)


def _raw_candles(
    start_ms: int,
    n:        int,
    tf_ms:    int = _TF_MS,
) -> list:
    """Lista de raw OHLCV candles como retorna CCXT."""
    return [
        [start_ms + i * tf_ms, 100.0, 110.0, 90.0, 105.0, 1000.0]
        for i in range(n)
    ]


# ══════════════════════════════════════════════════════════════════════════════
# execute_pair — camino principal
# ══════════════════════════════════════════════════════════════════════════════

class TestExecutePairSkip:

    @pytest.mark.asyncio
    async def test_skip_if_silver_empty(self):
        """Si Silver no tiene datos → result.skipped=True sin tocar el exchange."""
        strategy = RepairStrategy()
        ctx      = _make_ctx(df=None)

        result = await strategy.execute_pair(
            symbol=_SYMBOL, timeframe=_TF, idx=0, total=1, ctx=ctx,
        )

        assert result.skipped is True
        assert result.rows == 0
        assert result.gaps_found == 0

    @pytest.mark.asyncio
    async def test_skip_if_no_gaps(self):
        """DataFrame contiguo → scan_gaps vacío → result.skipped=True."""
        # 10 velas contiguas: ningún gap
        df       = _make_df(n_rows=10, start_ms=0)
        strategy = RepairStrategy()
        ctx      = _make_ctx(df=df)

        result = await strategy.execute_pair(
            symbol=_SYMBOL, timeframe=_TF, idx=0, total=1, ctx=ctx,
        )

        assert result.skipped is True
        assert result.gaps_found == 0

    @pytest.mark.asyncio
    async def test_skip_if_silver_raises(self):
        """Si storage.load_ohlcv lanza → _read_silver retorna None → skip."""
        strategy = RepairStrategy()
        ctx      = _make_ctx(raise_on_load=True)

        result = await strategy.execute_pair(
            symbol=_SYMBOL, timeframe=_TF, idx=0, total=1, ctx=ctx,
        )

        assert result.skipped is True


class TestExecutePairHealing:

    @pytest.mark.asyncio
    async def test_healed_count_full_fill(self):
        """Gap con fill_ratio >= 0.95 → healed_count=1, gaps_partial=0."""
        # DataFrame con 1 gap: vela 0 → vela 5 (gap de 4 velas)
        gap_end_ms   = 5 * _TF_MS

        rows_before = [
            pd.Timestamp(0,          unit="ms", tz="UTC"),
            pd.Timestamp(gap_end_ms, unit="ms", tz="UTC"),
        ]
        df = pd.DataFrame({
            "timestamp": rows_before,
            "open": [100.0, 100.0], "high": [110.0, 110.0],
            "low":  [90.0,  90.0],  "close":[105.0, 105.0],
            "volume": [1000.0, 1000.0],
        })

        # Exchange devuelve 4 velas que llenan el gap completamente
        gap_candles = _raw_candles(start_ms=_TF_MS, n=4)
        strategy    = RepairStrategy()
        ctx         = _make_ctx(df=df, chunks=[gap_candles])

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            result = await strategy.execute_pair(
                symbol=_SYMBOL, timeframe=_TF, idx=0, total=1, ctx=ctx,
            )

        assert result.gaps_found   >= 1
        assert result.gaps_healed  == 1
        assert result.gaps_partial == 0
        assert result.rows         == 4

    @pytest.mark.asyncio
    async def test_partial_count_low_fill_ratio(self):
        """Gap con fill_ratio < 0.95 → gaps_partial=1, healed_count=0."""
        gap_end_ms   = 20 * _TF_MS  # expected=19

        rows_before = [
            pd.Timestamp(0,          unit="ms", tz="UTC"),
            pd.Timestamp(gap_end_ms, unit="ms", tz="UTC"),
        ]
        df = pd.DataFrame({
            "timestamp": rows_before,
            "open": [100.0, 100.0], "high": [110.0, 110.0],
            "low":  [90.0,  90.0],  "close":[105.0, 105.0],
            "volume": [1000.0, 1000.0],
        })

        # Solo 5 velas de 19 esperadas → fill_ratio ≈ 0.26 < 0.95
        gap_candles = _raw_candles(start_ms=_TF_MS, n=5)
        strategy    = RepairStrategy()
        ctx         = _make_ctx(df=df, chunks=[gap_candles])

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            result = await strategy.execute_pair(
                symbol=_SYMBOL, timeframe=_TF, idx=0, total=1, ctx=ctx,
            )

        assert result.gaps_partial >= 1
        assert result.gaps_healed  == 0

    @pytest.mark.asyncio
    async def test_gap_too_large_skipped(self):
        """Gap > _MAX_HEALABLE_GAP_CANDLES → skip con métrica, no fetch."""
        from market_data.processing.strategies.repair import _MAX_HEALABLE_GAP_CANDLES

        # DataFrame con gap enorme
        gap_end_ms = (_MAX_HEALABLE_GAP_CANDLES + 100) * _TF_MS
        rows_before = [
            pd.Timestamp(0,          unit="ms", tz="UTC"),
            pd.Timestamp(gap_end_ms, unit="ms", tz="UTC"),
        ]
        df = pd.DataFrame({
            "timestamp": rows_before,
            "open": [100.0, 100.0], "high": [110.0, 110.0],
            "low":  [90.0,  90.0],  "close":[105.0, 105.0],
            "volume": [1000.0, 1000.0],
        })

        strategy = RepairStrategy()
        ctx      = _make_ctx(df=df, chunks=[])

        result = await strategy.execute_pair(
            symbol=_SYMBOL, timeframe=_TF, idx=0, total=1, ctx=ctx,
        )

        # El gap fue encontrado pero skipped — result no debe tener healed_count
        assert result.gaps_found  >= 1
        assert result.gaps_healed == 0
        # El fetcher no fue llamado (gap demasiado grande → guardrail antes del fetch)
        assert ctx.fetcher._call_count == 0

    @pytest.mark.asyncio
    async def test_irrecoverable_gap_skipped(self):
        """Gap marcado irrecuperable en registry → skip sin fetch."""
        gap_start_ms = 0
        gap_end_ms   = 5 * _TF_MS

        rows_before = [
            pd.Timestamp(0,          unit="ms", tz="UTC"),
            pd.Timestamp(gap_end_ms, unit="ms", tz="UTC"),
        ]
        df = pd.DataFrame({
            "timestamp": rows_before,
            "open": [100.0, 100.0], "high": [110.0, 110.0],
            "low":  [90.0,  90.0],  "close":[105.0, 105.0],
            "volume": [1000.0, 1000.0],
        })

        # El gap_start_ms=0 está marcado irrecuperable
        strategy = RepairStrategy()
        ctx      = _make_ctx(df=df, chunks=[], irrecoverable={gap_start_ms})

        result = await strategy.execute_pair(
            symbol=_SYMBOL, timeframe=_TF, idx=0, total=1, ctx=ctx,
        )

        assert result.gaps_found  >= 1
        assert result.gaps_healed == 0
        assert ctx.fetcher._call_count == 0

    @pytest.mark.asyncio
    async def test_exception_in_gather_counted_not_propagated(self):
        """Excepción en gather → contada en métricas, result sin crash."""
        gap_end_ms  = 5 * _TF_MS
        rows_before = [
            pd.Timestamp(0,         unit="ms", tz="UTC"),
            pd.Timestamp(gap_end_ms,unit="ms", tz="UTC"),
        ]
        df = pd.DataFrame({
            "timestamp": rows_before,
            "open": [100.0, 100.0], "high": [110.0, 110.0],
            "low":  [90.0,  90.0],  "close":[105.0, 105.0],
            "volume": [1000.0, 1000.0],
        })

        # Fetcher lanza RuntimeError — simulando fallo inesperado en _heal_gap
        bad_fetcher = MagicMock()
        bad_fetcher.fetch_chunk = AsyncMock(side_effect=RuntimeError("boom"))

        strategy = RepairStrategy()
        ctx      = _make_ctx(df=df)
        ctx      = PipelineContext(
            fetcher      = bad_fetcher,
            storage      = _StorageStub(df=df),
            bronze       = MagicMock(),
            cursor       = MagicMock(),
            quality      = _QualityStub(),
            exchange_id  = _EXCHANGE,
            market_type  = "spot",
            start_date   = "2021-01-01",
            gap_registry = _GapRegistryStub(),
        )

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            # No debe propagar — gather con return_exceptions=True
            result = await strategy.execute_pair(
                symbol=_SYMBOL, timeframe=_TF, idx=0, total=1, ctx=ctx,
            )

        assert result.gaps_healed == 0  # no healed
        assert result.error is None     # el error fue en gap, no en execute_pair

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates(self):
        """CancelledError no se traga — debe propagar."""
        df = _make_df(n_rows=2, start_ms=0)
        # df con gap artificial: segunda vela muy lejos
        df.loc[1, "timestamp"] = pd.Timestamp(10 * _TF_MS, unit="ms", tz="UTC")

        bad_fetcher = MagicMock()
        bad_fetcher.fetch_chunk = AsyncMock(side_effect=asyncio.CancelledError())

        strategy = RepairStrategy()
        ctx = PipelineContext(
            fetcher      = bad_fetcher,
            storage      = _StorageStub(df=df),
            bronze       = MagicMock(),
            cursor       = MagicMock(),
            quality      = _QualityStub(),
            exchange_id  = _EXCHANGE,
            market_type  = "spot",
            start_date   = "2021-01-01",
            gap_registry = _GapRegistryStub(),
        )

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            with pytest.raises(asyncio.CancelledError):
                await strategy.execute_pair(
                    symbol=_SYMBOL, timeframe=_TF, idx=0, total=1, ctx=ctx,
                )


# ══════════════════════════════════════════════════════════════════════════════
# _read_silver — SafeOps y filtrado de columnas
# ══════════════════════════════════════════════════════════════════════════════

class TestReadSilver:

    def test_returns_none_if_storage_raises(self):
        """SafeOps: si storage lanza → None sin propagar."""
        strategy = RepairStrategy()
        ctx      = _make_ctx(raise_on_load=True)

        result = strategy._read_silver(ctx, _SYMBOL, _TF)
        assert result is None

    def test_returns_none_if_df_empty(self):
        """Si storage retorna DataFrame vacío → None."""
        strategy = RepairStrategy()
        ctx      = _make_ctx(df=pd.DataFrame())

        result = strategy._read_silver(ctx, _SYMBOL, _TF)
        assert result is None

    def test_returns_none_if_storage_returns_none(self):
        """Si storage retorna None explícito → None."""
        strategy = RepairStrategy()
        ctx      = _make_ctx(df=None)

        result = strategy._read_silver(ctx, _SYMBOL, _TF)
        assert result is None

    def test_columns_only_filters_correctly(self):
        """columns_only=['timestamp'] → solo columna timestamp en resultado."""
        df       = _make_df(n_rows=5)
        strategy = RepairStrategy()
        ctx      = _make_ctx(df=df)

        result = strategy._read_silver(ctx, _SYMBOL, _TF, columns_only=["timestamp"])
        assert result is not None
        assert list(result.columns) == ["timestamp"]
        assert len(result) == 5

    def test_columns_only_ignores_missing_columns(self):
        """columns_only con columna inexistente → columnas válidas solamente."""
        df       = _make_df(n_rows=3)
        strategy = RepairStrategy()
        ctx      = _make_ctx(df=df)

        result = strategy._read_silver(
            ctx, _SYMBOL, _TF, columns_only=["timestamp", "nonexistent"]
        )
        assert result is not None
        assert "timestamp" in result.columns
        assert "nonexistent" not in result.columns

    def test_full_df_returned_without_columns_only(self):
        """Sin columns_only → retorna DataFrame completo."""
        df       = _make_df(n_rows=5)
        strategy = RepairStrategy()
        ctx      = _make_ctx(df=df)

        result = strategy._read_silver(ctx, _SYMBOL, _TF)
        assert result is not None
        assert set(result.columns) == {"timestamp", "open", "high", "low", "close", "volume"}


# ══════════════════════════════════════════════════════════════════════════════
# _heal_gap — caminos individuales
# ══════════════════════════════════════════════════════════════════════════════

class TestHealGap:

    def _forward_ctx(self, chunks=None, quality_accept=True, registry=None):
        """Helper: ctx con quirks forward (Bybit-like)."""
        if registry is _SENTINEL:
            registry = _GapRegistryStub()
        return PipelineContext(
            fetcher      = _FetcherStub(chunks=chunks or []),
            storage      = _StorageStub(),
            bronze       = MagicMock(),
            cursor       = MagicMock(),
            quality      = _QualityStub(accept=quality_accept),
            exchange_id  = _EXCHANGE,
            market_type  = "spot",
            start_date   = "2021-01-01",
            gap_registry = _GapRegistryStub() if registry is _SENTINEL else registry,
        )

    @pytest.mark.asyncio
    async def test_no_data_available_if_exchange_empty(self):
        """Exchange retorna lista vacía → NoDataAvailableError."""
        strategy = RepairStrategy()
        gap      = _make_gap(start_ms=_TF_MS, expected=4)
        ctx      = self._forward_ctx(chunks=[])

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            # _heal_gap debe retornar (False, 0, 0.0) — NoDataAvailableError atrapada
            result = await strategy._heal_gap(
                gap=gap, symbol=_SYMBOL, timeframe=_TF, ctx=ctx,
            )

        assert result == (False, 0, 0.0)

    @pytest.mark.asyncio
    async def test_no_data_available_if_df_empty_after_filter(self):
        """Exchange retorna velas fuera de la ventana del gap → NoDataAvailableError."""
        strategy = RepairStrategy()
        gap      = _make_gap(start_ms=100 * _TF_MS, expected=4)

        # Velas en timestamp=0, muy lejos del gap → filtradas → df vacío
        far_candles = _raw_candles(start_ms=0, n=4)
        ctx         = self._forward_ctx(chunks=[far_candles])

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            result = await strategy._heal_gap(
                gap=gap, symbol=_SYMBOL, timeframe=_TF, ctx=ctx,
            )

        assert result == (False, 0, 0.0)

    @pytest.mark.asyncio
    async def test_quality_rejected_returns_false(self):
        """QualityPipeline rechaza → (False, 0, 0.0) sin guardar en storage."""
        strategy = RepairStrategy()
        gap      = _make_gap(start_ms=_TF_MS, expected=4)
        candles  = _raw_candles(start_ms=_TF_MS, n=4)
        storage  = _StorageStub()
        ctx      = PipelineContext(
            fetcher      = _FetcherStub(chunks=[candles]),
            storage      = storage,
            bronze       = MagicMock(),
            cursor       = MagicMock(),
            quality      = _QualityStub(accept=False),
            exchange_id  = _EXCHANGE,
            market_type  = "spot",
            start_date   = "2021-01-01",
            gap_registry = _GapRegistryStub(),
        )

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            result = await strategy._heal_gap(
                gap=gap, symbol=_SYMBOL, timeframe=_TF, ctx=ctx,
            )

        assert result == (False, 0, 0.0)
        assert len(storage.saved) == 0

    @pytest.mark.asyncio
    async def test_happy_path_returns_true_rows_fill_ratio(self):
        """Camino feliz: 4 velas → (True, 4, fill_ratio)."""
        strategy = RepairStrategy()
        gap      = _make_gap(start_ms=_TF_MS, expected=4)
        candles  = _raw_candles(start_ms=_TF_MS, n=4)
        storage  = _StorageStub()
        ctx      = PipelineContext(
            fetcher      = _FetcherStub(chunks=[candles]),
            storage      = storage,
            bronze       = MagicMock(),
            cursor       = MagicMock(),
            quality      = _QualityStub(accept=True),
            exchange_id  = _EXCHANGE,
            market_type  = "spot",
            start_date   = "2021-01-01",
            gap_registry = _GapRegistryStub(),
        )

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            healed, rows, fill_ratio = await strategy._heal_gap(
                gap=gap, symbol=_SYMBOL, timeframe=_TF, ctx=ctx,
            )

        assert healed     is True
        assert rows       == 4
        assert fill_ratio == pytest.approx(1.0, abs=0.01)
        assert len(storage.saved) == 1

    @pytest.mark.asyncio
    async def test_no_data_available_marks_irrecoverable(self):
        """NoDataAvailableError → GapRegistry.mark_healed(irreversible=True)."""
        strategy = RepairStrategy()
        gap      = _make_gap(start_ms=_TF_MS, expected=4)
        registry = _GapRegistryStub()
        ctx      = PipelineContext(
            fetcher      = _FetcherStub(chunks=[]),  # vacío → NoDataAvailableError
            storage      = _StorageStub(),
            bronze       = MagicMock(),
            cursor       = MagicMock(),
            quality      = _QualityStub(),
            exchange_id  = _EXCHANGE,
            market_type  = "spot",
            start_date   = "2021-01-01",
            gap_registry = registry,
        )

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            await strategy._heal_gap(
                gap=gap, symbol=_SYMBOL, timeframe=_TF, ctx=ctx,
            )

        # Debe haber llamado mark_healed con irreversible=True
        assert any(
            call.get("irreversible") is True
            for call in registry.healed_calls
        )

    @pytest.mark.asyncio
    async def test_gap_registry_none_no_crash(self):
        """gap_registry=None (modo degradado) → SafeOps: no lanza."""
        strategy = RepairStrategy()
        gap      = _make_gap(start_ms=_TF_MS, expected=4)
        ctx      = PipelineContext(
            fetcher      = _FetcherStub(chunks=[]),
            storage      = _StorageStub(),
            bronze       = MagicMock(),
            cursor       = MagicMock(),
            quality      = _QualityStub(),
            exchange_id  = _EXCHANGE,
            market_type  = "spot",
            start_date   = "2021-01-01",
            gap_registry = None,  # modo degradado
        )

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            # No debe lanzar aunque gap_registry sea None
            result = await strategy._heal_gap(
                gap=gap, symbol=_SYMBOL, timeframe=_TF, ctx=ctx,
            )

        assert result == (False, 0, 0.0)

    @pytest.mark.asyncio
    async def test_chunk_fetch_error_returns_false(self):
        """ChunkFetchError → (False, 0, 0.0), no marca irrecuperable."""
        strategy = RepairStrategy()
        gap      = _make_gap(start_ms=_TF_MS, expected=4)
        registry = _GapRegistryStub()

        bad_fetcher = MagicMock()
        bad_fetcher.fetch_chunk = AsyncMock(
            side_effect=ChunkFetchError("rate limit")
        )
        ctx = PipelineContext(
            fetcher      = bad_fetcher,
            storage      = _StorageStub(),
            bronze       = MagicMock(),
            cursor       = MagicMock(),
            quality      = _QualityStub(),
            exchange_id  = _EXCHANGE,
            market_type  = "spot",
            start_date   = "2021-01-01",
            gap_registry = registry,
        )

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            result = await strategy._heal_gap(
                gap=gap, symbol=_SYMBOL, timeframe=_TF, ctx=ctx,
            )

        assert result == (False, 0, 0.0)
        # No debe marcar irrecuperable — ChunkFetchError es transitorio
        assert not any(
            call.get("irreversible") is True
            for call in registry.healed_calls
        )

    @pytest.mark.asyncio
    async def test_cancelled_error_propagates_from_heal_gap(self):
        """CancelledError en _heal_gap debe propagarse — no tragarse."""
        strategy = RepairStrategy()
        gap      = _make_gap(start_ms=_TF_MS, expected=4)

        bad_fetcher = MagicMock()
        bad_fetcher.fetch_chunk = AsyncMock(side_effect=asyncio.CancelledError())
        ctx = PipelineContext(
            fetcher      = bad_fetcher,
            storage      = _StorageStub(),
            bronze       = MagicMock(),
            cursor       = MagicMock(),
            quality      = _QualityStub(),
            exchange_id  = _EXCHANGE,
            market_type  = "spot",
            start_date   = "2021-01-01",
            gap_registry = _GapRegistryStub(),
        )

        with patch(
            "market_data.adapters.exchange.exchange_quirks.get_quirks"
        ) as mock_quirks:
            mock_quirks.return_value.backward_pagination = False
            with pytest.raises(asyncio.CancelledError):
                await strategy._heal_gap(
                    gap=gap, symbol=_SYMBOL, timeframe=_TF, ctx=ctx,
                )


# ══════════════════════════════════════════════════════════════════════════════
# GapRange — modelo de dominio
# ══════════════════════════════════════════════════════════════════════════════

class TestGapRange:
    """Tests del modelo GapRange — sin dependencias."""

    def test_severity_low(self):
        gap = GapRange(start_ms=0, end_ms=_TF_MS * 3, expected=2)
        assert gap.severity == "low"

    def test_severity_medium(self):
        gap = GapRange(start_ms=0, end_ms=_TF_MS * 11, expected=5)
        assert gap.severity == "medium"

    def test_severity_high(self):
        gap = GapRange(start_ms=0, end_ms=_TF_MS * 20, expected=11)
        assert gap.severity == "high"

    def test_duration_ms(self):
        gap = GapRange(start_ms=1000, end_ms=5000, expected=3)
        assert gap.duration_ms == 4000

    def test_str_representation(self):
        gap = GapRange(start_ms=0, end_ms=_TF_MS, expected=1)
        s   = str(gap)
        assert "Gap[" in s
        assert "expected=1" in s

    def test_frozen(self):
        """GapRange es frozen — no debe permitir mutación."""
        gap = GapRange(start_ms=0, end_ms=1000, expected=1)
        with pytest.raises((AttributeError, TypeError)):
            gap.expected = 99  # type: ignore[misc]
