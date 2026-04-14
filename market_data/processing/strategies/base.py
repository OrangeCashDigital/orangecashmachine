"""
market_data/processing/strategies/base.py
=====================================

Contrato central del sistema de pipeline unificado.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Protocol, runtime_checkable

from core.logging import bind_pipeline
from market_data.ingestion.rest.ohlcv_fetcher import HistoricalFetcherAsync
from market_data.storage.storage_protocol import OHLCVStorage
from market_data.storage.bronze.bronze_storage import BronzeStorage
from market_data.quality.pipeline import QualityPipeline
from infra.state.cursor_store import CursorStore

_log = bind_pipeline("base")


class PipelineMode(str, Enum):
    INCREMENTAL = "incremental"
    BACKFILL    = "backfill"
    REPAIR      = "repair"


@dataclass
class PipelineContext:
    fetcher:     HistoricalFetcherAsync
    storage:     OHLCVStorage
    bronze:      BronzeStorage
    cursor:      CursorStore
    quality:     QualityPipeline
    exchange_id: str
    market_type: str
    start_date:  str
    # Locks de commit Iceberg — serializan appends concurrentes a la misma tabla.
    # El fetch sigue siendo 100% paralelo; solo el commit (~200ms) se serializa.
    # Un lock por tabla (bronze.ohlcv / silver.ohlcv) — tablas distintas pueden
    # escribirse solapadas entre sí, solo se serializa dentro de cada tabla.
    # Ref: Iceberg OCC — "branch main has changed" cuando dos writers compiten.
    bronze_commit_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    silver_commit_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


# Tipos de excepción de stdlib/aiohttp que indican error transitorio.
# Mantenemos este set para excepciones externas que no controlamos
# (aiohttp, ccxt, OSError). Para nuestras propias excepciones usamos
# el atributo `is_transient` declarado en la clase.
_TRANSIENT_STDLIB_TYPES: tuple[type, ...] = (
    TimeoutError,
    ConnectionError,       # incluye ConnectionRefusedError, ConnectionResetError
    BrokenPipeError,
    OSError,
)

# Substrings en el mensaje de error que indican error transitorio de red/exchange.
# Solo se usa como fallback cuando no hay información de tipo disponible.
_TRANSIENT_ERROR_MSGS: tuple[str, ...] = (
    "timeout", "timed out", "rate limit", "429", "503", "502",
    "connection", "network", "session is closed", "temporarily",
    "too many requests", "service unavailable",
)

# Nombres de clase de aiohttp/ccxt (no importables sin instalar) que
# indican error transitorio. Mantenemos strings solo para estas.
_TRANSIENT_EXTERNAL_NAMES: frozenset[str] = frozenset({
    "ClientConnectorError", "ServerDisconnectedError",
    "ClientOSError", "ClientResponseError",
})


def classify_error(exc: BaseException) -> bool:
    """
    Retorna True si el error es transitorio (seguro para retry).

    Jerarquía de decisión:
    1. Nuestras excepciones con `is_transient` declarado → fuente de verdad
    2. Tipos de stdlib conocidos como transitorios
    3. Nombres de clase de librerías externas (aiohttp, ccxt)
    4. Substrings en el mensaje — fallback de último recurso

    SafeOps: nunca lanza excepción.
    """
    # 1. Nuestras propias excepciones — usar atributo de clase
    if hasattr(exc, "is_transient"):
        return bool(exc.is_transient)
    # 2. Tipos de stdlib
    if isinstance(exc, _TRANSIENT_STDLIB_TYPES):
        return True
    # 3. Nombres de clase externos
    if type(exc).__name__ in _TRANSIENT_EXTERNAL_NAMES:
        return True
    # 4. Fallback por mensaje
    err_lower = str(exc).lower()
    return any(msg in err_lower for msg in _TRANSIENT_ERROR_MSGS)


# Timeout por par individual (segundos).
# Evita workers colgados cuando un fetch nunca retorna.
# Repair usa un timeout más largo (gap healing puede paginar mucho).
# Timeout por par individual (segundos), por granularidad de timeframe.
# 1m backfill puede paginar meses de historia — necesita mucho más tiempo
# que 1d que solo requiere unos pocos chunks.
# Repair siempre usa _PAIR_TIMEOUT_REPAIR_S independientemente del timeframe
# porque el gap healing pagina en ambas direcciones sobre rangos arbitrarios.
_PAIR_TIMEOUT_BY_TF: dict[str, int] = {
    "1m":  7200,   # 2h — backfill completo de 1m puede ser meses
    "5m":  3600,   # 1h
    "15m": 1800,   # 30 min
    "1h":   600,   # 10 min
    "4h":   300,   # 5 min
    "1d":   300,   # 5 min
}
_PAIR_TIMEOUT_S:        int = 300   # fallback para timeframes no mapeados
_PAIR_TIMEOUT_REPAIR_S: int = 1800  # 30 min — repair (gaps grandes paginados)


class _TransientProxy:
    """
    Proxy mínimo para classify_error() cuando solo tenemos strings
    (error_type, error_msg) en lugar del objeto excepción original.

    Permite que PairResult.is_transient_error delegue en classify_error()
    sin duplicar lógica de clasificación.

    Prioridad en classify_error():
      1. hasattr(proxy, 'is_transient') → declarativo, para tipos conocidos
      2. fallback de strings sobre __str__() → para tipos externos/desconocidos
    """

    _KNOWN_TRANSIENT: frozenset = frozenset({
        "ChunkFetchError", "ExchangeConnectionError", "ExchangeCircuitOpenError",
        "TimeoutError", "ConnectionError", "OSError", "ConnectionRefusedError",
        "ConnectionResetError", "BrokenPipeError",
        "ClientConnectorError", "ServerDisconnectedError",
        "ClientOSError", "ClientResponseError",
    })
    _KNOWN_PERMANENT: frozenset = frozenset({
        "NoDataAvailableError", "MissingStartDateError",
        "SymbolNotFoundError", "InvalidMarketTypeError",
        "UnsupportedExchangeError", "AuthenticationError",
    })

    def __init__(self, error_type: str, error_msg: str) -> None:
        self._error_msg = error_msg
        if error_type in self._KNOWN_TRANSIENT:
            self.is_transient = True
        elif error_type in self._KNOWN_PERMANENT:
            self.is_transient = False
        # Tipos desconocidos: sin atributo → classify_error usa fallback de strings

    def __str__(self) -> str:
        return self._error_msg


@dataclass
class PairResult:
    symbol:      str
    timeframe:   str
    mode:        PipelineMode
    exchange_id: str  = ""
    rows:        int  = 0
    skipped:     bool = False
    error:       str  = ""
    error_type:  str  = ""
    duration_ms: int  = 0
    gaps_found:   int  = 0
    gaps_healed:  int  = 0
    gaps_partial: int  = 0
    chunks:      int  = 0

    @property
    def success(self) -> bool:
        return not self.error and not self.skipped

    @property
    def is_transient_error(self) -> bool:
        """
        Fuente de verdad única: delega completamente a classify_error()
        via _TransientProxy. No duplica lógica de clasificación aquí.
        """
        if not self.error_type and not self.error:
            return False
        return classify_error(_TransientProxy(self.error_type, self.error))

    def __str__(self) -> str:
        if self.error:
            tag = "TRANSIENT" if self.is_transient_error else "FATAL"
            return f"[{self.mode.value}] {self.symbol}/{self.timeframe} ERROR[{tag}]: {self.error}"
        if self.skipped:
            return f"[{self.mode.value}] {self.symbol}/{self.timeframe} SKIPPED"
        if self.mode == PipelineMode.REPAIR:
            return (
                f"[repair] {self.symbol}/{self.timeframe} "
                f"gaps_found={self.gaps_found} gaps_healed={self.gaps_healed} "
                f"gaps_partial={self.gaps_partial} "
                f"rows={self.rows} duration={self.duration_ms}ms"
            )
        if self.mode == PipelineMode.BACKFILL:
            return (
                f"[backfill] {self.symbol}/{self.timeframe} "
                f"chunks={self.chunks} rows={self.rows} duration={self.duration_ms}ms"
            )
        return (
            f"[incremental] {self.symbol}/{self.timeframe} "
            f"rows={self.rows} duration={self.duration_ms}ms"
        )


@dataclass
class PipelineSummary:
    results:             List[PairResult] = field(default_factory=list)
    duration_ms:         int              = 0
    mode:                PipelineMode     = PipelineMode.INCREMENTAL
    degraded_exchanges:  List[str]        = field(default_factory=list)

    @property
    def failed_exchanges(self) -> List[str]:
        seen = []
        for r in self.results:
            if r.error and r.exchange_id and r.exchange_id not in seen:
                seen.append(r.exchange_id)
        return seen

    @property
    def status(self) -> str:
        all_degraded = set(self.degraded_exchanges) | set(self.failed_exchanges)
        if self.failed == 0 and not all_degraded:
            return "ok"
        if all_degraded and self.succeeded > 0:
            return "degraded"
        return "failed"

    @property
    def total(self)     -> int: return len(self.results)

    @property
    def succeeded(self) -> int: return sum(1 for r in self.results if r.success)

    @property
    def skipped(self)   -> int: return sum(1 for r in self.results if r.skipped)

    @property
    def failed(self)    -> int: return sum(1 for r in self.results if r.error)

    @property
    def total_rows(self) -> int: return sum(r.rows for r in self.results)

    @property
    def total_gaps_found(self) -> int: return sum(r.gaps_found for r in self.results)

    @property
    def total_gaps_healed(self)  -> int: return sum(r.gaps_healed  for r in self.results)
    @property
    def total_gaps_partial(self) -> int: return sum(r.gaps_partial for r in self.results)

    @property
    def throughput_rows_per_sec(self) -> float:
        if not self.duration_ms:
            return 0.0
        return round(self.total_rows / (self.duration_ms / 1000), 2)

    def log(self, log) -> None:
        """Emite el resumen usando el logger inyectado (DI — no usa _log de módulo)."""
        all_degraded = (
            self.degraded_exchanges
            + [f"{e}(errors)" for e in self.failed_exchanges
               if e not in self.degraded_exchanges]
        ) or ["none"]
        log.info(
            "Pipeline summary",
            mode=self.mode.value, status=self.status,
            total=self.total, ok=self.succeeded,
            skipped=self.skipped, failed=self.failed,
            rows=self.total_rows,
            throughput_rows_per_sec=self.throughput_rows_per_sec,
            duration_ms=self.duration_ms,
            degraded=all_degraded,
        )
        if self.mode == PipelineMode.REPAIR:
            log.info(
                "Repair summary",
                gaps_found=self.total_gaps_found,
                gaps_healed=self.total_gaps_healed,
                gaps_partial=self.total_gaps_partial,
            )
        for r in self.results:
            if r.error:
                log.warning("Pair result", status="error", pair=str(r))
            elif r.skipped:
                log.debug("Pair result", status="skipped", pair=str(r))
            else:
                log.debug("Pair result", status="ok", pair=str(r))


# ==========================================================
# StrategyMixin
# ==========================================================

class StrategyMixin:
    """
    Boilerplate compartido: timing, error_type, métricas, logging de fallo.

    Uso
    ---
    class MiStrategy(StrategyMixin):
        _mode = PipelineMode.INCREMENTAL

        async def _run(self, symbol, timeframe, idx, total, ctx, result):
            ...  # solo lógica de negocio
    """

    _mode: "PipelineMode"

    async def execute_pair(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       "PipelineContext",
    ) -> "PairResult":
        from market_data.observability.metrics import PIPELINE_ERRORS  # evita circular

        result     = PairResult(symbol=symbol, timeframe=timeframe, mode=self._mode, exchange_id=ctx.exchange_id)
        pair_start = time.monotonic()

        pair_timeout = (
            _PAIR_TIMEOUT_REPAIR_S
            if self._mode == PipelineMode.REPAIR
            else _PAIR_TIMEOUT_BY_TF.get(timeframe, _PAIR_TIMEOUT_S)
        )

        try:
            await asyncio.wait_for(
                self._run(symbol, timeframe, idx, total, ctx, result),
                timeout=pair_timeout,
            )
        except asyncio.TimeoutError:
            result.error       = f"Pair timeout after {pair_timeout}s"
            result.error_type  = "TimeoutError"
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            PIPELINE_ERRORS.labels(
                exchange=ctx.exchange_id, error_type="transient",
            ).inc()
            _log.bind(
                mode=self._mode.value, exchange=ctx.exchange_id,
                symbol=symbol, timeframe=timeframe,
                timeout_s=pair_timeout,
            ).error("Pair timeout — worker liberado")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result.error       = str(exc)
            result.error_type  = type(exc).__name__
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            is_transient = classify_error(exc)
            error_label  = "transient" if is_transient else "fatal"
            PIPELINE_ERRORS.labels(
                exchange=ctx.exchange_id, error_type=error_label,
            ).inc()
            _log.bind(
                mode=self._mode.value, exchange=ctx.exchange_id,
                symbol=symbol, timeframe=timeframe,
                idx=idx, total=total,
                error_type=result.error_type, error=str(exc),
                is_transient=is_transient,
                duration_ms=result.duration_ms,
            ).error("Strategy fallida")
        finally:
            if not result.duration_ms:
                result.duration_ms = int((time.monotonic() - pair_start) * 1000)

        return result

    async def _run(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       "PipelineContext",
        result:    "PairResult",
    ) -> None:
        raise NotImplementedError(f"{type(self).__name__} debe implementar _run()")


@runtime_checkable
class PipelineStrategy(Protocol):
    async def execute_pair(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       PipelineContext,
    ) -> PairResult:
        ...
