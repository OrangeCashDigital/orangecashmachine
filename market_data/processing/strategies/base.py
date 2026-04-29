# -*- coding: utf-8 -*-
"""
market_data/processing/strategies/base.py
==========================================

Contrato central del sistema de pipeline unificado.

Responsabilidad
---------------
Definir los tipos de datos compartidos por todas las estrategias:
PipelineContext, PipelineMode, PairResult, PipelineSummary,
StrategyMixin y el Protocol PipelineStrategy.

Principios aplicados
--------------------
SOLID  — SRP: solo contratos y tipos de datos, sin lógica de negocio
DIP    — PipelineContext depende de OHLCVStorage (puerto), no de IcebergStorage
SSOT   — única fuente de verdad para classify_error y _TransientProxy
DRY    — StrategyMixin centraliza timeout, error handling y métricas
SafeOps — classify_error nunca lanza; StrategyMixin captura todo excepto CancelledError
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Protocol, runtime_checkable

from ocm_platform.observability import bind_pipeline

# ── Ports (contratos) — nunca importar implementaciones concretas aquí ──────
from market_data.ports.storage import OHLCVStorage
from market_data.ports.state import CursorStorePort as CursorStore
from market_data.ports.gap_registry import GapRegistryPort

_log = bind_pipeline("base")


# ==========================================================================
# PipelineMode
# ==========================================================================

class PipelineMode(str, Enum):
    INCREMENTAL = "incremental"
    BACKFILL    = "backfill"
    REPAIR      = "repair"


# ==========================================================================
# PipelineContext
# ==========================================================================

@dataclass
class PipelineContext:
    """
    Contexto inmutable de ejecución de un pipeline.

    Todos los campos son puertos o value objects — ninguna implementación
    concreta de infra (IcebergStorage, BronzeStorage, etc.) debe aparecer
    aquí como tipo. Las implementaciones se inyectan desde OHLCVPipeline.

    Locks de commit Iceberg
    -----------------------
    Serializan appends concurrentes a la misma tabla Iceberg.
    El fetch sigue siendo 100% paralelo; solo el commit (~200ms) se serializa.
    Un lock por tabla (bronze / silver) — tablas distintas pueden escribirse
    solapadas entre sí, solo se serializa dentro de cada tabla.

    Ref: Iceberg OCC — "branch main has changed" cuando dos writers compiten.
    """
    fetcher:     object          # HistoricalFetcherAsync — importación local evita circular
    storage:     OHLCVStorage
    bronze:      object          # BronzeStorage — importación local evita circular
    cursor:      CursorStore
    quality:     object          # QualityPipeline — importación local evita circular
    exchange_id: str
    market_type: str
    start_date:  str

    # Locks de serialización de commits Iceberg (un lock por tabla)
    # Puerto de registro de gaps irrecuperables — inyectado por OHLCVPipeline.
    # None = modo degradado (SafeOps): repair opera sin persistencia de estado.
    # Implementación concreta: infra.state.gap_registry.GapRegistry via DI.
    gap_registry: "GapRegistryPort | None" = field(default=None)
    bronze_commit_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    silver_commit_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


# ==========================================================================
# Error classification
# ==========================================================================

# Tipos de excepción de stdlib/aiohttp que indican error transitorio.
# Para nuestras propias excepciones usamos el atributo `is_transient`
# declarado en la clase — más robusto que isinstance con tipos externos.
_TRANSIENT_STDLIB_TYPES: tuple[type, ...] = (
    TimeoutError,
    ConnectionError,        # incluye ConnectionRefusedError, ConnectionResetError
    BrokenPipeError,
    OSError,
)

# Substrings en el mensaje de error que indican error transitorio de red/exchange.
# Usado solo como fallback cuando no hay información de tipo disponible.
_TRANSIENT_ERROR_MSGS: tuple[str, ...] = (
    "timeout", "timed out", "rate limit", "429", "503", "502",
    "connection", "network", "session is closed", "temporarily",
    "too many requests", "service unavailable",
)

# Nombres de clase de aiohttp/ccxt (no importables sin instalar).
# Mantenemos strings para no crear dependencias en tiempo de importación.
_TRANSIENT_EXTERNAL_NAMES: frozenset[str] = frozenset({
    "ClientConnectorError", "ServerDisconnectedError",
    "ClientOSError", "ClientResponseError",
})


def classify_error(exc: BaseException) -> bool:
    """
    Retorna True si el error es transitorio (seguro para retry).

    Jerarquía de decisión (orden de prioridad):
    1. Nuestras excepciones con `is_transient` declarado → fuente de verdad
    2. Tipos de stdlib conocidos como transitorios
    3. Nombres de clase de librerías externas (aiohttp, ccxt)
    4. Substrings en el mensaje — fallback de último recurso

    SafeOps: nunca lanza excepción — retorna False ante cualquier error interno.
    """
    try:
        # 1. Nuestras propias excepciones — atributo de clase (SSOT)
        if hasattr(exc, "is_transient"):
            return bool(exc.is_transient)
        # 2. Tipos de stdlib
        if isinstance(exc, _TRANSIENT_STDLIB_TYPES):
            return True
        # 3. Nombres de clase externos (sin importar los módulos)
        if type(exc).__name__ in _TRANSIENT_EXTERNAL_NAMES:
            return True
        # 4. Fallback por mensaje — último recurso
        err_lower = str(exc).lower()
        return any(msg in err_lower for msg in _TRANSIENT_ERROR_MSGS)
    except Exception:
        return False  # SafeOps: fallo en clasificación → asumir permanente


class _TransientProxy:
    """
    Proxy mínimo para classify_error() cuando solo tenemos strings
    (error_type, error_msg) en lugar del objeto excepción original.

    Permite que PairResult.is_transient_error delegue en classify_error()
    sin duplicar lógica de clasificación (DRY — SSOT).

    Prioridad en classify_error():
      1. hasattr(proxy, 'is_transient') → declarativo, para tipos conocidos
      2. fallback de strings sobre __str__() → para tipos externos/desconocidos
    """

    _KNOWN_TRANSIENT: frozenset[str] = frozenset({
        "ChunkFetchError", "ExchangeConnectionError", "ExchangeCircuitOpenError",
        "TimeoutError", "ConnectionError", "OSError", "ConnectionRefusedError",
        "ConnectionResetError", "BrokenPipeError",
        "ClientConnectorError", "ServerDisconnectedError",
        "ClientOSError", "ClientResponseError",
    })
    _KNOWN_PERMANENT: frozenset[str] = frozenset({
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


# ==========================================================================
# PairResult
# ==========================================================================

@dataclass
class PairResult:
    """
    Resultado de procesar un par (symbol, timeframe) por una estrategia.

    Inmutable post-construcción excepto por los campos que la estrategia
    actualiza durante la ejecución (rows, skipped, error, etc.).
    """
    symbol:       str
    timeframe:    str
    mode:         PipelineMode
    exchange_id:  str  = ""
    rows:         int  = 0
    skipped:      bool = False
    error:        str  = ""
    error_type:   str  = ""
    duration_ms:  int  = 0
    gaps_found:   int  = 0
    gaps_healed:  int  = 0
    gaps_partial: int  = 0
    chunks:       int  = 0

    @property
    def success(self) -> bool:
        """True si el par se procesó sin error y sin skip."""
        return not self.error and not self.skipped

    @property
    def is_transient_error(self) -> bool:
        """
        True si el error es transitorio (safe para retry).

        Delega completamente a classify_error() via _TransientProxy.
        SSOT: no duplica lógica de clasificación.
        """
        if not self.error_type and not self.error:
            return False
        return classify_error(_TransientProxy(self.error_type, self.error))

    def __str__(self) -> str:
        if self.error:
            tag = "TRANSIENT" if self.is_transient_error else "FATAL"
            return (
                f"[{self.mode.value}] {self.symbol}/{self.timeframe} "
                f"ERROR[{tag}]: {self.error}"
            )
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


# ==========================================================================
# PipelineSummary
# ==========================================================================

@dataclass
class PipelineSummary:
    """
    Resumen agregado del resultado de un pipeline completo.

    Calculado al finalizar run() en OHLCVPipeline y equivalentes.
    Inmutable post-construcción.
    """
    results:            List[PairResult] = field(default_factory=list)
    duration_ms:        int              = 0
    mode:               PipelineMode     = PipelineMode.INCREMENTAL
    degraded_exchanges: List[str]        = field(default_factory=list)

    @property
    def failed_exchanges(self) -> List[str]:
        """Exchanges con al menos un par fallido."""
        seen: List[str] = []
        for r in self.results:
            if r.error and r.exchange_id and r.exchange_id not in seen:
                seen.append(r.exchange_id)
        return seen

    @property
    def status(self) -> str:
        """Estado global: 'ok' | 'degraded' | 'failed'."""
        all_degraded = set(self.degraded_exchanges) | set(self.failed_exchanges)
        if self.failed == 0 and not all_degraded:
            return "ok"
        if all_degraded and self.succeeded > 0:
            return "degraded"
        return "failed"

    @property
    def total(self)      -> int: return len(self.results)

    @property
    def succeeded(self)  -> int: return sum(1 for r in self.results if r.success)

    @property
    def skipped(self)    -> int: return sum(1 for r in self.results if r.skipped)

    @property
    def failed(self)     -> int: return sum(1 for r in self.results if r.error)

    @property
    def total_rows(self) -> int: return sum(r.rows for r in self.results)

    @property
    def total_gaps_found(self)   -> int: return sum(r.gaps_found   for r in self.results)

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
        """
        Emite el resumen estructurado usando el logger inyectado.

        DI — no usa _log de módulo para permitir binding de contexto externo.
        """
        all_degraded = (
            self.degraded_exchanges
            + [
                f"{e}(errors)" for e in self.failed_exchanges
                if e not in self.degraded_exchanges
            ]
        ) or ["none"]

        log.info(
            "Pipeline summary",
            mode=self.mode.value,
            status=self.status,
            total=self.total,
            ok=self.succeeded,
            skipped=self.skipped,
            failed=self.failed,
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
                log.warning("Pair result", status="error",  pair=str(r))
            elif r.skipped:
                log.debug(  "Pair result", status="skipped", pair=str(r))
            else:
                log.debug(  "Pair result", status="ok",      pair=str(r))


# ==========================================================================
# Timeout constants
# ==========================================================================

# Timeout por par individual, calibrado por granularidad de timeframe.
# 1m backfill puede paginar meses de historia — necesita más tiempo que 1d.
# Repair usa _PAIR_TIMEOUT_REPAIR_S independientemente del timeframe porque
# el gap healing pagina en ambas direcciones sobre rangos arbitrarios.
_PAIR_TIMEOUT_BY_TF: dict[str, int] = {
    "1m":  7200,   # 2h  — backfill completo de 1m puede ser meses
    "5m":  3600,   # 1h
    "15m": 1800,   # 30 min
    "1h":   600,   # 10 min
    "4h":   300,   # 5 min
    "1d":   300,   # 5 min
}
_PAIR_TIMEOUT_S:        int = 300    # fallback para timeframes no mapeados
_PAIR_TIMEOUT_REPAIR_S: int = 1800   # 30 min — repair (gaps grandes paginados)


# ==========================================================================
# StrategyMixin
# ==========================================================================

class StrategyMixin:
    """
    Boilerplate compartido por todas las estrategias de pipeline.

    Gestiona: timeout por par, captura de errores, métricas Prometheus
    y logging estructurado. Las subclases implementan solo `_run()`.

    Patrón Template Method
    ----------------------
    execute_pair()  ← llamado por el worker pool (interfaz pública)
        └── _run() ← implementado por cada estrategia (lógica de negocio)

    SafeOps
    -------
    - Captura todas las excepciones excepto CancelledError
    - asyncio.TimeoutError se convierte en PairResult con error transitorio
    - CancelledError se re-lanza siempre — no interferir con shutdown
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

        result     = PairResult(
            symbol=symbol, timeframe=timeframe,
            mode=self._mode, exchange_id=ctx.exchange_id,
        )
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
            result.error      = f"Pair timeout after {pair_timeout}s"
            result.error_type = "TimeoutError"
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
            raise  # nunca capturar — permite shutdown limpio

        except Exception as exc:
            result.error      = str(exc)
            result.error_type = type(exc).__name__
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            is_transient = classify_error(exc)
            PIPELINE_ERRORS.labels(
                exchange=ctx.exchange_id,
                error_type="transient" if is_transient else "fatal",
            ).inc()
            _log.bind(
                mode=self._mode.value, exchange=ctx.exchange_id,
                symbol=symbol, timeframe=timeframe,
                idx=idx, total=total,
                error_type=result.error_type,
                error=str(exc),
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
        raise NotImplementedError(
            f"{type(self).__name__} debe implementar _run()"
        )


# ==========================================================================
# PipelineStrategy Protocol
# ==========================================================================

@runtime_checkable
class PipelineStrategy(Protocol):
    """
    Contrato estructural mínimo para estrategias de pipeline.

    Permite duck typing en OHLCVPipeline sin depender de StrategyMixin.
    Los mocks de test solo necesitan implementar execute_pair().
    """
    async def execute_pair(
        self,
        symbol:    str,
        timeframe: str,
        idx:       int,
        total:     int,
        ctx:       PipelineContext,
    ) -> PairResult:
        ...
