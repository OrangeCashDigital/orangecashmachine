# -*- coding: utf-8 -*-
"""
market_data/domain/policies/base.py
=====================================

Contratos puros del dominio de pipeline OHLCV.

Responsabilidad
---------------
Definir ÚNICAMENTE los tipos de datos compartidos por todas las estrategias
que son puramente del dominio — sin comportamiento de orquestación, sin
imports de ports/, sin NullObjects de infraestructura.

Qué vive aquí
-------------
· PipelineMode      — Enum de modos de operación
· PairResult        — resultado por par (symbol, timeframe); propiedades derivadas puras
· PipelineSummary   — resumen agregado; propiedades derivadas puras
· PipelineStrategy  — Protocol estructural mínimo (duck typing)
· classify_error    — SSOT de clasificación transitorio/permanente (re-exportado)
· _TransientProxy   — proxy para classify_error con strings

Qué NO vive aquí (movido a application/pipeline/runtime.py)
------------------------------------------------------------
· PipelineContext   — DI container que referencia ports/ → pertenece a application/
· StrategyMixin     — lógica de orquestación → pertenece a application/
· Timeout constants — configuración de ejecución → pertenece a application/
· NullMetrics       — implementación de infraestructura → pertenece a ports/

Principios aplicados
--------------------
SRP  — solo contratos y tipos de datos, sin lógica de negocio
DIP  — no importa ports/ ni infrastructure
DRY  — classify_error centralizado en runtime.py; aquí se re-exporta para
       compatibilidad con importadores existentes (ver nota de migración)
Pure — todo función/clase en este módulo es determinista y sin side-effects

Nota de migración
-----------------
classify_error y _TransientProxy se re-exportan desde runtime.py para que
PairResult.is_transient_error funcione sin ciclos. Una vez que todos los
importadores apunten a application/pipeline/runtime.py, este re-export
puede eliminarse.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, List, Protocol, runtime_checkable

_log = logging.getLogger(__name__)


# =============================================================================
# PipelineMode
# =============================================================================


class PipelineMode(str, Enum):
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"
    REPAIR = "repair"


# =============================================================================
# Error classification — re-exportado desde runtime para PairResult
# =============================================================================
# NOTA: Importación circular evitada porque runtime.py importa PairResult de
# aquí, y nosotros importamos classify_error de runtime. Python resuelve esto
# correctamente cuando los módulos están completamente inicializados.
# Si aparece ImportError en algún test, usar lazy import dentro del método.


def classify_error(exc: BaseException) -> bool:  # noqa: F811
    """
    Retorna True si el error es transitorio (seguro para retry).

    SSOT real en application/pipeline/runtime.py — esta copia existe para
    que PairResult.is_transient_error funcione sin importar application/.
    Ambas implementaciones son idénticas; se eliminará esta al completar
    la migración de todos los importadores.

    SafeOps: nunca lanza — retorna False ante cualquier fallo interno.
    """
    _TRANSIENT_STDLIB = (TimeoutError, ConnectionError, BrokenPipeError, OSError)
    _TRANSIENT_NAMES = frozenset(
        {
            "ClientConnectorError",
            "ServerDisconnectedError",
            "ClientOSError",
            "ClientResponseError",
        }
    )
    _TRANSIENT_MSGS = (
        "timeout",
        "timed out",
        "rate limit",
        "429",
        "503",
        "502",
        "connection",
        "network",
        "session is closed",
        "temporarily",
        "too many requests",
        "service unavailable",
    )
    try:
        if hasattr(exc, "is_transient"):
            return bool(exc.is_transient)
        if isinstance(exc, _TRANSIENT_STDLIB):
            return True
        if type(exc).__name__ in _TRANSIENT_NAMES:
            return True
        return any(msg in str(exc).lower() for msg in _TRANSIENT_MSGS)
    except Exception:
        return False


class _TransientProxy(Exception):
    """
    Proxy mínimo para classify_error() cuando solo tenemos strings.
    Ver docstring completo en application/pipeline/runtime.py.
    """

    _KNOWN_TRANSIENT: frozenset[str] = frozenset(
        {
            "ChunkFetchError",
            "ExchangeConnectionError",
            "ExchangeCircuitOpenError",
            "TimeoutError",
            "ConnectionError",
            "OSError",
            "ConnectionRefusedError",
            "ConnectionResetError",
            "BrokenPipeError",
            "ClientConnectorError",
            "ServerDisconnectedError",
            "ClientOSError",
            "ClientResponseError",
        }
    )
    _KNOWN_PERMANENT: frozenset[str] = frozenset(
        {
            "NoDataAvailableError",
            "MissingStartDateError",
            "SymbolNotFoundError",
            "InvalidMarketTypeError",
            "UnsupportedExchangeError",
            "AuthenticationError",
        }
    )

    def __init__(self, error_type: str, error_msg: str | None) -> None:
        super().__init__(error_msg or "")
        self._error_msg = error_msg or ""
        if error_type in self._KNOWN_TRANSIENT:
            self.is_transient = True
        elif error_type in self._KNOWN_PERMANENT:
            self.is_transient = False

    def __str__(self) -> str:
        return self._error_msg


# =============================================================================
# PairResult
# =============================================================================


@dataclass
class PairResult:
    """
    Resultado de procesar un par (symbol, timeframe) por una estrategia.

    Tipo de dominio puro — sin side effects, sin imports de ports/.
    Las propiedades derivadas son deterministas dado el estado del dataclass.
    """

    symbol: str
    timeframe: str
    mode: PipelineMode
    exchange_id: str = ""
    rows: int = 0
    skipped: bool = False
    error: str | None = None
    error_type: str = ""
    duration_ms: int = 0
    gaps_found: int = 0
    gaps_healed: int = 0
    gaps_partial: int = 0
    chunks: int = 0

    @property
    def success(self) -> bool:
        """True si el par se procesó sin error y sin skip."""
        return self.error is None and not self.skipped

    @property
    def is_transient_error(self) -> bool:
        """
        True si el error es transitorio (safe para retry).

        Delega completamente a classify_error() via _TransientProxy.
        SSOT: no duplica lógica de clasificación.
        """
        if not self.error_type and not self.error:
            return False
        return classify_error(_TransientProxy(self.error_type, self.error or ""))

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
        return f"[incremental] {self.symbol}/{self.timeframe} rows={self.rows} duration={self.duration_ms}ms"


# =============================================================================
# PipelineSummary
# =============================================================================


@dataclass
class PipelineSummary:
    """
    Resumen agregado del resultado de un pipeline completo.

    Tipo de dominio puro — calculado al finalizar run().
    Todas las propiedades son derivadas deterministas de `results`.
    """

    results: List[PairResult] = field(default_factory=list)
    duration_ms: int = 0
    mode: PipelineMode = PipelineMode.INCREMENTAL
    degraded_exchanges: List[str] = field(default_factory=list)

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
    def total(self) -> int:
        return len(self.results)

    @property
    def succeeded(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.skipped)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if r.error)

    @property
    def total_rows(self) -> int:
        return sum(r.rows for r in self.results)

    @property
    def total_gaps_found(self) -> int:
        return sum(r.gaps_found for r in self.results)

    @property
    def total_gaps_healed(self) -> int:
        return sum(r.gaps_healed for r in self.results)

    @property
    def total_gaps_partial(self) -> int:
        return sum(r.gaps_partial for r in self.results)

    @property
    def throughput_rows_per_sec(self) -> float:
        if not self.duration_ms:
            return 0.0
        return round(self.total_rows / (self.duration_ms / 1000), 2)

    def log(self, log: Any) -> None:
        """
        Emite el resumen estructurado usando el logger inyectado.

        DI — no usa _log de módulo para permitir binding de contexto externo.
        """
        all_degraded = (
            self.degraded_exchanges
            + [f"{e}(errors)" for e in self.failed_exchanges if e not in self.degraded_exchanges]
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
                log.warning("Pair result", status="error", pair=str(r))
            elif r.skipped:
                log.debug("Pair result", status="skipped", pair=str(r))
            else:
                log.debug("Pair result", status="ok", pair=str(r))


# =============================================================================
# PipelineStrategy — Protocol estructural mínimo
# =============================================================================


@runtime_checkable
class PipelineStrategy(Protocol):
    """
    Contrato estructural mínimo para estrategias de pipeline.

    Permite duck typing en OHLCVPipeline sin depender de StrategyMixin.
    Los mocks de test solo necesitan implementar execute_pair().
    """

    async def execute_pair(
        self,
        symbol: str,
        timeframe: str,
        idx: int,
        total: int,
        ctx: Any,  # PipelineContext — Any para evitar ciclo con runtime.py
    ) -> PairResult: ...
