# -*- coding: utf-8 -*-
"""
market_data/application/pipeline/runtime.py
============================================

Runtime de orquestación del pipeline OHLCV — Application Layer.

Responsabilidad
---------------
Concentra todo el comportamiento de ejecución que antes vivía mezclado
en domain/policies/base.py:

  · PipelineContext   — DI container de ejecución (ports + runtime state)
  · StrategyMixin     — Template Method: timeout, captura, métricas, logging
  · classify_error    — clasificación transitorio/permanente (SSOT)
  · _TransientProxy   — proxy para classify_error con strings en lugar de exc
  · Timeout constants — calibrados por granularidad de timeframe

Por qué aquí y no en domain/
-----------------------------
Domain debe ser puro: sin imports de ports/, sin NullObjects, sin lógica
de orquestación. PipelineContext es un DI container que referencia ports/
— pertenece a application/ por definición Clean Architecture.

Principios aplicados
--------------------
SRP    — cada clase tiene una sola razón de cambio
DIP    — PipelineContext depende de abstracciones (ports), no de implementaciones
SSOT   — classify_error es la única fuente de verdad para transitoriedad
DRY    — StrategyMixin centraliza timeout, error handling y métricas
SafeOps — classify_error nunca lanza; StrategyMixin captura todo excepto CancelledError
Fail-fast — get_chunk_converter lanza RuntimeError si el converter no fue inyectado
Fail-soft — _NullMetrics evita NPE cuando métricas no están disponibles
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

# ── Domain value types — contratos puros, sin comportamiento ─────────────────
from market_data.domain.policies.base import (
    PairResult,
    PipelineMode,
    PipelineSummary,  # noqa: F401 — re-exportado para que importadores usen runtime
)

# ── Ports — application/ puede importar ports/ directamente (Clean Architecture)
from market_data.ports.outbound.chunk_converter import OHLCVChunkConverterPort
from market_data.ports.outbound.gap_registry import GapRegistryPort
from market_data.ports.outbound.historical_fetcher import HistoricalFetcherPort
from market_data.ports.outbound.metrics import MetricsPort, NullMetrics
from market_data.ports.outbound.publisher import NullOHLCVPublisher, OHLCVPublisherPort
from market_data.ports.outbound.quality_pipeline import QualityPipelinePort
from market_data.ports.outbound.state import CursorStorePort
from market_data.ports.outbound.storage import OHLCVStorage
from market_data.ports.outbound.throttle import ThrottlePort

_log = logging.getLogger(__name__)


# =============================================================================
# Timeout constants
# =============================================================================
# Calibrados por granularidad de timeframe.
# 1m backfill puede paginar meses de historia → necesita más tiempo que 1d.
# Repair usa _PAIR_TIMEOUT_REPAIR_S independientemente del timeframe porque
# el gap healing pagina en ambas direcciones sobre rangos arbitrarios.

_PAIR_TIMEOUT_BY_TF: dict[str, int] = {
    "1m": 7200,  # 2 h  — backfill completo de 1m puede ser meses
    "5m": 3600,  # 1 h
    "15m": 1800,  # 30 min
    "1h": 600,  # 10 min
    "4h": 300,  # 5 min
    "1d": 300,  # 5 min
}
_PAIR_TIMEOUT_S: int = 300  # fallback para timeframes no mapeados
_PAIR_TIMEOUT_REPAIR_S: int = 1800  # 30 min — repair (gaps grandes paginados)


# =============================================================================
# Error classification — SSOT
# =============================================================================

# Tipos de excepción de stdlib/aiohttp que indican error transitorio.
# Para nuestras propias excepciones usamos el atributo `is_transient`
# declarado en la clase — más robusto que isinstance con tipos externos.
_TRANSIENT_STDLIB_TYPES: tuple[type, ...] = (
    TimeoutError,
    ConnectionError,  # incluye ConnectionRefusedError, ConnectionResetError
    BrokenPipeError,
    OSError,
)

# Substrings en el mensaje de error que indican error transitorio de red/exchange.
# Usado solo como fallback cuando no hay información de tipo disponible.
_TRANSIENT_ERROR_MSGS: tuple[str, ...] = (
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

# Nombres de clase de aiohttp/ccxt (no importables sin instalar).
# Strings para evitar dependencias en tiempo de importación.
_TRANSIENT_EXTERNAL_NAMES: frozenset[str] = frozenset(
    {
        "ClientConnectorError",
        "ServerDisconnectedError",
        "ClientOSError",
        "ClientResponseError",
    }
)


def classify_error(exc: BaseException) -> bool:
    """
    Retorna True si el error es transitorio (seguro para retry).

    Jerarquía de decisión (orden de prioridad):
    1. Nuestras excepciones con `is_transient` declarado → fuente de verdad
    2. Tipos de stdlib conocidos como transitorios
    3. Nombres de clase de librerías externas (aiohttp, ccxt)
    4. Substrings en el mensaje — fallback de último recurso

    SafeOps: nunca lanza — retorna False ante cualquier fallo interno.
    """
    try:
        # 1. Nuestras propias excepciones — atributo declarativo (SSOT)
        if hasattr(exc, "is_transient"):
            return bool(exc.is_transient)
        # 2. Tipos stdlib
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


class _TransientProxy(Exception):
    """
    Proxy mínimo para classify_error() cuando solo tenemos strings
    (error_type, error_msg) en lugar del objeto excepción original.

    Permite que PairResult.is_transient_error delegue en classify_error()
    sin duplicar lógica de clasificación (DRY — SSOT).

    Prioridad en classify_error():
      1. hasattr(proxy, 'is_transient') → declarativo, para tipos conocidos
      2. fallback de strings sobre __str__() → para tipos externos/desconocidos
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
        # Tipos desconocidos: sin atributo → classify_error usa fallback de strings

    def __str__(self) -> str:
        return self._error_msg


# =============================================================================
# PipelineContext — DI container de ejecución
# =============================================================================


@dataclass
class PipelineContext:
    """
    Contexto de ejecución de un pipeline — Kappa architecture.

    Actúa como DI container: agrega los ports necesarios para una corrida
    y los hace disponibles a las estrategias sin que estas conozcan las
    implementaciones concretas (DIP).

    Bounded contexts internos
    -------------------------
    Productores (BackfillStrategy, IncrementalStrategy):
      fetcher → publisher → Kafka → [consumer] → Iceberg

    Mantenimiento (RepairStrategy):
      storage → Iceberg directo (no es market truth, es maintenance)

    Invariantes post-construcción
    -----------------------------
    · metrics       NUNCA es None — __post_init__ inyecta NullMetrics si omitido
    · _chunk_converter puede ser None; get_chunk_converter() falla rápido (fail-fast)

    Campos obligatorios (sin default)
    ----------------------------------
    fail-fast en construcción si se omiten.
    RepairStrategy requiere storage; backfill/incremental no lo usan.

    Tipado fuerte
    -------------
    Todos los campos usan el Port formal como tipo — DIP completo.
    Los mocks de tests deben implementar el Protocol correspondiente
    (duck typing via runtime_checkable — sin herencia explícita).

    Principios: DIP · SRP · Kappa · Fail-Fast · Fail-Soft · ISP
    """

    # ── Productores — obligatorios (sin default) ──────────────────────────────
    fetcher: HistoricalFetcherPort  # REST poller (backfill + incremental)
    cursor: CursorStorePort  # Redis — SSOT del offset del productor
    quality: QualityPipelinePort  # quality gate antes de publicar
    exchange_id: str
    market_type: str
    start_date: str

    # ── Publisher Kappa ───────────────────────────────────────────────────────
    # Nunca None en producción — Null Object Pattern (NullOHLCVPublisher).
    # Tests inyectan NullOHLCVPublisher() explícitamente o usan el default.
    # Kappa: publisher=None es error de configuración, no modo degradado.
    publisher: OHLCVPublisherPort = field(default_factory=NullOHLCVPublisher)

    # ── Mantenimiento (RepairStrategy) ────────────────────────────────────────
    # storage=None es válido para backfill/incremental — solo Repair lo usa.
    # Inyectado por RepairPipelineFactory, no por ConcretePipelineFactory.
    storage: OHLCVStorage | None = field(default=None)

    # ── Observabilidad ────────────────────────────────────────────────────────
    # default_factory=NullMetrics garantiza NUNCA None — sin Optional.
    # mypy ve MetricsPort directamente — sin union-attr errors.
    metrics: MetricsPort = field(default_factory=NullMetrics)

    # ── Gap registry ──────────────────────────────────────────────────────────
    # None = modo degradado sin Redis (SafeOps — RepairStrategy degrada).
    gap_registry: GapRegistryPort | None = field(default=None)

    # ── Rate limiting ─────────────────────────────────────────────────────────
    # None = sin throttle (modo Kappa sin Bronze directo).
    throttle: ThrottlePort | None = field(default=None)

    # ── Kappa chunk converter ─────────────────────────────────────────────────
    # Inyectado por pipeline_factory antes de pasar el contexto a las strategies.
    # None hasta que el pipeline Kappa esté activo — get_chunk_converter() fail-fast.
    _chunk_converter: OHLCVChunkConverterPort | None = field(default=None)

    # ── Invariantes post-construcción ─────────────────────────────────────────

    def __post_init__(self) -> None:
        """
        Garantiza invariantes post-construcción.

        · metrics: default_factory=NullMetrics garantiza MetricsPort siempre.
          Sin Optional — mypy no requiere guards en las strategies.
        """
        pass  # invariante garantizado por default_factory=NullMetrics

    def get_chunk_converter(self) -> OHLCVChunkConverterPort:
        """
        Retorna el chunk converter inyectado.

        Fail-fast: lanza RuntimeError si _chunk_converter no fue inyectado.
        El pipeline Kappa debe inyectarlo antes de llamar a las strategies.
        """
        if self._chunk_converter is None:
            raise RuntimeError(
                "PipelineContext._chunk_converter no inyectado. "
                "pipeline_factory debe setear _chunk_converter "
                "antes de pasar el contexto a BackfillStrategy/IncrementalStrategy."
            )
        return self._chunk_converter


# =============================================================================
# StrategyMixin — Template Method compartido por todas las estrategias
# =============================================================================


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
    · Captura todas las excepciones excepto CancelledError
    · asyncio.TimeoutError se convierte en PairResult con error transitorio
    · CancelledError se re-lanza siempre — no interferir con shutdown

    Invariante de métricas
    ----------------------
    ctx.metrics NUNCA es None (garantizado por PipelineContext.__post_init__).
    No se requieren guards 'if ctx.metrics is not None' en ningún punto.
    """

    _mode: "PipelineMode"

    async def execute_pair(
        self,
        symbol: str,
        timeframe: str,
        idx: int,
        total: int,
        ctx: "PipelineContext",
    ) -> "PairResult":
        result = PairResult(
            symbol=symbol,
            timeframe=timeframe,
            mode=self._mode,
            exchange_id=ctx.exchange_id,
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
            result.error = f"Pair timeout after {pair_timeout}s"
            result.error_type = "TimeoutError"
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            # Invariante: ctx.metrics nunca es None — sin guard necesario
            ctx.metrics.record_error(ctx.exchange_id, "transient")
            _log.bind(
                mode=self._mode.value,
                exchange=ctx.exchange_id,
                symbol=symbol,
                timeframe=timeframe,
                timeout_s=pair_timeout,
            ).error("Pair timeout — worker liberado")

        except asyncio.CancelledError:
            raise  # nunca capturar — permite shutdown limpio

        except Exception as exc:
            result.error = str(exc)
            result.error_type = type(exc).__name__
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)
            is_transient = classify_error(exc)
            # Invariante: ctx.metrics nunca es None — sin guard necesario
            ctx.metrics.record_error(
                ctx.exchange_id,
                "transient" if is_transient else "fatal",
            )
            _log.bind(
                mode=self._mode.value,
                exchange=ctx.exchange_id,
                symbol=symbol,
                timeframe=timeframe,
                idx=idx,
                total=total,
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
        symbol: str,
        timeframe: str,
        idx: int,
        total: int,
        ctx: "PipelineContext",
        result: "PairResult",
    ) -> None:
        raise NotImplementedError(f"{type(self).__name__} debe implementar _run()")
