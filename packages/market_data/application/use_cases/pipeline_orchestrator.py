# -*- coding: utf-8 -*-
"""
market_data/application/use_cases/pipeline_orchestrator.py
============================================================

PipelineOrchestrator — caso de uso de ejecución de pipelines.

Responsabilidad
---------------
Seleccionar el pipeline correcto (OHLCV / Trades / Derivatives)
según el request, construirlo via factory, y ejecutarlo.

SSOT de la lógica de selección e instanciación de pipelines.

Por qué existe esta capa
------------------------
Sin esta capa, infrastructure/dagster/assets/ y main.py deben conocer:
  - Qué clase concreta instanciar (OHLCVPipeline vs TradesPipeline)
  - Cómo construir CCXTAdapter con credentials y resilience
  - Qué modo pasarle (incremental/backfill/repair)

Con PipelineOrchestrator:
  - El caller solo conoce PipelineRequest (DTO)
  - La selección y construcción está centralizada en ConcretePipelineFactory (SSOT)
  - Añadir DerivativesPipeline no cambia ningún caller (OCP)

Relación con PipelineTriggerPort
---------------------------------
PipelineTriggerPort (ports/inbound/) define el contrato abstracto
que los pipelines implementan. PipelineOrchestrator construye e
invoca cualquier PipelineTriggerPort concreto via factory.

Diagrama de flujo
-----------------
  Dagster asset / main.py / test
      │
      ▼
  PipelineOrchestrator.run(request)
      │
      ├─ _build_pipeline(request) → factory.build(request) → PipelineTriggerPort
      │       (ConcretePipelineFactory: ohlcv | trades | derivatives)
      │
      └─ pipeline.run(mode=request.mode) → PipelineSummary

Contrato de errores
-------------------
_build_pipeline → lanza PipelineBuildError     (Fail-Fast: construcción fallida)
run()           → lanza PipelineExecutionError (Fail-Fast: ejecución fallida)
El caller decide si recuperar o propagar — este módulo no silencia fallos.

Principios: SRP · DIP · OCP · SSOT · KISS · Fail-Fast
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from loguru import logger

from market_data.domain.exceptions import PipelineBuildError, PipelineExecutionError
from market_data.ports.inbound.pipeline_trigger import (
    PipelineTriggerPort,
    PipelineModeStr,
)
from market_data.ports.inbound.pipeline_factory import PipelineFactoryPort

if TYPE_CHECKING:  # pragma: no cover
    from ocm.config.schema import ResilienceConfig

# Type alias — definido después de imports para no romper orden E402.
# TODO: formalizar como Protocol en ports/inbound/
PipelineSummary = Any

# Re-export para que callers no dependan del port directamente
PipelineMode = PipelineModeStr

# Tipos de pipeline — SSOT de la taxonomía de datos del sistema
PipelineType = Literal["ohlcv", "trades", "derivatives"]


# =============================================================================
# DTO de request — entrada al caso de uso
# =============================================================================

@dataclass
class PipelineRequest:
    """
    DTO de entrada para PipelineOrchestrator.

    Inmutable en práctica: no modificar campos tras construcción.
    Fail-Fast: __post_init__ valida invariantes en construcción.

    Campos obligatorios
    -------------------
    exchange    : nombre canónico del exchange  ("bybit", "kucoin")
    market_type : tipo de mercado              ("spot" | "futures" | "linear" | "inverse")
    pipeline    : tipo de datos a ingestar     ("ohlcv" | "trades" | "derivatives")

    Campos de runtime — provistos por el caller desde AppConfig
    -----------------------------------------------------------
    credentials     : dict con API key/secret/passphrase.
                      None = CCXTAdapter usa variables de entorno (desarrollo).
    resilience      : dict con parámetros de retry/circuit_breaker del exchange.
                      None = CCXTAdapter usa defaults internos.
    symbols         : lista de pares a procesar. None = config interna del pipeline.
    timeframes      : lista de timeframes.       None = config interna del pipeline.
    start_date      : fecha de inicio de backfill. None = pipeline determina.
    auto_lookback_days: días de lookback automático. None = pipeline usa default.

    Campos de control
    -----------------
    mode     : "incremental" | "backfill" | "repair". Default: "incremental".
    run_id   : correlación externa. None = generado por el pipeline.
    dry_run  : True = no persiste datos. Útil en validación y tests.
    extra    : dict abierto para extensión sin romper el contrato (OCP).
    """

    exchange:    str
    market_type: str
    pipeline:    PipelineType

    # Modo de ejecución
    mode: PipelineModeStr = "incremental"

    # Parámetros de construcción del adapter
    credentials: Optional[dict]                                         = None
    resilience:  "Optional[Union[ResilienceConfig, dict[str, Any]]]"   = None

    # Parámetros de selección de datos
    symbols:             Optional[list[str]] = None
    timeframes:          Optional[list[str]] = None
    start_date:          Optional[str]       = None
    auto_lookback_days:  Optional[int]       = None

    # Datasets para DerivativesPipeline — ej: ['funding_rate', 'open_interest']
    datasets: Optional[list[str]] = None

    # Parámetros de control
    run_id:  Optional[str]    = None
    dry_run: bool             = False
    extra:   dict[str, Any]   = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Fail-Fast: valida invariantes del DTO en construcción."""
        if not self.exchange:
            raise ValueError("PipelineRequest.exchange no puede estar vacío")
        if not self.market_type:
            raise ValueError("PipelineRequest.market_type no puede estar vacío")
        if self.pipeline not in ("ohlcv", "trades", "derivatives"):
            raise ValueError(
                f"PipelineRequest.pipeline inválido: {self.pipeline!r}. "
                f"Valores válidos: 'ohlcv', 'trades', 'derivatives'"
            )
        if self.mode not in ("incremental", "backfill", "repair"):
            raise ValueError(
                f"PipelineRequest.mode inválido: {self.mode!r}. "
                f"Valores válidos: 'incremental', 'backfill', 'repair'"
            )

    def __str__(self) -> str:
        return (
            f"PipelineRequest({self.pipeline}/{self.exchange}/{self.market_type}"
            f" mode={self.mode} dry_run={self.dry_run})"
        )


# =============================================================================
# PipelineOrchestrator — caso de uso principal
# =============================================================================

class PipelineOrchestrator:
    """
    Orquestador de casos de uso de ingesta de market data.

    Selecciona, construye y ejecuta el pipeline correcto según el request.
    La construcción concreta está completamente delegada a la factory (DIP·OCP).
    Este módulo no instancia adaptadores ni infraestructura directamente.

    Contrato de errores
    -------------------
    _build_pipeline: lanza PipelineBuildError si la construcción falla.
    run():           lanza PipelineExecutionError si la ejecución falla.
    Ambos son Fail-Fast — el caller decide cómo recuperarse.

    Uso básico (desarrollo / tests)
    --------------------------------
    orchestrator = PipelineOrchestrator()
    summary = await orchestrator.run(PipelineRequest(
        exchange    = "bybit",
        market_type = "spot",
        pipeline    = "ohlcv",
        mode        = "incremental",
    ))

    Extensión (OCP)
    ---------------
    Añadir nuevo pipeline:
      1. Crear la clase (implementa PipelineTriggerPort)
      2. Añadir case en ConcretePipelineFactory.build()
      3. Añadir literal a PipelineType
      Ningún código aquí cambia.
    """

    def __init__(self, factory: PipelineFactoryPort | None = None) -> None:
        """
        Parámetros
        ----------
        factory : PipelineFactoryPort — composition root de pipelines.
                  None = caller debe inyectar en run().
                  Override en tests para inyectar mocks (DIP · OCP).
        """
        self._factory = factory

    async def run(
        self,
        request: PipelineRequest,
        factory: PipelineFactoryPort | None = None,
    ) -> object:
        """
        Ejecuta el pipeline descrito por el request.

        Parámetros
        ----------
        request : PipelineRequest — DTO con parámetros del pipeline.
        factory : PipelineFactoryPort — override por llamada. Si es None,
                  usa la factory del constructor.

        Returns
        -------
        PipelineSummary del pipeline ejecutado.

        Raises
        ------
        PipelineBuildError     : si la construcción del pipeline falla.
        PipelineExecutionError : si la ejecución del pipeline falla.
        """
        logger.info("PipelineOrchestrator.run | {}", request)

        f = factory or self._factory
        if f is None:
            raise PipelineBuildError(
                "PipelineOrchestrator: se requiere factory — "
                "inyectar via constructor o parámetro run()"
            )

        pipeline = self._build_pipeline(request, factory=f)

        try:
            summary = await pipeline.run(mode=request.mode)
            logger.info("PipelineOrchestrator.run OK | {} summary={}", request, summary)
            return summary
        except PipelineExecutionError:
            raise  # Ya es el tipo correcto — re-lanzar sin wrappear
        except Exception as exc:
            logger.opt(exception=True).error(
                "PipelineOrchestrator.run FAILED | {}", request,
            )
            raise PipelineExecutionError(
                f"Fallo durante ejecución del pipeline: {request}"
            ) from exc

    def _build_pipeline(
        self,
        request: PipelineRequest,
        factory: PipelineFactoryPort,
    ) -> PipelineTriggerPort:
        """
        Delega la construcción completa a la factory (DIP · SSOT).

        ConcretePipelineFactory es el único módulo con licencia para
        importar infraestructura concreta — este módulo no lo hace.

        Raises
        ------
        PipelineBuildError : si la factory no puede construir el pipeline.
        """
        try:
            return factory.build(request)
        except PipelineBuildError:
            raise  # Re-lanzar sin wrappear — ya tiene contexto
        except Exception as exc:
            logger.opt(exception=True).error(
                "PipelineOrchestrator._build_pipeline FAILED | {}", request,
            )
            raise PipelineBuildError(
                f"Error construyendo pipeline para: {request}"
            ) from exc
