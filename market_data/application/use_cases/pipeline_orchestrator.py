# -*- coding: utf-8 -*-
"""
market_data/application/use_cases/pipeline_orchestrator.py
============================================================

PipelineOrchestrator — caso de uso de ejecución de pipelines.

Responsabilidad
---------------
Seleccionar el pipeline correcto (OHLCV / Trades / Derivatives)
según el request, construirlo con sus dependencias, y ejecutarlo.

SSOT de la lógica de selección e instanciación de pipelines.

Por qué existe esta capa
------------------------
Sin esta capa, dagster_assets/ y main.py deben conocer:
  - Qué clase concreta instanciar (OHLCVPipeline vs TradesPipeline)
  - Cómo construir CCXTAdapter con credentials y resilience
  - Qué modo pasarle (incremental/backfill/repair)

Con PipelineOrchestrator:
  - El caller solo conoce PipelineRequest (DTO)
  - La selección y construcción está centralizada (SSOT)
  - Añadir DerivativesPipeline no cambia ningún caller (OCP)

Relación con PipelineTriggerPort
---------------------------------
PipelineTriggerPort (ports/input/) define el contrato abstracto
que los pipelines implementan. PipelineOrchestrator construye e
invoca cualquier PipelineTriggerPort concreto.

Diagrama de flujo
-----------------
  Dagster asset / main.py / test
      │
      ▼
  PipelineOrchestrator.run(request)
      │
      ├─ _build_pipeline(request) → PipelineTriggerPort
      │       ├─ ohlcv       → OHLCVPipeline(symbols, timeframes, ...)
      │       ├─ trades      → TradesPipeline(...)
      │       └─ derivatives → DerivativesPipeline(...)
      │
      └─ pipeline.run(mode=request.mode) → PipelineSummary

Principios: SRP · DIP · OCP · SSOT · KISS · Fail-Soft · Fail-Fast
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from loguru import logger

from market_data.ports.input.pipeline_trigger import (
    PipelineTriggerPort,
    PipelineModeStr,
)

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

    exchange:            str
    market_type:         str
    pipeline:            PipelineType

    # Modo de ejecución
    mode:                PipelineModeStr       = "incremental"

    # Parámetros de construcción del adapter — provistos por el caller
    credentials:         Optional[dict]        = None
    resilience:          Optional[Any]         = None

    # Parámetros de selección de datos — provistos por el caller
    symbols:             Optional[list[str]]   = None
    timeframes:          Optional[list[str]]   = None
    start_date:          Optional[str]         = None
    auto_lookback_days:  Optional[int]         = None

    # Parámetros de control
    run_id:              Optional[str]         = None
    dry_run:             bool                  = False
    extra:               dict                  = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Fail-Fast: valida invariantes del DTO en construcción."""
        if not self.exchange:
            raise ValueError("PipelineRequest.exchange no puede estar vacío")
        if not self.market_type:
            raise ValueError("PipelineRequest.market_type no puede estar vacío")
        if self.pipeline not in ("ohlcv", "trades", "derivatives"):
            raise ValueError(
                f"PipelineRequest.pipeline inválido: {self.pipeline!r}. "
                f"Valores: 'ohlcv', 'trades', 'derivatives'"
            )
        if self.mode not in ("incremental", "backfill", "repair"):
            raise ValueError(
                f"PipelineRequest.mode inválido: {self.mode!r}. "
                f"Valores: 'incremental', 'backfill', 'repair'"
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
    Composition root de la capa de aplicación: único lugar donde se
    instancian pipelines concretos (SSOT).

    Uso básico (desarrollo / tests)
    --------------------------------
    orchestrator = PipelineOrchestrator()
    summary = await orchestrator.run(PipelineRequest(
        exchange    = "bybit",
        market_type = "spot",
        pipeline    = "ohlcv",
        mode        = "incremental",
    ))

    Uso con credenciales (producción via Dagster)
    ----------------------------------------------
    request = PipelineRequest(
        exchange           = exc_name,
        market_type        = market_type,
        pipeline           = "ohlcv",
        mode               = "incremental",
        credentials        = exc_cfg.ccxt_credentials(),
        resilience         = exc_cfg.resilience,
        symbols            = exc_cfg.markets.spot_symbols,
        timeframes         = app_cfg.pipeline.historical.timeframes,
        start_date         = app_cfg.pipeline.historical.start_date,
        auto_lookback_days = app_cfg.pipeline.historical.auto_lookback_days,
        dry_run            = app_cfg.safety.dry_run,
    )
    summary = await orchestrator.run(request)

    Extensión (OCP)
    ---------------
    Añadir nuevo pipeline:
      1. Crear la clase (implementa PipelineTriggerPort)
      2. Añadir case en _build_pipeline()
      3. Añadir literal a PipelineType
      Ningún caller existente cambia.

    Fail-Soft
    ---------
    run() nunca lanza excepción al caller.
    _build_pipeline() falla → retorna None con log de error.
    pipeline.run() falla → retorna None con log de error.
    """

    async def run(
        self,
        request: PipelineRequest,
    ) -> Optional[object]:
        """
        Ejecuta el pipeline descrito por el request.

        Returns
        -------
        PipelineSummary del pipeline ejecutado, o None si algo falló.

        SafeOps: nunca lanza excepción al caller.
        """
        logger.info("PipelineOrchestrator.run | {}", request)

        pipeline = self._build_pipeline(request)
        if pipeline is None:
            logger.error(
                "PipelineOrchestrator: construcción fallida | {}", request,
            )
            return None

        try:
            summary = await pipeline.run(mode=request.mode)
            logger.info(
                "PipelineOrchestrator.run OK | {} summary={}",
                request, summary,
            )
            return summary
        except Exception as exc:
            logger.error(
                "PipelineOrchestrator.run FAILED | {} err={} type={}",
                request, exc, type(exc).__name__,
            )
            return None

    def _build_pipeline(
        self,
        request: PipelineRequest,
    ) -> Optional[PipelineTriggerPort]:
        """
        Factory interna — construye el pipeline correcto para el request.

        Imports lazy dentro del método (DIP): application/ no depende
        de adapters/ en nivel de módulo — la dependencia se resuelve
        solo en construcción, nunca en import time.

        Returns None si la construcción falla (Fail-Soft hacia el caller).
        """
        try:
            if request.pipeline == "ohlcv":
                return self._build_ohlcv_pipeline(request)
            elif request.pipeline == "trades":
                return self._build_trades_pipeline(request)
            elif request.pipeline == "derivatives":
                return self._build_derivatives_pipeline(request)
            else:
                # Defensivo — PipelineRequest.__post_init__ ya validó.
                logger.error(
                    "PipelineOrchestrator._build_pipeline: tipo desconocido {}",
                    request.pipeline,
                )
                return None
        except Exception as exc:
            logger.error(
                "PipelineOrchestrator._build_pipeline error | {} err={} type={}",
                request, exc, type(exc).__name__,
            )
            return None

    # ------------------------------------------------------------------
    # Builders privados — SRP: uno por tipo de pipeline
    # ------------------------------------------------------------------

    def _build_ohlcv_pipeline(
        self,
        request: PipelineRequest,
    ) -> PipelineTriggerPort:
        """
        Construye OHLCVPipeline completo con sus dependencias.

        credentials y resilience se pasan a CCXTAdapter si el caller
        los proveyó (producción). Si son None, CCXTAdapter usa defaults
        (desarrollo / tests sin infra real).

        symbols, timeframes, start_date y auto_lookback_days se pasan
        a OHLCVPipeline si el caller los proveyó. Si son None,
        OHLCVPipeline usa su configuración interna.
        """
        from market_data.processing.pipelines.ohlcv_pipeline import OHLCVPipeline
        from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter

        adapter_kwargs: dict = {
            "exchange_id": request.exchange,
            "market_type": request.market_type,
        }
        if request.credentials is not None:
            adapter_kwargs["credentials"] = request.credentials
        if request.resilience is not None:
            adapter_kwargs["resilience"] = request.resilience

        adapter = CCXTAdapter(**adapter_kwargs)

        pipeline_kwargs: dict = {
            "exchange":    request.exchange,
            "market_type": request.market_type,
            "adapter":     adapter,
            "dry_run":     request.dry_run,
        }
        if request.symbols is not None:
            pipeline_kwargs["symbols"] = request.symbols
        if request.timeframes is not None:
            pipeline_kwargs["timeframes"] = request.timeframes
        if request.start_date is not None:
            pipeline_kwargs["start_date"] = request.start_date
        if request.auto_lookback_days is not None:
            pipeline_kwargs["auto_lookback_days"] = request.auto_lookback_days

        return OHLCVPipeline(**pipeline_kwargs)

    def _build_trades_pipeline(
        self,
        request: PipelineRequest,
    ) -> PipelineTriggerPort:
        """Construye TradesPipeline. Import lazy (DIP)."""
        from market_data.processing.pipelines.trades_pipeline import TradesPipeline
        from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter

        adapter_kwargs: dict = {
            "exchange_id": request.exchange,
            "market_type": request.market_type,
        }
        if request.credentials is not None:
            adapter_kwargs["credentials"] = request.credentials
        if request.resilience is not None:
            adapter_kwargs["resilience"] = request.resilience

        adapter = CCXTAdapter(**adapter_kwargs)

        return TradesPipeline(
            exchange    = request.exchange,
            market_type = request.market_type,
            adapter     = adapter,
            dry_run     = request.dry_run,
        )

    def _build_derivatives_pipeline(
        self,
        request: PipelineRequest,
    ) -> PipelineTriggerPort:
        """Construye DerivativesPipeline. Import lazy (DIP)."""
        from market_data.processing.pipelines.derivatives_pipeline import DerivativesPipeline
        from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter

        adapter_kwargs: dict = {
            "exchange_id": request.exchange,
            "market_type": request.market_type,
        }
        if request.credentials is not None:
            adapter_kwargs["credentials"] = request.credentials
        if request.resilience is not None:
            adapter_kwargs["resilience"] = request.resilience

        adapter = CCXTAdapter(**adapter_kwargs)

        return DerivativesPipeline(
            exchange    = request.exchange,
            market_type = request.market_type,
            adapter     = adapter,
            dry_run     = request.dry_run,
        )
