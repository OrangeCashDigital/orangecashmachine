# -*- coding: utf-8 -*-
"""
market_data/application/use_cases/pipeline_orchestrator.py
============================================================

PipelineOrchestrator — caso de uso de ejecución de pipelines.

Responsabilidad
---------------
Seleccionar el pipeline correcto (OHLCV / Trades / Derivatives)
según el request, construirlo con sus dependencias, y ejecutarlo.

Es el único lugar del sistema donde se toma la decisión de qué
pipeline instanciar — SSOT de la lógica de selección.

Por qué existe esta capa
------------------------
Sin esta capa, dagster_assets/ y main.py deben conocer:
  - Qué clase concreta instanciar (OHLCVPipeline vs TradesPipeline)
  - Cómo construir CCXTAdapter con los parámetros correctos
  - Qué modo pasarle (incremental/backfill/repair)

Con PipelineOrchestrator:
  - El caller solo conoce PipelineRequest (DTO)
  - La selección y construcción está centralizada (SSOT)
  - Añadir DerivativesPipeline no cambia ningún caller (OCP)

Relación con PipelineTriggerPort
---------------------------------
PipelineTriggerPort (ports/input/) define el contrato abstracto
que los pipelines implementan. PipelineOrchestrator es el caso de
uso que construye e invoca cualquier PipelineTriggerPort concreto.

Diagrama de flujo
-----------------
  Caller (dagster_asset / main.py / test)
      │
      ▼
  PipelineOrchestrator.run(request)
      │
      ├─ _build_pipeline(request) → PipelineTriggerPort
      │       ├─ OHLCV    → OHLCVPipeline(...)
      │       ├─ trades   → TradesPipeline(...)
      │       └─ deriv.   → DerivativesPipeline(...)
      │
      └─ pipeline.run(mode=request.mode) → PipelineSummary

Principios: SRP · DIP · OCP · SSOT · KISS · Fail-Soft
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Literal, Optional

from loguru import logger

from market_data.ports.input.pipeline_trigger import (
    PipelineTriggerPort,
    PipelineModeStr,
)

# PipelineMode re-exportado para que los callers no dependan del port directamente
PipelineMode = PipelineModeStr

# Tipos de pipeline soportados — SSOT de la taxonomía de datos
PipelineType = Literal["ohlcv", "trades", "derivatives"]


# =============================================================================
# DTO de request — entrada al caso de uso
# =============================================================================

@dataclass
class PipelineRequest:
    """
    DTO de entrada para PipelineOrchestrator.

    Inmutable en práctica: no modificar campos tras construcción.
    Fail-Fast: __post_init__ valida invariantes básicas.

    Campos obligatorios
    -------------------
    exchange    : nombre canónico del exchange  (e.g. "bybit", "kucoin")
    market_type : tipo de mercado              ("spot" | "futures" | "linear" | "inverse")
    pipeline    : tipo de datos a ingestar     ("ohlcv" | "trades" | "derivatives")

    Campos opcionales
    -----------------
    mode        : modo de ejecución            ("incremental" | "backfill" | "repair")
                  default: "incremental"
    symbols     : lista de pares a procesar    (None = usar config del pipeline)
    timeframes  : lista de timeframes          (None = usar config del pipeline)
    run_id      : correlación externa          (None = generado por pipeline)
    dry_run     : True = no persiste datos     (útil para validación y tests)
    """

    exchange:    str
    market_type: str
    pipeline:    PipelineType
    mode:        PipelineModeStr          = "incremental"
    symbols:     Optional[list[str]]      = None
    timeframes:  Optional[list[str]]      = None
    run_id:      Optional[str]            = None
    dry_run:     bool                     = False
    extra:       dict                     = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Fail-Fast: valida invariantes del request en construcción."""
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
    Es el composition root de la capa de aplicación — el único lugar
    donde se instancian pipelines concretos (SSOT de selección).

    Uso
    ---
    orchestrator = PipelineOrchestrator()
    summary = await orchestrator.run(PipelineRequest(
        exchange    = "bybit",
        market_type = "spot",
        pipeline    = "ohlcv",
        mode        = "incremental",
    ))

    Extensión (OCP)
    ---------------
    Añadir un nuevo tipo de pipeline:
      1. Crear la clase (implementa PipelineTriggerPort)
      2. Añadir case en _build_pipeline()
      3. Añadir "nuevo_tipo" a PipelineType
      Ningún caller existente cambia.

    Fail-Soft
    ---------
    run() nunca lanza excepción hacia el caller.
    Si _build_pipeline() falla → retorna None con log de error.
    Si pipeline.run() falla internamente → retorna PipelineSummary con error.
    """

    async def run(
        self,
        request: PipelineRequest,
    ) -> Optional[object]:
        """
        Ejecuta el pipeline descrito por el request.

        Parameters
        ----------
        request : PipelineRequest con todos los parámetros de ejecución.

        Returns
        -------
        PipelineSummary del pipeline ejecutado, o None si la construcción falló.

        SafeOps: nunca lanza excepción al caller.
        """
        logger.info(
            "PipelineOrchestrator.run | {}",
            request,
        )

        pipeline = self._build_pipeline(request)
        if pipeline is None:
            logger.error(
                "PipelineOrchestrator: construcción de pipeline fallida | {}",
                request,
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
            # Fail-Soft: capturar cualquier escape inesperado del pipeline
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

        SSOT de la lógica de selección e instanciación de pipelines.
        Imports dentro del método para evitar imports circulares y
        garantizar que la capa application/ no tenga dependencias
        en nivel de módulo hacia adapters/ concretos (DIP).

        Returns
        -------
        PipelineTriggerPort listo para run(), o None si falla la construcción.
        """
        try:
            if request.pipeline == "ohlcv":
                return self._build_ohlcv_pipeline(request)
            elif request.pipeline == "trades":
                return self._build_trades_pipeline(request)
            elif request.pipeline == "derivatives":
                return self._build_derivatives_pipeline(request)
            else:
                # Nunca debería llegar aquí — PipelineRequest.__post_init__
                # valida pipeline antes. Defensivo por si se bypasea el DTO.
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
    # Builders privados — uno por tipo de pipeline (SRP)
    # ------------------------------------------------------------------

    def _build_ohlcv_pipeline(
        self,
        request: PipelineRequest,
    ) -> PipelineTriggerPort:
        """
        Construye OHLCVPipeline con sus dependencias.

        Import lazy: evita que application/ dependa de adapters/
        en nivel de módulo — la dependencia se resuelve en runtime (DIP).
        """
        # Import lazy dentro del método — DIP: application/ no depende
        # de CCXTAdapter en nivel de módulo, solo en construcción.
        from market_data.processing.pipelines.ohlcv_pipeline import OHLCVPipeline
        from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter

        adapter = CCXTAdapter(
            exchange_id = request.exchange,
            market_type = request.market_type,
        )
        return OHLCVPipeline(
            exchange    = request.exchange,
            market_type = request.market_type,
            adapter     = adapter,
            dry_run     = request.dry_run,
        )

    def _build_trades_pipeline(
        self,
        request: PipelineRequest,
    ) -> PipelineTriggerPort:
        """Construye TradesPipeline. Import lazy (DIP)."""
        from market_data.processing.pipelines.trades_pipeline import TradesPipeline
        from market_data.adapters.exchange.ccxt_adapter import CCXTAdapter

        adapter = CCXTAdapter(
            exchange_id = request.exchange,
            market_type = request.market_type,
        )
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

        adapter = CCXTAdapter(
            exchange_id = request.exchange,
            market_type = request.market_type,
        )
        return DerivativesPipeline(
            exchange    = request.exchange,
            market_type = request.market_type,
            adapter     = adapter,
            dry_run     = request.dry_run,
        )
