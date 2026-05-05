# -*- coding: utf-8 -*-
"""
market_data/application/use_cases/resample_ohlcv.py
=====================================================

ResampleUseCase — caso de uso de resampling OHLCV local.

Responsabilidad
---------------
Orquestar el resampling 1m → [5m, 15m, 1h, 4h, 1d] para un exchange
y market_type dados, leyendo configuración desde AppConfig e inyectando
storage via StorageFactoryPort.

Por qué existe esta capa
------------------------
Sin esta capa, los inbound adapters (dagster_assets/, resample_flow.py)
deben conocer:
  - Cómo extraer símbolos y targets desde AppConfig
  - Cómo construir storage via factory con dry_run correcto
  - Cómo instanciar ResamplePipeline y manejar el loop async
  - Cómo acumular y estructurar el resultado

Con ResampleUseCase:
  - Los adapters solo conocen el DTO ResampleRequest
  - Toda la lógica de orquestación está aquí (SSOT)
  - Añadir un nuevo adapter (CLI, HTTP) = cero cambios aquí (OCP)

Relación con ResamplePipeline
-----------------------------
ResamplePipeline es el motor puro de resampling (procesamiento).
ResampleUseCase lo orquesta: resuelve dependencias, decide qué datos
procesar, y delega la ejecución al pipeline.

Diagrama
--------
  Dagster asset / resample_flow / CLI
      │
      ▼
  ResampleUseCase.execute(request)
      │
      ├─ valida request (Fail-Fast)
      ├─ extrae símbolos y targets desde AppConfig
      ├─ construye storage via StorageFactoryPort (DIP)
      ├─ instancia ResamplePipeline(symbols, timeframes, exchange, storage)
      └─ asyncio.run(pipeline.run()) → ResampleSummary

Principios: SRP · DIP · OCP · SSOT · KISS · Fail-Fast · SafeOps
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, List, Optional

from loguru import logger

from market_data.ports.storage_factory import StorageFactoryPort


# =============================================================================
# DTO de request — entrada al caso de uso
# =============================================================================

@dataclass
class ResampleRequest:
    """
    DTO de entrada para ResampleUseCase.

    Fail-Fast: __post_init__ valida invariantes en construcción.
    Inmutable en práctica: no modificar campos tras construcción.

    Campos obligatorios
    -------------------
    exchange     : nombre canónico del exchange ("bybit", "kucoin")
    market_type  : tipo de mercado ("spot" | "futures")
    app_config   : AppConfig completo — fuente de símbolos, targets, source_tf

    Campos opcionales
    -----------------
    dry_run      : True = no persiste datos. Default: False.
    run_id       : correlación externa para trazabilidad. None = sin correlación.
    now_ms       : timestamp de referencia en ms UTC.
                   None = ResamplePipeline usa time.time() internamente.
                   Inyectable en tests para reproducibilidad.
    """
    exchange:    str
    market_type: str
    app_config:  Any   # AppConfig — tipado como Any para evitar import circular

    dry_run:     bool          = False
    run_id:      Optional[str] = None
    now_ms:      Optional[int] = None

    def __post_init__(self) -> None:
        """Fail-Fast: valida invariantes del DTO."""
        if not self.exchange:
            raise ValueError("ResampleRequest.exchange no puede estar vacío")
        if not self.market_type:
            raise ValueError("ResampleRequest.market_type no puede estar vacío")
        if self.app_config is None:
            raise ValueError("ResampleRequest.app_config no puede ser None")

    def __str__(self) -> str:
        return (
            f"ResampleRequest({self.exchange}/{self.market_type}"
            f" dry_run={self.dry_run} run_id={self.run_id})"
        )


# =============================================================================
# Result type — salida del caso de uso
# =============================================================================

@dataclass
class ResampleUseCaseResult:
    """
    Resultado de ResampleUseCase.execute().

    status: "ok" | "skipped" | "partial" | "failed"
    """
    status:       str
    exchange:     str
    market_type:  str
    rows:         int  = 0
    symbols:      List[str] = field(default_factory=list)
    targets:      List[str] = field(default_factory=list)
    source_tf:    str  = "1m"
    run_id:       Optional[str] = None
    error:        str  = ""

    @property
    def succeeded(self) -> bool:
        return self.status in ("ok", "partial")


# =============================================================================
# ResampleUseCase
# =============================================================================

class ResampleUseCase:
    """
    Caso de uso: resampling OHLCV local 1m → timeframes target.

    Depende de StorageFactoryPort (DIP) — nunca de IcebergStorage directamente.
    El storage concreto se inyecta en construcción (composition root).

    Uso
    ---
        factory = IcebergStorageFactory()
        use_case = ResampleUseCase(storage_factory=factory)
        result = use_case.execute(ResampleRequest(
            exchange    = "bybit",
            market_type = "spot",
            app_config  = app_cfg,
            dry_run     = False,
        ))

    Fail-Fast en construcción (storage_factory None).
    Fail-Soft en ejecución (retorna ResampleUseCaseResult con error).
    """

    def __init__(self, storage_factory: StorageFactoryPort) -> None:
        if storage_factory is None:
            raise ValueError(
                "ResampleUseCase: storage_factory no puede ser None —"
                " inyéctalo desde el composition root."
            )
        self._storage_factory = storage_factory

    # =========================================================================
    # Public API
    # =========================================================================

    def execute(self, request: ResampleRequest) -> ResampleUseCaseResult:
        """
        Ejecuta el resampling para el exchange/market_type del request.

        Fail-Fast: valida configuración antes de instanciar pipeline.
        Fail-Soft: captura excepciones y retorna resultado con error.

        Returns
        -------
        ResampleUseCaseResult con status y métricas de la ejecución.
        """
        logger.info("ResampleUseCase.execute | {}", request)

        # ── 1. Resolver configuración desde AppConfig (Fail-Fast) ──────────────
        try:
            symbols, targets, source_tf = self._resolve_config(request)
        except Exception as exc:
            logger.error(
                "ResampleUseCase: config error | {} err={}", request, exc,
            )
            return ResampleUseCaseResult(
                status      = "failed",
                exchange    = request.exchange,
                market_type = request.market_type,
                run_id      = request.run_id,
                error       = str(exc),
            )

        # ── 2. Guard: sin símbolos configurados → skip explícito ──────────────────
        if not symbols:
            logger.warning(
                "ResampleUseCase: sin símbolos para {}/{} — skip",
                request.exchange, request.market_type,
            )
            return ResampleUseCaseResult(
                status      = "skipped",
                exchange    = request.exchange,
                market_type = request.market_type,
                symbols     = [],
                targets     = targets,
                source_tf   = source_tf,
                run_id      = request.run_id,
            )

        # ── 3. Construir storage via factory (DIP) ────────────────────────────
        try:
            storage = self._storage_factory.get_storage(
                exchange    = request.exchange,
                market_type = request.market_type,
                dry_run     = request.dry_run,
            )
        except Exception as exc:
            logger.error(
                "ResampleUseCase: storage error | {} err={}", request, exc,
            )
            return ResampleUseCaseResult(
                status      = "failed",
                exchange    = request.exchange,
                market_type = request.market_type,
                symbols     = symbols,
                targets     = targets,
                source_tf   = source_tf,
                run_id      = request.run_id,
                error       = f"storage init failed: {exc}",
            )

        # ── 4. Instanciar y ejecutar ResamplePipeline ─────────────────────────
        try:
            summary = self._run_pipeline(
                request   = request,
                symbols   = symbols,
                targets   = targets,
                source_tf = source_tf,
                storage   = storage,
            )
        except Exception as exc:
            logger.error(
                "ResampleUseCase: pipeline error | {} err={}", request, exc,
            )
            return ResampleUseCaseResult(
                status      = "failed",
                exchange    = request.exchange,
                market_type = request.market_type,
                symbols     = symbols,
                targets     = targets,
                source_tf   = source_tf,
                run_id      = request.run_id,
                error       = str(exc),
            )

        result = ResampleUseCaseResult(
            status      = summary.status,
            exchange    = request.exchange,
            market_type = request.market_type,
            rows        = summary.total_rows,
            symbols     = symbols,
            targets     = targets,
            source_tf   = source_tf,
            run_id      = request.run_id,
        )
        logger.info(
            "ResampleUseCase.execute OK | {} status={} rows={}",
            request, result.status, result.rows,
        )
        return result

    # =========================================================================
    # Internals
    # =========================================================================

    def _resolve_config(
        self,
        request: ResampleRequest,
    ) -> tuple[list, list, str]:
        """
        Extrae símbolos, targets y source_tf desde AppConfig.

        Fail-Fast: lanza ValueError si la configuración del exchange no existe.

        Returns
        -------
        (symbols, targets, source_tf)
        """
        app_cfg = request.app_config
        exc_cfg = app_cfg.get_exchange(request.exchange)

        if exc_cfg is None:
            raise ValueError(
                f"Exchange '{request.exchange}' no encontrado en AppConfig. "
                f"Exchanges configurados: {[e.name for e in app_cfg.exchanges]}"
            )

        symbols = (
            exc_cfg.markets.spot_symbols
            if request.market_type == "spot"
            else exc_cfg.markets.futures_symbols
        )
        targets   = list(app_cfg.pipeline.resample.targets)
        source_tf = app_cfg.pipeline.resample.source_tf

        return list(symbols), targets, source_tf

    def _run_pipeline(
        self,
        request:   ResampleRequest,
        symbols:   List[str],
        targets:   List[str],
        source_tf: str,
        storage:   object,
    ) -> object:
        """
        Instancia ResamplePipeline y ejecuta su loop async.

        Import lazy (DIP): application/ no depende de processing/ en
        nivel de módulo — la dependencia se resuelve solo en ejecución.

        ResamplePipeline.run() es async — se ejecuta via asyncio.run()
        porque Dagster y Prefect no exponen un event loop al caller.
        """
        from market_data.processing.pipelines.resample_pipeline import ResamplePipeline

        pipeline = ResamplePipeline(
            symbols     = symbols,
            timeframes  = targets,
            exchange    = request.exchange,
            market_type = request.market_type,
            storage     = storage,
            dry_run     = request.dry_run,
        )

        logger.info(
            "ResampleUseCase: pipeline iniciando | {}/{} symbols={} targets={} source_tf={}",
            request.exchange, request.market_type,
            len(symbols), targets, source_tf,
        )

        # asyncio.run() crea un event loop nuevo si no existe uno activo.
        # Correcto para callers sincónicos (Dagster assets, Prefect tasks).
        # Si el caller ya tiene un loop activo, usar await en su lugar.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Caller tiene event loop activo (ej: entorno Jupyter / tests async)
            # Crear una tarea y esperar en un loop anidado via nest_asyncio si disponible.
            # En producción (Dagster sync assets) este branch no se ejecuta.
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(asyncio.run, pipeline.run(now_ms=request.now_ms))
                return future.result()
        else:
            return asyncio.run(pipeline.run(now_ms=request.now_ms))
