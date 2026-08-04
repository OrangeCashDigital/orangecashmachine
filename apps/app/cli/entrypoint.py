# -*- coding: utf-8 -*-
"""
app/cli/entrypoint.py
======================

entrypoint.run — Runner por defecto de pipeline_runner para el CLI Hydra.

Responsabilidad única
---------------------
Traducir RuntimeContext (config validada + run metadata) en una corrida
batch de PipelineOrchestrator para cada exchange/market_type configurado,
y devolver un exit code int para main.py.

Por qué existe
--------------
main.py necesitaba un default de PipelineRunner que NO fuera la clase
OHLCVPipeline importada directo (bug: instanciarla así se salta
PipelineOrchestrator + ConcretePipelineFactory y rompe con TypeError por
5 argumentos posicionales faltantes — start_date, exchange_client, fetcher,
metrics, quality). Este módulo es ese default correcto.

Reutiliza el mismo patrón de market_data/main.py::_ingestion_loop, en
versión batch (una sola pasada) en vez de loop infinito — el CLI es por
diseño un proceso que corre y termina, no un microservicio.

Contrato de errores
--------------------
Fail-Soft por exchange/market_type: un fallo en uno no aborta los demás
(se loguea y se cuenta). El run global falla (exit 1) solo si NINGÚN
exchange/market_type produjo una corrida exitosa.

Principios: SRP · DIP · KISS · SafeOps
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Literal

from loguru import logger

if TYPE_CHECKING:
    from market_data.ports.outbound.observability import MetricsPusherPort

    from ocm.runtime.context import RuntimeContext

_log = logger.bind(component="cli_entrypoint")


def run(ctx: "RuntimeContext", pusher: "MetricsPusherPort | None" = None) -> int:
    """
    Ejecuta una pasada batch de ingestión OHLCV para todos los exchanges
    configurados en AppConfig.

    Returns
    -------
    int
        0 si al menos una corrida fue exitosa, 1 si todas fallaron o no
        había ningún exchange/market_type habilitado con símbolos.
    """
    # Import lazy — application layer no se importa a nivel de módulo (DIP · startup cost)
    from market_data.application.use_cases.pipeline_orchestrator import (
        PipelineOrchestrator,
        PipelineRequest,
    )
    from market_data.infrastructure.bootstrap.composition_root import CompositionRoot

    app_cfg = ctx.app_config
    mode: Literal["incremental", "backfill", "repair"] = (
        "backfill" if app_cfg.pipeline.historical.backfill_mode else "incremental"
    )

    async def _run_all() -> tuple[int, int]:
        # Composition Root — único punto de ensamblado (BC-38). El factory se
        # obtiene de CompositionRoot.assemble(); PipelineOrchestrator exige la
        # factory por inyección y NO construye la suya propia (pipeline_orchestrator.py).
        root = CompositionRoot.assemble(app_cfg)
        orchestrator = PipelineOrchestrator(factory=root.factory)
        successes = 0
        failures = 0

        for exc_name in app_cfg.exchange_names:
            exc_cfg = app_cfg.get_exchange(exc_name)
            if exc_cfg is None:
                continue

            for market_type, symbols in [
                ("spot", getattr(exc_cfg.markets, "spot_symbols", [])),
                ("futures", getattr(exc_cfg.markets, "futures_symbols", [])),
            ]:
                if not symbols:
                    continue

                request = PipelineRequest(
                    exchange=exc_name,
                    market_type=market_type,
                    pipeline="ohlcv",
                    mode=mode,
                    credentials=exc_cfg.ccxt_credentials(),
                    resilience=exc_cfg.resilience,
                    symbols=symbols,
                    timeframes=app_cfg.pipeline.historical.timeframes,
                    start_date=app_cfg.pipeline.historical.start_date,
                    auto_lookback_days=app_cfg.pipeline.historical.auto_lookback_days,
                    run_id=ctx.run_id,
                    dry_run=app_cfg.safety.dry_run,
                )

                try:
                    _log.info(
                        "entrypoint_run_starting",
                        exchange=exc_name,
                        market_type=market_type,
                        mode=mode,
                    )
                    await orchestrator.run(request)
                    successes += 1
                except Exception as exc:
                    failures += 1
                    _log.opt(exception=True).error(
                        "entrypoint_run_failed",
                        exchange=exc_name,
                        market_type=market_type,
                        error=str(exc),
                    )

        return successes, failures

    successes, failures = asyncio.run(_run_all())

    # SafeOps: MetricsPusherPort.push() nunca lanza — implementaciones
    # (PrometheusPusher/NoopPusher) garantizan esto por contrato.
    if pusher is not None:
        pusher.push()

    if successes == 0:
        _log.error("entrypoint_no_successful_runs", failures=failures)
        return 1

    return 0
