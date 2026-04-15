"""
market_data/orchestration/flows/resample_flow.py
=================================================

Flow Prefect de resampling OHLCV.

Responsabilidad
---------------
Ejecutar ResamplePipeline cada minuto, filtrando qué timeframes
tienen su vela cerrada en el tick actual.

Diseño de schedule
------------------
Este flow corre cada 1 minuto. En cada tick, timeframes_due(now_ms)
calcula qué TF tienen su período cerrado en ese instante:

  now_ms=14:05:00  → "5m" cerrado  (14:05 % 5min == 0)
  now_ms=14:15:00  → "5m", "15m" cerrados
  now_ms=15:00:00  → "5m", "15m", "1h" cerrados

Si ningún TF tiene vela cerrada en este tick, el flow retorna
inmediatamente (skip explícito con log). Esto es correcto — un tick
que no cierra ningún período no tiene trabajo que hacer.

Separación de responsabilidades
--------------------------------
- timeframes_due()    : lógica de schedule pura — testeable sin Prefect
- ResamplePipeline    : lógica de datos — testeable sin Prefect
- resample_flow       : orquestación Prefect — glue code mínimo

Principios
----------
SOLID  – SRP: flow solo orquesta, no agrega datos
SSOT   – timeframes_due() es la única fuente de verdad del schedule
KISS   – sin estado externo, sin Redis — el schedule es matemático
SafeOps – exchange sin símbolo activo → skip explícito, no excepción

Ref: Prefect flow docs — https://docs.prefect.io/latest/develop/write-flows
"""
from __future__ import annotations

import time
from typing import List, Optional

from prefect import flow, get_run_logger

from core.config.schema import AppConfig
from core.config.runtime_context import RuntimeContext
from market_data.processing.utils.timeframe import timeframe_to_ms
from market_data.processing.pipelines.resample_pipeline import ResamplePipeline
from infra.observability.server import push_metrics


# ==============================================================================
# Fallbacks en módulo — usados solo si resample_flow recibe runtime_context=None
# (tests unitarios, ejecución directa sin Prefect).
# En producción estos valores vienen de config.pipeline.resample (SSOT en YAML).
# ==============================================================================

_FALLBACK_TFS: List[str] = ["5m", "15m", "1h", "4h", "1d"]
_FALLBACK_SOURCE_TF: str = "1m"

# Tolerancia de alineación al grid: ±30s
# Permite que el scheduler llegue un poco tarde sin saltarse el tick.
# Ejemplo: tick programado a 14:05:00 llega a 14:05:15 → sigue contando como cerrado.
_GRID_TOLERANCE_MS: int = 30_000


# ==============================================================================
# Schedule puro — testeable sin Prefect
# ==============================================================================

def timeframes_due(
    now_ms:    int,
    supported: List[str] = _FALLBACK_TFS,
    source_tf: str       = _FALLBACK_SOURCE_TF,
) -> List[str]:
    """
    Retorna los timeframes cuya vela acaba de cerrar en now_ms.

    Algoritmo
    ---------
    Una vela de TF T cierra cuando el minuto actual (floor a 1m) es
    múltiplo exacto del período T. Tolerancia ±_GRID_TOLERANCE_MS
    para absorber drift del scheduler.

    Parámetros
    ----------
    now_ms    : timestamp actual en milisegundos UTC
    supported : lista de TF a evaluar (default: _SUPPORTED_TFS)

    Retorno
    -------
    Lista de TF con vela cerrada, ordenada de menor a mayor período.
    Lista vacía si ningún TF tiene vela cerrada en este tick.

    Ejemplos
    --------
    >>> timeframes_due(14 * 3600_000 + 5 * 60_000)   # 14:05:00 UTC
    ["5m"]
    >>> timeframes_due(14 * 3600_000 + 15 * 60_000)  # 14:15:00 UTC
    ["5m", "15m"]
    >>> timeframes_due(15 * 3600_000)                 # 15:00:00 UTC
    ["5m", "15m", "1h"]
    """
    # Alinear now_ms al minuto cerrado más reciente (floor a 1m)
    minute_ms    = timeframe_to_ms(source_tf)
    minute_floor = (now_ms // minute_ms) * minute_ms

    due = []
    for tf in supported:
        period_ms = timeframe_to_ms(tf)
        # Cierra si el minuto actual es múltiplo del período, con tolerancia
        remainder = minute_floor % period_ms
        if remainder <= _GRID_TOLERANCE_MS or remainder >= (period_ms - _GRID_TOLERANCE_MS):
            due.append(tf)

    # Ordenar por período ascendente — procesar TF cortos antes que largos
    return sorted(due, key=lambda t: timeframe_to_ms(t))


# ==============================================================================
# Prefect Flow
# ==============================================================================

@flow(
    name="resample_ohlcv",
    description=(
        "Resamplea candles 1m Silver a timeframes agregados (5m, 15m, 1h, 4h, 1d). "
        "Corre cada minuto; solo procesa TF con vela cerrada en el tick actual."
    ),
    log_prints=True,
    retries=0,
)
async def resample_flow(
    runtime_context: Optional[RuntimeContext] = None,
) -> None:
    """
    Flow de resampling OHLCV.

    Flujo
    -----
    1. Resolver configuración desde RuntimeContext
    2. Calcular timeframes con vela cerrada en este tick (timeframes_due)
    3. Si ninguno → skip explícito y retornar
    4. Por cada exchange activo:
       a. Obtener símbolos spot activos
       b. Ejecutar ResamplePipeline(symbols, due_tfs, exchange, market_type)
    5. Push métricas → Pushgateway
    """
    log = get_run_logger()

    if isinstance(runtime_context, dict):
        runtime_context = RuntimeContext.from_dict(runtime_context)  # type: ignore[assignment]

    if runtime_context is None:
        raise RuntimeError(
            "resample_flow requiere un RuntimeContext resuelto por el entrypoint."
        )

    config: AppConfig = runtime_context.app_config
    now_ms = int(time.time() * 1000)

    # ── 1. Qué TF tienen vela cerrada ahora ─────────────────────────────────
    resample_cfg = config.pipeline.resample
    due_tfs = timeframes_due(
        now_ms,
        supported=resample_cfg.targets,
        source_tf=resample_cfg.source_tf,
    )

    if not due_tfs:
        log.debug(
            "resample_flow skip — no hay timeframes con vela cerrada en este tick"
        )
        return

    log.info(
        "resample_flow starting | due_tfs=%s now_ms=%s",
        due_tfs, now_ms,
    )

    # ── 2. Procesar cada exchange ────────────────────────────────────────────
    for exc_cfg in config.exchanges:
        exchange_name = exc_cfg.name.value

        # Símbolos spot activos — ResamplePipeline opera sobre spot por defecto.
        # Para futuros: crear instancia separada con market_type="swap".
        symbols: List[str] = exc_cfg.markets.spot_symbols
        if not symbols:
            log.debug(
                "resample_flow skip exchange — sin símbolos spot | exchange=%s",
                exchange_name,
            )
            continue

        try:
            pipeline = ResamplePipeline(
                symbols     = symbols,
                timeframes  = due_tfs,
                source_tf   = resample_cfg.source_tf,
                exchange    = exchange_name,
                market_type = "spot",
                dry_run     = config.safety.dry_run,
            )
            summary = await pipeline.run(now_ms=now_ms)

            log.info(
                "resample_flow exchange done | exchange=%s "
                "ok=%s skipped=%s failed=%s rows=%s duration_ms=%s",
                exchange_name,
                summary.succeeded, summary.skipped, summary.failed,
                summary.total_rows, summary.duration_ms,
            )

        except Exception as exc:
            # SafeOps: fallo de un exchange no aborta los demás
            log.error(
                "resample_flow exchange failed | exchange=%s error=%s",
                exchange_name, exc,
            )

    # ── 3. Push métricas ─────────────────────────────────────────────────────
    if config.observability.metrics.enabled:
        for exc_cfg in config.exchanges:
            push_metrics(
                exchange = exc_cfg.name.value,
                gateway  = runtime_context.run_config.pushgateway
                           if hasattr(runtime_context, "run_config")
                           and runtime_context.run_config is not None
                           else "http://localhost:9091",
            )
