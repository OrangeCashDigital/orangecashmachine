"""
market_data/processing/pipelines/resample_pipeline.py
======================================================

Pipeline de resampling OHLCV local.

Responsabilidad
---------------
Construir timeframes agregados (5m, 15m, 1h, 4h, 1d) desde
datos Silver 1m almacenados en Iceberg, sin contactar al exchange.

Diseño
------
- Fuente única de verdad: Silver 1m  (IcebergStorage.load_ohlcv)
- Destino:               Silver Nt   (IcebergStorage.save_ohlcv, mismo catalog)
- Agregación:            align_to_grid  (open/high/low/close/volume)
- Ventana de lectura:    configurable via lookback_candles (default: 2× período)
- Idempotencia:          append con dedup nativo de IcebergStorage._normalize_df

Principios
----------
SOLID  – SRP: solo resampling, sin fetch ni network
SSOT   – una sola fuente (1m Silver), una sola función de agregación (align_to_grid)
OCP    – agregar nuevo TF = agregar entrada a _RESAMPLE_TARGETS, sin tocar lógica
KISS   – sin worker pool, sin throttle — es CPU-bound local, no I/O de red
SafeOps – cada par con try/except independiente; fallo de uno no aborta los demás
DRY    – align_to_grid es la única función de agregación OHLCV del sistema

Uso
---
    pipeline = ResamplePipeline(
        symbols      = ["BTC/USDT"],
        timeframes   = ["5m", "1h"],
        exchange     = "bybit",
        market_type  = "spot",
    )
    summary = await pipeline.run(now_ms=int(time.time() * 1000))

Integración con schedules
-------------------------
El caller (resample_flow.py) decide qué timeframes correr en cada tick
via timeframes_due(now_ms). ResamplePipeline ejecuta exactamente la lista
recibida — no tiene opinión sobre el schedule.

Ref: Pandas resample docs — https://pandas.pydata.org/docs/reference/resampling.html
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd

from ocm_platform.observability import bind_pipeline
from market_data.processing.utils.timeframe import (
    timeframe_to_ms,
    VALID_TIMEFRAMES,
    InvalidTimeframeError,
)
from market_data.processing.utils.timeframe import align_to_grid
from market_data.storage.iceberg.iceberg_storage import IcebergStorage

_log = bind_pipeline("resample_pipeline")

# ==============================================================================
# Constantes de dominio
# ==============================================================================

# Timeframes que ResamplePipeline puede producir.
# 1m NO está aquí — es la fuente, no un target de resampling.
# OCP: agregar un TF = agregar su string aquí. Cero cambios en lógica.
_RESAMPLE_TARGETS: frozenset[str] = frozenset({
    "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d",
})

# Multiplicador de ventana de lectura.
# Leemos 2× el período del TF target para garantizar al menos 1 vela cerrada
# incluso si la última candle 1m llegó con lag.
# Ejemplo: resample a 1h → leer 120 candles 1m (2 horas de historia)
_LOOKBACK_MULTIPLIER: int = 2

# Mínimo de candles 1m necesarios para producir al menos 1 vela en el TF target.
# Evita writes vacíos que generan snapshots Iceberg innecesarios.
_MIN_SOURCE_CANDLES: int = 2


# ==============================================================================
# Result types
# ==============================================================================

@dataclass
class ResampleResult:
    """Resultado de resampling de un par (symbol, target_timeframe)."""
    symbol:      str
    timeframe:   str          # target timeframe (ej: "5m")
    exchange:    str  = ""
    market_type: str  = ""
    rows:        int  = 0
    skipped:     bool = False  # True si no había datos 1m suficientes
    error:       str  = ""
    duration_ms: int  = 0

    @property
    def success(self) -> bool:
        return not self.error and not self.skipped


@dataclass
class ResampleSummary:
    """Resumen agregado de una ejecución de ResamplePipeline."""
    results:     List[ResampleResult] = field(default_factory=list)
    duration_ms: int                  = 0
    exchange:    str                  = ""
    market_type: str                  = ""

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
    def status(self) -> str:
        if self.failed == 0 and self.total > 0:
            return "ok"
        if self.failed > 0 and self.succeeded > 0:
            return "partial"
        return "failed"

    def log(self, log) -> None:
        log.info(
            "ResampleSummary | exchange=%s market=%s status=%s "
            "total=%s ok=%s skipped=%s failed=%s rows=%s duration_ms=%s",
            self.exchange, self.market_type, self.status,
            self.total, self.succeeded, self.skipped, self.failed,
            self.total_rows, self.duration_ms,
        )
        for r in self.results:
            if r.error:
                log.warning(
                    "  \u2717 %s/%s error=%s duration=%sms",
                    r.symbol, r.timeframe, r.error, r.duration_ms,
                )
            elif r.skipped:
                log.debug("  \u2197 %s/%s skipped (sin datos 1m)", r.symbol, r.timeframe)
            else:
                log.info(
                    "  \u2713 %s/%s rows=%s duration=%sms",
                    r.symbol, r.timeframe, r.rows, r.duration_ms,
                )


# ==============================================================================
# Helpers internos
# ==============================================================================

def _validate_targets(timeframes: List[str]) -> None:
    """
    Fail-Fast: valida que todos los timeframes target sean resampleables.

    Reglas:
    - Deben estar en VALID_TIMEFRAMES (formato reconocido)
    - Deben estar en _RESAMPLE_TARGETS (no incluye 1m — es fuente, no target)

    Raises
    ------
    InvalidTimeframeError  si el formato es inválido
    ValueError             si el TF es válido pero no resampleable (ej: "1m")
    """
    for tf in timeframes:
        if tf not in VALID_TIMEFRAMES:
            raise InvalidTimeframeError(tf)
        if tf not in _RESAMPLE_TARGETS:
            raise ValueError(
                f"Timeframe '{tf}' no es un target de resampling v\xe1lido. "
                f"V\xe1lidos: {sorted(_RESAMPLE_TARGETS)}"
            )


def _lookback_candles(target_tf: str) -> int:
    """
    Calcula cuántas candles 1m leer para producir el TF target.

    Ventana = _LOOKBACK_MULTIPLIER × (período_target / período_1m).
    Ejemplo:
      "1h"  → 2 × 60  = 120 candles 1m
      "4h"  → 2 × 240 = 480 candles 1m
      "1d"  → 2 × 1440 = 2880 candles 1m
    """
    period_ms    = timeframe_to_ms(target_tf)
    candle_ms    = timeframe_to_ms("1m")
    return _LOOKBACK_MULTIPLIER * (period_ms // candle_ms)


def _resample_df(
    df_1m:      pd.DataFrame,
    target_tf:  str,
    symbol:     str,
    exchange:   str,
) -> pd.DataFrame:
    """
    Resamplea un DataFrame OHLCV de 1m al target_tf.

    Delega en align_to_grid — SSOT de agregación OHLCV del sistema.
    align_to_grid aplica floor + groupby con semántica OHLCV correcta
    (open=first, high=max, low=min, close=last, volume=sum).

    Excluye la vela parcial (en curso) del output:
    La última vela del TF target puede estar incompleta si now_ms
    no coincide exactamente con el cierre del período. Se elimina
    la última fila del resultado para garantizar que solo se escriben
    velas cerradas.

    Invariante: retorna DataFrame con columnas [timestamp, open, high, low, close, volume]
    """
    if df_1m is None or df_1m.empty:
        return pd.DataFrame()

    resampled = align_to_grid(
        df        = df_1m,
        timeframe = target_tf,
        exchange  = exchange,
        symbol    = symbol,
    )

    if resampled.empty:
        return resampled

    # Eliminar la última fila — puede ser vela parcial (en curso).
    # Solo escribimos velas cuyo período ha cerrado completamente.
    # El caller (resample_flow) ya filtra por timeframes_due() antes de llamar,
    # pero este guard es una segunda línea de defensa (SafeOps).
    return resampled.iloc[:-1].reset_index(drop=True)


# ==============================================================================
# ResamplePipeline
# ==============================================================================

class ResamplePipeline:
    """
    Pipeline de resampling OHLCV local.

    Lee Silver 1m desde IcebergStorage, produce y persiste los TF
    target configurados. Sin acceso al exchange — 100% local.

    Parámetros
    ----------
    symbols      : lista de símbolos a procesar (ej: ["BTC/USDT"])
    timeframes   : lista de TF target (ej: ["5m", "1h", "1d"])
    exchange     : nombre del exchange (ej: "bybit")
    market_type  : tipo de mercado (ej: "spot")
    dry_run      : si True, lee pero no escribe en Iceberg
    """

    def __init__(
        self,
        symbols:     List[str],
        timeframes:  List[str],
        exchange:    str,
        market_type: str  = "spot",
        dry_run:     bool = False,
    ) -> None:
        if not symbols:
            raise ValueError("symbols no puede estar vac\xedo")
        if not timeframes:
            raise ValueError("timeframes no puede estar vac\xedo")
        _validate_targets(timeframes)   # Fail-Fast

        self._symbols     = symbols
        self._timeframes  = timeframes
        self._exchange    = exchange.lower()
        self._market_type = market_type.lower()
        self._dry_run     = dry_run

        # Storage de lectura (1m source) y escritura (target TFs).
        # Misma instancia — misma tabla Iceberg, distinto timeframe en el filtro.
        self._storage = IcebergStorage(
            exchange     = self._exchange,
            market_type  = self._market_type,
            dry_run      = dry_run,
        )
        self._log = bind_pipeline(
            "resample_pipeline",
            exchange    = self._exchange,
            market_type = self._market_type,
        )

    # =========================================================================
    # Public
    # =========================================================================

    async def run(
        self,
        now_ms: Optional[int] = None,
    ) -> ResampleSummary:
        """
        Ejecuta el resampling para todos los pares (symbol × timeframe).

        Parámetros
        ----------
        now_ms : timestamp de referencia en ms UTC (default: tiempo actual).
                 Determina el límite de lectura de datos 1m.
                 Permite inyección en tests sin mock de time.time().

        Retorno
        -------
        ResampleSummary con resultados por par.
        """
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        pairs      = [(s, tf) for s in self._symbols for tf in self._timeframes]
        total      = len(pairs)
        results    = []
        run_start  = time.monotonic()

        self._log.info(
            "ResamplePipeline iniciando | symbols=%s timeframes=%s pairs=%s dry_run=%s",
            len(self._symbols), len(self._timeframes), total, self._dry_run,
        )

        for idx, (symbol, target_tf) in enumerate(pairs, 1):
            result = await self._resample_pair(
                symbol    = symbol,
                target_tf = target_tf,
                now_ms    = now_ms,
                idx       = idx,
                total     = total,
            )
            results.append(result)

        duration_ms = int((time.monotonic() - run_start) * 1000)
        summary = ResampleSummary(
            results     = results,
            duration_ms = duration_ms,
            exchange    = self._exchange,
            market_type = self._market_type,
        )
        summary.log(self._log)
        return summary

    # =========================================================================
    # Internals
    # =========================================================================

    async def _resample_pair(
        self,
        symbol:    str,
        target_tf: str,
        now_ms:    int,
        idx:       int,
        total:     int,
    ) -> ResampleResult:
        """
        Resamplea un par individual. SafeOps: nunca relanza excepción.

        Flujo
        -----
        1. Calcular ventana de lectura (lookback_candles × 1m)
        2. load_ohlcv(symbol, "1m", start=window_start)
        3. Validar mínimo de candles fuente
        4. _resample_df → align_to_grid → excluir vela parcial
        5. save_ohlcv(result, symbol, target_tf)
        6. Emitir métricas Prometheus
        """
        from market_data.observability.metrics import (  # evita import circular en módulo
            RESAMPLE_ROWS_TOTAL,
            RESAMPLE_DURATION_MS,
        )

        pair_start = time.monotonic()
        result = ResampleResult(
            symbol      = symbol,
            timeframe   = target_tf,
            exchange    = self._exchange,
            market_type = self._market_type,
        )

        try:
            # Ventana de lectura: 2× período del TF target hacia atrás desde now_ms
            lookback      = _lookback_candles(target_tf)
            period_1m_ms  = timeframe_to_ms("1m")
            window_start_ms = now_ms - (lookback * period_1m_ms)
            window_start    = pd.Timestamp(window_start_ms, unit="ms", tz="UTC")

            self._log.debug(
                "Resampling pair %s/%s | idx=%s/%s lookback=%s",
                symbol, target_tf, idx, total, lookback,
            )

            # Carga sincrónica en hilo executor para no bloquear el event loop.
            # IcebergStorage.load_ohlcv es síncrono (scan Iceberg + to_pandas).
            import asyncio
            loop   = asyncio.get_event_loop()
            df_1m  = await loop.run_in_executor(
                None,
                lambda: self._storage.load_ohlcv(
                    symbol    = symbol,
                    timeframe = "1m",
                    start     = window_start,
                ),
            )

            # Guard: sin datos fuente suficientes → skip explícito
            if df_1m is None or len(df_1m) < _MIN_SOURCE_CANDLES:
                result.skipped = True
                self._log.debug(
                    "Resample skipped | %s/%s — sin datos 1m suficientes "
                    "(candles=%s min=%s)",
                    symbol, target_tf,
                    0 if df_1m is None else len(df_1m),
                    _MIN_SOURCE_CANDLES,
                )
                return result

            # Resampleado puro — CPU-bound, sin I/O
            resampled = _resample_df(
                df_1m     = df_1m,
                target_tf = target_tf,
                symbol    = symbol,
                exchange  = self._exchange,
            )

            if resampled.empty:
                result.skipped = True
                return result

            # Escritura en Iceberg — síncrono en executor
            await loop.run_in_executor(
                None,
                lambda: self._storage.save_ohlcv(
                    df        = resampled,
                    symbol    = symbol,
                    timeframe = target_tf,
                ),
            )

            result.rows = len(resampled)

            # Métricas Prometheus (SafeOps: nunca lanza)
            try:
                RESAMPLE_ROWS_TOTAL.labels(
                    exchange    = self._exchange,
                    symbol      = symbol,
                    timeframe   = target_tf,
                    market_type = self._market_type,
                ).inc(result.rows)
                RESAMPLE_DURATION_MS.labels(
                    exchange    = self._exchange,
                    timeframe   = target_tf,
                    market_type = self._market_type,
                ).observe(int((time.monotonic() - pair_start) * 1000))
            except Exception:
                pass

        except Exception as exc:
            result.error = str(exc)
            self._log.bind(
                symbol    = symbol,
                timeframe = target_tf,
                error     = str(exc),
                error_type = type(exc).__name__,
            ).error("Resample pair failed (non-fatal)")

        finally:
            result.duration_ms = int((time.monotonic() - pair_start) * 1000)

        return result
