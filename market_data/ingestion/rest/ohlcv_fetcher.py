"""
market_data/ingestion/rest/ohlcv_fetcher.py
==========

Fetcher profesional OHLCV con:

• Descarga incremental con overlap configurable
• Idempotencia (compatible con storage)
• Retry + backoff exponencial
• Circuit breaker correcto
• Protección contra loops y gaps
• Validación robusta de datos

SafeOps Ready 🚀
"""

from __future__ import annotations

import asyncio
import math
import random
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
from ocm_platform.observability import bind_pipeline

from market_data.storage.storage_protocol import OHLCVStorage
from market_data.processing.transformer import OHLCVTransformer
from market_data.adapters.exchange import (
    CCXTAdapter,
    ExchangeCircuitOpenError,
    RetryExhaustedError,
)
from market_data.observability.metrics import (
    FETCH_CHUNK_DURATION,
    FETCH_CHUNKS_TOTAL,
    CANDLE_DELAY_MS,
    FETCH_CHUNK_ERRORS_TOTAL,
)
import time


# ==========================================================
# Constants
# ==========================================================

DEFAULT_CHUNK_LIMIT   = 500
MAX_CHUNKS_PER_RUN    = 100_000
DEFAULT_OVERLAP_BARS  = 3

# ==========================================================
# Overlap model basado en principios de event-time processing
# ==========================================================
#
# _ALLOWED_LATENESS_MS:
#   Tiempo máximo que un exchange puede tardar en corregir un candle
#   después de su cierre (event-time lateness).
#   Valor conservador: 15 min. Pendiente calibración empírica.
#
# _MIN_OVERLAP_BARS:
#   Floor operativo: incluso si lateness < timeframe, los exchanges
#   pueden corregir el candle previo al cerrar el siguiente.
#
# Diseño:
#   overlap_bars = max(
#       _MIN_OVERLAP_BARS,
#       ceil(_ALLOWED_LATENESS_MS / timeframe_ms)
#   )
#
# Ref: Akidau et al. (2015) — The Dataflow Model
# Valores iniciales conservadores. Recalibrar con p99 real de cada exchange
# una vez acumulados datos de candle_delay_ms en producción.
# ==========================================================

# Lateness por exchange (ms). Captura diferencias reales de corrección de velas:
#   bybit       — correcciones rápidas, jitter bajo
#   kucoin      — correcciones más lentas, históricamente ~15 min
#   kucoinfutures — igual que kucoin spot por infraestructura compartida
# Fallback para exchanges no listados: 15 min (conservador).
_ALLOWED_LATENESS_MS_BY_EXCHANGE: dict[str, int] = {
    "bybit":          10 * 60_000,  # 10 min — pendiente calibración empírica
    "kucoin":         15 * 60_000,  # 15 min — pendiente calibración empírica
    "kucoinfutures":  15 * 60_000,  # 15 min — pendiente calibración empírica
}
_ALLOWED_LATENESS_MS_DEFAULT: int = 15 * 60_000  # fallback para exchanges no listados

# Floor diferenciado por magnitud de timeframe.
# Para TF largos, 3 barras sería excesivo (ej: 1w → 21 días de reingesta).
_MIN_OVERLAP_BARS_BY_TF: dict[str, int] = {
    "1m":  1, "3m":  1, "5m":  1, "15m": 1, "30m": 1,
    "1h":  2, "2h":  2, "4h":  2, "6h":  2, "8h":  2, "12h": 2,
    "1d":  1, "1w":  1,
}

_TF_MS: dict[str, int] = {
    "1m":  60_000,
    "3m":  3  * 60_000,
    "5m":  5  * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h":  3_600_000,
    "2h":  2  * 3_600_000,
    "4h":  4  * 3_600_000,
    "6h":  6  * 3_600_000,
    "8h":  8  * 3_600_000,
    "12h": 12 * 3_600_000,
    "1d":  86_400_000,
    "1w":  7  * 86_400_000,
}

# _OVERLAP_BY_TIMEFRAME usa el lateness default (fallback).
# Para lateness por exchange, usar overlap_for_timeframe(tf, exchange).
_OVERLAP_BY_TIMEFRAME: dict[str, int] = {
    tf: max(_MIN_OVERLAP_BARS_BY_TF[tf], math.ceil(_ALLOWED_LATENESS_MS_DEFAULT / tf_ms))
    for tf, tf_ms in _TF_MS.items()
}


def overlap_for_timeframe(timeframe: str, exchange: str | None = None) -> int:
    """Devuelve el overlap en barras para el timeframe y exchange dados.

    Orden de resolución de lateness_ms:
    1. Redis (calibración empírica via scripts/calibrate_lateness.py) — p99 real
    2. _ALLOWED_LATENESS_MS_BY_EXCHANGE (hardcoded conservador) — fallback
    3. _ALLOWED_LATENESS_MS_DEFAULT (15 min) — fallback final

    La calibración Redis se activa cuando hay ≥50 observaciones incrementales
    acumuladas en ocm_candle_delay_ms{mode="incremental"}.
    """
    if exchange is not None:
        # Prioridad 1: calibración empírica desde Redis
        lateness_ms = _get_calibrated_lateness_ms(exchange, timeframe)
        # Prioridad 2: hardcoded conservador
        if lateness_ms is None:
            lateness_ms = _ALLOWED_LATENESS_MS_BY_EXCHANGE.get(
                exchange, _ALLOWED_LATENESS_MS_DEFAULT
            )
        tf_ms = _TF_MS.get(timeframe)
        if tf_ms is not None:
            floor = _MIN_OVERLAP_BARS_BY_TF.get(timeframe, 1)
            return max(floor, math.ceil(lateness_ms / tf_ms))
    return _OVERLAP_BY_TIMEFRAME.get(timeframe, DEFAULT_OVERLAP_BARS)


# Cache del store de calibración — se inicializa una vez por proceso.
# Evita reconstruir la conexión Redis en cada llamada a overlap_for_timeframe().
# None significa "no inicializado aún" o "Redis no disponible".
_CALIBRATION_STORE = None
_CALIBRATION_STORE_INITIALIZED = False


def _get_calibrated_lateness_ms(exchange: str, timeframe: str) -> int | None:
    """
    Lee lateness_ms calibrado desde Redis. None si no disponible.

    El store se inicializa una vez por proceso (lazy singleton).
    SafeOps: nunca lanza — si Redis falla, overlap_for_timeframe()
    usa el hardcoded como fallback sin interrumpir el pipeline.
    """
    global _CALIBRATION_STORE, _CALIBRATION_STORE_INITIALIZED
    try:
        if not _CALIBRATION_STORE_INITIALIZED:
            from ocm_platform.infra.state.factories import build_lateness_calibration_store
            _CALIBRATION_STORE = build_lateness_calibration_store()
            _CALIBRATION_STORE_INITIALIZED = True
        if _CALIBRATION_STORE is None:
            return None
        return _CALIBRATION_STORE.get_lateness_ms(exchange, timeframe)
    except Exception:
        return None

OHLCV_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")

# ==========================================================
# Exceptions
from market_data.processing.exceptions import (  # noqa: E402
    MissingStartDateError, ChunkFetchError,
    InvalidMarketTypeError,
)
# ==========================================================




# ==========================================================
# Result
# ==========================================================

@dataclass(slots=True)
class DownloadResult:
    symbol:     str
    timeframe:  str
    df:         pd.DataFrame
    chunks:     int = 0
    total_rows: int = 0

    @property
    def has_data(self) -> bool:
        return not self.df.empty


# ==========================================================
# CursorStore import (aqui para evitar circular import)
# ==========================================================

from market_data.ports.state import CursorStorePort as CursorStore


# ==========================================================
# Fetcher
# ==========================================================

class HistoricalFetcherAsync:

    async def ensure_exchange(self) -> None:
        if not await self._exchange.is_healthy():
            await self._exchange.reconnect()

    def __init__(
        self,
        exchange_client:    CCXTAdapter,
        storage:            Optional[OHLCVStorage]      = None,
        transformer:        Optional[OHLCVTransformer]  = None,
        overlap_bars:       int                         = DEFAULT_OVERLAP_BARS,
        cursor_store:       Optional[CursorStore]       = None,
        backfill_mode:        bool                      = False,
        market_type:          Optional[str]             = None,
        config_start_date:    Optional[str]             = None,
        auto_lookback_days:   int                       = 1825,
    ) -> None:
        self._exchange          = exchange_client
        if storage is None:
            raise TypeError(
                "OHLCVFetcher requiere 'storage' explícito — "
                "inyectar desde ohlcv_pipeline via storage_factory()"
            )
        self._storage = storage
        self._transformer       = transformer or OHLCVTransformer()
        self._overlap           = overlap_bars
        self._cursor: CursorStore = cursor_store or InMemoryCursorStore()
        self._backfill_mode     = backfill_mode
        self._market_type       = market_type
        self._config_start_date  = config_start_date
        self._auto_lookback_days = auto_lookback_days
        self._log = bind_pipeline(
            "fetcher",
            exchange=getattr(exchange_client, "_exchange_id", "unknown"),
        )

        # Circuit breaker eliminado — vive en CCXTAdapter._get_breaker(),
        # compartido por exchange_id. No se duplica aquí.

    # ======================================================
    # Public API
    # ======================================================

    async def download_data(
        self,
        symbol:     str,
        timeframe:  str,
        start_date: Optional[str] = None,
        limit:      int           = DEFAULT_CHUNK_LIMIT,
    ) -> pd.DataFrame:

        self._validate_inputs(symbol, timeframe, limit)
        self._validate_market(symbol, self._market_type)

        result = await self._download_chunked(symbol, timeframe, start_date, limit)

        if not result.has_data:
            self._log.bind(symbol=symbol, timeframe=timeframe).info("No new data")
            return pd.DataFrame(columns=list(OHLCV_COLUMNS))

        self._log.bind(symbol=symbol, timeframe=timeframe, chunks=result.chunks, rows=result.total_rows).info("Download complete")
        return result.df

    async def fetch_chunk(
        self,
        symbol:    str,
        timeframe: str,
        since:     Optional[int],
        limit:     int = DEFAULT_CHUNK_LIMIT,
        end_ms:    Optional[int] = None,
    ) -> List[list]:
        """API pública de fetch de un chunk. Usada por BackfillStrategy."""
        return await self._fetch_chunk_with_retry(symbol, timeframe, since, limit, end_ms=end_ms)

    async def close(self) -> None:
        await self._exchange.close()

    # ======================================================
    # Core
    # ======================================================

    async def _download_chunked(
        self,
        symbol:     str,
        timeframe:  str,
        start_date: Optional[str],
        limit:      int,
    ) -> DownloadResult:

        since_ts = await self._resolve_start_timestamp(symbol, timeframe, start_date)
        if since_ts is None:
            # _resolve_start_timestamp no encontró cursor, storage ni config_start_date.
            # No es un error de red — el par no puede iniciar sin un punto de arranque.
            # MissingStartDateError debería haberse lanzado antes; esta guardia es
            # defensa en profundidad para evitar TypeError en el loop (int <= None).
            self._log.bind(
                symbol=symbol, timeframe=timeframe,
            ).error("since_ts=None — sin punto de inicio resuelto, abortando par")
            raise MissingStartDateError(
                f"No se pudo resolver since_ts para {symbol}/{timeframe}"
            )
        tf_ms    = timeframe_to_ms(timeframe)
        _mode    = "backfill" if self._backfill_mode else "incremental"
        # overlap por par: resuelve lateness calibrado por exchange y timeframe.
        # Sobrescribe self._overlap (valor global del constructor) con el valor
        # específico para este (exchange, timeframe). SafeOps: si falla, usa
        # self._overlap como fallback (DEFAULT_OVERLAP_BARS).
        _exchange_name = getattr(self._exchange, "_exchange_id", None)
        try:
            _pair_overlap = overlap_for_timeframe(timeframe, exchange=_exchange_name)
        except Exception:
            _pair_overlap = self._overlap
        _effective_overlap = _pair_overlap

        collected:    List[pd.DataFrame] = []
        for chunk_idx in range(MAX_CHUNKS_PER_RUN):

            # Startup jitter: solo en el primer chunk de cada par.
            # Segunda línea de defensa contra thundering herd — actúa
            # dentro del fetcher independientemente del caller.
            # Rango: [0, 200ms] — imperceptible individualmente,
            # pero distribuye N workers uniformemente en 200ms.
            # SafeOps: random.uniform es determinista si se siembra —
            # aquí NO se siembra deliberadamente para maximizar dispersión.
            if chunk_idx == 0:
                await asyncio.sleep(random.uniform(0.0, 0.2))

            exchange_name = getattr(self._exchange, "_exchange_id", "unknown")
            _t0 = time.perf_counter()
            _status = "success"
            try:
                raw = await self._fetch_chunk_with_retry(symbol, timeframe, since_ts, limit)
            except ExchangeCircuitOpenError:
                _status = "circuit_open"
                self._log.bind(symbol=symbol, timeframe=timeframe, chunk=chunk_idx).warning("Circuit open — aborting chunked download")
                FETCH_CHUNKS_TOTAL.labels(
                    exchange=exchange_name, timeframe=timeframe,
                    status=_status, mode=_mode,
                ).inc()
                if not collected:
                    # Sin datos — propagar para que el pipeline aborte el exchange.
                    raise
                # Datos parciales — devolver lo que hay.
                break
            except Exception as _chunk_exc:
                _status = "error"
                FETCH_CHUNK_ERRORS_TOTAL.labels(
                    exchange=exchange_name,
                    symbol=symbol,
                    timeframe=timeframe,
                    error_type=type(_chunk_exc).__name__,
                ).inc()
                raise
            finally:
                FETCH_CHUNK_DURATION.labels(
                    exchange=exchange_name, symbol=symbol, timeframe=timeframe,
                ).observe(time.perf_counter() - _t0)
                # Contar solo success aquí — circuit_open, empty y
                # stale/regression se cuentan en sus bloques explícitos.
                # Evita doble conteo de empty (bug previo).
                if _status == "success":
                    FETCH_CHUNKS_TOTAL.labels(
                        exchange=exchange_name, timeframe=timeframe,
                        status=_status, mode=_mode,
                    ).inc()

            if not raw:
                _status = "empty"
                FETCH_CHUNKS_TOTAL.labels(
                    exchange=exchange_name, timeframe=timeframe,
                    status=_status, mode=_mode,
                ).inc()
                break

            df = _raw_to_dataframe(raw)
            df = self._transformer.transform(
                df,
                symbol    = symbol,
                timeframe = timeframe,
                exchange  = getattr(self._exchange, "_exchange_id", "unknown"),
            )
            df = _sanitize_dataframe(df)

            if df.empty:
                break

            collected.append(df)

            last_ts = int(df["timestamp"].max().timestamp() * 1000)

            # candle_delay_ms: tiempo entre cierre esperado del candle y ahora.
            # Fuente empírica para calibrar lateness por exchange.
            # Solo se registra si el candle es del pasado (delay > 0).
            _now_ms         = int(time.time() * 1000)
            _candle_close   = last_ts + tf_ms  # cierre esperado
            _delay_ms       = _now_ms - _candle_close
            if _delay_ms > 0:
                CANDLE_DELAY_MS.labels(
                    exchange=exchange_name, timeframe=timeframe, mode=_mode,
                ).observe(_delay_ms)

            if last_ts <= since_ts:
                _stale_severity = "regression" if last_ts < since_ts else "stale"
                self._log.bind(symbol=symbol, timeframe=timeframe, severity=_stale_severity, last_ts=last_ts, since_ts=since_ts).warning("Stale window — aborting")
                FETCH_CHUNKS_TOTAL.labels(
                    exchange=exchange_name, timeframe=timeframe,
                    status=_stale_severity, mode=_mode,
                ).inc()
                break

            since_ts     = last_ts - (_effective_overlap * tf_ms)

            if len(raw) < limit:
                break

        if not collected:
            return DownloadResult(symbol, timeframe, pd.DataFrame())

        combined = (
            pd.concat(collected, ignore_index=True)
            .sort_values("timestamp")
            .drop_duplicates(subset="timestamp", keep="last")
            .reset_index(drop=True)
        )

        return DownloadResult(
            symbol     = symbol,
            timeframe  = timeframe,
            df         = combined,
            chunks     = len(collected),
            total_rows = len(combined),
        )

    # ======================================================
    # Timestamp logic
    # ======================================================

    async def _resolve_start_timestamp(
        self,
        symbol:     str,
        timeframe:  str,
        start_date: Optional[str],
    ) -> int:
        """
        Jerarquía de resolución de timestamp de inicio.

        backfill_mode=True:
            1. config_start_date como floor (sin discovery dinámico)

        backfill_mode=False (incremental):
            1. Cursor Redis/async  → reanuda desde último punto guardado
            2. Parquet last_ts     → fallback si cursor vacío
            3. start_date del arg  → primer inicio manual
            4. _config_start_date  → primer inicio desde config YAML
            5. MissingStartDateError → falla explícita
        """
        exchange_name = getattr(self._exchange, "_exchange_id", "unknown")
        tf_ms = timeframe_to_ms(timeframe)

        if self._backfill_mode:
            candidate = start_date or self._config_start_date
            if not candidate:
                raise MissingStartDateError(
                    f"backfill_mode=True requiere config_start_date | {symbol}/{timeframe}"
                )
            self._log.bind(symbol=symbol, timeframe=timeframe, desde=candidate).info("Backfill mode")
            return self._exchange.parse8601(candidate)

        # A. Cursor async — await obligatorio
        cursor_ts = await self._cursor.get(exchange_name, symbol, timeframe)
        if cursor_ts is not None:
            self._log.bind(symbol=symbol, timeframe=timeframe, ts_ms=cursor_ts).debug("Cursor hit")
            return cursor_ts - (self._overlap * tf_ms)

        # B. Fallback Iceberg (get_last_timestamp — L1/L2/L3)
        last_ts = self._storage.get_last_timestamp(symbol, timeframe)
        if last_ts is not None:
            if not isinstance(last_ts, pd.Timestamp):
                # Guardia defensiva: _to_utc_timestamp debe devolver siempre
                # pd.Timestamp. Si llega otro tipo, el caché L1 está corrupto.
                self._log.bind(
                    symbol=symbol, timeframe=timeframe,
                    last_ts_type=type(last_ts).__name__, last_ts_repr=repr(last_ts),
                ).error("get_last_timestamp devolvió tipo inesperado — abortando par")
                raise MissingStartDateError(
                    f"get_last_timestamp devolvió {type(last_ts).__name__} "
                    f"en lugar de pd.Timestamp para {symbol}/{timeframe}"
                )
            since_ms  = int(last_ts.timestamp() * 1000) - (self._overlap * tf_ms)
            delta_ms  = int(last_ts.timestamp() * 1000) - since_ms
            self._log.bind(
                symbol=symbol, timeframe=timeframe,
                last_ts=last_ts.isoformat(), since_ms=since_ms,
                delta_ms=delta_ms, overlap_candles=self._overlap,
            ).debug("Cursor miss — fallback Iceberg")
            return since_ms

        # C. Primer inicio desde arg o config (ignora el sentinel "auto")
        for candidate in [start_date, self._config_start_date]:
            if candidate and candidate != "auto":
                self._log.bind(symbol=symbol, timeframe=timeframe, desde=candidate).info("Primer inicio — fecha configurada")
                return self._exchange.parse8601(candidate)

        # D. Sin fecha configurada o start_date=="auto": lookback fijo.
        # Política deliberada: descargamos todo el historial disponible
        # usando un lookback conservador de 5 años. El sistema es idempotente
        # (cursor + Iceberg evitan reingestas) y el usuario puede acortar
        # con una fecha ISO explícita en config.pipeline.historical.start_date.
        _lookback_days = self._auto_lookback_days
        import time as _time
        _auto_since_ms = int((_time.time() - _lookback_days * 86400) * 1000)
        self._log.bind(
            symbol=symbol, timeframe=timeframe,
            lookback_days=_lookback_days,
            since_ms=_auto_since_ms,
        ).info("Primer inicio — modo auto (lookback fijo)")
        return _auto_since_ms

    # ======================================================
    # Fetch with single retry boundary
    # ======================================================

    async def _fetch_chunk_with_retry(
        self,
        symbol:    str,
        timeframe: str,
        since:     Optional[int],
        limit:     int,
        end_ms:    Optional[int] = None,
    ) -> List[list]:
        """
        Ejecuta fetch_ohlcv con una sola frontera de retry.

        Arquitectura de responsabilidades (SRP)
        ----------------------------------------
        CCXTAdapter es self-contained para toda la resiliencia:
        - Reconexión proactiva de sesión aiohttp en _get_client()
          antes de cada llamada (is_healthy() check)
        - Retry por tipo de error en ResilienceLayer (rate_limit,
          timeout, network) con backoff y circuit breaker
        - AuthenticationError re-raised como ExchangeAdapterError

        Este método es una llamada directa — sin loop propio, sin
        backoff manual, sin gestión de sesión. Una sola frontera
        de retry en el sistema (ResilienceLayer).

        Flujo de errores
        ----------------
        ExchangeCircuitOpenError  → Fail-Fast (breaker abierto)
        ExchangeAdapterError      → Fail-Fast (auth permanente)
        RetryExhaustedError       → Fail-Fast + log con causa raíz
        Cualquier otro            → Fail-Fast (bug de infra)
        """
        try:
            return await self._exchange.fetch_ohlcv(
                symbol, timeframe, since, limit, end_ms=end_ms,
            )

        except ExchangeCircuitOpenError:
            # Breaker abierto — outage confirmado en el exchange.
            # El adapter ya registró la métrica EXCHANGE_CIRCUIT_OPEN.
            # Fail-Fast sin log adicional — no añade información.
            raise

        except RetryExhaustedError as exc:
            # ResilienceLayer agotó todos sus reintentos.
            # Extraer original_exc para exponer causa raíz real.
            root_cause = exc.original_exc
            self._log.bind(
                symbol     = symbol,
                timeframe  = timeframe,
                error_type = exc.error_type,
                root_cause = type(root_cause).__name__,
                root_err   = str(root_cause),
            ).warning(
                "Adapter retries exhausted — failing chunk | "
                "error_type={} root_cause={} root_err={}",
                exc.error_type,
                type(root_cause).__name__,
                str(root_cause),
            )
            raise ChunkFetchError(
                f"{symbol}/{timeframe} retry exhausted ({exc.error_type}): "
                f"{type(root_cause).__name__}: {root_cause}"
            ) from exc

        except Exception as exc:
            # Cualquier error que escape del adapter es un bug de infra
            # o un error permanente (auth, config). Fail-Fast: no enmascarar.
            err_str = str(exc)
            self._log.bind(
                symbol     = symbol,
                timeframe  = timeframe,
                error_type = type(exc).__name__,
                err        = err_str,
            ).error(
                "Unexpected error from adapter — failing chunk | "
                "error_type={} err={}",
                type(exc).__name__,
                err_str,
            )
            raise ChunkFetchError(
                f"{symbol}/{timeframe} unexpected error: "
                f"{type(exc).__name__}: {exc}"
            ) from exc

    # ======================================================
    # Validation
    # ======================================================

    @staticmethod
    def _validate_inputs(symbol: str, timeframe: str, limit: int) -> None:
        if not symbol:
            raise ValueError("symbol required")
        if limit <= 0:
            raise ValueError("limit > 0 required")
        timeframe_to_ms(timeframe)

    def _validate_market(self, symbol: str, market_type: Optional[str] = None) -> None:
        market = self._exchange.get_market(symbol)
        if not market:
            return
        exchange_id = getattr(self._exchange, "_exchange_id", "unknown")
        if market_type == "swap" and not market.get("swap", False):
            raise InvalidMarketTypeError(
                f"Symbol '{symbol}' is not swap/futures in {exchange_id} "
                f"(type={market.get('type', '?')}) — fix config"
            )
        if market_type == "spot" and not market.get("spot", False):
            raise InvalidMarketTypeError(
                f"Symbol '{symbol}' is not spot in {exchange_id} "
                f"(type={market.get('type', '?')}) — fix config"
            )


# ==========================================================
# Helpers
# ==========================================================

from market_data.processing.utils.timeframe import timeframe_to_ms  # noqa: E402


def _raw_to_dataframe(raw: List[list]) -> pd.DataFrame:
    df = pd.DataFrame(raw, columns=list(OHLCV_COLUMNS))
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


def _sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["timestamp"])
    return df.sort_values("timestamp")


from market_data.processing.utils.timeframe import timeframe_to_ms  # noqa: E402,F811
