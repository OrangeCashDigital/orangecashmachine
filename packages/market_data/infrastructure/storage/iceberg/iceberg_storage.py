"""
market_data/storage/iceberg/iceberg_storage.py
===============================================

Capa Silver — único backend de storage OHLCV.

Tabla: silver.ohlcv (Apache Iceberg sobre SQLite catalog)
Particionado por: exchange / market_type / symbol / timeframe / ts_month

Interfaz pública (OHLCVStorage Protocol)
-----------------------------------------
  save_ohlcv()          — append transaccional con snapshot consistency
  get_last_timestamp()  — scan con partition pruning, sin abrir archivos
  get_oldest_timestamp()— simétrico, para backfill boundary detection
  load_ohlcv()          — scan con pushdown de filtros temporales
  commit_version()      — no-op (Iceberg versiona por snapshot)

Uso
---
  storage = IcebergStorage(exchange="bybit", market_type="spot")
  fetcher = HistoricalFetcherAsync(exchange_client=..., storage=storage)

Notas de implementación
-----------------------
• row_filter usa pyiceberg.expressions (EqualTo, And, etc.) — NO pc.field().
  pc.field() es PyArrow compute — sistemas de expresiones incompatibles.
• pc (pyarrow.compute) se usa SOLO post-scan: pc.max(), pc.min().
• Timestamps normalizados a microsegundos (us) — pyiceberg 0.8 no soporta ns.
"""

from __future__ import annotations

import datetime as _dt
import time
from typing import Optional

import polars as pl
import pyarrow.compute as pc
from loguru import logger
from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThanOrEqual,
    LessThanOrEqual,
)

from market_data.infrastructure.storage.iceberg.catalog import (
    ensure_silver_table,
    get_catalog,
)
from market_data.infrastructure.storage.iceberg.timestamp_cache import (
    TimestampCacheService,
)
from market_data.ports.outbound.state import CursorStorePort as CursorStore

# =============================================================================
# Timeouts — SSOT de límites de I/O (segundos)
# =============================================================================
# Iceberg scans sobre SQLite catalog pueden bloquearse bajo contención.
# Timeout conservador: suficientemente alto para scans legítimos,
# suficientemente bajo para detectar deadlocks en CI/staging.
# Ajustar via variable de entorno en el futuro si se necesita tuning.
_ICEBERG_SCAN_TIMEOUT_S: float = 30.0  # get_last_timestamp, get_oldest_timestamp
_ICEBERG_LOAD_TIMEOUT_S: float = 120.0  # load_ohlcv — puede retornar mucho volumen


# Columnas OHLCV en el orden del schema Iceberg
_OHLCV_COLS = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "exchange",
    "market_type",
    "symbol",
    "timeframe",
]


def _to_utc_timestamp(dt: object) -> Optional[_dt.datetime]:
    """
    Convierte el resultado de pc.max()/pc.min() a datetime UTC nativo.

    pyiceberg 0.8 almacena timestamps como datetime64[us, UTC].
    pc.max()/pc.min() sobre esa columna devuelve:
      - datetime con tzinfo  → columnas tz-aware (caso normal)
      - int en microsegundos → columnas tz-naive almacenadas como us epoch
      - None                 → tabla vacía
    """
    if dt is None:
        return None
    if isinstance(dt, int):
        # pc.max()/pc.min() devolvió microsegundos epoch — convertir explícitamente.
        return _dt.datetime.fromtimestamp(dt / 1_000_000, tz=_dt.timezone.utc)
    if isinstance(dt, _dt.datetime):
        return dt.replace(tzinfo=_dt.timezone.utc) if dt.tzinfo is None else dt.astimezone(_dt.timezone.utc)
    return None


# =============================================================================
# IcebergStorage
# =============================================================================


class IcebergStorage:
    """
    Capa Silver sobre Apache Iceberg.

    Implementación única del contrato OHLCVStorage.
    """

    def __init__(
        self,
        exchange: Optional[str] = None,
        market_type: Optional[str] = None,
        dry_run: bool = False,
        cursor_store: Optional[CursorStore] = None,
    ) -> None:
        self._exchange = exchange
        self._market_type = market_type
        self._dry_run = dry_run
        # TimestampCacheService gestiona L1 (in-process) y L2 (Redis).
        # SRP: IcebergStorage delega todo cache management aquí.
        # Inyectado desde container para testabilidad (DIP).
        # TimestampCacheService es SSOT del cache L1/L2.
        # No mantener un _last_ts_cache local — delegar todo a _ts_cache (SRP).
        self._ts_cache = TimestampCacheService(cursor_store=cursor_store)
        self._cursor = cursor_store  # CursorStorePort | None — acceso directo a Redis L2
        # SafeOps: en dry_run skip bootstrap y carga de tabla — sin I/O al catálogo.
        # En tests/CI el catálogo SQLite puede no existir. Todos los métodos de
        # escritura son no-op en dry_run. Los de lectura retornan None si _table=None.
        self._table = None
        if not dry_run:
            # Bootstrap idempotente: crea silver.ohlcv si no existe.
            # Patrón "ensure before load" — self-healing sin script externo.
            # No-op si la tabla ya existe. Ref: catalog.ensure_silver_table()
            ensure_silver_table()
            self._table = get_catalog().load_table("silver.ohlcv")

    # =========================================================================
    # Helpers internos
    # =========================================================================

    def _base_filter(self, symbol: str, timeframe: str):
        """
        Filtro Iceberg nativo para las cuatro columnas de identidad.

        IMPORTANTE: usa pyiceberg.expressions.EqualTo/And, NO pc.field().
        pc.field() es PyArrow compute y lanza "Cannot visit unsupported
        expression" cuando se pasa a scan(). Son sistemas distintos.
        """
        exchange = self._exchange or "unknown"
        market_type = self._market_type or "unknown"
        return And(
            And(
                EqualTo(term="exchange", literal=exchange),  # type: ignore[call-arg,arg-type]  # pyiceberg stubs: term/literal no declarados como kwargs
                EqualTo(term="symbol", literal=symbol),  # type: ignore[call-arg,arg-type]  # pyiceberg stubs: term/literal no declarados como kwargs
            ),
            And(
                EqualTo(term="timeframe", literal=timeframe),  # type: ignore[call-arg,arg-type]  # pyiceberg stubs
                EqualTo(term="market_type", literal=market_type),  # type: ignore[call-arg,arg-type]  # pyiceberg stubs
            ),
        )

    @staticmethod
    def _normalize_df(
        df: pl.DataFrame,
        symbol: str,
        timeframe: str,
        exchange: str,
        market_type: str,
    ) -> pl.DataFrame:
        """
        Prepara el DataFrame para escritura en Iceberg:
        - Normaliza timestamp a Datetime("us", "UTC") — pyiceberg 0.8 no soporta ns
        - Inyecta columnas de partición
        - Deduplica y ordena

        Manejo de dtype defensivo — mismo patrón que ohlcv_transformer.py:
        acepta timestamp como Int64 epoch-ms, Datetime tz-naive, o ya
        Datetime("us", "UTC") y normaliza siempre al último caso.

        Dedup semantics — last-write-wins (keep="last"): si la misma vela
        (timestamp, exchange, symbol, timeframe) aparece más de una vez en
        el batch, prevalece la última fila. Este es el contrato oficial de
        Silver — ver OHLCVStorage.save_ohlcv docstring — no un detalle de
        implementación. Motivo: una escritura posterior (WebSocket con más
        trades, backfill con corrección del exchange, gap healing) se
        asume más correcta que la anterior.
        """
        ts_dtype = df["timestamp"].dtype
        if ts_dtype == pl.Int64:
            df = df.with_columns(
                pl.col("timestamp").cast(pl.Int64, strict=False).cast(pl.Datetime("ms")).dt.replace_time_zone("UTC")
            )
        elif ts_dtype.time_zone is None:  # type: ignore[attr-defined]
            df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))

        if df["timestamp"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp").dt.cast_time_unit("us"))

        df = df.with_columns(
            pl.lit(exchange).alias("exchange"),
            pl.lit(market_type).alias("market_type"),
            pl.lit(symbol).alias("symbol"),
            pl.lit(timeframe).alias("timeframe"),
        )

        return (
            df.select(_OHLCV_COLS)
            .unique(subset=["timestamp", "exchange", "symbol", "timeframe"], keep="last")
            .sort("timestamp")
        )

    # =========================================================================
    # Public API — OHLCVStorage Protocol
    # =========================================================================

    def save_ohlcv(
        self,
        df: pl.DataFrame,
        symbol: str,
        timeframe: str,
        run_id: Optional[str] = None,
        skip_versioning: bool = False,  # no-op — Iceberg versiona por snapshot
    ) -> None:
        """
        Persiste OHLCV en silver.ohlcv via append atómico (Iceberg snapshot).

        Append-only — Iceberg no soporta overwrite en pyiceberg 0.8.
        Dedup por (timestamp, exchange, symbol, timeframe) en _normalize_df.
        Snapshot consistency garantizada por Iceberg en cada append.

        Fail-Fast: lanza si _table no está inicializado (bug de configuración).
        SafeOps  : retorna silenciosamente si df está vacío (no es un error).
        """
        if self._dry_run:
            logger.info(
                "[DRY RUN] IcebergStorage.save_ohlcv skipped | {}/{} exchange={} rows={}",
                symbol,
                timeframe,
                self._exchange or "shared",
                len(df),
            )
            return

        if df is None or df.is_empty():
            return

        if self._table is None:
            raise RuntimeError(
                "IcebergStorage.save_ohlcv: _table no inicializado. "
                "Llamar con dry_run=False o verificar bootstrap del catálogo."
            )

        _t0 = time.monotonic()
        prepared = self._normalize_df(
            df,
            symbol=symbol,
            timeframe=timeframe,
            exchange=self._exchange or "unknown",
            market_type=self._market_type or "unknown",
        )

        # Polars.to_arrow() marca columnas nullable=True por defecto — el
        # schema Iceberg exige required=True en todos los campos OHLCV base.
        # .cast() fuerza el schema exacto (tipos + nullability), igual que
        # antes hacía pa.Table.from_pandas(..., schema=...) — ahora usa pa.table.
        self._table.append(prepared.to_arrow().cast(self._table.schema().as_arrow()))

        # Invalidar cache L1/L2 tras escritura exitosa (SSOT: _ts_cache)
        self._ts_cache.invalidate(symbol, timeframe)

        logger.debug(
            "IcebergStorage saved | {}/{} exchange={} rows={} duration={}ms",
            symbol,
            timeframe,
            self._exchange or "shared",
            len(prepared),
            int((time.monotonic() - _t0) * 1000),
        )

    def get_last_timestamp(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[_dt.datetime]:
        """Obtiene el último timestamp disponible para symbol/timeframe.

        Scan Iceberg con filtros nativos (partition pruning activo).
        Solo lee la columna timestamp — mínimo I/O.

        Resultado cacheado en memoria por instancia — el cache se invalida
        automáticamente después de cada save_ohlcv exitoso. Safe para uso
        concurrente dentro del mismo proceso (GIL protege el dict).
        """
        # L1/L2 — delegar a TimestampCacheService (SSOT del cache).
        # _last_ts_cache eliminado — IcebergStorage no gestiona cache directamente (SRP).
        ts_cached = self._ts_cache.get(
            symbol=symbol,
            timeframe=timeframe,
            exchange=self._exchange or "unknown",
            market_type=self._market_type or "unknown",
        )
        if ts_cached is not None:
            return ts_cached

        # L3 — scan Iceberg (fuente de verdad persistente).
        # Solo se ejecuta si L1 y L2 son miss.
        table = self._table
        if table is None:
            return None
        try:
            result = table.scan(
                row_filter=self._base_filter(symbol, timeframe),
                selected_fields=("timestamp",),
            ).to_arrow()

            ts = None if result.num_rows == 0 else _to_utc_timestamp(pc.max(result.column("timestamp")).as_py())

            # Poblar L1 con el resultado del scan L3 (fuente de verdad).
            # L2 (Redis) lo actualiza IncrementalStrategy tras cada write exitoso.
            self._ts_cache.set(symbol, timeframe, ts)
            return ts

        except Exception:
            logger.opt(exception=True).warning(
                "IcebergStorage.get_last_timestamp failed | {}/{}",
                symbol,
                timeframe,
            )
            return None

    def get_oldest_timestamp(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[_dt.datetime]:
        """
        Obtiene el timestamp más antiguo disponible para symbol/timeframe.

        Scan Iceberg con pc.min() — simétrico a get_last_timestamp.
        """
        table = self._table
        if table is None:
            return None
        try:
            result = table.scan(
                row_filter=self._base_filter(symbol, timeframe),
                selected_fields=("timestamp",),
            ).to_arrow()
            if result.num_rows == 0:
                return None
            return _to_utc_timestamp(pc.min(result.column("timestamp")).as_py())
        except Exception:
            logger.opt(exception=True).warning(
                "IcebergStorage.get_oldest_timestamp failed | {}/{}",
                symbol,
                timeframe,
            )
            return None

    def get_current_snapshot(self) -> Optional[dict]:
        # Expone el snapshot actual sin acceso directo a _table.
        # GoldStorage usa este metodo para anclar lineage antes del build.
        # SafeOps: nunca lanza — retorna None si tabla nueva o Iceberg degradado.
        try:
            assert self._table is not None, "_table no inicializado en get_current_snapshot"
            snap = self._table.current_snapshot()
            if snap is None:
                return None
            return {
                "snapshot_id": snap.snapshot_id,
                "timestamp_ms": snap.timestamp_ms,
            }
        except Exception as _snap_exc:
            logger.debug(
                "get_snapshot_info failed (tabla nueva o Iceberg no init)",
            )
            return None

    def load_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: Optional[_dt.datetime] = None,
        end: Optional[_dt.datetime] = None,
    ) -> Optional[pl.DataFrame]:
        """
        Lee datos OHLCV desde Iceberg con pushdown de filtros temporales.

        Combina filtro de identidad (exchange/symbol/timeframe/market_type)
        con rango temporal opcional. Partition pruning activo en ambos ejes.
        """
        table = self._table
        if table is None:
            return None
        try:
            row_filter = self._base_filter(symbol, timeframe)

            if start is not None:
                # Microsegundos epoch — tipo interno de Iceberg TimestampType.
                # isoformat() con tz-aware produce "...+00:00" que pyiceberg
                # puede rechazar dependiendo de la versión. int epoch es seguro.
                row_filter = And(
                    row_filter,
                    GreaterThanOrEqual("timestamp", int(start.timestamp() * 1_000_000)),  # type: ignore[call-arg,arg-type,misc]
                )
            if end is not None:
                row_filter = And(
                    row_filter,
                    LessThanOrEqual("timestamp", int(end.timestamp() * 1_000_000)),  # type: ignore[call-arg,arg-type,misc]
                )

            import concurrent.futures as _cf

            with _cf.ThreadPoolExecutor(max_workers=1) as _pool:
                _future = _pool.submit(lambda: table.scan(row_filter=row_filter).to_arrow())
                try:
                    arrow_table = _future.result(timeout=_ICEBERG_LOAD_TIMEOUT_S)
                except _cf.TimeoutError:
                    logger.opt(exception=True).error(
                        "IcebergStorage.load_ohlcv TIMEOUT ({:.0f}s) | {}/{}",
                        _ICEBERG_LOAD_TIMEOUT_S,
                        symbol,
                        timeframe,
                    )
                    return None

            df = pl.DataFrame(arrow_table)

            if df.is_empty():
                return None

            return df.sort("timestamp").unique(subset=["timestamp"], keep="last")

        except Exception:
            logger.opt(exception=True).warning(
                "IcebergStorage.load_ohlcv failed | {}/{}",
                symbol,
                timeframe,
            )
            return None

    # =========================================================================
    # Protocol stubs — no-op en Iceberg
    # =========================================================================

    def commit_version(
        self,
        symbol: str,
        timeframe: str,
        run_id: Optional[str] = None,
    ) -> None:
        """No-op: Iceberg versiona automáticamente por snapshot."""
        pass

    def get_version(
        self,
        symbol: str,
        timeframe: str,
        version: str = "latest",
    ) -> Optional[dict]:
        """Retorna metadata del snapshot actual como proxy de versión."""
        try:
            assert self._table is not None, "_table no inicializado en get_current_snapshot"
            snap = self._table.current_snapshot()
            if snap is None:
                return None
            return {
                "version_id": str(snap.snapshot_id),
                "written_at": str(snap.timestamp_ms),
                "symbol": symbol,
                "timeframe": timeframe,
                "exchange": self._exchange,
                "market_type": self._market_type,
            }
        except Exception as _ver_exc:
            logger.debug(
                "get_version_info failed (tabla nueva o Iceberg no init)",
            )
            return None

    def find_partition_files(
        self,
        symbol: str,
        timeframe: str,
        since: Optional[_dt.datetime] = None,
        until: Optional[_dt.datetime] = None,
    ) -> list:
        """
        No-op: Iceberg no expone archivos físicos de partición.
        RepairStrategy usará scan() directamente cuando soporte Iceberg.
        Retorna [] para que RepairStrategy salte silenciosamente.
        """
        return []
