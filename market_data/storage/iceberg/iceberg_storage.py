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
• pd.Timestamp(max_ts, tz=...) falla si el objeto ya tiene tzinfo —
  usar tz_localize solo si viene sin tz.
"""

from __future__ import annotations

import time
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger
from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThanOrEqual,
    LessThanOrEqual,
)

from market_data.storage.iceberg.catalog import get_catalog, ensure_silver_table
from market_data.ports.state import CursorStorePort as CursorStore


# Columnas OHLCV en el orden del schema Iceberg
_OHLCV_COLS = [
    "timestamp", "open", "high", "low", "close", "volume",
    "exchange", "market_type", "symbol", "timeframe",
]


def _to_utc_timestamp(dt: object) -> Optional[pd.Timestamp]:
    """
    Convierte el resultado de pc.max() a pd.Timestamp UTC.

    pyiceberg 0.8 almacena timestamps como datetime64[us, UTC].
    pc.max() sobre esa columna devuelve:
      - datetime con tzinfo  → columnas tz-aware (caso normal)
      - int en microsegundos → columnas tz-naive almacenadas como us epoch
      - None                 → tabla vacía

    pd.Timestamp(int) interpreta el int como nanosegundos — incorrecto.
    Hay que detectar int y usar unit="us" explícitamente.
    """
    if dt is None:
        return None
    if isinstance(dt, int):
        # pc.max() devolvió microsegundos epoch — convertir explícitamente.
        return pd.Timestamp(dt, unit="us", tz="UTC")
    ts = pd.Timestamp(dt)
    return ts if ts.tzinfo is not None else ts.tz_localize("UTC")


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
        exchange:     Optional[str]           = None,
        market_type:  Optional[str]           = None,
        dry_run:      bool                    = False,
        cursor_store: Optional[CursorStore]   = None,
    ) -> None:
        self._exchange    = exchange
        self._market_type = market_type
        self._dry_run     = dry_run
        # cursor_store opcional — actúa como cache L2 distribuido (cross-process)
        # en get_last_timestamp. Sin él, solo cache L1 in-process.
        # Inyectado desde _build_storage() en ohlcv_pipeline.
        self._cursor: Optional[CursorStore] = cursor_store
        # Cache L1 de metadatos por symbol/timeframe — evita scans repetidos
        # en el mismo proceso. Invalidado en save_ohlcv.
        # Para cache cross-process ver self._cursor (L2).
        self._last_ts_cache: dict[tuple[str, str], object] = {}
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
        exchange    = self._exchange    or "unknown"
        market_type = self._market_type or "unknown"
        return And(
            And(
                EqualTo("exchange",    exchange),
                EqualTo("symbol",      symbol),
            ),
            And(
                EqualTo("timeframe",   timeframe),
                EqualTo("market_type", market_type),
            ),
        )

    @staticmethod
    def _normalize_df(
        df:          pd.DataFrame,
        symbol:      str,
        timeframe:   str,
        exchange:    str,
        market_type: str,
    ) -> pd.DataFrame:
        """
        Prepara el DataFrame para escritura en Iceberg:
        - Convierte timestamp a us (pyiceberg 0.8 no soporta ns)
        - Inyecta columnas de partición
        - Deduplica y ordena
        """
        df = df.copy()
        df["timestamp"] = (
            pd.to_datetime(df["timestamp"], utc=True)
            .astype("datetime64[us, UTC]")
        )
        df["exchange"]    = exchange
        df["market_type"] = market_type
        df["symbol"]      = symbol
        df["timeframe"]   = timeframe

        return (
            df[_OHLCV_COLS]
            .drop_duplicates(subset=["timestamp", "exchange", "symbol", "timeframe"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

    # =========================================================================
    # Public API — OHLCVStorage Protocol
    # =========================================================================

    def save_ohlcv(
        self,
        df:              pd.DataFrame,
        symbol:          str,
        timeframe:       str,
        mode:            str           = "append",
        run_id:          Optional[str] = None,
        skip_versioning: bool          = False,   # no-op — Iceberg versiona solo
    ) -> None:
        """
        Guarda OHLCV en la tabla Iceberg silver.ohlcv.

        Append atómico con snapshot consistency garantizada por Iceberg.
        mode='overwrite' no está soportado en pyiceberg 0.8 — se trata
        como append con dedup y se emite un warning.
        """
        if self._dry_run:
            logger.info(
                "[DRY RUN] IcebergStorage.save_ohlcv skipped | {}/{} "
                "exchange={} rows={}",
                symbol, timeframe, self._exchange or "shared", len(df),
            )
            return

        if df is None or df.empty:
            return

        if mode == "overwrite":
            logger.warning(
                "IcebergStorage: mode=overwrite no soportado en pyiceberg 0.8 "
                "— usando append con dedup | {}/{}", symbol, timeframe,
            )

        _t0      = time.monotonic()
        prepared = self._normalize_df(
            df,
            symbol      = symbol,
            timeframe   = timeframe,
            exchange    = self._exchange    or "unknown",
            market_type = self._market_type or "unknown",
        )

        arrow_schema = self._table.schema().as_arrow()
        self._table.append(
            pa.Table.from_pandas(prepared, schema=arrow_schema, preserve_index=False)
        )

        self._last_ts_cache.pop((symbol, timeframe), None)
        logger.debug(
            "IcebergStorage saved | {}/{} exchange={} rows={} duration={}ms",
            symbol, timeframe, self._exchange or "shared",
            len(prepared), int((time.monotonic() - _t0) * 1000),
        )

    def get_last_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        """Obtiene el último timestamp disponible para symbol/timeframe.

        Scan Iceberg con filtros nativos (partition pruning activo).
        Solo lee la columna timestamp — mínimo I/O.

        Resultado cacheado en memoria por instancia — el cache se invalida
        automáticamente después de cada save_ohlcv exitoso. Safe para uso
        concurrente dentro del mismo proceso (GIL protege el dict).
        """
        cache_key = (symbol, timeframe)

        # L1 — cache in-process (mismo worker). Invalidado en save_ohlcv.
        if cache_key in self._last_ts_cache:
            return self._last_ts_cache[cache_key]

        # L2 — cursor Redis (cross-process, si inyectado).
        # get_raw() es síncrono. Clave: cursor usa prefijo 'cursor:env:exchange:symbol:tf'
        # pero get_raw acepta clave raw — usamos la clave interna del CursorStore.
        # No se propaga al L1: el cursor puede estar adelantado respecto a Iceberg
        # (escritura pendiente en otro worker). L1 solo se llena desde L3.
        if self._cursor is not None:
            exchange_key = (self._exchange or "unknown").lower()
            market_key   = (self._market_type or "unknown").lower()
            # Formato de clave interno de RedisCursorStore (base64-encoded segments).
            # No podemos reconstruir la clave codificada aquí sin acoplar implementación.
            # Usamos get_raw con la clave legible como best-effort; si falla → L3.
            # NOTA: el cursor puede estar adelantado respecto a Iceberg si otro worker
            # escribió y actualizó Redis pero el snapshot Iceberg aún no es visible.
            # Por eso NO propagamos L2 al caché L1 — L1 solo se llena desde L3.
            try:
                raw = self._cursor.get_raw(
                    f"{exchange_key}:{symbol}:{market_key}:{timeframe}"
                )
                if raw is not None:
                    ts_l2 = pd.Timestamp(int(raw), unit="ms", tz="UTC")
                    logger.debug(
                        "get_last_timestamp L2 hit | {}/{} ts={}",
                        symbol, timeframe, ts_l2,
                    )
                    # Sanity cross-layer: si L3 está disponible en caché L1,
                    # loggear mismatch para detectar cursor adelantado/regresión.
                    ts_l1 = self._last_ts_cache.get((symbol, timeframe))
                    if ts_l1 is not None and isinstance(ts_l1, pd.Timestamp):
                        delta_ms = int((ts_l2 - ts_l1).total_seconds() * 1000)
                        if delta_ms < 0:
                            logger.warning(
                                "get_last_timestamp L2 < L1 (cursor regresión) | "
                                "{}/{} l2={} l1={} delta_ms={}",
                                symbol, timeframe, ts_l2, ts_l1, delta_ms,
                            )
                        elif delta_ms > 0:
                            logger.debug(
                                "get_last_timestamp L2 ahead of L1 | "
                                "{}/{} delta_ms={}",
                                symbol, timeframe, delta_ms,
                            )
                    return ts_l2
            except Exception as _l2_exc:
                logger.debug(
                    "get_last_timestamp L2 miss | {}/{} err={}",
                    symbol, timeframe, _l2_exc,
                )

        # L3 — scan Iceberg (fuente de verdad persistente).
        if self._table is None:
            return None
        try:
            result = (
                self._table
                .scan(
                    row_filter      = self._base_filter(symbol, timeframe),
                    selected_fields = ("timestamp",),
                )
                .to_arrow()
            )

            ts = (
                None if result.num_rows == 0
                else _to_utc_timestamp(pc.max(result.column("timestamp")).as_py())
            )

            # Solo cachear en L1 resultado del scan Iceberg.
            # El cursor (L2) lo actualiza IncrementalStrategy tras cada write.
            self._last_ts_cache[cache_key] = ts
            return ts

        except Exception as exc:
            logger.warning(
                "IcebergStorage.get_last_timestamp failed | {}/{} error={}",
                symbol, timeframe, exc,
            )
            return None


    def get_oldest_timestamp(
        self,
        symbol:    str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        """
        Obtiene el timestamp más antiguo disponible para symbol/timeframe.

        Scan Iceberg con pc.min() — simétrico a get_last_timestamp.
        """
        if self._table is None:
            return None
        try:
            result = (
                self._table
                .scan(
                    row_filter      = self._base_filter(symbol, timeframe),
                    selected_fields = ("timestamp",),
                )
                .to_arrow()
            )
            if result.num_rows == 0:
                return None
            return _to_utc_timestamp(pc.min(result.column("timestamp")).as_py())
        except Exception as exc:
            logger.warning(
                "IcebergStorage.get_oldest_timestamp failed | {}/{} error={}",
                symbol, timeframe, exc,
            )
            return None
    def get_current_snapshot(self) -> Optional[dict]:
        # Expone el snapshot actual sin acceso directo a _table.
        # GoldStorage usa este metodo para anclar lineage antes del build.
        # SafeOps: nunca lanza — retorna None si tabla nueva o Iceberg degradado.
        try:
            snap = self._table.current_snapshot()
            if snap is None:
                return None
            return {
                "snapshot_id":  snap.snapshot_id,
                "timestamp_ms": snap.timestamp_ms,
            }
        except Exception as _snap_exc:
            logger.debug(
                "get_snapshot_info failed (tabla nueva o Iceberg no init)",
                error=str(_snap_exc),
            )
            return None

    def load_ohlcv(
        self,
        symbol:    str,
        timeframe: str,
        start:     Optional[pd.Timestamp] = None,
        end:       Optional[pd.Timestamp] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Lee datos OHLCV desde Iceberg con pushdown de filtros temporales.

        Combina filtro de identidad (exchange/symbol/timeframe/market_type)
        con rango temporal opcional. Partition pruning activo en ambos ejes.
        """
        if self._table is None:
            return None
        try:
            row_filter = self._base_filter(symbol, timeframe)

            if start is not None:
                # Microsegundos epoch — tipo interno de Iceberg TimestampType.
                # isoformat() con tz-aware produce "...+00:00" que pyiceberg
                # puede rechazar dependiendo de la versión. int epoch es seguro.
                row_filter = And(
                    row_filter,
                    GreaterThanOrEqual("timestamp", int(start.timestamp() * 1_000_000)),
                )
            if end is not None:
                row_filter = And(
                    row_filter,
                    LessThanOrEqual("timestamp", int(end.timestamp() * 1_000_000)),
                )

            df = (
                self._table
                .scan(row_filter=row_filter)
                .to_arrow()
                .to_pandas()
            )

            if df.empty:
                return None

            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return (
                df.sort_values("timestamp")
                .drop_duplicates(subset=["timestamp"])
                .reset_index(drop=True)
            )

        except Exception as exc:
            logger.warning(
                "IcebergStorage.load_ohlcv failed | {}/{} error={}",
                symbol, timeframe, exc,
            )
            return None

    # =========================================================================
    # Protocol stubs — no-op en Iceberg
    # =========================================================================

    def commit_version(
        self,
        symbol:    str,
        timeframe: str,
        run_id:    Optional[str] = None,
    ) -> None:
        """No-op: Iceberg versiona automáticamente por snapshot."""
        pass

    def get_version(
        self,
        symbol:    str,
        timeframe: str,
        version:   str = "latest",
    ) -> Optional[dict]:
        """Retorna metadata del snapshot actual como proxy de versión."""
        try:
            snap = self._table.current_snapshot()
            if snap is None:
                return None
            return {
                "version_id":  str(snap.snapshot_id),
                "written_at":  str(snap.timestamp_ms),
                "symbol":      symbol,
                "timeframe":   timeframe,
                "exchange":    self._exchange,
                "market_type": self._market_type,
            }
        except Exception as _ver_exc:
            logger.debug(
                "get_version_info failed (tabla nueva o Iceberg no init)",
                error=str(_ver_exc),
            )
            return None

    def find_partition_files(
        self,
        symbol:    str,
        timeframe: str,
        since:     Optional[pd.Timestamp] = None,
        until:     Optional[pd.Timestamp] = None,
    ) -> list:
        """
        No-op: Iceberg no expone archivos físicos de partición.
        RepairStrategy usará scan() directamente cuando soporte Iceberg.
        Retorna [] para que RepairStrategy salte silenciosamente.
        """
        return []
