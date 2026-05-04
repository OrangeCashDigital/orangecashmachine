# -*- coding: utf-8 -*-
"""
market_data/storage/gold/gold_storage.py
=========================================

Capa Gold — overwrite por dataset sobre Apache Iceberg.

Tabla : gold.features
Particionado : exchange / market_type / symbol / timeframe / ts_month

Responsabilidad
---------------
Leer Silver (Iceberg), calcular features via GoldTransformer, y persistir
el dataset enriquecido en gold.features con overwrite atómico por dataset.

Versionado
----------
Iceberg gestiona el historial via snapshots. Cada build() genera un nuevo
snapshot que reemplaza los datos del dataset (exchange/symbol/market_type/
timeframe). El snapshot anterior queda accesible para reproducibilidad
mediante GoldLoader con time travel (snapshot_id o as_of).

Lineage
-------
silver_snapshot_id y silver_snapshot_ms se capturan ANTES del load —
anclan el manifest al punto exacto en el tiempo que Gold usó como fuente.

Principios
----------
DIP   — depende de GoldTransformer (dominio), nunca del shim FeatureEngineer.
SSOT  — GoldTransformer es la única fuente de feature engineering.
SRP   — GoldStorage orquesta; GoldTransformer transforma.
"""
from __future__ import annotations

from typing import List, Optional

import pandas as pd
import pyarrow as pa
from loguru import logger
from pyiceberg.expressions import And, EqualTo

from market_data.gold.transformer import GoldTransformer, VERSION as _TRANSFORMER_VERSION
from market_data.storage.iceberg.catalog import ensure_gold_table, get_catalog
from market_data.storage.iceberg.iceberg_storage import IcebergStorage

# Columnas en orden canónico para gold.features
_GOLD_COLS = [
    "timestamp", "open", "high", "low", "close", "volume",
    "exchange", "market_type", "symbol", "timeframe",
    "return_1", "log_return", "volatility_20", "high_low_spread", "vwap",
    "run_id", "engineer_version", "silver_snapshot_id", "silver_snapshot_ms",
]


# =============================================================================
# GoldStorage
# =============================================================================

class GoldStorage:
    """
    Construye y persiste datasets Gold (Silver + features) en Iceberg.

    build() hace overwrite completo del dataset para el cuarteto
    exchange/symbol/market_type/timeframe — datos previos son reemplazados
    pero el snapshot anterior queda accesible (time travel via GoldLoader).

    Uso
    ---
    gold = GoldStorage()
    df   = gold.build(
        exchange="bybit", symbol="BTC/USDT",
        market_type="spot", timeframe="1h",
    )
    """

    def __init__(
        self,
        dry_run:   bool   = False,
        # gold_path: aceptado por compatibilidad con código legacy que inyectaba
        # un path de filesystem. Ya no tiene efecto — Gold usa Iceberg.
        gold_path: object = None,
    ) -> None:
        self._dry_run            = dry_run
        self._engineer_version   = _TRANSFORMER_VERSION
        ensure_gold_table()
        self._table = get_catalog().load_table("gold.features")
        logger.info(
            "GoldStorage ready | backend=iceberg dry_run={} transformer_version={}",
            self._dry_run,
            self._engineer_version,
        )

    # =========================================================================
    # Public API
    # =========================================================================

    def build(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        start:       Optional[pd.Timestamp] = None,
        end:         Optional[pd.Timestamp] = None,
        run_id:      Optional[str]          = None,
    ) -> Optional[pd.DataFrame]:
        """
        Lee Silver (Iceberg), calcula features y hace overwrite en Gold.

        El overwrite es atómico: Iceberg reemplaza las filas del dataset
        en un único snapshot — sin ventanas de inconsistencia.

        Parameters
        ----------
        exchange / symbol / market_type / timeframe : identidad del dataset.
        start / end : filtro temporal opcional sobre Silver.
        run_id      : correlación con el run de ingestión.

        Returns
        -------
        pd.DataFrame con features, o None si Silver está vacío o falla.
        """
        silver   = IcebergStorage(exchange=exchange, market_type=market_type)

        # Capturar snapshot ANTES del load — ancla el lineage al punto exacto
        # en el tiempo que Gold usó como fuente.
        _snap    = silver.get_current_snapshot() or {}
        _snap_id = _snap.get("snapshot_id", 0)
        _snap_ms = _snap.get("timestamp_ms", 0)

        # ── Cargar Silver ─────────────────────────────────────────────────────
        try:
            df = silver.load_ohlcv(
                symbol=symbol, timeframe=timeframe, start=start, end=end,
            )
        except Exception as exc:
            logger.warning(
                "Gold build: Silver load failed | {}/{}/{}/{} err={}",
                exchange, symbol, market_type, timeframe, exc,
            )
            return None

        if df is None or df.empty:
            logger.warning(
                "Gold build: sin datos en Silver | {}/{}/{}/{}",
                exchange, symbol, market_type, timeframe,
            )
            return None

        # ── Limpiar timestamps NaN antes de feature engineering ───────────────
        nan_ts = df["timestamp"].isna().sum()
        if nan_ts > 0:
            logger.warning(
                "Gold build: {} timestamps NaN eliminados | {}/{}/{}/{}",
                nan_ts, exchange, symbol, market_type, timeframe,
            )
            df = df.dropna(subset=["timestamp"]).reset_index(drop=True)

        # ── Feature engineering — GoldTransformer (estático, sin estado) ──────
        df = GoldTransformer.transform(
            df,
            symbol    = symbol,
            timeframe = timeframe,
            exchange  = exchange,
        )

        # ── DRY RUN ───────────────────────────────────────────────────────────
        if self._dry_run:
            logger.info(
                "[DRY RUN] Gold build skipped | {}/{}/{}/{} rows={} run_id={}",
                exchange, symbol, market_type, timeframe, len(df), run_id,
            )
            return df

        # ── Preparar DataFrame para Iceberg ───────────────────────────────────
        prepared = _prepare_gold_df(
            df,
            exchange           = exchange,
            symbol             = symbol,
            market_type        = market_type,
            timeframe          = timeframe,
            run_id             = run_id,
            engineer_version   = self._engineer_version,
            silver_snapshot_id = _snap_id,
            silver_snapshot_ms = _snap_ms,
        )

        # ── Overwrite atómico — reemplaza solo este dataset ───────────────────
        self._table.overwrite(
            pa.Table.from_pandas(
                prepared,
                schema         = self._table.schema().as_arrow(),
                preserve_index = False,
            ),
            overwrite_filter    = _dataset_filter(
                exchange, symbol, market_type, timeframe,
            ),
            snapshot_properties = {
                "ocm.exchange":    exchange,
                "ocm.symbol":      symbol,
                "ocm.market_type": market_type,
                "ocm.timeframe":   timeframe,
                "ocm.run_id":      run_id or "",
            },
        )

        logger.info(
            "Gold saved | {}/{}/{}/{} rows={} features={} silver_snap={}",
            exchange, symbol, market_type, timeframe,
            len(df), len(df.columns), _snap_id,
        )
        return df

    def build_all(
        self,
        exchange:    str,
        symbols:     List[str],
        market_type: str,
        timeframes:  List[str],
        run_id:      Optional[str] = None,
    ) -> None:
        """Construye Gold para todos los pares/timeframes de un exchange."""
        total  = len(symbols) * len(timeframes)
        done   = 0
        failed = 0
        for symbol in symbols:
            for tf in timeframes:
                try:
                    result = self.build(
                        exchange    = exchange,
                        symbol      = symbol,
                        market_type = market_type,
                        timeframe   = tf,
                        run_id      = run_id,
                    )
                    if result is None:
                        failed += 1
                    else:
                        done += 1
                except Exception as exc:
                    failed += 1
                    logger.error(
                        "Gold build_all error | {}/{}/{}/{} err={}",
                        exchange, symbol, market_type, tf, exc,
                    )
                logger.debug(
                    "Gold build_all progress | {}/{} done={} failed={}",
                    done + failed, total, done, failed,
                )
        logger.info(
            "Gold build_all finished | exchange={} market={} done={} failed={}/{}",
            exchange, market_type, done, failed, total,
        )

    def list_versions(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
    ) -> List[int]:
        """
        Retorna snapshot_ids que construyeron este dataset específico,
        en orden cronológico.

        Filtra por ocm.* properties inyectadas en cada overwrite().
        Snapshots anteriores a esta feature (sin properties) se omiten.
        """
        try:
            result = []
            for entry in self._table.history():
                snap = self._table.snapshot_by_id(entry.snapshot_id)
                if snap is None:
                    continue
                props = getattr(snap.summary, "additional_properties", {}) or {}
                if (
                    props.get("ocm.exchange")    == exchange
                    and props.get("ocm.symbol")      == symbol
                    and props.get("ocm.market_type") == market_type
                    and props.get("ocm.timeframe")   == timeframe
                ):
                    result.append(entry.snapshot_id)
            return result
        except Exception as _snap_exc:
            logger.debug(
                "list_snapshots_for failed — retornando vacío",
                error=str(_snap_exc),
            )
            return []


# =============================================================================
# Helpers internos
# =============================================================================

def _dataset_filter(
    exchange:    str,
    symbol:      str,
    market_type: str,
    timeframe:   str,
):
    """Filtro Iceberg que identifica un dataset específico."""
    return And(
        And(EqualTo("exchange", exchange), EqualTo("symbol", symbol)),
        And(EqualTo("market_type", market_type), EqualTo("timeframe", timeframe)),
    )


def _prepare_gold_df(
    df:                 pd.DataFrame,
    exchange:           str,
    symbol:             str,
    market_type:        str,
    timeframe:          str,
    run_id:             Optional[str],
    engineer_version:   str,
    silver_snapshot_id: int,
    silver_snapshot_ms: int,
) -> pd.DataFrame:
    """
    Prepara el DataFrame para escritura en Iceberg:
    - Timestamp a microsegundos UTC.
    - Inyecta columnas de identidad y lineage.
    - Rellena features ausentes con None (columnas nullable en schema).
    - Reordena al orden canónico _GOLD_COLS.
    """
    df = df.copy()
    df["timestamp"] = (
        pd.to_datetime(df["timestamp"], utc=True)
        .astype("datetime64[us, UTC]")
    )
    df["exchange"]           = exchange
    df["market_type"]        = market_type
    df["symbol"]             = symbol
    df["timeframe"]          = timeframe
    df["run_id"]             = run_id or ""
    df["engineer_version"]   = engineer_version
    df["silver_snapshot_id"] = silver_snapshot_id
    df["silver_snapshot_ms"] = silver_snapshot_ms

    for col in _GOLD_COLS:
        if col not in df.columns:
            df[col] = None

    return df[_GOLD_COLS].reset_index(drop=True)
