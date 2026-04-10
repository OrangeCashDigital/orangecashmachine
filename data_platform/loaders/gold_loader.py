"""
data_platform/loaders/gold_loader.py
======================================

GoldLoader — lee features desde Iceberg gold.features.

Reemplaza la implementación Parquet + _versions/*.json (legacy filesystem).
El versionado ahora es via Iceberg snapshots — más limpio, sin estado en disco,
con time travel garantizado por el motor.

Filosofía de versionado
------------------------
  version="latest"             → snapshot actual (default)
  version=1234567890           → snapshot_id exacto (reproducible)
  as_of="2026-03-17T22:40:00Z" → snapshot vigente en ese instante

Compatibilidad de interfaz
---------------------------
La firma pública de load_features() es compatible con la versión legacy,
excepto que version ahora acepta int (snapshot_id) además de "latest".
Los parámetros gold_path y version="v000003" (formato legacy) se ignoran
o lanzan VersionNotFoundError con mensaje claro.

Principios
----------
• SRP   — solo lee, no construye (construcción = GoldStorage)
• KISS  — API mínima: load_features(symbol, market_type, timeframe)
• SafeOps — errores explícitos, nunca silenciosos
"""
from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd
from loguru import logger
from pyiceberg.expressions import And, EqualTo

from data_platform.ohlcv_utils import (
    DataNotFoundError,
    DataReadError,
    VersionNotFoundError,
)
from market_data.storage.iceberg.catalog import ensure_gold_table, get_catalog

# Re-exportar aliases semánticos para compatibilidad con imports existentes
GoldLoaderError     = DataNotFoundError
GoldVersionNotFound = VersionNotFoundError

_BASE_COLS = (
    "timestamp", "open", "high", "low", "close", "volume",
    "exchange", "market_type", "symbol", "timeframe",
    "return_1", "log_return", "volatility_20", "high_low_spread", "vwap",
)


class GoldLoader:
    """
    Carga DataFrames de features Gold desde Iceberg (gold.features).

    Uso
    ---
    loader = GoldLoader(exchange="bybit")
    df = loader.load_features("BTC/USDT", "spot", "1h")
    df = loader.load_features("BTC/USDT", "spot", "1h",
                              as_of="2026-03-17T22:40:00Z")
    """

    def __init__(
        self,
        exchange:  Optional[str] = None,
        # gold_path ignorado — legacy compat, Gold usa Iceberg
        gold_path: object = None,
    ) -> None:
        self._exchange = exchange.lower() if exchange else None
        ensure_gold_table()
        self._table = get_catalog().load_table("gold.features")
        logger.info(
            "GoldLoader ready | backend=iceberg exchange={}",
            self._exchange or "any",
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def load_features(
        self,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        version:     str = "latest",
        as_of:       Optional[str] = None,
        columns:     Optional[List[str]] = None,
        exchange:    Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Carga features Gold para un símbolo/timeframe.

        Parameters
        ----------
        symbol      : e.g. "BTC/USDT"
        market_type : "spot" | "swap"
        timeframe   : e.g. "1h", "4h", "1d"
        version     : "latest" o snapshot_id entero como string/int
        as_of       : ISO 8601 — snapshot vigente en ese momento
        columns     : subconjunto de columnas (optimiza memoria)
        exchange    : override del exchange del loader

        Returns
        -------
        pd.DataFrame ordenado por timestamp.

        Raises
        ------
        DataNotFoundError    : sin datos para este símbolo/timeframe
        DataReadError        : error al leer desde Iceberg
        VersionNotFoundError : snapshot_id no existe o as_of sin candidatos
        """
        exch       = (exchange or self._exchange or "").lower()
        row_filter = _dataset_filter(exch, symbol, market_type, timeframe)
        snap_id    = self._resolve_snapshot(version, as_of)

        try:
            scan = self._table.scan(
                row_filter      = row_filter,
                selected_fields = tuple(columns) if columns else _BASE_COLS,
                **({"snapshot_id": snap_id} if snap_id is not None else {}),
            )
            df = scan.to_arrow().to_pandas()
        except VersionNotFoundError:
            raise
        except Exception as exc:
            raise DataReadError(
                f"Gold Iceberg read failed | "
                f"{exch}/{symbol}/{market_type}/{timeframe} | {exc}"
            ) from exc

        if df is None or df.empty:
            raise DataNotFoundError(
                f"No Gold data | {exch}/{symbol}/{market_type}/{timeframe}"
            )

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp").reset_index(drop=True)

        logger.info(
            "Gold features loaded | {}/{}/{}/{} rows={} snap={}",
            exch, symbol, market_type, timeframe,
            len(df), snap_id or "latest",
        )
        return df

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

        Filtra por ocm.* properties inyectadas por GoldStorage.build().
        Snapshots sin properties (anteriores a esta feature) se omiten.
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
        except Exception:
            return []

    def get_manifest(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        version:     str = "latest",
        as_of:       Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Metadata del snapshot resuelto para este dataset.

        Incluye las ocm.* properties inyectadas por GoldStorage.build()
        más estadísticas del snapshot (registros, archivos, tamaño).
        """
        snap_id = self._resolve_snapshot(version, as_of)
        try:
            snap = (
                self._table.snapshot_by_id(snap_id)
                if snap_id is not None
                else self._table.current_snapshot()
            )
            if snap is None:
                return None
            props = getattr(snap.summary, "additional_properties", {}) or {}
            return {
                "snapshot_id":    snap.snapshot_id,
                "timestamp_ms":   snap.timestamp_ms,
                # Identidad del dataset desde properties del snapshot (SSOT)
                "exchange":       props.get("ocm.exchange",    exchange),
                "symbol":         props.get("ocm.symbol",      symbol),
                "market_type":    props.get("ocm.market_type", market_type),
                "timeframe":      props.get("ocm.timeframe",   timeframe),
                "run_id":         props.get("ocm.run_id",      ""),
                # Estadísticas del snapshot
                "added_records":  props.get("added-records"),
                "total_records":  props.get("total-records"),
                "total_files":    props.get("total-data-files"),
                "total_size":     props.get("total-files-size"),
            }
        except Exception:
            return None

    # ── Internal ──────────────────────────────────────────────────────────────

    def _resolve_snapshot(
        self,
        version: str,
        as_of:   Optional[str],
    ) -> Optional[int]:
        """
        Resuelve snapshot_id:
          as_of   → el más reciente con timestamp_ms <= target
          latest  → None (Iceberg usa current snapshot por defecto)
          integer → int(version) directamente

        Nota: versiones legacy formato "v000003" lanzan VersionNotFoundError
        con mensaje claro — no intentamos compatibilidad silenciosa.
        """
        if as_of is not None:
            target_ms = int(pd.Timestamp(as_of, tz="UTC").timestamp() * 1000)
            try:
                history  = self._table.history()
                eligible = [s for s in history if s.timestamp_ms <= target_ms]
                if not eligible:
                    raise VersionNotFoundError(
                        f"No Gold snapshot before as_of={as_of}"
                    )
                return max(eligible, key=lambda s: s.timestamp_ms).snapshot_id
            except VersionNotFoundError:
                raise
            except Exception as exc:
                raise DataReadError(
                    f"Gold history scan failed: {exc}"
                ) from exc

        if version == "latest":
            return None

        # Formato legacy "v000003" — no compatible con Iceberg snapshot_ids
        if isinstance(version, str) and version.startswith("v"):
            raise VersionNotFoundError(
                f"Formato de versión legacy '{version}' no compatible con "
                f"backend Iceberg. Usa snapshot_id entero o version='latest'."
            )

        try:
            return int(version)
        except (ValueError, TypeError):
            raise VersionNotFoundError(
                f"Versión inválida '{version}' — "
                f"usa 'latest', un snapshot_id entero, o as_of=ISO timestamp"
            )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dataset_filter(
    exchange:    str,
    symbol:      str,
    market_type: str,
    timeframe:   str,
):
    return And(
        And(EqualTo("exchange", exchange), EqualTo("symbol", symbol)),
        And(EqualTo("market_type", market_type), EqualTo("timeframe", timeframe)),
    )
