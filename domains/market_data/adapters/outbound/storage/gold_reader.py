# -*- coding: utf-8 -*-
"""
market_data/adapters/outbound/storage/gold_reader.py
=====================================================

GoldReader — adaptador concreto de FeatureReaderPort.

Responsabilidad única
---------------------
Leer features desde Iceberg gold.features usando pyiceberg directamente.
Implementa FeatureReaderPort — el único adaptador activo para Gold.

Migrado desde data_platform/loaders/gold_loader.py.
data_platform NO debe acceder al catalog Iceberg directamente (DIP).

Versionado
----------
  version="latest"   → snapshot actual (default)
  version=<int>      → snapshot_id exacto (reproducible)
  as_of=<ISO 8601>   → snapshot vigente en ese instante (time travel)

SafeOps
-------
list_versions, list_datasets, get_manifest: nunca lanzan — retornan []/None.
load_features: lanza DataNotFoundError / DataReadError — errores explícitos.

Principios: DIP · SRP · KISS · SafeOps · Fail-Fast en load, Fail-Soft en meta
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
from market_data.infrastructure.storage.iceberg.catalog import (
    ensure_gold_table,
    get_catalog,
)

_BASE_COLS = (
    "timestamp", "open", "high", "low", "close", "volume",
    "exchange", "market_type", "symbol", "timeframe",
    "return_1", "log_return", "volatility_20", "high_low_spread", "vwap",
)


class GoldReader:
    """
    Adaptador de lectura Gold sobre Apache Iceberg.

    Implementa FeatureReaderPort estructuralmente (Protocol — duck typing).
    No hereda explícitamente para evitar acoplamiento a la interfaz abstracta.

    Uso
    ---
        reader = GoldReader(exchange="bybit")
        df = reader.load_features("BTC/USDT", "spot", "1h")
        df = reader.load_features("BTC/USDT", "spot", "1h",
                                  as_of="2026-03-17T22:40:00Z")
    """

    def __init__(
        self,
        exchange:  Optional[str] = None,
    ) -> None:
        self._exchange = exchange.lower() if exchange else None
        self._table    = None  # lazy — solo en el primer acceso real
        logger.debug(
            "GoldReader created | backend=iceberg exchange={}",
            self._exchange or "any",
        )

    # ── Lazy init ─────────────────────────────────────────────────────────────

    def _get_table(self):
        """Inicializa el catalog Iceberg en el primer acceso. Thread-safe vía GIL."""
        if self._table is None:
            ensure_gold_table()
            self._table = get_catalog().load_table("gold.features")
            logger.info(
                "GoldReader ready | backend=iceberg exchange={}",
                self._exchange or "any",
            )
        return self._table

    # ── FeatureReaderPort ─────────────────────────────────────────────────────

    def load_features(
        self,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        version:     str                 = "latest",
        as_of:       Optional[str]       = None,
        columns:     Optional[List[str]] = None,
        exchange:    Optional[str]       = None,
    ) -> pd.DataFrame:
        """Carga features Gold para un símbolo/timeframe desde Iceberg."""
        exch       = (exchange or self._exchange or "").lower()
        row_filter = _build_filter(exch, symbol, market_type, timeframe)
        snap_id    = self._resolve_snapshot(version, as_of)

        try:
            scan = self._get_table().scan(
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
        """Snapshot_ids que construyeron este dataset. SafeOps: retorna [] ante error."""
        try:
            result = []
            for entry in self._get_table().history():
                snap  = self._table.snapshot_by_id(entry.snapshot_id)
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

    def list_datasets(
        self,
        exchange:    str,
        market_type: str,
    ) -> List[Dict]:
        """Datasets Gold disponibles para exchange/market_type. SafeOps: retorna [] ante error."""
        try:
            seen:   set       = set()
            result: List[Dict] = []
            for entry in self._get_table().history():
                snap  = self._table.snapshot_by_id(entry.snapshot_id)
                if snap is None:
                    continue
                props = getattr(snap.summary, "additional_properties", {}) or {}
                exch  = props.get("ocm.exchange")
                sym   = props.get("ocm.symbol")
                mkt   = props.get("ocm.market_type")
                tf    = props.get("ocm.timeframe")
                if not all([exch, sym, mkt, tf]):
                    continue
                if exch != exchange or mkt != market_type:
                    continue
                key = (exch, sym, mkt, tf)
                if key not in seen:
                    seen.add(key)
                    result.append({
                        "exchange":    exch,
                        "symbol":      sym,
                        "market_type": mkt,
                        "timeframe":   tf,
                    })
            return sorted(result, key=lambda d: (d["symbol"], d["timeframe"]))
        except Exception as exc:
            logger.warning("GoldReader.list_datasets failed: {}", exc)
            return []

    def get_manifest(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        version:     str           = "latest",
        as_of:       Optional[str] = None,
    ) -> Optional[Dict]:
        """Metadata del snapshot resuelto. SafeOps: retorna None ante error."""
        snap_id = self._resolve_snapshot(version, as_of)
        try:
            snap = (
                self._get_table().snapshot_by_id(snap_id)
                if snap_id is not None
                else self._get_table().current_snapshot()
            )
            if snap is None:
                return None
            props = getattr(snap.summary, "additional_properties", {}) or {}
            return {
                "snapshot_id":   snap.snapshot_id,
                "timestamp_ms":  snap.timestamp_ms,
                "exchange":      props.get("ocm.exchange",    exchange),
                "symbol":        props.get("ocm.symbol",      symbol),
                "market_type":   props.get("ocm.market_type", market_type),
                "timeframe":     props.get("ocm.timeframe",   timeframe),
                "run_id":        props.get("ocm.run_id",      ""),
                "added_records": props.get("added-records"),
                "total_records": props.get("total-records"),
                "total_files":   props.get("total-data-files"),
                "total_size":    props.get("total-files-size"),
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
        Resuelve snapshot_id desde version/as_of.

        as_of   → snapshot más reciente con timestamp_ms <= target
        latest  → None (Iceberg usa current snapshot por defecto)
        int str → int(version) directamente
        v000003 → VersionNotFoundError (formato legacy incompatible)
        """
        if as_of is not None:
            target_ms = int(pd.Timestamp(as_of, tz="UTC").timestamp() * 1000)
            try:
                history  = self._get_table().history()
                eligible = [s for s in history if s.timestamp_ms <= target_ms]
                if not eligible:
                    raise VersionNotFoundError(
                        f"No Gold snapshot before as_of={as_of}"
                    )
                return max(eligible, key=lambda s: s.timestamp_ms).snapshot_id
            except VersionNotFoundError:
                raise
            except Exception as exc:
                raise DataReadError(f"Gold history scan failed: {exc}") from exc

        if version == "latest":
            return None

        if isinstance(version, str) and version.startswith("v"):
            raise VersionNotFoundError(
                f"Formato legacy '{version}' incompatible con Iceberg. "
                f"Usa snapshot_id entero o version='latest'."
            )

        try:
            return int(version)
        except (ValueError, TypeError):
            raise VersionNotFoundError(
                f"Versión inválida '{version}' — "
                f"usa 'latest', snapshot_id entero, o as_of=ISO timestamp"
            )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_filter(
    exchange:    str,
    symbol:      str,
    market_type: str,
    timeframe:   str,
):
    return And(
        And(EqualTo("exchange", exchange), EqualTo("symbol", symbol)),
        And(EqualTo("market_type", market_type), EqualTo("timeframe", timeframe)),
    )


__all__ = ["GoldReader"]
