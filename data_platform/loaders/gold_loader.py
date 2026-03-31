"""
gold_loader.py
==============

GoldLoader — OrangeCashMachine Data Platform

Responsabilidad
---------------
Cargar datasets Gold (OHLCV + features técnicos) desde la capa Gold
del Data Lakehouse, con el mismo sistema de versionado que MarketDataLoader.

Filosofía de versionado
------------------------
• "latest"        → lee la última versión del dataset de features
• version="v000003" → versión exacta (reproducible para backtests)
• as_of="2026-03-17T22:40:00Z" → versión vigente en ese timestamp

Estructura esperada en disco
-----------------------------
gold/features/ohlcv/
  exchange={exchange}/
    symbol={symbol}/
      market_type={market_type}/
        timeframe={timeframe}/
          {symbol}_{tf}_features.parquet
          _versions/
            v000001.json
            latest.json

Principios aplicados
--------------------
• SOLID  – SRP: solo lee, no construye (construcción = GoldStorage)
• DRY    – reutiliza las mismas excepciones que MarketDataLoader
• KISS   – API mínima: load_features(exchange, symbol, market_type, timeframe)
• SafeOps – errores explícitos, versionado reproducible
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from core.config.paths import gold_features_root
from data_platform.ohlcv_utils import safe_symbol
from data_platform.loaders.market_data_loader import (
    DataNotFoundError,
    DataReadError,
    VersionNotFoundError,
)


# ==========================================================
# Exceptions propias de Gold (re-exportadas para claridad)
# ==========================================================

GoldLoaderError     = DataNotFoundError   # alias semántico
GoldVersionNotFound = VersionNotFoundError


# ==========================================================
# GoldLoader
# ==========================================================

class GoldLoader:
    """
    Carga DataFrames de features Gold desde el Data Lakehouse.

    Soporta carga versionada para reproducibilidad total:
    - load_features("bybit", "BTC/USDT", "spot", "1h")                     → última versión
    - load_features(..., version="v000003")                                  → versión exacta
    - load_features(..., as_of="2026-03-17T22:40:00Z")                      → snapshot temporal

    Uso típico
    ----------
    loader = GoldLoader(exchange="bybit")

    df = loader.load_features("BTC/USDT", "spot", "1h")
    df = loader.load_features("BTC/USDT", "spot", "1h", version="v000002")
    """

    def __init__(
        self,
        gold_path: Optional[str | Path] = None,
        exchange:  Optional[str] = None,
    ) -> None:
        self._gold     = Path(gold_path) if gold_path else gold_features_root()
        self._exchange = exchange.lower() if exchange else None
        logger.info(
            "GoldLoader ready | path={} exchange={}",
            self._gold, self._exchange or "any",
        )

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

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
        Carga el dataset Gold de features para un símbolo/timeframe.

        Parameters
        ----------
        symbol      : Par de trading, e.g. "BTC/USDT".
        market_type : "spot" | "swap".
        timeframe   : Intervalo, e.g. "1h".
        version     : "latest" o versión específica como "v000003".
        as_of       : ISO 8601. Resuelve la versión vigente en ese momento.
        columns     : Subconjunto de columnas para reducir memoria.
        exchange    : Override del exchange del loader.

        Returns
        -------
        pd.DataFrame
            DataFrame con OHLCV + features, ordenado por timestamp.

        Raises
        ------
        VersionNotFoundError
            Si la versión solicitada no existe.
        DataNotFoundError
            Si no hay datos Gold para el símbolo/timeframe.
        DataReadError
            Si el archivo Parquet no puede leerse.
        """
        exch     = (exchange or self._exchange or "").lower()
        manifest = self._resolve_version(exch, symbol, market_type, timeframe, version, as_of)

        if manifest is None:
            raise DataNotFoundError(
                f"No Gold data for {exch}/{symbol}/{market_type}/{timeframe}"
            )

        parquet_path = self._gold / manifest["file"]

        if not parquet_path.exists():
            raise DataNotFoundError(
                f"Gold parquet missing: {parquet_path} "
                f"(version={manifest['version_id']})"
            )

        try:
            df = pd.read_parquet(parquet_path, engine="pyarrow", columns=columns)
        except Exception as exc:
            raise DataReadError(
                f"Failed reading Gold parquet {parquet_path}: {exc}"
            ) from exc

        df = df.sort_values("timestamp").reset_index(drop=True)

        logger.info(
            "Gold features loaded | {}/{}/{}/{} version={} rows={} features={}",
            exch, symbol, market_type, timeframe,
            manifest["version_id"], len(df), len(df.columns),
        )
        return df

    def get_manifest(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        version:     str = "latest",
        as_of:       Optional[str] = None,
    ) -> Optional[Dict]:
        """Devuelve el manifest de la versión solicitada, o None si no existe."""
        return self._resolve_version(
            exchange.lower(), symbol, market_type, timeframe, version, as_of
        )

    def list_versions(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
    ) -> List[str]:
        """Lista todas las versiones Gold disponibles para un dataset."""
        vdir = self._versions_dir(exchange.lower(), symbol, market_type, timeframe)
        if not vdir.exists():
            return []
        return sorted(p.stem for p in vdir.glob("v*.json"))

    # ----------------------------------------------------------
    # Internal — versioning
    # ----------------------------------------------------------

    def _resolve_version(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        version:     str,
        as_of:       Optional[str],
    ) -> Optional[Dict]:
        """
        Resuelve el manifest Gold según la misma lógica que MarketDataLoader:
        1. Si as_of → versión más reciente cuyo written_at <= as_of
        2. Si version="latest" → latest.json
        3. Si version="v000042" → ese manifest exacto
        """
        vdir = self._versions_dir(exchange, symbol, market_type, timeframe)

        if not vdir.exists():
            return None

        if as_of is not None:
            return self._resolve_as_of(vdir, as_of)

        if version == "latest":
            latest = vdir / "latest.json"
            if not latest.exists():
                return None
            try:
                return json.loads(latest.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.warning("Gold latest.json unreadable | {} | {}", latest, exc)
                return None

        # Versión explícita
        vpath = vdir / f"{version}.json"
        if not vpath.exists():
            raise VersionNotFoundError(
                f"Gold version '{version}' not found for "
                f"{exchange}/{symbol}/{market_type}/{timeframe}"
            )
        try:
            return json.loads(vpath.read_text(encoding="utf-8"))
        except Exception as exc:
            raise DataReadError(f"Cannot read Gold manifest {vpath}: {exc}") from exc

    def _resolve_as_of(self, vdir: Path, as_of: str) -> Optional[Dict]:
        """Versión más reciente cuyo written_at <= as_of."""
        try:
            as_of_ts = pd.Timestamp(as_of, tz="UTC")
        except Exception:
            raise ValueError(f"Invalid as_of timestamp: '{as_of}'")

        candidates = []
        for vpath in sorted(vdir.glob("v*.json")):
            try:
                m = json.loads(vpath.read_text(encoding="utf-8"))
                written_at = pd.Timestamp(m["written_at"]).tz_convert("UTC")
                if written_at <= as_of_ts:
                    candidates.append((written_at, m))
            except Exception as exc:
                logger.warning("Gold manifest unreadable | {} | {}", vpath, exc)

        if not candidates:
            return None

        _, manifest = max(candidates, key=lambda x: x[0])
        logger.debug(
            "Gold resolved as_of={} → {}", as_of, manifest.get("version_id")
        )
        return manifest

    def _dataset_dir(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
    ) -> Path:
        return (
            self._gold
            / f"exchange={exchange}"
            / f"symbol={safe_symbol(symbol)}"
            / f"market_type={market_type}"
            / f"timeframe={timeframe}"
        )

    def _versions_dir(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
    ) -> Path:
        return self._dataset_dir(exchange, symbol, market_type, timeframe) / "_versions"
