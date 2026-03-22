"""
gold_storage.py
===============

Capa Gold del Data Lakehouse.

Responsabilidad
---------------
Leer datos Silver limpios, calcular features técnicos via FeatureEngineer,
y persistir el dataset enriquecido listo para estrategias/backtesting.

Estructura de partición
-----------------------
gold/features/ohlcv/
  exchange={exchange}/
    symbol={symbol}/
      market_type={market_type}/
        timeframe={timeframe}/
          {symbol}_{tf}_features.parquet
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, List

import pandas as pd
from loguru import logger

from market_data.batch.storage.feature_engineer import FeatureEngineer


# ==========================================================
# Path resolution — absoluta, nunca relativa
# ==========================================================

_LAKE_SUBPATH   = ("data_platform", "data_lake")
_SILVER_SUBPATH = _LAKE_SUBPATH + ("silver", "ohlcv")
_GOLD_SUBPATH   = _LAKE_SUBPATH + ("gold", "features", "ohlcv")

def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]

def _default_silver() -> Path:
    return _repo_root().joinpath(*_SILVER_SUBPATH)

def _default_gold() -> Path:
    return _repo_root().joinpath(*_GOLD_SUBPATH)


# ==========================================================
# Partition resolver
# ==========================================================

def _resolve_partitions(
    base_dir: Path,
    start: Optional[pd.Timestamp],
    end: Optional[pd.Timestamp],
) -> list:
    parquets = sorted(base_dir.rglob("*.parquet"))
    if start is None and end is None:
        return parquets
    result = []
    for p in parquets:
        parts = p.parts
        try:
            year_idx = next(i for i, x in enumerate(parts) if x.isdigit() and len(x) == 4)
            year  = int(parts[year_idx])
            month = int(parts[year_idx + 1]) if year_idx + 1 < len(parts) and parts[year_idx + 1].isdigit() else 1
            day   = int(parts[year_idx + 2]) if year_idx + 2 < len(parts) and parts[year_idx + 2].isdigit() else 1
            partition_start = pd.Timestamp(year=year, month=month, day=day, tz="UTC")
            partition_end   = (
                partition_start + pd.offsets.MonthEnd(1)
                if day == 1
                else partition_start + pd.Timedelta(days=1)
            )
            if start and partition_end < start:
                continue
            if end and partition_start > end:
                continue
        except (StopIteration, ValueError, IndexError):
            pass
        result.append(p)
    return result


# ==========================================================
# GoldStorage
# ==========================================================

class GoldStorage:
    """
    Construye datasets Gold (Silver + features) para trading/backtesting.

    Uso
    ---
    gold = GoldStorage()
    df   = gold.build(exchange="bybit", symbol="BTC/USDT", market_type="spot", timeframe="1h")
    """

    def __init__(
        self,
        silver_path: Optional[Path] = None,
        gold_path:   Optional[Path] = None,
    ) -> None:
        self._silver   = Path(silver_path) if silver_path else _default_silver()
        self._gold     = Path(gold_path)   if gold_path   else _default_gold()
        self._gold.mkdir(parents=True, exist_ok=True)
        self._engineer = FeatureEngineer()
        logger.info("GoldStorage ready | silver={} gold={}", self._silver, self._gold)

    def build(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        start:       Optional[pd.Timestamp] = None,
        end:         Optional[pd.Timestamp] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Lee Silver, calcula features y persiste en Gold.

        Parameters
        ----------
        exchange    : e.g. "bybit"
        symbol      : e.g. "BTC/USDT"
        market_type : "spot" | "swap"
        timeframe   : e.g. "1h"
        start / end : filtro de tiempo opcional
        """
        symbol_safe = symbol.replace("/", "_")
        silver_dir = (
            self._silver
            / f"exchange={exchange}"
            / f"symbol={symbol_safe}"
            / f"market_type={market_type}"
            / f"timeframe={timeframe}"
        )

        if not silver_dir.exists():
            logger.warning(
                "Gold build: Silver no existe | {}/{}/{}/{}",
                exchange, symbol, market_type, timeframe,
            )
            return None

        parquets = _resolve_partitions(silver_dir, start, end)
        if not parquets:
            logger.warning(
                "Gold build: sin parquets | {}/{}/{}/{}",
                exchange, symbol, market_type, timeframe,
            )
            return None

        frames = []
        for p in parquets:
            try:
                frames.append(pd.read_parquet(p))
            except Exception as exc:
                logger.warning("Gold build: error leyendo parquet | {} err={}", p, exc)

        if not frames:
            return None

        df = (
            pd.concat(frames, ignore_index=True)
            .drop_duplicates(subset="timestamp")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )

        df = self._engineer.compute(df, symbol=symbol, timeframe=timeframe)

        out_dir = (
            self._gold
            / f"exchange={exchange}"
            / f"symbol={symbol_safe}"
            / f"market_type={market_type}"
            / f"timeframe={timeframe}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{symbol_safe}_{timeframe}_features.parquet"
        df.to_parquet(out_file, index=False)

        logger.info(
            "Gold saved | {}/{}/{}/{} rows={} features={} file={}",
            exchange, symbol, market_type, timeframe,
            len(df), len(df.columns), out_file,
        )
        return df

    def build_all(
        self,
        exchange:    str,
        symbols:     List[str],
        market_type: str,
        timeframes:  List[str],
    ) -> None:
        """Construye Gold para todos los pares/timeframes de un exchange."""
        for symbol in symbols:
            for tf in timeframes:
                try:
                    self.build(
                        exchange=exchange,
                        symbol=symbol,
                        market_type=market_type,
                        timeframe=tf,
                    )
                except Exception as exc:
                    logger.error(
                        "Gold build_all error | {}/{}/{}/{} err={}",
                        exchange, symbol, market_type, tf, exc,
                    )
