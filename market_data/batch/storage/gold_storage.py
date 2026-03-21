from __future__ import annotations
from pathlib import Path
from typing import Optional, List
import pandas as pd
from loguru import logger
from market_data.batch.storage.feature_engineer import FeatureEngineer

_SILVER_PATH = Path("data_platform/data_lake/silver/ohlcv")
_GOLD_PATH   = Path("data_platform/data_lake/gold/features/ohlcv")

class GoldStorage:
    def __init__(self, silver_path: Optional[Path] = None, gold_path: Optional[Path] = None) -> None:
        self._silver = silver_path or _SILVER_PATH
        self._gold   = gold_path   or _GOLD_PATH
        self._gold.mkdir(parents=True, exist_ok=True)
        self._engineer = FeatureEngineer()
        logger.info("GoldStorage ready | gold_path={}", self._gold)

    def build(self, exchange: str, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        symbol_path = symbol.replace("/", "_")
        silver_dir = self._silver / f"exchange={exchange}" / f"symbol={symbol_path}" / f"timeframe={timeframe}"
        if not silver_dir.exists():
            logger.warning("Gold build: no existe Silver | {}/{}/{}", exchange, symbol, timeframe)
            return None
        parquets = sorted(silver_dir.rglob("*.parquet"))
        if not parquets:
            logger.warning("Gold build: sin parquets | {}/{}/{}", exchange, symbol, timeframe)
            return None
        frames = []
        for p in parquets:
            try:
                frames.append(pd.read_parquet(p))
            except Exception as exc:
                logger.warning("Gold build: error leyendo parquet | {} err={}", p, exc)
        if not frames:
            return None
        df = pd.concat(frames, ignore_index=True).drop_duplicates(subset="timestamp").sort_values("timestamp").reset_index(drop=True)
        df = self._engineer.compute(df, symbol=symbol, timeframe=timeframe)
        out_path = self._gold / f"exchange={exchange}" / f"symbol={symbol_path}" / f"timeframe={timeframe}"
        out_path.mkdir(parents=True, exist_ok=True)
        out_file = out_path / f"{symbol_path}_{timeframe}_features.parquet"
        df.to_parquet(out_file, index=False)
        logger.info("Gold saved | {}/{}/{} rows={} file={}", exchange, symbol, timeframe, len(df), out_file)
        return df

    def build_all(self, exchange: str, symbols: List[str], timeframes: List[str]) -> None:
        for symbol in symbols:
            for tf in timeframes:
                try:
                    self.build(exchange=exchange, symbol=symbol, timeframe=tf)
                except Exception as exc:
                    logger.error("Gold build_all error | {}/{}/{} err={}", exchange, symbol, tf, exc)
