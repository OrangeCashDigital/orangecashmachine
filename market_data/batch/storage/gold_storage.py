from __future__ import annotations
from pathlib import Path
from typing import Optional, List
import pandas as pd
from loguru import logger
from market_data.batch.storage.feature_engineer import FeatureEngineer

_SILVER_PATH = Path("data_platform/data_lake/silver/ohlcv")
_GOLD_PATH   = Path("data_platform/data_lake/gold/features/ohlcv")

def _resolve_partitions(base_dir: "Path", start: "Optional[pd.Timestamp]", end: "Optional[pd.Timestamp]") -> list:
    from pathlib import Path
    parquets = sorted(base_dir.rglob("*.parquet"))
    if start is None and end is None:
        return parquets
    result = []
    for p in parquets:
        parts = p.parts
        try:
            year_idx  = next(i for i, x in enumerate(parts) if x.isdigit() and len(x) == 4)
            year  = int(parts[year_idx])
            month = int(parts[year_idx + 1]) if year_idx + 1 < len(parts) and parts[year_idx + 1].isdigit() else 1
            day   = int(parts[year_idx + 2]) if year_idx + 2 < len(parts) and parts[year_idx + 2].isdigit() else 1
            import pandas as _pd
            partition_start = _pd.Timestamp(year=year, month=month, day=day, tz="UTC")
            partition_end   = partition_start + _pd.offsets.MonthEnd(1) if day == 1 else partition_start + _pd.Timedelta(days=1)
            if start and partition_end < start: continue
            if end   and partition_start > end:  continue
        except (StopIteration, ValueError, IndexError):
            pass
        result.append(p)
    return result


class GoldStorage:
    def __init__(self, silver_path: Optional[Path] = None, gold_path: Optional[Path] = None) -> None:
        self._silver = silver_path or _SILVER_PATH
        self._gold   = gold_path   or _GOLD_PATH
        self._gold.mkdir(parents=True, exist_ok=True)
        self._engineer = FeatureEngineer()
        logger.info("GoldStorage ready | gold_path={}", self._gold)

    def build(self, exchange: str, symbol: str, timeframe: str, **kwargs) -> Optional[pd.DataFrame]:
        symbol_path = symbol.replace("/", "_")
        silver_dir = self._silver / f"exchange={exchange}" / f"symbol={symbol_path}" / f"timeframe={timeframe}"
        if not silver_dir.exists():
            logger.warning("Gold build: no existe Silver | {}/{}/{}", exchange, symbol, timeframe)
            return None
        start_ts = kwargs.get("start", None)
        end_ts   = kwargs.get("end", None)
        parquets = _resolve_partitions(silver_dir, start_ts, end_ts)
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
