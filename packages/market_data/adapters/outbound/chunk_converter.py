from __future__ import annotations

from typing import Optional

import pandas as pd


class PassthroughChunkConverter:
    def to_chunk(
        self,
        df: pd.DataFrame,
        exchange: str,
        symbol: str,
        timeframe: str,
        source: str = "rest",
        run_id: str = "",
        chunk_index: int = 0,
        total_chunks: Optional[int] = None,
    ) -> object:
        from market_data.adapters.inbound.pandas_to_domain import ohlcv_df_to_chunk

        return ohlcv_df_to_chunk(
            df=df,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            source=source,
            run_id=run_id,
            chunk_index=chunk_index,
            total_chunks=total_chunks,
        )


def get_default_converter() -> PassthroughChunkConverter:
    """
    Retorna la implementación por defecto de OHLCVChunkConverterPort.

    SSOT del default — evita duplicar _default_converter() en cada strategy.
    Las strategies llaman get_default_converter() en lugar de construir
    PassthroughChunkConverter directamente.

    DRY: definición única del converter por defecto en el composition root
    de adapters, no en cada caller.
    """
    return PassthroughChunkConverter()


__all__ = ["PassthroughChunkConverter", "get_default_converter"]
