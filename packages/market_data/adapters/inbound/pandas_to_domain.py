"""
market_data/adapters/pandas_to_domain.py
=========================================

Adapter/Anti-Corruption Layer entre pandas (externo) y el dominio OHLCV.

Responsabilidad
---------------
Traducir pd.DataFrame (formato tabular externo) a objetos de dominio
(OHLCVChunk, Candle) con validación completa.

Clean Architecture
------------------
Este adapter está en la frontera entre infraestructura y dominio.
- pandas CONSUME dominio (importa domain.value_objects)
- el dominio NO conoce pandas (no importa este módulo)

Principios: SRP · Fail-Fast · SafeOps · DIP · KISS · SSOT
"""

from __future__ import annotations

from typing import Optional

import polars as pl

from market_data.domain.value_objects.candle import Candle
from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk

# ── Validación SSOT ──────────────────────────────────────────────────────────

_REQUIRED_COLUMNS: frozenset[str] = frozenset({"timestamp", "open", "high", "low", "close", "volume"})
"""Columnas obligatorias del DataFrame OHLCV (SSOT)."""


# ── Excepción específica ─────────────────────────────────────────────────────


class DataFrameMappingError(ValueError):
    """Error de mapping DataFrame → dominio. Columnas faltantes o datos inválidos."""


# ── Mapper ───────────────────────────────────────────────────────────────────


def ohlcv_df_to_chunk(
    df: pl.DataFrame,
    exchange: str,
    symbol: str,
    timeframe: str,
    source: str = "rest",
    run_id: str = "",
    chunk_index: int = 0,
    total_chunks: Optional[int] = None,
) -> OHLCVChunk:
    """
    Traduce un DataFrame OHLCV a OHLCVChunk del dominio.

    Fail-Fast
    ---------
    - Lanza DataFrameMappingError si faltan columnas requeridas.
    - Lanza DataFrameMappingError si alguna Candle viola invariantes.

    SafeOps
    -------
    No silencia errores — el caller decide cómo manejar datos corruptos.
    Este mapper es ACL, no quality gate.

    Parameters
    ----------
    df : DataFrame con columnas [timestamp, open, high, low, close, volume].
         timestamp puede ser pd.Timestamp o int epoch ms.
    """
    # ── Fail-Fast: validar columnas ──────────────────────────────────────────
    missing = _REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise DataFrameMappingError(
            f"DataFrame OHLCV faltan columnas: {sorted(missing)}. Requeridas: {sorted(_REQUIRED_COLUMNS)}."
        )

    if df.is_empty():
        return OHLCVChunk(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            candles=(),
            source=source,
            run_id=run_id,
            chunk_index=chunk_index,
            total_chunks=total_chunks,
        )

    # ── Normalizar tipos antes del mapping (garantía runtime — sin cast, sin type: ignore) ─
    df_norm = df.with_columns(
        [pl.col(c).cast(pl.Float64, strict=False) for c in ["open", "high", "low", "close", "volume"]]
    )
    ts_dtype = df_norm["timestamp"].dtype
    if ts_dtype == pl.Int64:
        df_norm = df_norm.with_columns(pl.col("timestamp").cast(pl.Datetime("ms")).dt.replace_time_zone("UTC"))
    elif ts_dtype.time_zone is None:  # type: ignore[attr-defined]
        df_norm = df_norm.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))

    # ── Mapping con listas planas (evita unión de tipos de itertuples) ──────
    timestamps_ms = df_norm["timestamp"].dt.epoch("ms").to_list()
    opens = df_norm["open"].to_list()
    highs = df_norm["high"].to_list()
    lows = df_norm["low"].to_list()
    closes = df_norm["close"].to_list()
    volumes = df_norm["volume"].to_list()

    candles: list[Candle] = []
    for ts_ms, open_val, high_val, low_val, close_val, volume_val in zip(
        timestamps_ms,
        opens,
        highs,
        lows,
        closes,
        volumes,
        strict=True,
    ):
        candle = Candle(
            timestamp_ms=int(ts_ms),
            open=open_val,
            high=high_val,
            low=low_val,
            close=close_val,
            volume=volume_val,
        )
        # Fail-Fast: invariantes de dominio en el ACL
        if not candle.is_valid():
            raise DataFrameMappingError(
                f"Candle inválida en timestamp_ms={ts_ms}: "
                f"open={candle.open} high={candle.high} "
                f"low={candle.low} close={candle.close} "
                f"volume={candle.volume} — exchange={exchange} "
                f"symbol={symbol} timeframe={timeframe}"
            )
        candles.append(candle)

    return OHLCVChunk(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        candles=tuple(candles),
        source=source,
        run_id=run_id,
        chunk_index=chunk_index,
        total_chunks=total_chunks,
    )


__all__ = ["ohlcv_df_to_chunk", "DataFrameMappingError"]
