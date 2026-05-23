"""Funciones puras de normalización, particionado, defaults y validación.
SSOT de transformaciones de DataFrame para la pipeline de persistencia.
NO es SSOT de dominio — el modelo de dominio está en domain/value_objects/.

Migración Fase 3 — pandas → polars
------------------------------------
Cada función opera sobre pl.DataFrame nativo.
Los storages (trades_storage, derivatives_storage) que importan estas
funciones son los únicos callers — ambos reciben pl.DataFrame desde
los fetchers ACL.

Principios
----------
- SSOT  — única fuente de transforms de storage; sin duplicación
- SRP   — cada función hace exactamente una cosa
- KISS  — sin estado, sin clases, funciones puras
- DRY   — tipos y columnas requeridas definidos como constantes
- Fail-Fast — assert_columns_exist lanza antes de cualquier I/O
"""

from __future__ import annotations

from typing import Any

import polars as pl

# ── Columnas de partición estándar ────────────────────────────────────────────
_PARTITION_COLS = ("exchange", "market_type")


def normalize_timestamps(df: pl.DataFrame) -> pl.DataFrame:
    """Convierte timestamp a Datetime('us', 'UTC').

    Soporta:
      - Int/Float epoch ms   → cast a Datetime('ms') → UTC → us
      - Datetime sin tz      → replace_time_zone('UTC')
      - Datetime tz != UTC   → convert_time_zone('UTC')
      - Datetime('us','UTC') → no-op
    """
    if "timestamp" not in df.columns:
        return df

    dtype = df["timestamp"].dtype

    if isinstance(dtype, pl.Datetime):
        if dtype.time_zone is None:
            df = df.with_columns(pl.col("timestamp").dt.replace_time_zone("UTC"))
        elif dtype.time_zone != "UTC":
            df = df.with_columns(pl.col("timestamp").dt.convert_time_zone("UTC"))
        if df["timestamp"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp").dt.cast_time_unit("us"))
    else:
        # Int / Float / String → epoch ms (contrato CCXT / exchange)
        df = df.with_columns(
            pl.col("timestamp")
            .cast(pl.Int64, strict=False)
            .cast(pl.Datetime("ms"))
            .dt.replace_time_zone("UTC")
            .dt.cast_time_unit("us")
        )

    return df


def add_partition_columns(
    df: pl.DataFrame,
    exchange: str,
    market_type: str,
) -> pl.DataFrame:
    """Añade columnas de partición si no existen."""
    exprs = []
    if "exchange" not in df.columns:
        exprs.append(pl.lit(exchange).alias("exchange"))
    if "market_type" not in df.columns:
        exprs.append(pl.lit(market_type).alias("market_type"))
    return df.with_columns(exprs) if exprs else df


def ensure_default_columns(
    df: pl.DataFrame,
    defaults: list[tuple[str, Any]],
) -> pl.DataFrame:
    """Añade columnas con valores por defecto si faltan."""
    exprs = [pl.lit(default).alias(col) for col, default in defaults if col not in df.columns]
    return df.with_columns(exprs) if exprs else df


def assert_columns_exist(df: pl.DataFrame, expected: list[str]) -> None:
    """Fail-fast: lanza ValueError si falta alguna columna."""
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Columnas faltantes: {missing}")
