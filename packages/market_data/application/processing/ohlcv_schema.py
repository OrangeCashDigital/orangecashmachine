"""
OHLCV Schema – OrangeCashMachine
================================

Responsabilidad
---------------
Validar la integridad estructural y lógica de DataFrames OHLCV
antes de almacenamiento o uso en pipelines cuantitativos.

Garantiza
---------
• Tipos correctos
• Valores estrictamente positivos (precio) y no negativos (volumen)
• Consistencia OHLC (relaciones lógicas entre columnas)
• Integridad temporal (orden creciente, sin duplicados)
• Ausencia de nulls

Migración Fase 2
----------------
Migrado de pandera.pandas → pandera.polars (pandera 0.31.1, polars 1.x).
- import pandas as pd           →  import polars as pl
- import pandera.pandas as pa   →  import pandera.polars as pa
- pd.DataFrame                  →  pl.DataFrame
- pd.DatetimeTZDtype(tz="UTC")  →  pl.Datetime("us", "UTC")
- ts.isnull().any()             →  ts.null_count() > 0
- ts.is_monotonic_increasing    →  ts.is_sorted()
- df["timestamp"].is_unique     →  df["timestamp"].is_unique().all()
- df.empty                      →  df.is_empty()
- hasattr(dtype, "tz")          →  isinstance(dtype, pl.Datetime)

Principios aplicados
--------------------
• SOLID – cada función tiene una única responsabilidad
• DRY   – checks reutilizables definidos una sola vez
• KISS  – sin sobre-ingeniería
• SafeOps – fallos explícitos, logging granular, sin swallow silencioso
"""

from __future__ import annotations

from datetime import datetime, timezone  # stdlib — MIN_TIMESTAMP sin polars

import polars as pl
from loguru import logger
from pandera.errors import SchemaError, SchemaErrors
from pandera.polars import Check, Column, DataFrameSchema

from market_data.domain.value_objects.timeframe import (
    InvalidTimeframeError,
    timeframe_to_ms,
)

# ==========================================================
# Constants
# ==========================================================

# Bitcoin genesis block: primer timestamp válido del mercado crypto
MIN_TIMESTAMP: datetime = datetime(2009, 1, 3, tzinfo=timezone.utc)

# Columnas numéricas de precio (deben ser > 0, no solo >= 0)
PRICE_COLUMNS: tuple[str, ...] = ("open", "high", "low", "close")

# Volumen puede ser 0 en mercados ilíquidos, pero no negativo
VOLUME_COLUMNS: tuple[str, ...] = ("volume",)

NUMERIC_COLUMNS: tuple[str, ...] = (*PRICE_COLUMNS, *VOLUME_COLUMNS)

REQUIRED_COLUMNS: tuple[str, ...] = ("timestamp", *NUMERIC_COLUMNS)


# ==========================================================
# Internal Validation Helpers
# ==========================================================
# Cada función valida exactamente una invariante (SRP).
# Los checks de nivel DataFrame reciben pl.DataFrame.
# Los checks de nivel columna reciben pl.Series.
# ==========================================================


def _check_ohlc_relationship(df: pl.DataFrame) -> bool:
    """
    Verifica las relaciones lógicas entre columnas OHLC.

    Invariantes:
        low  <= open, close, high
        high >= open, close, low

    Nota: vectorizado sobre todo el DataFrame, O(n) sin loops.
    """
    low = df["low"]
    high = df["high"]
    open_ = df["open"]
    close = df["close"]

    low_valid = (low <= open_) & (low <= close) & (low <= high)
    high_valid = (high >= open_) & (high >= close) & (high >= low)

    return bool((low_valid & high_valid).all())


def _check_timestamp_monotonic(df: pl.DataFrame) -> bool:
    """
    Verifica que los timestamps estén en orden estrictamente creciente.

    Rechaza series con nulls o flatlines temporales que indicarían
    datos duplicados o feeds con errores.
    """
    ts = df["timestamp"]

    if ts.null_count() > 0:
        return False

    # is_sorted() es equivalente a pandas is_monotonic_increasing:
    # permite iguales consecutivos; la deduplicación la garantiza
    # _check_no_duplicate_timestamps.
    return ts.is_sorted()


def _check_no_duplicate_timestamps(df: pl.DataFrame) -> bool:
    """
    Verifica que no existan timestamps duplicados.

    is_unique() → Boolean Series: True donde el elemento aparece exactamente
    una vez. .all() → True si todos son únicos.

    Los duplicados son síntoma de doble ingestión o bugs en el fetcher.
    """
    return bool(df["timestamp"].is_unique().all())


# ==========================================================
# Reusable Column Check Factories
# ==========================================================
# DRY: en lugar de repetir Check.gt(0) para cada columna de precio,
# lo definimos una vez y lo referenciamos.
# ==========================================================


def _positive_price_check() -> Check:
    """Precio debe ser estrictamente mayor que 0."""
    return Check.gt(0, error="Price columns must be > 0")


def _non_negative_volume_check() -> Check:
    """Volumen puede ser 0 (mercado ilíquido) pero nunca negativo."""
    return Check.ge(0, error="Volume must be >= 0")


def make_grid_alignment_check(timeframe: str) -> Check | None:
    """
    Genera un Check de pandera que verifica alineación al grid del timeframe.

    Por qué es estricto (SchemaError y no warning)
    -----------------------------------------------
    Si llega aquí un timestamp desalineado, align_to_grid no se ejecutó
    o falló upstream — es un bug en el pipeline, no dato ruidoso.
    El schema es la última línea de defensa y debe fallar ruidosamente.

    Retorna None si el timeframe es desconocido (skip seguro).
    """
    try:
        tf_ms = timeframe_to_ms(timeframe)
    except InvalidTimeframeError:
        return None

    def _check(df: pl.DataFrame) -> bool:
        ts_ms = df["timestamp"].dt.timestamp("ms")
        return bool((ts_ms % tf_ms == 0).all())

    return Check(
        _check,
        element_wise=False,
        error=(
            f"Timestamp grid misalignment for timeframe={timeframe}: "
            f"all timestamps must be multiples of {tf_ms}ms. "
            f"align_to_grid() must run before validate_ohlcv()."
        ),
    )


# ==========================================================
# OHLCV Schema Definition
# ==========================================================

OHLCV_SCHEMA: DataFrameSchema = DataFrameSchema(
    columns={
        "timestamp": Column(
            pl.Datetime("us", "UTC"),
            nullable=False,
            coerce=True,
            checks=[
                Check(
                    lambda s: s >= MIN_TIMESTAMP,
                    element_wise=False,
                    error=f"Timestamp before market origin ({MIN_TIMESTAMP.date()})",
                ),
            ],
            description="Marca temporal UTC de apertura de la vela",
        ),
        "open": Column(
            pl.Float64,
            nullable=False,
            coerce=True,
            checks=_positive_price_check(),
            description="Precio de apertura",
        ),
        "high": Column(
            pl.Float64,
            nullable=False,
            coerce=True,
            checks=_positive_price_check(),
            description="Precio máximo de la vela",
        ),
        "low": Column(
            pl.Float64,
            nullable=False,
            coerce=True,
            checks=_positive_price_check(),
            description="Precio mínimo de la vela",
        ),
        "close": Column(
            pl.Float64,
            nullable=False,
            coerce=True,
            checks=_positive_price_check(),
            description="Precio de cierre",
        ),
        "volume": Column(
            pl.Float64,
            nullable=False,
            coerce=True,
            checks=_non_negative_volume_check(),
            description="Volumen negociado en la vela",
        ),
    },
    # Checks a nivel DataFrame (invariantes multi-columna)
    checks=[
        Check(
            _check_ohlc_relationship,
            element_wise=False,
            error="OHLC relationship violated: low > open/close/high or high < open/close/low",
        ),
        Check(
            _check_timestamp_monotonic,
            element_wise=False,
            error="Timestamps are not monotonically increasing",
        ),
        Check(
            _check_no_duplicate_timestamps,
            element_wise=False,
            error="Duplicate timestamps detected – possible double ingestion",
        ),
    ],
    strict=True,  # Rechaza columnas extra no declaradas
    coerce=True,  # Intenta coerción de tipos antes de validar
    name="ohlcv_schema",
)


# ==========================================================
# Public Validation API
# ==========================================================


def validate_ohlcv(df: pl.DataFrame, timeframe: str = "unknown") -> pl.DataFrame:
    """
    Valida un DataFrame OHLCV contra el schema canónico.

    Aplica coerción de tipos, luego valida todas las invariantes
    estructurales y lógicas en modo lazy (recopila todos los errores
    antes de lanzar, en lugar de fallar en el primero).

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame OHLCV crudo. Debe contener las columnas:
        timestamp, open, high, low, close, volume.
    timeframe : str
        Timeframe para validar alineación al grid. "unknown" omite el check.

    Returns
    -------
    pl.DataFrame
        DataFrame validado y con tipos garantizados.

    Raises
    ------
    ValueError
        Si el DataFrame es None o está vacío.
    TypeError
        Si timestamp no es Datetime UTC.
    SchemaError
        Si los timestamps no están alineados al grid del timeframe.
    SchemaErrors
        Si alguna invariante del schema no se cumple.
        El atributo `failure_cases` contiene el detalle completo.
    """
    _assert_non_empty(df)
    _assert_utc(df)

    # Grid alignment — ejecutado antes del schema, inline para evitar
    # llamar Check() directamente (no es parte de la API pública de pandera).
    if timeframe != "unknown":
        try:
            tf_ms = timeframe_to_ms(timeframe)
        except InvalidTimeframeError:
            pass
        else:
            ts_ms = df["timestamp"].dt.timestamp("ms")
            if not bool((ts_ms % tf_ms == 0).all()):
                raise SchemaError(
                    schema=OHLCV_SCHEMA,
                    data=df,
                    message=(
                        f"Timestamp grid misalignment for timeframe={timeframe}: "
                        f"timestamps must be multiples of {tf_ms}ms. "
                        f"align_to_grid() must run before validate_ohlcv()."
                    ),
                )

    try:
        validated_df = OHLCV_SCHEMA.validate(df, lazy=True)

    except SchemaErrors as exc:
        _log_validation_failure(exc)
        raise

    logger.debug(
        "OHLCV validation passed | rows={} cols={}",
        len(validated_df),
        list(validated_df.columns),
    )

    return validated_df


# ==========================================================
# Private Helpers
# ==========================================================


def _assert_utc(df: pl.DataFrame) -> None:
    """
    Lanza TypeError si timestamp no es Datetime UTC.

    Falla rápido antes de entrar al schema — evita errores crípticos
    de polars enterrados en pandera al comparar tz-naive vs tz-aware.
    """
    dtype = df["timestamp"].dtype
    if not (isinstance(dtype, pl.Datetime) and dtype.time_zone == "UTC"):
        raise TypeError(
            f"timestamp must be Datetime with UTC timezone, got {dtype!r}. "
            "Call series.dt.convert_time_zone('UTC') or cast to pl.Datetime('us','UTC') "
            "before validate_ohlcv()."
        )


def _assert_non_empty(df: pl.DataFrame) -> None:
    """
    Lanza ValueError si el DataFrame es None o vacío.

    Separado de validate_ohlcv para mantener la función principal legible
    y para poder testear esta guarda de forma independiente.
    """
    if df is None or df.is_empty():
        raise ValueError(
            "Cannot validate an empty or None DataFrame. "
            "Ensure the fetcher returned data before calling validate_ohlcv()."
        )


def _log_validation_failure(exc: SchemaErrors) -> None:
    """
    Loguea un resumen estructurado de los errores de validación.

    Separa el conteo de errores del sample para facilitar
    el triage en producción sin volcar tablas enteras en ERROR.

    Nota: pandera 0.31.x retorna failure_cases como pandas DataFrame
    independientemente del backend — se usa la API pandas aquí.
    """
    failure_cases = exc.failure_cases
    n_errors = len(failure_cases)

    # pandera puede retornar pandas o polars según versión — manejar ambos
    col = failure_cases["check"]
    if hasattr(col, "tolist"):  # pandas Series
        checks_failed = col.unique().tolist()
        sample_str = failure_cases.head(5).to_string()
    else:  # polars Series
        checks_failed = col.unique().to_list()
        sample_str = str(failure_cases.head(5))

    logger.error(
        "OHLCV validation FAILED | total_errors={} | checks={}",
        n_errors,
        checks_failed,
    )

    # Sample en DEBUG: no contamina logs de producción con datos crudos
    logger.debug("Failure sample (top 5):\n{}", sample_str)
