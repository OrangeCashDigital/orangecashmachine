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
• Ausencia de NaNs

Principios aplicados
--------------------
• SOLID – cada función tiene una única responsabilidad
• DRY   – checks reutilizables definidos una sola vez
• KISS  – sin sobre-ingeniería
• SafeOps – fallos explícitos, logging granular, sin swallow silencioso
"""

from __future__ import annotations

import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from loguru import logger


# ==========================================================
# Constants
# ==========================================================

# Bitcoin genesis block: primer timestamp válido del mercado crypto
MIN_TIMESTAMP: pd.Timestamp = pd.Timestamp("2009-01-03")

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
# Retornan bool para ser usadas directamente como Pandera Checks.
# ==========================================================

def _check_ohlc_relationship(df: pd.DataFrame) -> bool:
    """
    Verifica las relaciones lógicas entre columnas OHLC.

    Invariantes:
        low  <= open, close, high
        high >= open, close, low

    Nota: vectorizado sobre todo el DataFrame, O(n) sin loops.
    """
    low  = df["low"]
    high = df["high"]
    open_ = df["open"]
    close = df["close"]

    low_valid  = (low  <= open_) & (low  <= close) & (low  <= high)
    high_valid = (high >= open_) & (high >= close) & (high >= low)

    return bool((low_valid & high_valid).all())


def _check_timestamp_monotonic(df: pd.DataFrame) -> bool:
    """
    Verifica que los timestamps estén en orden estrictamente creciente.

    Rechaza series con timestamps iguales consecutivos (flatlines temporales),
    que indicarían datos duplicados o feeds con errores.
    """
    ts = df["timestamp"]

    if ts.isnull().any():
        return False

    # is_monotonic_increasing permite iguales; diff > 0 es más estricto.
    # Usamos is_monotonic_increasing aquí porque la deduplicación
    # la garantiza _check_no_duplicate_timestamps.
    return bool(ts.is_monotonic_increasing)


def _check_no_duplicate_timestamps(df: pd.DataFrame) -> bool:
    """
    Verifica que no existan timestamps duplicados.

    Los duplicados son síntoma de doble ingestión o bugs en el fetcher.
    """
    return bool(df["timestamp"].is_unique)


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


# ==========================================================
# OHLCV Schema Definition
# ==========================================================

OHLCV_SCHEMA: DataFrameSchema = DataFrameSchema(

    columns={

        "timestamp": Column(
            pa.Timestamp,
            nullable=False,
            coerce=True,
            checks=[
                Check.ge(
                    MIN_TIMESTAMP,
                    error=f"Timestamp before market origin ({MIN_TIMESTAMP.date()})",
                ),
            ],
            description="Marca temporal UTC de apertura de la vela",
        ),

        "open": Column(
            float,
            nullable=False,
            coerce=True,
            checks=_positive_price_check(),
            description="Precio de apertura",
        ),

        "high": Column(
            float,
            nullable=False,
            coerce=True,
            checks=_positive_price_check(),
            description="Precio máximo de la vela",
        ),

        "low": Column(
            float,
            nullable=False,
            coerce=True,
            checks=_positive_price_check(),
            description="Precio mínimo de la vela",
        ),

        "close": Column(
            float,
            nullable=False,
            coerce=True,
            checks=_positive_price_check(),
            description="Precio de cierre",
        ),

        "volume": Column(
            float,
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

    strict=True,    # Rechaza columnas extra no declaradas
    coerce=True,    # Intenta coerción de tipos antes de validar
    name="ohlcv_schema",
)


# ==========================================================
# Public Validation API
# ==========================================================

def validate_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida un DataFrame OHLCV contra el schema canónico.

    Aplica coerción de tipos, luego valida todas las invariantes
    estructurales y lógicas en modo lazy (recopila todos los errores
    antes de lanzar, en lugar de fallar en el primero).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame OHLCV crudo. Debe contener las columnas:
        timestamp, open, high, low, close, volume.

    Returns
    -------
    pd.DataFrame
        DataFrame validado y con tipos garantizados.

    Raises
    ------
    ValueError
        Si el DataFrame es None o está vacío.
    pandera.errors.SchemaErrors
        Si alguna invariante del schema no se cumple.
        El atributo `failure_cases` contiene el detalle completo.
    """
    _assert_non_empty(df)

    try:
        validated_df = OHLCV_SCHEMA.validate(df, lazy=True)

    except pa.errors.SchemaErrors as exc:
        _log_validation_failure(exc)
        raise

    # Happy path: DEBUG para no saturar logs en producción
    logger.debug(
        "OHLCV validation passed | rows={} cols={}",
        len(validated_df),
        list(validated_df.columns),
    )

    return validated_df


# ==========================================================
# Private Helpers
# ==========================================================

def _assert_non_empty(df: pd.DataFrame) -> None:
    """
    Lanza ValueError si el DataFrame es None o vacío.

    Separado de validate_ohlcv para mantener la función principal legible
    y para poder testear esta guarda de forma independiente.
    """
    if df is None or df.empty:
        raise ValueError(
            "Cannot validate an empty or None DataFrame. "
            "Ensure the fetcher returned data before calling validate_ohlcv()."
        )


def _log_validation_failure(exc: pa.errors.SchemaErrors) -> None:
    """
    Loguea un resumen estructurado de los errores de validación.

    Separa el conteo de errores del sample para facilitar
    el triage en producción sin volcar tablas enteras en ERROR.
    """
    failure_cases = exc.failure_cases
    n_errors      = len(failure_cases)
    checks_failed = failure_cases["check"].unique().tolist()

    logger.error(
        "OHLCV validation FAILED | total_errors={} | checks={}",
        n_errors,
        checks_failed,
    )

    # Sample en DEBUG: no contamina logs de producción con datos crudos
    logger.debug(
        "Failure sample (top 5):\n{}",
        failure_cases.head(5).to_string(),
    )
