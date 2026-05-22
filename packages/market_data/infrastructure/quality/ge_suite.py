# -*- coding: utf-8 -*-
"""
market_data/infrastructure/quality/ge_suite.py
===============================================

Definición declarativa de la suite de expectativas Great Expectations
para candles OHLCV Silver.

Responsabilidad única
---------------------
Declarar QUÉ se valida, no CÓMO se ejecuta.
La ejecución vive en ge_checker.py.

Principios
----------
OCP  — nuevas expectativas = nuevas funciones aquí, sin tocar ge_checker
SSOT — única fuente de verdad para expectativas OHLCV
DRY  — helpers reutilizables entre suites (spot, futures, etc.)

Dependencias
------------
Solo great_expectations — sin imports de market_data (infra pura).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import great_expectations as gx
    from great_expectations import ExpectationSuite


# ---------------------------------------------------------------------------
# Constantes de dominio — umbrales de validación OHLCV
# ---------------------------------------------------------------------------

_OHLCV_REQUIRED_COLUMNS = ("open", "high", "low", "close", "volume")

# Precio mínimo absoluto — filtra ticks basura / overflow de exchange
_MIN_PRICE_ABSOLUTE: float = 0.0

# Volumen mínimo — 0.0 permitido (candles sin trades son válidas)
_MIN_VOLUME: float = 0.0

# Tasa máxima de nulos tolerable antes de escalar a issue crítico
_MAX_NULL_RATE: float = 0.01  # 1 %

# Ratio máximo aceptable open/close vs high (sanity check OHLCV)
_MAX_PRICE_SPREAD_RATIO: float = 10.0  # high no puede ser 10x > low


# ---------------------------------------------------------------------------
# Builders de expectativas por categoría
# ---------------------------------------------------------------------------


def _add_schema_expectations(suite: "ExpectationSuite") -> None:
    """Columnas requeridas y tipos correctos."""
    import great_expectations.expectations as gxe

    for col in _OHLCV_REQUIRED_COLUMNS:
        suite.add_expectation(gxe.ExpectColumnToExist(column=col))

    # Tipos numéricos para todas las columnas OHLCV
    for col in _OHLCV_REQUIRED_COLUMNS:
        suite.add_expectation(gxe.ExpectColumnValuesToBeOfType(column=col, type_="float64"))


def _add_null_expectations(suite: "ExpectationSuite") -> None:
    """Nulos: ninguna columna OHLCV debe tener nulos."""
    import great_expectations.expectations as gxe

    for col in _OHLCV_REQUIRED_COLUMNS:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))


def _add_price_sanity_expectations(suite: "ExpectationSuite") -> None:
    """
    Integridad de precios OHLCV:
      - open, high, low, close > 0
      - high >= low  (invariante de candle)
      - high >= open, high >= close
      - low  <= open, low  <= close
    """
    import great_expectations.expectations as gxe

    # Precios positivos
    for col in ("open", "high", "low", "close"):
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(
                column=col,
                min_value=_MIN_PRICE_ABSOLUTE,
                strict_min=True,  # estrictamente > 0
            )
        )

    # high >= low — columna derivada
    suite.add_expectation(
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high",
            column_B="low",
            or_equal=True,
        )
    )

    # high >= open
    suite.add_expectation(
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high",
            column_B="open",
            or_equal=True,
        )
    )

    # high >= close
    suite.add_expectation(
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="high",
            column_B="close",
            or_equal=True,
        )
    )

    # low <= open
    suite.add_expectation(
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="open",
            column_B="low",
            or_equal=True,
        )
    )

    # low <= close
    suite.add_expectation(
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="close",
            column_B="low",
            or_equal=True,
        )
    )


def _add_volume_expectations(suite: "ExpectationSuite") -> None:
    """Volumen >= 0 (candles sin trades tienen volume == 0, son válidas)."""
    import great_expectations.expectations as gxe

    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="volume",
            min_value=_MIN_VOLUME,
            strict_min=False,
        )
    )


def _add_index_expectations(suite: "ExpectationSuite") -> None:
    """
    Índice temporal:
      - Debe ser único (sin timestamps duplicados)
      - Debe ser monotónicamente creciente
    """
    import great_expectations.expectations as gxe

    suite.add_expectation(gxe.ExpectTableRowCountToBeGreaterThan(value=0))  # type: ignore[attr-defined]  # GE v1.x API

    # GE valida el índice como columna reset; el checker lo resetea antes
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="_ts_index"))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeIncreasing(
            column="_ts_index",
            strictly=True,
        )
    )


# ---------------------------------------------------------------------------
# API pública: builder de suite completa
# ---------------------------------------------------------------------------


def build_ohlcv_suite(context: "gx.DataContext") -> "ExpectationSuite":  # type: ignore[name-defined]  # gx importado localmente
    """
    Construye y registra la suite completa de expectativas OHLCV.

    Llama a todos los builders por categoría — OCP: añadir categoría =
    añadir función _add_X + llamarla aquí.

    Parameters
    ----------
    context : DataContext efímero de GE (ephemeral mode)

    Returns
    -------
    ExpectationSuite registrada en el contexto, lista para validar.
    """
    import great_expectations as gx

    suite = gx.ExpectationSuite(name="ohlcv_silver_suite")
    suite = context.suites.add(suite)

    _add_schema_expectations(suite)
    _add_null_expectations(suite)
    _add_price_sanity_expectations(suite)
    _add_volume_expectations(suite)
    _add_index_expectations(suite)

    return suite


__all__ = ["build_ohlcv_suite"]
