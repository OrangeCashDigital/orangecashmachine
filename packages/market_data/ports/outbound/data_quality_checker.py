# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/data_quality_checker.py
====================================================

Puerto OUTBOUND: contrato de validación de calidad de datos.

Responsabilidad
---------------
Desacoplar quality/pipeline.py de cualquier implementación concreta
(DataQualityChecker nativo, Great Expectations, Soda, etc.).

Principios
----------
DIP  — pipeline depende de abstracción, no de GE ni de implementación concreta
ISP  — interfaz mínima: solo lo que QualityPipeline necesita
OCP  — nuevas implementaciones sin modificar este contrato
BC-31 — quality/ importa este port, nunca infrastructure/

CheckerFactory
--------------
Callable que recibe los parámetros de runtime (timeframe, exchange,
rows_removed) y retorna una instancia lista para ejecutar.

    factory: CheckerFactory = lambda tf, ex, rr: GreatExpectationsChecker(tf, ex, rr)
    checker = factory("1h", "bybit", 0)
    report  = checker.check(df, symbol="BTC/USDT")
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Protocol, runtime_checkable

import pandas as pd

if TYPE_CHECKING:
    # Evitar import circular en runtime — solo para type checkers
    from market_data.quality.report import DataQualityReport


@runtime_checkable
class DataQualityCheckerPort(Protocol):
    """
    Contrato mínimo de un validador de calidad de datos.

    Implementaciones
    ----------------
    market_data.quality.checker.DataQualityChecker         (nativo, legacy)
    market_data.infrastructure.quality.ge_checker.GEChecker (Great Expectations)

    SafeOps
    -------
    Implementaciones deben ser fail-soft: no propagar excepciones al pipeline.
    En caso de error interno, retornar DataQualityReport con issue INTERNAL_ERROR.
    """

    def check(
        self,
        df:     pd.DataFrame,
        *,
        symbol: str,
    ) -> "DataQualityReport":
        """
        Valida df y retorna un reporte con todos los issues detectados.

        Parameters
        ----------
        df     : DataFrame Silver (inmutable — no modificar)
        symbol : par de trading para contexto del reporte (e.g. "BTC/USDT")

        Returns
        -------
        DataQualityReport con issues detectados. Lista vacía = sin problemas.
        """
        ...


# ---------------------------------------------------------------------------
# Factory type alias — único punto de configuración del checker concreto
# ---------------------------------------------------------------------------

CheckerFactory = Callable[
    [str, str, int],   # (timeframe, exchange, rows_removed)
    DataQualityCheckerPort,
]
"""
Callable que construye un DataQualityCheckerPort listo para usar.

Signature: (timeframe: str, exchange: str, rows_removed: int) -> DataQualityCheckerPort

Uso en QualityPipeline:
    checker = self._checker_factory(timeframe, exchange, rows_removed)
    report  = checker.check(df, symbol=symbol)

Uso en tests:
    factory = lambda tf, ex, rr: MockChecker(expected_report)

Uso en producción (GE):
    factory = ge_checker_factory   # definida en infrastructure/quality/ge_checker.py
"""


class NullChecker:
    """
    Implementación vacía de DataQualityCheckerPort.

    Siempre retorna DataQualityReport limpio (sin issues).
    Útil en tests que no necesitan validar calidad.
    """

    def check(
        self,
        df:     pd.DataFrame,
        *,
        symbol: str,
    ) -> "DataQualityReport":
        from market_data.quality.report import DataQualityReport  # late import
        return DataQualityReport(symbol=symbol, rows=len(df), issues=[])


def native_checker_factory(
    timeframe:    str,
    exchange:     str,
    rows_removed: int,
) -> DataQualityCheckerPort:
    """
    Factory que produce el DataQualityChecker nativo (legacy).

    Úsala como fallback o en entornos donde GE no está disponible.
    """
    from market_data.quality.checker import DataQualityChecker  # late import — BC-31 safe
    return DataQualityChecker(
        timeframe=timeframe,
        exchange=exchange,
        rows_removed=rows_removed,
    )


__all__ = [
    "DataQualityCheckerPort",
    "CheckerFactory",
    "NullChecker",
    "native_checker_factory",
]
