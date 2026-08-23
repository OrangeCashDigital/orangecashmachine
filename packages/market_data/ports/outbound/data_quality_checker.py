# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/data_quality_checker.py
==================================================

Puerto OUTBOUND: contrato de validacion de calidad de datos.

Responsabilidad
---------------
Desacoplar quality/pipeline.py de cualquier implementacion concreta
(DataQualityChecker nativo, Soda, etc.).

Principios
----------
DIP  — pipeline depende de abstraccion, no de implementacion concreta
ISP  — interfaz minima: solo lo que QualityPipeline necesita
OCP  — nuevas implementaciones sin modificar este contrato
BC-31 — quality/ importa este port, nunca infrastructure/

CheckerFactory
--------------
Callable que recibe los parametros de runtime (timeframe, exchange,
rows_removed) y retorna una instancia lista para ejecutar.

    factory: CheckerFactory = native_checker_factory
    checker = factory("1h", "bybit", 0)
    report  = checker.check(df, symbol="BTC/USDT")

SSOT de implementaciones
------------------------
- Nativo (produccion):  market_data.application.quality.data_quality.DataQualityChecker
- Null (tests):         NullChecker (este modulo)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Protocol, runtime_checkable

import polars as pl

if TYPE_CHECKING:
    # Solo para type checkers — evita import circular en runtime.
    # SSOT real: market_data.domain.quality.types.DataQualityReport
    from market_data.domain.quality.types import DataQualityReport


@runtime_checkable
class DataQualityCheckerPort(Protocol):
    """
    Contrato minimo de un validador de calidad de datos.

    Implementaciones
    ----------------
    market_data.application.quality.data_quality.DataQualityChecker  (nativo)

    SafeOps
    -------
    Implementaciones deben ser fail-soft: no propagar excepciones al pipeline.
    En caso de error interno, retornar DataQualityReport con issue INTERNAL_ERROR.
    """

    def check(
        self,
        df: pl.DataFrame,
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
        DataQualityReport con issues detectados. Lista vacia = sin problemas.
        """
        ...


# ---------------------------------------------------------------------------
# Factory type alias — unico punto de configuracion del checker concreto
# ---------------------------------------------------------------------------

CheckerFactory = Callable[
    [str, str, int, str],  # (timeframe, exchange, rows_removed, git_hash)
    DataQualityCheckerPort,
]
"""
Callable que construye un DataQualityCheckerPort listo para usar.

Signature: (timeframe: str, exchange: str, rows_removed: int, git_hash: str) -> DataQualityCheckerPort

Uso en QualityPipeline:
    checker = self._checker_factory(timeframe, exchange, rows_removed, git_hash)
    report  = checker.check(df, symbol=symbol)

Uso en tests:
    factory = lambda tf, ex, rr, gh: MockChecker(expected_report)

Uso en produccion (nativo):
    factory = native_checker_factory  # application/quality/data_quality.py
"""


class NullChecker:
    """
    Implementacion vacia de DataQualityCheckerPort.

    Siempre retorna DataQualityReport limpio (sin issues).
    Util en dry_run o tests que no necesitan validar calidad.

    SafeOps: nunca lanza excepciones.
    """

    def check(
        self,
        df: pl.DataFrame,
        *,
        symbol: str,
    ) -> "DataQualityReport":
        # Late import — BC-31: port no importa quality/ en module-level
        from datetime import datetime, timezone

        from market_data.domain.quality.types import (
            DataQualityReport,
        )

        return DataQualityReport(
            symbol=symbol,
            timeframe="unknown",
            exchange="unknown",
            rows=len(df),
            checked_at=datetime.now(timezone.utc).isoformat(),
            git_hash="null-checker",
            issues=[],
        )


__all__ = [
    "DataQualityCheckerPort",
    "CheckerFactory",
    "NullChecker",
]
