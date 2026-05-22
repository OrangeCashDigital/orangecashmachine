# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/quality_pipeline.py
===============================================

Puerto OUTBOUND: contrato del pipeline de calidad de datos OHLCV.

Responsabilidad
---------------
Declarar el contrato mínimo que cualquier implementación del quality gate
debe cumplir para ser inyectada en PipelineContext.quality via composition root.

Implementación canónica
-----------------------
market_data.application.quality.pipeline.QualityPipeline

Por qué existe este puerto
--------------------------
PipelineContext.quality era Any — acoplado implícitamente a QualityPipeline
(clase concreta de application/). Con este puerto, runtime.py depende de
una abstracción y QualityPipeline satisface estructuralmente el contrato
sin herencia explícita (duck typing via runtime_checkable).

Contrato semántico
------------------
check() evalúa un DataFrame Silver y retorna la decisión de calidad.
Es la única operación que BackfillStrategy e IncrementalStrategy necesitan
del quality gate — ISP: no exponemos métodos de configuración interna.

Decisión de diseño — sync vs async
-----------------------------------
QualityPipeline.check() es síncrono (CPU-bound: estadísticas, scoring).
El port refleja la implementación real — no añadir async sin necesidad.

Principios
----------
DIP   — runtime.py depende de este port, no de QualityPipeline directamente
ISP   — solo el método que las strategies usan (check)
SRP   — el port declara el contrato; QualityPipeline implementa la lógica
OCP   — nuevas implementaciones (mock, A/B testing) no modifican este port
SSOT  — única fuente de verdad del contrato del quality gate
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

import pandas as pd

if TYPE_CHECKING:
    # Solo para type hints — evita importar application/ desde ports/
    # QualityPipelineResult vive en application/quality/pipeline.py
    from market_data.application.quality.pipeline import QualityPipelineResult


@runtime_checkable
class QualityPipelinePort(Protocol):
    """
    Contrato del quality gate de datos OHLCV.

    Implementación canónica
    -----------------------
    market_data.application.quality.pipeline.QualityPipeline

    Implementación nula (tests)
    ---------------------------
    Cualquier objeto con check() — duck typing. Ver NullQualityPipeline abajo.

    SafeOps
    -------
    check() nunca lanza — retorna QualityPipelineResult con decision="reject"
    ante errores internos. El caller (BackfillStrategy) solo lee .accepted.
    """

    def check(
        self,
        df: pd.DataFrame,
        exchange: str,
        symbol: str,
        timeframe: str,
        market_type: str = "spot",
        rows_removed: int = 0,
    ) -> "QualityPipelineResult":
        """
        Evalúa la calidad de un DataFrame Silver.

        Parameters
        ----------
        df          : DataFrame OHLCV post-transformación (Silver layer).
        exchange    : Identificador del exchange.
        symbol      : Par canónico ("BTC/USDT").
        timeframe   : Resolución canónica ("1m", "1h", …).
        market_type : "spot" | "futures".
        rows_removed: Velas eliminadas upstream por el transformer.
                      Descuenta gaps pipeline-induced (no son dato faltante).

        Returns
        -------
        QualityPipelineResult con campos:
          .accepted  : bool — True si el DataFrame pasa el quality gate.
          .decision  : str  — "accept" | "reject" | "warn".
          .score     : float — 0.0–1.0 (1.0 = calidad perfecta).
          .report    : DataQualityReport con métricas detalladas.

        SafeOps: nunca lanza — decision="reject" ante error interno.
        """
        ...


class NullQualityPipeline:
    """
        Implementación nula de QualityPipelinePort.

        Uso: tests unitarios que no quieren testear el quality gate.
        Acepta todos los DataFrames incondicionalmente.

        Por qué accepted=True y no False
        ----------------------------------
        False rechazaría todos los chunks en tests de happy path,
        activando el código de skip en las strategies bajo test.
        Para testear rechazos de calidad, usar un
    mock explícito.
    """

    def check(
        self,
        df: pd.DataFrame,
        exchange: str,
        symbol: str,
        timeframe: str,
        market_type: str = "spot",
        rows_removed: int = 0,
    ) -> "QualityPipelineResult":
        # Import local — evita ciclo ports/→application/ en module-level
        from market_data.application.quality.pipeline import QualityPipelineResult  # noqa: PLC0415

        return QualityPipelineResult(accepted=True, decision="accept", score=1.0)


__all__ = ["QualityPipelinePort", "NullQualityPipeline"]
