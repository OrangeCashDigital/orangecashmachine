# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/quality_pipeline.py
===============================================

Puerto OUTBOUND: contrato del quality gate OHLCV.

Responsabilidad
---------------
Definir QualityPipelinePort — el contrato mínimo que OHLCVPipeline
y las strategies necesitan del quality gate.

QualityPipelineResult vive aquí (SSOT) porque es un tipo de contrato
compartido entre ports/ y application/ — no lógica de aplicación.
application/quality/pipeline.py lo re-exporta desde aquí.

Dependencias permitidas
-----------------------
Solo domain/ — BC-04 garantizado.
Cero imports de application/, adapters/, infrastructure/.

Implementación canónica
-----------------------
market_data.application.quality.pipeline.QualityPipeline

No-op (tests)
-------------
NullQualityPipeline — acepta todos los DataFrames incondicionalmente.

Principios: DIP · ISP · SSOT · runtime_checkable · SafeOps
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

import pandas as pd

from market_data.domain.entities import DataTier

# =========================================================================== #
# Result — SSOT del tipo de retorno del quality gate                          #
# =========================================================================== #


@dataclass(frozen=True)
class QualityPipelineResult:
    """
    Resultado del quality gate sobre un DataFrame Silver.

    Tipo de contrato compartido entre ports/ y application/.
    SSOT: definido aquí; application/quality/pipeline.py lo re-exporta.

    Attributes
    ----------
    accepted  : True si el DataFrame supera el quality gate (decision != "reject").
    decision  : "accept" | "flag" | "reject" — decisión de política.
    score     : Score de calidad [0.0, 1.0]. 1.0 = perfecto.
    tier      : DataTier resultante. None en modo degradado (sin policy).
    """

    accepted: bool
    decision: str
    score: float
    tier: DataTier | None = field(default=None)


# =========================================================================== #
# Port                                                                        #
# =========================================================================== #


@runtime_checkable
class QualityPipelinePort(Protocol):
    """
    Contrato mínimo del quality gate OHLCV.

    Implementación canónica
    -----------------------
    market_data.application.quality.pipeline.QualityPipeline

    Contrato semántico
    ------------------
    check() evalúa un DataFrame Silver y retorna QualityPipelineResult.
    Nunca lanza — SafeOps: errores internos se loguean y retornan
    QualityPipelineResult con accepted=False en modo degradado.

    DIP
    ---
    OHLCVPipeline y las strategies dependen de este port,
    nunca de QualityPipeline concreto.

    ISP
    ---
    Un solo método — solo lo que las strategies necesitan.
    """

    def check(
        self,
        df: pd.DataFrame,
        exchange: str,
        symbol: str,
        timeframe: str,
        market_type: str = "spot",
        rows_removed: int = 0,
    ) -> QualityPipelineResult:
        """
        Evalúa la calidad de un DataFrame Silver.

        Parameters
        ----------
        df            : DataFrame Silver con columnas OHLCV canónicas.
        exchange      : Identificador del exchange ("bybit", "kucoin", …).
        symbol        : Par canónico ("BTC/USDT").
        timeframe     : Resolución canónica ("1m", "1h", …).
        market_type   : "spot" | "futures" | "perp".
        rows_removed  : Velas CORRUPT eliminadas upstream por el transformer.
                        Se descuentan del gap scan para evitar falsos positivos.

        Returns
        -------
        QualityPipelineResult con accepted, decision, score y tier.
        """
        ...


# =========================================================================== #
# Null Object                                                                  #
# =========================================================================== #


class NullQualityPipeline:
    """
    Implementación nula de QualityPipelinePort.

    Uso: tests unitarios que no necesitan testear el quality gate.
    Acepta todos los DataFrames incondicionalmente.

    Por qué accepted=True y no False
    ---------------------------------
    False rechazaría todos los chunks en tests de happy path,
    activando el código de skip en las strategies bajo test.
    Para testear rechazos de calidad, usar un mock explícito.

    SafeOps: nunca lanza. Siempre retorna resultado válido.
    """

    def check(
        self,
        df: pd.DataFrame,
        exchange: str,
        symbol: str,
        timeframe: str,
        market_type: str = "spot",
        rows_removed: int = 0,
    ) -> QualityPipelineResult:
        return QualityPipelineResult(
            accepted=True,
            decision="accept",
            score=1.0,
            tier=DataTier.CLEAN,
        )


__all__ = ["QualityPipelineResult", "QualityPipelinePort", "NullQualityPipeline"]
