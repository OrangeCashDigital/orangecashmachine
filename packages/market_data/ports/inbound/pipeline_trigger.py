"""
market_data/ports/inbound/pipeline_trigger.py
============================================
Puerto de ENTRADA al sistema de market data.

Por qué existe este archivo
---------------------------
ports/ ya tiene puertos de salida bien definidos (storage, exchange, state).
Faltaba formalizar la dirección de entrada: quién puede disparar un pipeline
y con qué contrato.

Beneficio inmediato
-------------------
Dagster assets pueden tipear su retorno como PipelineTriggerPort en lugar
de OHLCVPipeline concreto — el día que exista TradesPipeline o
DerivativesPipeline, el asset de Dagster no cambia (OCP).

OCP: añadir TradesPipeline implementando este port → Dagster no toca.
DIP: Dagster depende de la abstracción, no del pipeline concreto.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Literal, Any

if TYPE_CHECKING:
    # Evitar import circular — solo para type hints
    pass  # no type-only imports needed

PipelineModeStr = Literal["incremental", "backfill", "repair"]


class PipelineTriggerPort(ABC):
    """
    Contrato de entrada para pipelines de ingestion.

    Implementaciones actuales
    -------------------------
    - OHLCVPipeline       (application/pipelines/ohlcv_pipeline.py)
    - DerivativesPipeline (application/pipelines/derivatives_pipeline.py)
    - TradesPipeline      (application/pipelines/trades_pipeline.py)

    Implementar este port requiere solo añadir la herencia:
        class OHLCVPipeline(PipelineTriggerPort):
            async def run(self, mode="incremental") -> Any:
                ...
    """

    @abstractmethod
    async def run(self, mode: PipelineModeStr = "incremental") -> Any:
        """
        Ejecuta el pipeline en el modo indicado.

        Fail-Soft: nunca lanza excepción.
        Retorna PipelineSummary con status="failed" si el run falla internamente.
        """
        ...
