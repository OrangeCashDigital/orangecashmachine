# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/provenance.py
================================================

Provenance — origen conceptual de hechos sobre identidad de instrumentos.

G1-D11 (ADR-0049) establece una autoridad híbrida con FAIL-CLOSED ante
conflicto. Este módulo es el SSOT de la taxonomía de orígenes y del
registro inmutable que los porta.

Jerarquía de autoridad (G1-D11)
-------------------------------
- OBSERVED_FROM_VENUE : hechos observables propios del mercado, publicados
  por el venue (listing, lifecycle contractual, cambio de símbolo anunciado,
  sucesión/reemplazo declarado). Autoridad primaria sobre su hecho de origen.
- ADMINISTRATIVE_OCM  : estados administrativos propios de la operación de
  OCM (feed habilitado/deshabilitado, cuarentena, bloqueo interno). NO se
  reinterpretan como hechos contractuales del venue.
- DERIVED             : resultado derivado por regla aprobada o cómputo,
  sin autoridad directa de origen.
- UNKNOWN_CONFLICT    : conflicto entre fuentes sin regla de resolución
  aprobada → FAIL-CLOSED (G1-D11). Nunca se elige silenciosamente.

Principios
----------
DDD   — VO puro: inmutable (frozen dataclass), definido por valor
SSOT  — única definición del concepto de provenance en el dominio
DIP   — dominio no depende de infraestructura (solo stdlib)
FAIL-CLOSED — UNKNOWN_CONFLICT forza aborto por diseño, no resolución
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

# ===========================================================================
# ProvenanceSource — taxonomía de origen (G1-D11)
# ===========================================================================


class ProvenanceSource(str, Enum):
    """
    Origen conceptual de una transición/relación/hecho de identidad.

    str-compatible: conversión a su notación canónica para logs/metrics.
    """

    OBSERVED_FROM_VENUE = "observed_from_venue"
    ADMINISTRATIVE_OCM = "administrative_ocm"
    DERIVED = "derived"
    UNKNOWN_CONFLICT = "unknown_conflict"


# ===========================================================================
# Provenance — registro inmutable
# ===========================================================================


@dataclass(frozen=True)
class Provenance:
    """
    Registro inmutable del origen de un hecho de identidad.

    Un hecho sin provenance NO es admisible en el modelo G1: toda
    transición/relación relevante conserva su origen (G1-D11, G1-D13
    regla 5).

    Fields
    ------
    source     : ProvenanceSource — de dónde proviene el hecho.
    observed   : texto libre que documenta la fuente específica
                 (p. ej. "bybit_announcement_2026-03-01") o la evidencia
                 del conflicto cuando source == UNKNOWN_CONFLICT.
    recorded_at: instante UTC en el que OCM registró el hecho. No es
                 el instante del hecho en sí (ese pertenece a la transición
                 o relación que porta este Provenance).
    """

    source: ProvenanceSource
    observed: str = ""
    recorded_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


__all__ = ["ProvenanceSource", "Provenance"]
