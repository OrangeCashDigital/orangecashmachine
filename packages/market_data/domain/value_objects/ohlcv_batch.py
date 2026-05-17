# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/ohlcv_batch.py
=================================================

DEPRECATED — alias de compatibilidad hacia OHLCVChunk.

Motivo
------
OHLCVBatch como subclase de OHLCVChunk (frozen dataclass) era frágil:
  - frozen=True + herencia dataclass requiere __init__ explícito en Python 3.10+
  - __post_init__ en frozen subclass no se ejecuta correctamente sin super()
  - el DeprecationWarning nunca se emitía en construcción real

Solución
--------
OHLCVBatch es ahora un alias de tipo puro:
    OHLCVBatch = OHLCVChunk

Cualquier isinstance(x, OHLCVBatch) sigue funcionando porque
OHLCVBatch IS OHLCVChunk — son el mismo objeto en memoria.

Migración
---------
    # Antes
    from market_data.domain.value_objects.ohlcv_batch import OHLCVBatch
    chunk = OHLCVBatch(exchange=..., symbol=..., ...)

    # Ahora
    from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk
    chunk = OHLCVChunk(exchange=..., symbol=..., ...)

Este módulo se eliminará en la siguiente release mayor.
"""
from __future__ import annotations

import warnings

from market_data.domain.value_objects.ohlcv_chunk import OHLCVChunk

warnings.warn(
    "market_data.domain.value_objects.ohlcv_batch is deprecated. "
    "Import OHLCVChunk from market_data.domain.value_objects.ohlcv_chunk instead. "
    "ohlcv_batch will be removed in the next major release.",
    DeprecationWarning,
    stacklevel=2,
)

# Alias puro — no subclase. isinstance() y type checks siguen funcionando.
OHLCVBatch = OHLCVChunk

__all__ = ["OHLCVBatch"]
