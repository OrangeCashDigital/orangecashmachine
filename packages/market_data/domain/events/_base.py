"""
market_data.domain.events._base
=================================
Clase base mínima para todos los eventos de dominio.

Por qué existe este módulo
--------------------------
BC-11 establece que los módulos de datos crudos (level-0) no pueden
importar módulos derivados (OHLCV, candle, ingestion).

DomainEvent es base compartida por TODOS los eventos — tanto crudos
(orderbook_events, replay_events, trade_events) como derivados (ingestion).
Vivir en ingestion.py crea una dependencia upward prohibida por BC-11.

Este módulo es neutral: importa solo stdlib.
Puede ser importado por cualquier capa sin violar ningún contrato.

Principios
----------
- SSOT  : única definición de DomainEvent en todo el bounded context
- KISS  : clase mínima — solo los campos que todo evento necesita
- DRY   : elimina el import cruzado entre level-0 y level-derived
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class DomainEvent:
    """
    Raíz común de todos los domain events.

    event_id    : UUID v4 — idempotencia y deduplicación downstream
    occurred_at : ISO-8601 UTC del momento de creación del evento
    """
    event_id:    str = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


__all__ = ["DomainEvent"]
