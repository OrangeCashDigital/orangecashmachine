from __future__ import annotations

"""
market_data/streaming/dedup.py
================================

SeenFilter — deduplicación de eventos por event_id.

Responsabilidad
---------------
Evitar que el mismo event_id sea procesado más de una vez en una
ventana de tiempo, incluso si el mensaje llega duplicado desde Redis
(at-least-once delivery implica posibles duplicados en reentregas).

Implementación
--------------
Set en memoria con tamaño máximo (LRU implícito via eviction FIFO).
No usa Redis — agregar un round-trip por evento destruye la latencia.
El filtro es local al worker: duplicados entre workers distintos son
responsabilidad de handlers idempotentes (capa de negocio).

Cuándo se limpian entradas
--------------------------
Cuando el set supera max_size, se eliminan las entradas más antiguas
(FIFO). Esto garantiza memoria acotada sin TTL explícito.

Trade-offs
----------
- Pro: cero latencia adicional, sin dependencias externas.
- Con: duplicados entre reinicios del worker no se detectan.
- Con: duplicados entre workers distintos no se detectan.
→ Para esos casos, los handlers deben ser idempotentes por diseño.

Principios: SRP · memoria acotada · zero-latency · SafeOps
"""

from collections import OrderedDict
from typing import Set


_DEFAULT_MAX_SIZE: int = 10_000


class SeenFilter:
    """
    Filtro de deduplicación por event_id en memoria.

    Parámetros
    ----------
    max_size : int
        Máximo de event_ids a recordar. Al superar el límite,
        los más antiguos se eliminan (FIFO).

    Uso
    ---
        f = SeenFilter(max_size=10_000)
        if f.is_duplicate(event.event_id):
            # ya procesado — skip
        else:
            process(event)
            f.mark_seen(event.event_id)
    """

    def __init__(self, max_size: int = _DEFAULT_MAX_SIZE) -> None:
        self._max_size = max_size
        # OrderedDict preserva orden de inserción para eviction FIFO
        self._seen: OrderedDict[str, None] = OrderedDict()

    def is_duplicate(self, event_id: str) -> bool:
        """Retorna True si event_id ya fue procesado."""
        return event_id in self._seen

    def mark_seen(self, event_id: str) -> None:
        """
        Marca event_id como procesado.

        Si el set supera max_size, elimina la entrada más antigua.
        """
        if event_id in self._seen:
            # Ya existe — mover al final para refrescar orden
            self._seen.move_to_end(event_id)
            return
        self._seen[event_id] = None
        if len(self._seen) > self._max_size:
            # Evict el más antiguo (primer elemento)
            self._seen.popitem(last=False)

    def __len__(self) -> int:
        return len(self._seen)

    def __contains__(self, event_id: str) -> bool:
        return self.is_duplicate(event_id)
