from __future__ import annotations

"""
market_data/streaming/dedup.py
================================

Deduplicación de eventos por event_id — dos capas.

Arquitectura de dos capas
--------------------------
L1 — SeenFilter (memoria):
  OrderedDict LRU. O(1), cero latencia. Muere con el proceso.
  Cubre duplicados dentro de una sesión del worker.

L2 — PersistentSeenFilter (Redis):
  SET/GET con TTL. Sobrevive a reinicios y es compartido entre
  workers. Un round-trip Redis por evento nuevo (no duplicado),
  cero round-trip para duplicados detectados en L1.

Estrategia combinada (CompositeSeenFilter):
  1. Chequear L1 → duplicado en memoria → skip sin tocar Redis
  2. Chequear L2 → duplicado en Redis → skip, marcar L1
  3. Nuevo evento → procesar → marcar L1 + L2

Trade-offs
----------
- L1 solo: cero latencia, no sobrevive restarts, no entre workers.
- L2 solo: sobrevive restarts + entre workers, +1 round-trip/evento.
- Composite: mejor de ambos — L1 amortigua el 99% del tráfico normal,
  L2 cubre el caso de restart o reentrega cross-worker.

Principios: SRP · memoria acotada · degradación controlada · SafeOps
"""

from collections import OrderedDict
from typing import Optional, Protocol, runtime_checkable


_DEFAULT_MAX_SIZE: int = 10_000
_DEFAULT_TTL_DAYS: int = 7
_DEDUP_KEY_PREFIX: str = "ocm:dedup:"


# --------------------------------------------------
# Protocolo — contrato mínimo de cualquier filtro
# --------------------------------------------------

@runtime_checkable
class DedupFilter(Protocol):
    def is_duplicate(self, event_id: str) -> bool: ...
    def mark_seen(self, event_id: str) -> None: ...


# --------------------------------------------------
# L1 — SeenFilter (memoria)
# --------------------------------------------------

class SeenFilter:
    """
    Filtro de deduplicación en memoria. LRU con eviction FIFO.

    Parámetros
    ----------
    max_size : int — máximo de event_ids en memoria.
    """

    def __init__(self, max_size: int = _DEFAULT_MAX_SIZE) -> None:
        self._max_size = max_size
        self._seen: OrderedDict[str, None] = OrderedDict()

    def is_duplicate(self, event_id: str) -> bool:
        return event_id in self._seen

    def mark_seen(self, event_id: str) -> None:
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
            return
        self._seen[event_id] = None
        if len(self._seen) > self._max_size:
            self._seen.popitem(last=False)

    def __len__(self) -> int:
        return len(self._seen)

    def __contains__(self, event_id: str) -> bool:
        return self.is_duplicate(event_id)


# --------------------------------------------------
# L2 — PersistentSeenFilter (Redis)
# --------------------------------------------------

class PersistentSeenFilter:
    """
    Filtro de deduplicación persistente via Redis SET/GET con TTL.

    Sobrevive a reinicios del worker y es compartido entre múltiples
    consumers del mismo grupo.

    Parámetros
    ----------
    store    : RedisCursorStore — provee get_raw/set_raw (inyectado).
    ttl_days : int — TTL en días para cada event_id en Redis.

    SafeOps: si Redis falla, is_duplicate retorna False (fail-open)
    y mark_seen loguea el error. El pipeline nunca se detiene por
    un fallo del filtro persistente.
    """

    def __init__(
        self,
        store,
        ttl_days: int = _DEFAULT_TTL_DAYS,
    ) -> None:
        self._store    = store
        self._ttl_secs = ttl_days * 86_400

    def is_duplicate(self, event_id: str) -> bool:
        """SafeOps: retorna False si Redis falla (fail-open)."""
        try:
            return self._store.get_raw(self._key(event_id)) is not None
        except Exception:
            return False

    def mark_seen(self, event_id: str) -> None:
        """SafeOps: loguea y continúa si Redis falla."""
        try:
            self._store.set_raw(self._key(event_id), "1", self._ttl_secs)
        except Exception:
            pass

    def _key(self, event_id: str) -> str:
        return f"{_DEDUP_KEY_PREFIX}{event_id}"

    def __contains__(self, event_id: str) -> bool:
        return self.is_duplicate(event_id)


# --------------------------------------------------
# Composite — L1 + L2
# --------------------------------------------------

class CompositeSeenFilter:
    """
    Filtro compuesto: L1 (memoria) + L2 (Redis persistente).

    Estrategia de consulta:
      1. L1 hit  → duplicado, cero round-trips Redis
      2. L1 miss → consultar L2
         2a. L2 hit  → duplicado, marcar L1 para futuros checks
         2b. L2 miss → nuevo evento

    Degradación controlada:
    Si store es None, opera solo con L1 (modo memoria).
    """

    def __init__(
        self,
        store         = None,
        max_size: int = _DEFAULT_MAX_SIZE,
        ttl_days: int = _DEFAULT_TTL_DAYS,
    ) -> None:
        self._l1 = SeenFilter(max_size=max_size)
        self._l2: Optional[PersistentSeenFilter] = (
            PersistentSeenFilter(store=store, ttl_days=ttl_days)
            if store is not None
            else None
        )

    def is_duplicate(self, event_id: str) -> bool:
        if self._l1.is_duplicate(event_id):
            return True
        if self._l2 is not None and self._l2.is_duplicate(event_id):
            self._l1.mark_seen(event_id)  # warm up L1
            return True
        return False

    def mark_seen(self, event_id: str) -> None:
        self._l1.mark_seen(event_id)
        if self._l2 is not None:
            self._l2.mark_seen(event_id)

    def __len__(self) -> int:
        return len(self._l1)

    def __contains__(self, event_id: str) -> bool:
        return self.is_duplicate(event_id)
