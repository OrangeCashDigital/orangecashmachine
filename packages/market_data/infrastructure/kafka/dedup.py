# -*- coding: utf-8 -*-
"""
market_data/infrastructure/kafka/dedup.py
==========================================

Filtros de deduplicación para el pipeline Kafka.

Problema
--------
Kafka at-least-once garantiza que un mensaje puede llegar más de
una vez (rebalanceo, retry de producer, replay). Los stream processors
necesitan detectar y descartar duplicados por event_id.

Estrategia L1 + L2
------------------
  SeenFilter           — L1: LRU en memoria, O(1), muere con el proceso
  PersistentSeenFilter — L2: backend externo (Redis SET/TTL o similar),
                          sobrevive reinicios, compartido entre workers
  CompositeSeenFilter  — combina L1 + L2:
                          L1 hit  → duplicado, sin round-trip externo
                          L1 miss → consulta L2, calienta L1 si hay hit

Degradación controlada
----------------------
  CompositeSeenFilter acepta store=None → opera solo con L1.
  PersistentSeenFilter es fail-soft: backend caído → fail-open (no bloquea).

Principios: SRP · DIP · SafeOps · fail-open · KISS · Resiliencia
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Optional, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_DEFAULT_MAX_SIZE: int = 10_000
_DEFAULT_TTL_DAYS: int = 7
_DEDUP_KEY_PREFIX: str = "ocm:kafka:dedup:"


# ---------------------------------------------------------------------------
# DeduplicatonStoreProtocol — contrato del backend externo (DIP)
# ---------------------------------------------------------------------------


@runtime_checkable
class DeduplicationStoreProtocol(Protocol):
    """
    Contrato mínimo para cualquier backend de deduplicación persistente.

    Structural subtyping — no requiere herencia.
    Compatible con RedisCursorStore y cualquier implementación que
    exponga get_raw / set_raw.
    """

    def get_raw(self, key: str) -> Optional[str]:
        """Retorna el valor asociado a key, o None si no existe."""
        ...

    def set_raw(self, key: str, value: str, ttl_secs: int) -> None:
        """Almacena value con el TTL dado en segundos."""
        ...


# ---------------------------------------------------------------------------
# SeenFilter — L1, en memoria, LRU
# ---------------------------------------------------------------------------


class SeenFilter:
    """
    Filtro de deduplicación en memoria (LRU).

    Thread-safety: NO thread-safe. Usar una instancia por worker/loop.
    Para uso cross-worker usar CompositeSeenFilter con backend externo.

    Parámetros
    ----------
    max_size : entradas máximas antes de evictar la más antigua (LRU).
    """

    def __init__(self, max_size: int = _DEFAULT_MAX_SIZE) -> None:
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._max_size: int = max_size

    def is_duplicate(self, event_id: str) -> bool:
        return event_id in self._seen

    def mark_seen(self, event_id: str) -> None:
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
        else:
            self._seen[event_id] = None
            if len(self._seen) > self._max_size:
                self._seen.popitem(last=False)  # evict oldest

    def __len__(self) -> int:
        return len(self._seen)

    def __contains__(self, event_id: str) -> bool:
        return self.is_duplicate(event_id)


# ---------------------------------------------------------------------------
# PersistentSeenFilter — L2, backend externo
# ---------------------------------------------------------------------------


class PersistentSeenFilter:
    """
    Filtro de deduplicación con backend externo (Redis u otro).

    SafeOps: backend caído → fail-open (is_duplicate retorna False,
    mark_seen no lanza). El pipeline continúa — la deduplicación L1
    sigue activa en CompositeSeenFilter.

    Parámetros
    ----------
    store    : implementación de DeduplicationStoreProtocol.
    ttl_days : TTL de cada event_id en el backend.
    """

    def __init__(
        self,
        store: DeduplicationStoreProtocol,
        ttl_days: int = _DEFAULT_TTL_DAYS,
    ) -> None:
        self._store = store
        self._ttl_secs = ttl_days * 86_400

    def is_duplicate(self, event_id: str) -> bool:
        """Fail-open: backend caído → retorna False (no bloquea pipeline)."""
        try:
            return self._store.get_raw(self._key(event_id)) is not None
        except Exception:
            return False

    def mark_seen(self, event_id: str) -> None:
        """SafeOps: loguea silenciosamente si backend falla."""
        try:
            self._store.set_raw(self._key(event_id), "1", self._ttl_secs)
        except Exception:
            pass

    def _key(self, event_id: str) -> str:
        return f"{_DEDUP_KEY_PREFIX}{event_id}"

    def __contains__(self, event_id: str) -> bool:
        return self.is_duplicate(event_id)


# ---------------------------------------------------------------------------
# CompositeSeenFilter — L1 + L2
# ---------------------------------------------------------------------------


class CompositeSeenFilter:
    """
    Filtro compuesto: L1 (memoria) + L2 (backend externo).

    Estrategia de consulta:
      1. L1 hit  → duplicado, cero round-trips externos
      2. L1 miss → consultar L2
         2a. L2 hit  → duplicado, calentar L1 para futuros checks
         2b. L2 miss → evento nuevo

    Degradación controlada:
      store=None → opera solo con L1 (modo memoria puro).
    """

    def __init__(
        self,
        store=None,
        max_size: int = _DEFAULT_MAX_SIZE,
        ttl_days: int = _DEFAULT_TTL_DAYS,
    ) -> None:
        self._l1 = SeenFilter(max_size=max_size)
        self._l2: Optional[PersistentSeenFilter] = (
            PersistentSeenFilter(store=store, ttl_days=ttl_days) if store is not None else None
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


__all__ = [
    "DeduplicationStoreProtocol",
    "SeenFilter",
    "PersistentSeenFilter",
    "CompositeSeenFilter",
]
