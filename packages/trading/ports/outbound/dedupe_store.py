# -*- coding: utf-8 -*-
"""
trading/ports/outbound/dedupe_store.py
======================================

DedupeStorePort — contrato mínimo de deduplicación persistente de eventos.

Por qué un port propio (DIP / BC-50)
------------------------------------
BC-50 prohíbe a trading.* importar market_data: un consumer de trading no
puede tiparse contra `market_data.ports.outbound.state.CursorStorePort`.
Este módulo define SOLO el contrato (stdlib + typing) y cualquier store con
`get_raw`/`set_raw` lo satisface estructuralmente — p. ej.
`ocm.runtime.state.cursor_store.{Redis,InMemory}CursorStore`, que el
composition root inyecta. Mismo patrón que `transport.py` (ADR-0004/0016).

Semántica
---------
- `get_raw` devuelve None si la clave no existe (evento no visto).
- `set_raw` persiste el valor con TTL en segundos (0 = sin TTL).

Principios: SRP · DIP · BC-50 · SafeOps
"""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class DedupeStorePort(Protocol):
    """Store clave-valor con TTL para deduplicar eventos ya procesados."""

    def get_raw(self, key: str) -> Optional[str]:
        """Retorna el valor almacenado o None si la clave no existe."""
        ...

    def set_raw(self, key: str, value: str, ttl_seconds: int) -> None:
        """Persiste el valor con TTL en segundos (0 = sin TTL)."""
        ...
