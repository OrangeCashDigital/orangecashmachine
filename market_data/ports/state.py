from __future__ import annotations

"""
market_data/ports/state.py
==========================
Puerto de persistencia de cursores de ingesta.

Contrato que infra.state debe cumplir para ser inyectado.
El dominio depende de esta abstracción, nunca de la
implementación concreta (DIP — SOLID).

Función encode_cursor_key
-------------------------
Lógica pura de codificación; pertenece al dominio porque
define la semántica de las claves, no al backend de storage.
Movida desde infra.state.cursor_store para eliminar la
dependencia inversa.
"""

from typing import Optional, Protocol, runtime_checkable


# ── Función pura de dominio ──────────────────────────────────────────────────

def encode_cursor_key(exchange: str, symbol: str, timeframe: str) -> str:
    """Codifica una clave canónica de cursor.

    Pura: sin I/O, sin estado, sin deps externas.
    Puede usarse en dominio, infra y tests sin riesgo de acoplamiento.

    Returns
    -------
    str
        Formato: ``<exchange>:<symbol>:<timeframe>``
        Ejemplo: ``"binance:BTC/USDT:1m"``
    """
    return f"{exchange}:{symbol}:{timeframe}"


# ── Puerto (contrato) ────────────────────────────────────────────────────────

@runtime_checkable
class CursorStorePort(Protocol):
    """Contrato mínimo de persistencia de cursores.

    Cualquier clase que satisfaga esta interfaz estructuralmente
    (duck typing estricto con runtime_checkable) puede inyectarse.
    Implementaciones concretas: infra.state.cursor_store.{Redis,InMemory}CursorStore

    OCP: agregar backends nuevos no modifica este contrato.
    """

    def get(self, key: str) -> Optional[str]:
        """Retorna el cursor almacenado o None si no existe."""
        ...

    def set(self, key: str, value: str) -> None:
        """Persiste o actualiza un cursor."""
        ...

    def delete(self, key: str) -> None:
        """Elimina un cursor. No-op si no existe (idempotente)."""
        ...

    def exists(self, key: str) -> bool:
        """True si el cursor existe en el store."""
        ...
