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

from __future__ import annotations

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

    # ── API extendida — alias sobre get/set para callers legacy ──────────────
    # Mantiene OCP: no rompe implementaciones que ya satisfacen el Port mínimo.
    # Las implementaciones concretas PUEDEN sobreescribir para añadir TTL real.

    def get_raw(self, key: str) -> Optional[str]:
        """
        Alias de get() para callers que usan la API extendida.

        Semántica idéntica a get(): retorna el valor o None.
        El prefijo _raw indica que el valor no está deserializado.
        """
        return self.get(key)

    def set_raw(self, key: str, value: str, ttl_s: int = 0) -> None:
        """
        Alias de set() para callers que pasan TTL.

        ttl_s es ignorado en el Port base — las implementaciones concretas
        que soportan TTL (Redis) deben sobreescribir este método.
        Fail-soft: callers no deben depender del TTL para correctitud.
        """
        self.set(key, value)

    def update(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        ts_ms: int,
    ) -> None:
        """
        Persiste el último timestamp procesado usando la clave canónica.

        Shorthand semántico para:
            self.set(encode_cursor_key(exchange, symbol, timeframe), str(ts_ms))

        Usado por IncrementalStrategy para actualizar el cursor después de
        cada batch exitoso.
        """
        self.set(encode_cursor_key(exchange, symbol, timeframe), str(ts_ms))


# ── Puerto async de cursores (fetchers) ──────────────────────────────────────


@runtime_checkable
class AsyncCursorStorePort(Protocol):
    """Contrato async de persistencia de cursores para fetchers de ingesta.

    API semántica de 3 argumentos — exchange, symbol, timeframe — que refleja
    el dominio de mercado directamente sin requerir que el caller construya
    la clave manualmente (encode_cursor_key queda encapsulado en la impl).

    Implementaciones concretas (ocm.runtime.state.cursor_store):
        - RedisCursorStore    — producción (Redis con TTL configurable)
        - InMemoryCursorStore — tests y modos sin Redis

    Diferencia con CursorStorePort
    --------------------------------
    CursorStorePort: API de clave plana (get(key: str)) para callers que
        construyen la clave explícitamente (backfill, iceberg_storage).
    AsyncCursorStorePort: API semántica async para fetchers de ingesta.

    Principios: ISP · DIP · OCP · SSOT
    """

    async def get(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> Optional[int]:
        """Retorna el último timestamp procesado (ms) o None si no existe."""
        ...

    async def update(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        timestamp_ms: int,
    ) -> bool:
        """Persiste el último timestamp procesado. Retorna True si exitoso."""
        ...

    def get_raw(self, key: str) -> Optional[str]:
        """Acceso de clave plana para compatibilidad con callers legacy."""
        ...

    def set_raw(self, key: str, value: str, ttl_seconds: int = 0) -> None:
        """Escritura de clave plana para compatibilidad con callers legacy."""
        ...
