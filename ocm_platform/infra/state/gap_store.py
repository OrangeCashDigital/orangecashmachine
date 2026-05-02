# -*- coding: utf-8 -*-
"""
ocm_platform/infra/state/gap_store.py
=======================================

GapStorePort  — contrato mínimo de almacenamiento para GapRegistry.
RedisGapStore — implementación concreta sobre redis.Redis.

Por qué existe este módulo
--------------------------
GapRegistry necesita ops raw de Redis (get/set/setex/delete/exists/ttl/
scan/pipeline) porque sus claves son compuestas y su semántica es propia
(gaps con TTL, contadores de intentos, pipeline atómico).

RedisCursorStore gestiona cursores (exchange, symbol, timeframe) → int.
Son bounded contexts distintos — no comparten la misma abstracción.

DIP: GapRegistry depende de GapStorePort, no de redis.Redis ni de
RedisCursorStore. Cambiar backend → solo cambiar la implementación.

OCP: InMemoryGapStore para tests sin Redis real.
"""
from __future__ import annotations

from typing import Any, List, Optional, Protocol, runtime_checkable


# ── Protocolo de pipeline ────────────────────────────────────────────────────

@runtime_checkable
class PipelinePort(Protocol):
    """
    Contrato mínimo de pipeline para GapRegistry.

    GapRegistry usa exclusivamente get(), set() y execute() sobre
    el pipeline — estas tres operaciones forman el contrato verificable
    en tiempo de análisis estático.

    DIP: GapRegistry depende de PipelinePort, no de redis.Pipeline
    ni de _InMemoryPipeline directamente.
    """

    def get(self, key: str) -> "PipelinePort": ...
    def set(self, key: str, value: Any, ex: Optional[int] = None) -> "PipelinePort": ...
    def execute(self) -> List[Any]: ...


# ── Protocolo de store ───────────────────────────────────────────────────────

@runtime_checkable
class GapStorePort(Protocol):
    """Contrato mínimo de almacenamiento raw para GapRegistry."""

    def get(self, key: str) -> Optional[bytes]: ...
    def set(self, key: str, value: Any, ex: Optional[int] = None) -> None: ...
    def setex(self, key: str, ttl: int, value: Any) -> None: ...
    def delete(self, key: str) -> bool: ...
    def exists(self, key: str) -> bool: ...
    def ttl(self, key: str) -> int: ...
    def scan(self, cursor: int, match: str, count: int) -> tuple: ...
    def pipeline(self) -> PipelinePort: ...


# ── Implementación Redis ─────────────────────────────────────────────────────

class RedisGapStore:
    """
    Wrapper fino sobre redis.Redis que satisface GapStorePort.

    Recibe un redis.Redis ya construido (DI).
    No crea conexiones — esa responsabilidad es de factories.py.
    """

    def __init__(self, client: Any) -> None:
        # Tipado débil (Any) para no forzar import de redis en domain.
        # El tipo real es redis.Redis o cualquier objeto compatible.
        self._client = client

    def get(self, key: str) -> Optional[bytes]:
        return self._client.get(key)

    def set(self, key: str, value: Any, ex: Optional[int] = None) -> None:
        self._client.set(key, value, ex=ex)

    def setex(self, key: str, ttl: int, value: Any) -> None:
        self._client.setex(key, ttl, value)

    def delete(self, key: str) -> bool:
        return bool(self._client.delete(key))

    def exists(self, key: str) -> bool:
        return bool(self._client.exists(key))

    def ttl(self, key: str) -> int:
        return int(self._client.ttl(key))

    def scan(self, cursor: int, match: str, count: int) -> tuple:
        return self._client.scan(cursor=cursor, match=match, count=count)

    def pipeline(self) -> PipelinePort:
        return self._client.pipeline()  # type: ignore[return-value]  # redis.Pipeline satisface PipelinePort estructuralmente


# ── Implementación en memoria (tests sin Redis) ──────────────────────────────

class InMemoryGapStore:
    """
    Implementación in-memory de GapStorePort para tests.

    No require Redis. Semántica simplificada:
    - TTL no expira (test unitario — no necesita reloj real).
    - pipeline() retorna un objeto que acumula y ejecuta ops en memoria.
    """

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._ttls: dict[str, int] = {}

    def get(self, key: str) -> Optional[bytes]:
        val = self._data.get(key)
        return val.encode() if isinstance(val, str) else val

    def set(self, key: str, value: Any, ex: Optional[int] = None) -> None:
        self._data[key] = value
        if ex is not None:
            self._ttls[key] = ex

    def setex(self, key: str, ttl: int, value: Any) -> None:
        self._data[key] = value
        self._ttls[key] = ttl

    def delete(self, key: str) -> bool:
        existed = key in self._data
        self._data.pop(key, None)
        self._ttls.pop(key, None)
        return existed

    def exists(self, key: str) -> bool:
        return key in self._data

    def ttl(self, key: str) -> int:
        return self._ttls.get(key, -1)

    def scan(self, cursor: int, match: str, count: int) -> tuple:
        import fnmatch
        keys = [k for k in self._data if fnmatch.fnmatch(k, match.replace("*", "?*"))]
        return (0, keys)

    def pipeline(self) -> PipelinePort:
        return _InMemoryPipeline(self)


class _InMemoryPipeline:
    """Pipeline mínimo para InMemoryGapStore.

    Soporta get() y set() — las únicas ops que GapRegistry usa
    sobre el pipeline (list_pending y operaciones atómicas).
    """

    def __init__(self, store: InMemoryGapStore) -> None:
        self._store = store
        self._cmds: list = []

    def get(self, key: str) -> "PipelinePort":
        self._cmds.append(("get", key))
        return self

    def set(self, key: str, value: Any, ex: Optional[int] = None) -> "PipelinePort":
        self._cmds.append(("set", key, value, ex))
        return self

    def execute(self) -> list:
        results = []
        for cmd in self._cmds:
            if cmd[0] == "get":
                results.append(self._store.get(cmd[1]))
            elif cmd[0] == "set":
                self._store.set(cmd[1], cmd[2], ex=cmd[3])
                results.append(True)
        self._cmds.clear()
        return results
