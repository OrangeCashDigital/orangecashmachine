# -*- coding: utf-8 -*-
"""
portfolio/infra/redis_store.py
================================

RedisPositionStore — implementación Redis del PositionStore Protocol.

Responsabilidad
---------------
Persistir posiciones abiertas en Redis con TTL configurable.
Supervivencia a reinicios del proceso — las posiciones sobreviven
si el proceso cae y vuelve a arrancar.

Diseño
------
- Cada posición → Redis Hash bajo clave:  ocm:positions:{exchange}:{order_id}
- Índice de order_ids → Redis Set bajo:   ocm:positions:{exchange}:_index
- TTL: 7 días por defecto (posición abierta no debe vivir más)
- Serialización: JSON — sin pickle, reproducible, inspeccionable con redis-cli

Contrato
--------
Implementa portfolio.ports.PositionStore (Protocol).
OCP: intercambiable con InMemoryPositionStore sin tocar PortfolioService.

SafeOps
-------
- Todas las operaciones bajo try/except — nunca lanzan al caller.
- Errores de conexión logueados con warning, no error (Redis es best-effort).
- Si Redis no está disponible, retorna estado vacío (Fail-Soft).

Principios: SRP · DIP · OCP · SafeOps · KISS · DRY
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from typing import Optional

from loguru import logger

from portfolio.models.position import PositionSnapshot


# ---------------------------------------------------------------------------
# Serialización — SSOT: JSON bidireccional para PositionSnapshot
# ---------------------------------------------------------------------------

def _serialize(pos: PositionSnapshot) -> str:
    """PositionSnapshot → JSON string. Fail-fast ante campos inesperados."""
    return json.dumps({
        "order_id":      pos.order_id,
        "symbol":        pos.symbol,
        "exchange":      pos.exchange,
        "side":          pos.side,
        "entry_price":   pos.entry_price,
        "size_pct":      pos.size_pct,
        "entry_at":      pos.entry_at.isoformat(),
        "current_price": pos.current_price,
    })


def _deserialize(raw: str) -> Optional[PositionSnapshot]:
    """
    JSON string → PositionSnapshot.

    SafeOps: retorna None si el JSON es inválido o faltan campos.
    """
    try:
        data = json.loads(raw)
        return PositionSnapshot(
            order_id      = data["order_id"],
            symbol        = data["symbol"],
            exchange      = data["exchange"],
            side          = data["side"],
            entry_price   = float(data["entry_price"]),
            size_pct      = float(data["size_pct"]),
            entry_at      = datetime.fromisoformat(data["entry_at"]),
            current_price = data.get("current_price"),
        )
    except Exception as exc:
        logger.warning("RedisPositionStore: deserialización fallida | {}", exc)
        return None


# ---------------------------------------------------------------------------
# RedisPositionStore
# ---------------------------------------------------------------------------

class RedisPositionStore:
    """
    Implementación Redis del PositionStore Protocol.

    Usa Redis como backend de persistencia — las posiciones sobreviven
    reinicios del proceso a diferencia de InMemoryPositionStore.

    Parameters
    ----------
    redis_client : redis.Redis — cliente ya construido (DI, no crea la conexión)
    exchange     : str         — prefijo de namespace en Redis
    ttl_seconds  : int         — TTL por posición (default: 7 días)
    """

    _TTL_DEFAULT = 7 * 24 * 3600   # 7 días en segundos

    def __init__(
        self,
        redis_client,               # redis.Redis — tipado débil para no forzar import
        exchange:    str,
        ttl_seconds: int = _TTL_DEFAULT,
    ) -> None:
        self._redis      = redis_client
        self._exchange   = exchange
        self._ttl        = ttl_seconds
        self._lock       = threading.Lock()   # serializa operaciones multi-key
        self._log        = logger.bind(
            component = "RedisPositionStore",
            exchange  = exchange,
        )

    # ------------------------------------------------------------------
    # Keys — SSOT: namespace consistente, inspeccionable con redis-cli
    # ------------------------------------------------------------------

    def _key(self, order_id: str) -> str:
        """Clave de posición individual."""
        return f"ocm:positions:{self._exchange}:{order_id}"

    @property
    def _index_key(self) -> str:
        """Set con todos los order_ids activos para este exchange."""
        return f"ocm:positions:{self._exchange}:_index"

    # ------------------------------------------------------------------
    # PositionStore Protocol
    # ------------------------------------------------------------------

    def save(self, position: PositionSnapshot) -> None:
        """
        Persiste posición en Redis.

        Atomicidad: SET + SADD en pipeline — ambas operaciones o ninguna.
        SafeOps: nunca lanza.
        """
        try:
            raw = _serialize(position)
            key = self._key(position.order_id)
            with self._lock:
                pipe = self._redis.pipeline(transaction=True)
                pipe.set(key, raw, ex=self._ttl)
                pipe.sadd(self._index_key, position.order_id)
                pipe.expire(self._index_key, self._ttl)
                pipe.execute()
            self._log.debug("save | order_id={} symbol={}", position.order_id, position.symbol)
        except Exception as exc:
            self._log.warning("save error | order_id={} {}", position.order_id, exc)

    def delete(self, order_id: str) -> None:
        """
        Elimina posición de Redis.

        Atomicidad: DEL + SREM en pipeline.
        SafeOps: nunca lanza. Idempotente — borrar inexistente es OK.
        """
        try:
            with self._lock:
                pipe = self._redis.pipeline(transaction=True)
                pipe.delete(self._key(order_id))
                pipe.srem(self._index_key, order_id)
                pipe.execute()
            self._log.debug("delete | order_id={}", order_id)
        except Exception as exc:
            self._log.warning("delete error | order_id={} {}", order_id, exc)

    def get(self, order_id: str) -> Optional[PositionSnapshot]:
        """
        Recupera posición por order_id.

        SafeOps: retorna None si no existe o hay error de conexión.
        """
        try:
            raw = self._redis.get(self._key(order_id))
            if raw is None:
                return None
            return _deserialize(raw)
        except Exception as exc:
            self._log.warning("get error | order_id={} {}", order_id, exc)
            return None

    def all(self) -> list[PositionSnapshot]:
        """
        Todas las posiciones abiertas para este exchange.

        Estrategia: leer el índice → GET paralelo vía pipeline.
        Fail-Soft: si un GET individual falla, se omite esa posición.
        SafeOps: retorna lista vacía si Redis no está disponible.
        """
        try:
            order_ids = self._redis.smembers(self._index_key)
            if not order_ids:
                return []

            # Pipeline para GET paralelo — eficiente, una sola RTT
            pipe = self._redis.pipeline(transaction=False)
            for oid in order_ids:
                pipe.get(self._key(oid.decode() if isinstance(oid, bytes) else oid))
            raws = pipe.execute()

            positions = []
            for raw in raws:
                if raw is None:
                    continue
                pos = _deserialize(raw)
                if pos is not None:
                    positions.append(pos)
            return positions
        except Exception as exc:
            self._log.warning("all() error — retornando lista vacía | {}", exc)
            return []

    def clear(self) -> None:
        """
        Elimina todas las posiciones para este exchange.

        Usado en reset de sesión. SafeOps: nunca lanza.
        """
        try:
            order_ids = self._redis.smembers(self._index_key)
            if not order_ids:
                return
            with self._lock:
                pipe = self._redis.pipeline(transaction=True)
                for oid in order_ids:
                    key = self._key(oid.decode() if isinstance(oid, bytes) else oid)
                    pipe.delete(key)
                pipe.delete(self._index_key)
                pipe.execute()
            self._log.info("clear | {} posiciones eliminadas", len(order_ids))
        except Exception as exc:
            self._log.warning("clear error | {}", exc)

    def __repr__(self) -> str:
        try:
            count = self._redis.scard(self._index_key)
        except Exception:
            count = "?"
        return f"RedisPositionStore(exchange={self._exchange!r} positions={count})"


