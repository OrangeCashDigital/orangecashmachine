# -*- coding: utf-8 -*-
from __future__ import annotations

"""
infra/state/gap_registry.py
============================

Registro persistente de gaps detectados en Silver.

Responsabilidad
---------------
Escribir, leer, borrar y listar gaps pendientes de reparación en Redis.
Sin lógica de negocio — solo persistencia y consulta.

Key schema
----------
  {env}:gap:{exchange_enc}:{symbol_enc}:{timeframe_enc}:{start_ms}

Valor
-----
  JSON: {
    "exchange":    str,
    "symbol":      str,
    "timeframe":   str,
    "start_ms":    int,
    "end_ms":      int,
    "expected":    int,        # velas esperadas en el gap
    "gap_seconds": float,
    "detected_at": str,        # ISO 8601 UTC
    "source":      str,        # "validate" | "repair"
    "repair_attempts": int,    # incrementado por RepairStrategy
  }

TTL
---
  90 días — gaps no reparados expiran solos.
  Un gap reparado se borra explícitamente.

SafeOps
-------
  Todos los métodos capturan excepciones y nunca lanzan al caller.
  Si Redis no está disponible, el registry degrada silenciosamente.
"""

import json
import time
from datetime import datetime, timezone
from typing import List, Optional

from loguru import logger

from infra.state.cursor_store import _encode, _decode, _retry, RedisCursorStore

_GAP_TTL_DAYS   = 90
_GAP_TTL        = _GAP_TTL_DAYS * 86_400
_SCAN_COUNT     = 100


def _gap_key(env: str, exchange: str, symbol: str, timeframe: str, start_ms: int) -> str:
    return (
        f"{env}:gap:{_encode(exchange)}:{_encode(symbol)}"
        f":{_encode(timeframe)}:{start_ms}"
    )


def _gap_prefix(env: str, exchange: str = "") -> str:
    if exchange:
        return f"{env}:gap:{_encode(exchange)}:"
    return f"{env}:gap:"


class GapRegistry:
    """
    Registry de gaps pendientes backed by Redis.

    Patrón de uso
    -------------
    registry = GapRegistry(cursor_store)

    # Al detectar un gap (validate_silver):
    registry.register(exchange, symbol, timeframe, start_ms, end_ms, expected, gap_seconds)

    # Al sanar un gap (repair):
    registry.mark_healed(exchange, symbol, timeframe, start_ms)

    # Al intentar reparar (para tracking de intentos):
    registry.increment_attempts(exchange, symbol, timeframe, start_ms)

    # Para consultar gaps pendientes:
    gaps = registry.list_pending(exchange)
    """

    def __init__(self, store: RedisCursorStore) -> None:
        self._store = store
        self._env   = store._env

    # ----------------------------------------------------------
    # Write
    # ----------------------------------------------------------

    def register(
        self,
        exchange:    str,
        symbol:      str,
        timeframe:   str,
        start_ms:    int,
        end_ms:      int,
        expected:    int,
        gap_seconds: float,
        source:      str = "validate",
    ) -> bool:
        """
        Registra un gap detectado. Idempotente — si ya existe no sobreescribe
        repair_attempts ni detected_at originales.

        Returns True si fue escrito, False si ya existía o hubo error.
        """
        key = _gap_key(self._env, exchange, symbol, timeframe, start_ms)
        try:
            existing = _retry(lambda: self._store._client.get(key))
            if existing is not None:
                logger.debug(
                    "GapRegistry: gap ya registrado | key={}", key
                )
                return False

            payload = json.dumps({
                "exchange":        exchange,
                "symbol":          symbol,
                "timeframe":       timeframe,
                "start_ms":        start_ms,
                "end_ms":          end_ms,
                "expected":        expected,
                "gap_seconds":     gap_seconds,
                "detected_at":     datetime.now(timezone.utc).isoformat(),
                "source":          source,
                "repair_attempts": 0,
            })
            _retry(lambda: self._store._client.set(key, payload, ex=_GAP_TTL))
            logger.info(
                "GapRegistry: gap registrado | exchange={} symbol={} "
                "timeframe={} start_ms={} expected={}",
                exchange, symbol, timeframe, start_ms, expected,
            )
            return True
        except Exception as exc:
            logger.warning(
                "GapRegistry.register failed (non-critical) | key={} error={}", key, exc
            )
            return False

    def mark_healed(
        self,
        exchange:  str,
        symbol:    str,
        timeframe: str,
        start_ms:  int,
    ) -> bool:
        """
        Borra el gap del registry. Llamar cuando healed=True en repair.

        Returns True si existía y fue borrado.
        """
        key = _gap_key(self._env, exchange, symbol, timeframe, start_ms)
        try:
            deleted = bool(_retry(lambda: self._store._client.delete(key)))
            if deleted:
                logger.info(
                    "GapRegistry: gap sanado — eliminado | exchange={} "
                    "symbol={} timeframe={} start_ms={}",
                    exchange, symbol, timeframe, start_ms,
                )
            return deleted
        except Exception as exc:
            logger.warning(
                "GapRegistry.mark_healed failed (non-critical) | key={} error={}", key, exc
            )
            return False

    def increment_attempts(
        self,
        exchange:  str,
        symbol:    str,
        timeframe: str,
        start_ms:  int,
    ) -> int:
        """
        Incrementa repair_attempts en el JSON del gap.
        Retorna el nuevo valor, o -1 si no existe o hubo error.
        """
        key = _gap_key(self._env, exchange, symbol, timeframe, start_ms)
        try:
            raw = _retry(lambda: self._store._client.get(key))
            if raw is None:
                return -1
            data = json.loads(raw)
            data["repair_attempts"] = data.get("repair_attempts", 0) + 1
            ttl = _retry(lambda: self._store._client.ttl(key))
            ttl = ttl if ttl > 0 else _GAP_TTL
            _retry(lambda: self._store._client.set(key, json.dumps(data), ex=ttl))
            return data["repair_attempts"]
        except Exception as exc:
            logger.warning(
                "GapRegistry.increment_attempts failed | key={} error={}", key, exc
            )
            return -1

    # ----------------------------------------------------------
    # Read
    # ----------------------------------------------------------

    def list_pending(self, exchange: str = "") -> List[dict]:
        """
        Lista todos los gaps pendientes. Si exchange="", devuelve todos.

        Returns lista de dicts con el payload JSON de cada gap.
        """
        prefix  = _gap_prefix(self._env, exchange)
        pattern = prefix + "*"
        results = []
        try:
            cursor = 0
            while True:
                scan_result = self._store._client.scan(
                    cursor=cursor, match=pattern, count=_SCAN_COUNT
                )
                cursor, keys = scan_result[0], scan_result[1]
                if keys:
                    pipe   = self._store._client.pipeline()
                    for k in keys:
                        pipe.get(k)
                    values = pipe.execute()
                    for raw in values:
                        if raw:
                            try:
                                results.append(json.loads(raw))
                            except Exception:
                                pass
                if cursor == 0:
                    break
            logger.debug(
                "GapRegistry.list_pending | exchange={} found={}",
                exchange or "*", len(results),
            )
            return results
        except Exception as exc:
            logger.warning(
                "GapRegistry.list_pending failed | exchange={} error={}", exchange, exc
            )
            return []

    def count_pending(self, exchange: str = "") -> int:
        """Conteo rápido de gaps pendientes sin cargar payloads."""
        prefix  = _gap_prefix(self._env, exchange)
        pattern = prefix + "*"
        count   = 0
        try:
            cursor = 0
            while True:
                scan_result = self._store._client.scan(
                    cursor=cursor, match=pattern, count=_SCAN_COUNT
                )
                cursor, keys = scan_result[0], scan_result[1]
                count += len(keys)
                if cursor == 0:
                    break
            return count
        except Exception as exc:
            logger.warning(
                "GapRegistry.count_pending failed | exchange={} error={}", exchange, exc
            )
            return 0

    def get(
        self,
        exchange:  str,
        symbol:    str,
        timeframe: str,
        start_ms:  int,
    ) -> Optional[dict]:
        """Obtiene un gap específico por clave. None si no existe."""
        key = _gap_key(self._env, exchange, symbol, timeframe, start_ms)
        try:
            raw = _retry(lambda: self._store._client.get(key))
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as exc:
            logger.warning(
                "GapRegistry.get failed | key={} error={}", key, exc
            )
            return None


def build_gap_registry_from_env(env: Optional[str] = None) -> Optional[GapRegistry]:
    """
    Factory: construye GapRegistry desde env.
    Retorna None si Redis no está disponible — el caller decide si degradar.
    """
    try:
        from infra.state.cursor_store import build_cursor_store_from_env
        store = build_cursor_store_from_env(env=env)
        if not store.is_healthy():
            logger.warning("GapRegistry: Redis no disponible — registry deshabilitado")
            return None
        return GapRegistry(store)
    except Exception as exc:
        logger.warning("GapRegistry: no se pudo inicializar | error={}", exc)
        return None
