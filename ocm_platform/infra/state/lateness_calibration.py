# -*- coding: utf-8 -*-
from __future__ import annotations

"""
infra/state/lateness_calibration.py
=====================================

Persistencia de calibración de lateness por exchange+timeframe.

Responsabilidad
---------------
Leer, escribir y aplicar valores de lateness_ms calibrados empíricamente
desde datos reales de ocm_candle_delay_ms{mode="incremental"}.

Desacoplamiento intencional
---------------------------
Este módulo NO consulta Prometheus directamente — eso es responsabilidad
del job de calibración externo (scripts/calibrate_lateness.py).
Este módulo solo persiste y expone los valores para que overlap_for_timeframe()
los consuma en runtime sin dependencia de Prometheus.

Key schema Redis
----------------
  {env}:lateness_calibration:{exchange_enc}:{timeframe_enc}

Valor
-----
  JSON: {
    "exchange":     str,
    "timeframe":    str,
    "lateness_ms":  int,      # p99 empírico en ms
    "p95_ms":       int,      # p95 empírico en ms (informativo)
    "sample_count": int,      # observaciones usadas
    "calibrated_at": str,     # ISO 8601 UTC
    "window_hours": int,      # ventana de datos usada
    "source":       str,      # "prometheus" | "manual"
  }

TTL
---
  7 días — calibración expira si no se renueva.
  overlap_for_timeframe() usa hardcoded como fallback si expira.

SafeOps
-------
  Todos los métodos capturan excepciones y nunca lanzan al caller.
  Si Redis no está disponible, overlap_for_timeframe() usa hardcoded.
"""

import json
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from ocm_platform.infra.state.gap_store    import GapStorePort
from ocm_platform.infra.state.encoding import encode_redis_key as _encode
from ocm_platform.infra.state.retry    import redis_retry      as _retry

_CALIBRATION_TTL_DAYS = 7
_CALIBRATION_TTL      = _CALIBRATION_TTL_DAYS * 86_400

# Guardrails: evita que un p99 anómalo corrompa el overlap.
# Si el p99 empírico supera el cap, se usa el cap (con warning).
# Si el p99 empírico es menor que el floor, se usa el floor.
_LATENESS_MS_CAP   = 60 * 60_000   # 60 min — cap absoluto
_LATENESS_MS_FLOOR =  1 * 60_000   #  1 min — floor absoluto


def _cal_key(env: str, exchange: str, timeframe: str) -> str:
    return f"{env}:lateness_calibration:{_encode(exchange)}:{_encode(timeframe)}"


class LatenessCalibrationStore:
    """
    Store de calibración de lateness backed by Redis.

    Patrón de uso
    -------------
    store = LatenessCalibrationStore(store=gap_store_port, env=env_str)

    # Desde el job de calibración externo:
    store.set(exchange, timeframe, lateness_ms=p99, p95_ms=p95,
              sample_count=n, window_hours=24)

    # Desde overlap_for_timeframe() en runtime:
    ms = store.get_lateness_ms(exchange, timeframe)
    # None si no hay calibración — usar hardcoded
    """

    def __init__(self, store: GapStorePort, env: str) -> None:
        self._store = store
        self._env   = env

    def set(
        self,
        exchange:     str,
        timeframe:    str,
        lateness_ms:  int,
        p95_ms:       int       = 0,
        sample_count: int       = 0,
        window_hours: int       = 24,
        source:       str       = "prometheus",
    ) -> bool:
        """
        Persiste calibración de lateness. Aplica guardrails antes de escribir.

        Returns True si fue escrito, False si hubo error.
        """
        key = _cal_key(self._env, exchange, timeframe)
        try:
            # Guardrails: protege contra p99 anómalos
            raw_ms = lateness_ms
            lateness_ms = max(_LATENESS_MS_FLOOR, min(_LATENESS_MS_CAP, lateness_ms))
            if lateness_ms != raw_ms:
                logger.warning(
                    "LatenessCalibration: p99 fuera de guardrails | "
                    "exchange={} timeframe={} raw_ms={} clamped_ms={}",
                    exchange, timeframe, raw_ms, lateness_ms,
                )

            payload = json.dumps({
                "exchange":      exchange,
                "timeframe":     timeframe,
                "lateness_ms":   lateness_ms,
                "p95_ms":        p95_ms,
                "sample_count":  sample_count,
                "calibrated_at": datetime.now(timezone.utc).isoformat(),
                "window_hours":  window_hours,
                "source":        source,
            })
            _retry(lambda: self._store.set(key, payload, ex=_CALIBRATION_TTL))
            logger.info(
                "LatenessCalibration: calibración persistida | "
                "exchange={} timeframe={} lateness_ms={} p95_ms={} samples={} source={}",
                exchange, timeframe, lateness_ms, p95_ms, sample_count, source,
            )
            return True
        except Exception as exc:
            logger.warning(
                "LatenessCalibration.set failed (non-critical) | "
                "exchange={} timeframe={} error={}",
                exchange, timeframe, exc,
            )
            return False

    def get_lateness_ms(
        self,
        exchange:  str,
        timeframe: str,
    ) -> Optional[int]:
        """
        Obtiene lateness_ms calibrado para exchange+timeframe.

        Returns int en ms si existe calibración válida, None si no hay
        calibración o expiró — el caller debe usar hardcoded como fallback.
        """
        key = _cal_key(self._env, exchange, timeframe)
        try:
            raw = _retry(lambda: self._store.get(key))
            if raw is None:
                return None
            data = json.loads(raw)
            return int(data["lateness_ms"])
        except Exception as exc:
            logger.warning(
                "LatenessCalibration.get_lateness_ms failed (non-critical) | "
                "exchange={} timeframe={} error={}",
                exchange, timeframe, exc,
            )
            return None

    def get(
        self,
        exchange:  str,
        timeframe: str,
    ) -> Optional[dict]:
        """Obtiene el payload completo de calibración. None si no existe."""
        key = _cal_key(self._env, exchange, timeframe)
        try:
            raw = _retry(lambda: self._store.get(key))
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as exc:
            logger.warning(
                "LatenessCalibration.get failed | exchange={} timeframe={} error={}",
                exchange, timeframe, exc,
            )
            return None

    def delete(self, exchange: str, timeframe: str) -> bool:
        """Elimina calibración (útil para tests o reset manual)."""
        key = _cal_key(self._env, exchange, timeframe)
        try:
            return bool(_retry(lambda: self._store.delete(key)))
        except Exception as exc:
            logger.warning(
                "LatenessCalibration.delete failed | exchange={} timeframe={} error={}",
                exchange, timeframe, exc,
            )
            return False


def build_calibration_store_from_env(
    env: Optional[str] = None,
) -> Optional["LatenessCalibrationStore"]:
    """
    .. deprecated::
        Usar ``infra.state.factories.build_lateness_calibration_store``
        en código nuevo. Shim mantenido por compatibilidad — será eliminado
        en la próxima limpieza de deuda técnica una vez migrados todos los
        callers.
    """
    import warnings
    warnings.warn(
        "build_calibration_store_from_env está deprecada — "
        "usa infra.state.factories.build_lateness_calibration_store",
        DeprecationWarning,
        stacklevel=2,
    )
    from ocm_platform.infra.state.factories import build_lateness_calibration_store
    return build_lateness_calibration_store(env=env)
