"""
services/exchange/throttle.py
==============================
AdaptiveThrottle — concurrencia dinámica por pipeline (exchange:market:dataset).

Arquitectura de keys
--------------------
Key = "exchange_id:market_type:dataset"
Ejemplos:
  "bybit:spot:ohlcv"
  "bybit:swap:ohlcv"
  "kucoin:spot:trades"

Spot y futures del mismo exchange tienen rate limits y patrones de
degradación distintos. Un pipeline lento de futures no penaliza la
concurrencia del spot y viceversa.
"""
from __future__ import annotations

import time
from typing import Dict, Optional

from loguru import logger

__all__ = [
    "AdaptiveThrottle",
    "_THROTTLES",
    "get_or_create_throttle",
    "get_throttle_state",
]


class AdaptiveThrottle:
    """
    Throttle adaptivo por pipeline (exchange:market_type:dataset).

    Señales de scale_down (cualquiera dispara):
    - error_rate  > error_threshold  (429 pesa doble, timeout normal, network leve)
    - p95_latency > latency_target_ms

    Señales de scale_up (ambas condiciones):
    - error_rate  < error_threshold / 2
    - p95_latency < latency_target_ms * 0.7

    Cooldown: no escala más de una vez cada cooldown_seconds.
    """

    _RATE_LIMIT_WEIGHT: float = 2.0   # 429 cuenta doble
    _TIMEOUT_WEIGHT:    float = 1.0
    _NETWORK_WEIGHT:    float = 0.5   # glitch transitorio — pesa menos

    def __init__(
        self,
        exchange_id:        str,
        initial:            int   = 5,
        maximum:            int   = 20,
        minimum:            int   = 1,
        error_threshold:    float = 0.2,
        latency_target_ms:  int   = 500,
        cooldown_seconds:   float = 10.0,
        window:             int   = 20,
    ) -> None:
        self._exchange_id       = exchange_id
        self.current            = max(minimum, min(initial, maximum))
        self._maximum           = maximum
        self._minimum           = minimum
        self._error_threshold   = error_threshold
        self._latency_target_ms = latency_target_ms
        self._cooldown_seconds  = cooldown_seconds
        self._window            = window
        self._results:    list[float] = []   # 0=éxito, >0=error ponderado
        self._latencies:  list[float] = []
        self._last_scale: float       = 0.0

    # ── registro ──────────────────────────────────────────────────────────────

    def record_success(self, latency_ms: float | None = None) -> None:
        self._results.append(0.0)
        if latency_ms is not None:
            self._latencies.append(latency_ms)
        self._trim()
        self._maybe_scale()

    def record_error(
        self,
        error_type: str = "network",
        latency_ms: float | None = None,
    ) -> None:
        """
        error_type: "rate_limit" | "timeout" | "network"
        rate_limit pesa doble → scale_down agresivo ante 429s.
        """
        weight = {
            "rate_limit": self._RATE_LIMIT_WEIGHT,
            "timeout":    self._TIMEOUT_WEIGHT,
            "network":    self._NETWORK_WEIGHT,
        }.get(error_type, self._TIMEOUT_WEIGHT)
        self._results.append(weight)
        if latency_ms is not None:
            self._latencies.append(latency_ms)
        self._trim()
        self._maybe_scale()

    # ── métricas ──────────────────────────────────────────────────────────────

    def _error_rate(self) -> float:
        if not self._results:
            return 0.0
        total_weight = sum(self._results)
        max_possible = len(self._results) * self._RATE_LIMIT_WEIGHT
        return total_weight / max_possible if max_possible else 0.0

    def _p95_latency(self) -> float:
        if not self._latencies:
            return 0.0
        sorted_lats = sorted(self._latencies)
        idx = max(0, int(len(sorted_lats) * 0.95) - 1)
        return sorted_lats[idx]

    # ── lógica de escala ──────────────────────────────────────────────────────

    def _maybe_scale(self) -> None:
        now = time.monotonic()
        if now - self._last_scale < self._cooldown_seconds:
            return  # cooldown activo — evita thrashing

        error_rate  = self._error_rate()
        p95         = self._p95_latency()
        should_down = (
            error_rate > self._error_threshold
            or (p95 > 0 and p95 > self._latency_target_ms)
        )
        should_up = (
            error_rate < self._error_threshold / 2
            and (p95 == 0 or p95 < self._latency_target_ms * 0.7)
            and self.current < self._maximum
        )

        if should_down and self.current > self._minimum:
            # 429 → más agresivo (50%), latencia/timeout → menos (70%)
            factor = 0.5 if error_rate > self._error_threshold else 0.7
            self.current     = max(self._minimum, int(self.current * factor))
            self._last_scale = now
            logger.debug(
                "AdaptiveThrottle DOWN | exchange={} current={} "
                "error_rate={:.0%} p95={:.0f}ms",
                self._exchange_id, self.current, error_rate, p95,
            )
        elif should_up:
            self.current     = min(self._maximum, self.current + 1)
            self._last_scale = now
            logger.debug(
                "AdaptiveThrottle UP | exchange={} current={}",
                self._exchange_id, self.current,
            )

    def _trim(self) -> None:
        if len(self._results) > self._window:
            self._results = self._results[-self._window:]
        if len(self._latencies) > self._window:
            self._latencies = self._latencies[-self._window:]


# ==========================================================
# Singleton registry + factories
# ==========================================================

_THROTTLES: Dict[str, AdaptiveThrottle] = {}


def get_or_create_throttle(
    exchange_id:       str,
    market_type:       str,
    dataset:           str,
    initial:           int,
    maximum:           int,
    latency_target_ms: int = 500,
) -> AdaptiveThrottle:
    """
    Singleton de AdaptiveThrottle por key "exchange:market:dataset".

    El probe (initial/maximum) define el starting point la primera vez.
    En runs subsiguientes del mismo proceso el estado persiste —
    el throttle recuerda la presión acumulada del exchange.
    """
    key = f"{exchange_id}:{market_type}:{dataset}"
    if key not in _THROTTLES:
        _THROTTLES[key] = AdaptiveThrottle(
            exchange_id       = key,
            initial           = initial,
            maximum           = maximum,
            latency_target_ms = latency_target_ms,
        )
    return _THROTTLES[key]


def get_throttle_state(
    exchange_id: str,
    market_type: str = "spot",
    dataset:     str = "ohlcv",
) -> dict:
    """Estado observable del throttle. SafeOps: nunca lanza excepción."""
    try:
        key = f"{exchange_id}:{market_type}:{dataset}"
        t   = _THROTTLES.get(key)
        if t is None:
            return {"key": key, "concurrent": 0, "error_rate": 0.0, "p95_ms": 0.0}
        return {
            "key":        key,
            "concurrent": t.current,
            "maximum":    t._maximum,
            "error_rate": t._error_rate(),
            "p95_ms":     t._p95_latency(),
        }
    except Exception:
        return {
            "key":        f"{exchange_id}:{market_type}:{dataset}",
            "concurrent": 0,
            "error_rate": 0.0,
            "p95_ms":     0.0,
        }
