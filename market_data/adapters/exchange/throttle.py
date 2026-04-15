"""
market_data/adapters/exchange/throttle.py
==========================================
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

Señales de escala
-----------------
DOWN (cualquiera dispara):
  - health_score < HEALTH_DOWN_THRESHOLD  (señal compuesta)
  - error_rate   > error_threshold        (429×2.0, timeout×1.0, network×0.5)
  - p95_latency  > latency_target_ms
  - occ_rate     > occ_threshold          (contención storage — señal independiente)

UP (todas deben cumplirse):
  - health_score  > HEALTH_UP_THRESHOLD
  - error_rate    < error_threshold / 2
  - p95_latency   < latency_target_ms × 0.7
  - latency_slope ≤ 0                     (tendencia estable o mejorando)
  - occ_rate      < occ_threshold / 2
  - current       < maximum

Anti-thrashing
--------------
Cooldowns direccionales independientes: reducción (cooldown_seconds) y
aumento (cooldown_seconds / 2). Slope guard bloquea scale_up si la
tendencia de latencia es creciente aunque las métricas puntuales sean buenas.

Rate-limit cooldown
-------------------
record_rate_limit_hit(): post-429 / CircuitBreakerOpenError — cae a
concurrencia mínima y bloquea scale_up durante RATE_LIMIT_COOLDOWN_S
segundos. is_in_cooldown() expone el estado para decisiones upstream.

Ventana mínima
--------------
_MIN_WINDOW_SAMPLES: no se actúa hasta tener suficientes observaciones —
evita reacciones a señales estadísticamente insignificantes.
"""

from __future__ import annotations

import statistics
import time
from collections import deque
from typing import TYPE_CHECKING, Dict, Optional

from loguru import logger

if TYPE_CHECKING:
    from market_data.adapters.exchange.limiter import AdaptiveLimiter

__all__ = [
    "AdaptiveThrottle",
    "_THROTTLES",
    "get_or_create_throttle",
    "get_throttle_state",
]

# ── Constantes de módulo ─────────────────────────────────────────────────────
_MIN_WINDOW_SAMPLES:    int   = 5     # mínimo de observaciones antes de actuar
_RATE_LIMIT_COOLDOWN_S: float = 30.0  # hard pause post-429 / CircuitOpen (s)

# Umbrales del health score — invariantes de dominio, no expuestos como parámetros
_HEALTH_DOWN_THRESHOLD: float = 0.40  # por debajo → scale_down
_HEALTH_UP_THRESHOLD:   float = 0.85  # por encima (+ condiciones) → scale_up


class AdaptiveThrottle:
    """
    Throttle adaptivo por pipeline (exchange:market_type:dataset).

    Cuatro familias de señales integradas en un health score compuesto:
      1. Error rate ponderado — errores clasificados por severidad
      2. p95 latency          — detecta saturación antes de que emerjan errores
      3. Latency slope        — detecta tendencia creciente (anti-thrashing)
      4. OCC conflicts        — contención de storage; señal independiente del exchange

    Separar OCC de errores de exchange es correcto: un OCC alto con
    error_rate=0% indica presión en el storage (Iceberg/Bronze), no en el
    exchange — reducir concurrencia aliviaría la contención de escritura.

    Anti-thrashing:
      - Cooldowns direccionales: reducción y aumento con tiempos distintos
      - Latency slope guard: bloquea scale_up si la tendencia es creciente
      - Rate-limit cooldown: hard pause post-429, caída a concurrencia mínima
    """

    # Pesos de error — contribución al error rate ponderado
    _RATE_LIMIT_WEIGHT: float = 2.0  # 429 → máxima penalización
    _TIMEOUT_WEIGHT:    float = 1.0  # timeout → penalización estándar
    _NETWORK_WEIGHT:    float = 0.5  # glitch transitorio → penalización suave

    # Factores de reducción de concurrencia según causa dominante
    _FACTOR_RATE_LIMIT: float = 0.5  # 429 → reducción agresiva (-50%)
    _FACTOR_LATENCY:    float = 0.7  # latencia/timeout → reducción moderada (-30%)
    _FACTOR_OCC:        float = 0.8  # storage → reducción suave (-20%)

    # Pesos del health score compuesto (suman 1.0)
    _HEALTH_W_LATENCY: float = 0.35  # p95 — señal más predictiva de saturación
    _HEALTH_W_SLOPE:   float = 0.30  # slope — tendencia más informativa que estado puntual
    _HEALTH_W_ERRORS:  float = 0.25  # error rate — señal definitiva cuando alta
    _HEALTH_W_OCC:     float = 0.10  # occ rate — menor impacto en throughput

    def __init__(
        self,
        exchange_id: str,
        initial: int = 5,
        maximum: int = 20,
        minimum: int = 1,
        error_threshold: float = 0.20,
        latency_target_ms: int = 500,
        occ_threshold: float = 0.30,
        cooldown_seconds: float = 10.0,
        window: int = 20,
        limiter: Optional["AdaptiveLimiter"] = None,
    ) -> None:
        """
        Parameters
        ----------
        exchange_id       : key identificador (exchange:market:dataset)
        initial           : concurrencia inicial
        maximum           : techo de concurrencia
        minimum           : piso de concurrencia
        error_threshold   : fracción de errores ponderados que dispara scale_down
        latency_target_ms : p95 máximo tolerable en ms
        occ_threshold     : fracción de OCC conflicts que dispara scale_down suave
        cooldown_seconds  : cooldown de reducción (s); aumento usa cooldown / 2
        window            : tamaño de las ventanas deslizantes de métricas
        limiter           : AdaptiveLimiter a sincronizar cuando cambia current
        """
        self._exchange_id       = exchange_id
        self.current            = max(minimum, min(initial, maximum))
        self._maximum           = maximum
        self._minimum           = minimum
        self._error_threshold   = error_threshold
        self._latency_target_ms = latency_target_ms
        self._occ_threshold     = occ_threshold
        self._limiter           = limiter

        # Cooldowns direccionales anti-thrashing (ratio 2:1 reducción:aumento)
        self._reduction_cooldown_s: float = cooldown_seconds
        self._increase_cooldown_s:  float = max(1.0, cooldown_seconds / 2.0)

        # Ventanas deslizantes con maxlen — sin _trim() manual
        _w = max(window, _MIN_WINDOW_SAMPLES * 2)
        self._results:    deque[float] = deque(maxlen=_w)  # 0.0=ok, >0=peso error
        self._latencies:  deque[float] = deque(maxlen=_w)  # ms por operación
        self._occ_window: deque[int]   = deque(maxlen=_w)  # 0=ok, 1=conflict

        # Timestamps de último ajuste (cooldowns direccionales)
        self._last_reduction_ts: float = 0.0
        self._last_increase_ts:  float = 0.0

        # Hard cooldown post-rate-limit
        self._rate_limit_cooldown_until: float = 0.0

    # ── API pública — registro de señales ────────────────────────────────────

    def record_success(self, latency_ms: float | None = None) -> None:
        """Registra una operación exitosa con latencia opcional."""
        self._results.append(0.0)
        if latency_ms is not None:
            self._latencies.append(latency_ms)
        self._occ_window.append(0)
        self._maybe_scale()

    def record_error(
        self,
        error_type: str = "network",
        latency_ms: float | None = None,
    ) -> None:
        """
        Registra un error de exchange en la ventana de señales.

        Parameters
        ----------
        error_type : "rate_limit" | "timeout" | "network"
                     rate_limit pesa ×2 → mayor impacto en error_rate ponderado.
        latency_ms : latencia hasta el error (opcional).

        Nota: para un 429 explícito, llamar también record_rate_limit_hit()
        para activar el hard cooldown. record_error solo actualiza la ventana
        estadística — no activa el cooldown por sí mismo.
        """
        weight = {
            "rate_limit": self._RATE_LIMIT_WEIGHT,
            "timeout":    self._TIMEOUT_WEIGHT,
            "network":    self._NETWORK_WEIGHT,
        }.get(error_type, self._TIMEOUT_WEIGHT)
        self._results.append(weight)
        if latency_ms is not None:
            self._latencies.append(latency_ms)
        self._occ_window.append(0)
        self._maybe_scale()

    def record_occ_conflict(self) -> None:
        """
        Registra un OCC conflict de storage (Bronze/Silver).

        No registra en _results (no es error de exchange) ni en _latencies
        (no tenemos la latencia del conflict, solo el evento booleano).
        La contención de storage es una señal independiente del exchange.
        """
        self._occ_window.append(1)
        self._maybe_scale()

    def record_rate_limit_hit(self) -> None:
        """
        Activa el hard cooldown post-429 / CircuitBreakerOpenError.

        Efectos inmediatos:
          - Concurrencia cae a _minimum
          - Scale_up bloqueado durante RATE_LIMIT_COOLDOWN_S segundos
          - Cooldown de reducción reseteado (evita doble reducción)

        No llama record_error — el caller ya registró la señal en la ventana.
        Llamar desde CCXTAdapter cuando:
          - RetryExhaustedError.error_type == "rate_limit"
          - CircuitBreakerOpenError capturado
        """
        now = time.monotonic()
        self._rate_limit_cooldown_until = now + _RATE_LIMIT_COOLDOWN_S
        self._last_reduction_ts = now  # resetea cooldown de reducción

        if self.current > self._minimum:
            self.current = self._minimum
            if self._limiter is not None:
                self._limiter.update_concurrency(self.current)

        logger.warning(
            "AdaptiveThrottle RATE_LIMIT_COOLDOWN | exchange={} "
            "current={}/{} cooldown_s={:.0f}",
            self._exchange_id,
            self.current,
            self._maximum,
            _RATE_LIMIT_COOLDOWN_S,
        )

    # ── API pública — estado observable ──────────────────────────────────────

    def is_in_cooldown(self) -> bool:
        """True durante el período de hard cooldown post-429."""
        return time.monotonic() < self._rate_limit_cooldown_until

    # ── Métricas internas ─────────────────────────────────────────────────────

    def _error_rate(self) -> float:
        """
        Fracción de error ponderada sobre la ventana actual.

        Normalizada contra el peso máximo posible (rate_limit_weight × n)
        para que sea comparable entre ventanas de distinto tamaño.
        """
        if not self._results:
            return 0.0
        max_possible = len(self._results) * self._RATE_LIMIT_WEIGHT
        return sum(self._results) / max_possible

    def _p95_latency(self) -> float:
        """
        Percentil 95 de latencia en ms.

        Retorna 0.0 si hay menos de _MIN_WINDOW_SAMPLES observaciones —
        un p95 con pocos datos no es estadísticamente válido.
        """
        if len(self._latencies) < _MIN_WINDOW_SAMPLES:
            return 0.0
        sorted_lats = sorted(self._latencies)
        idx = max(0, int(len(sorted_lats) * 0.95) - 1)
        return sorted_lats[idx]

    def _latency_slope(self) -> float:
        """
        Pendiente de latencia: mean(segunda mitad) − mean(primera mitad).

        Positivo → latencia creciente (presión incremental en el exchange).
        Negativo → latencia decreciente (sistema recuperándose).
        0.0      → ventana insuficiente o latencia estable.

        Señal anti-thrashing: scale_up bloqueado cuando slope > 0.
        Requiere al menos 2 × _MIN_WINDOW_SAMPLES observaciones.
        """
        lats = list(self._latencies)
        if len(lats) < _MIN_WINDOW_SAMPLES * 2:
            return 0.0
        mid = len(lats) // 2
        return statistics.mean(lats[mid:]) - statistics.mean(lats[:mid])

    def _occ_rate(self) -> float:
        """Fracción de OCC conflicts sobre la ventana actual."""
        if not self._occ_window:
            return 0.0
        return sum(self._occ_window) / len(self._occ_window)

    def _health_score(self) -> float:
        """
        Health score compuesto: 1.0 = sistema óptimo, 0.0 = sistema colapsando.

        Combina cuatro señales normalizadas en un único valor comparable.
        Señal primaria de ajuste sostenido; los thresholds individuales actúan
        como safety net ante degradación severa y repentina.

        Pesos (suman 1.0):
          p95 latency  0.35 — más predictivo que error rate bajo carga sostenida
          slope        0.30 — tendencia más informativa que estado puntual
          error rate   0.25 — señal definitiva cuando alcanza threshold
          occ rate     0.10 — señal de storage, menor impacto en throughput

        Retorna 1.0 cuando la ventana está vacía (sin datos = sin presión conocida).
        """
        p95   = self._p95_latency()
        slope = self._latency_slope()
        error = self._error_rate()
        occ   = self._occ_rate()

        # Cada componente normalizado → [0.0, 1.0] donde 1.0 = sano
        lat_score   = max(0.0, 1.0 - p95 / self._latency_target_ms)   if p95 > 0   else 1.0
        slope_score = max(0.0, 1.0 - slope / self._latency_target_ms)  if slope > 0 else 1.0
        err_score   = (
            max(0.0, 1.0 - error / self._error_threshold)
            if self._error_threshold > 0 else 1.0
        )
        occ_score   = (
            max(0.0, 1.0 - occ / self._occ_threshold)
            if self._occ_threshold > 0 else 1.0
        )

        return (
            lat_score   * self._HEALTH_W_LATENCY
            + slope_score * self._HEALTH_W_SLOPE
            + err_score   * self._HEALTH_W_ERRORS
            + occ_score   * self._HEALTH_W_OCC
        )

    # ── Lógica de escala ──────────────────────────────────────────────────────

    def _can_reduce(self, now: float) -> bool:
        """True si el cooldown de reducción ha expirado."""
        return (now - self._last_reduction_ts) >= self._reduction_cooldown_s

    def _can_increase(self, now: float) -> bool:
        """
        True si el cooldown de aumento ha expirado y no hay rate-limit activo.

        Bloquea scale_up durante el hard cooldown post-429: aumentar
        concurrencia mientras el exchange está saturado amplificaría el error.
        """
        return (
            (now - self._last_increase_ts) >= self._increase_cooldown_s
            and not self.is_in_cooldown()
        )

    def _maybe_scale(self) -> None:
        """
        Evalúa si ajustar la concurrencia.

        Política (Fail-Fast first, luego inteligencia compuesta):
          1. Guardia de ventana mínima — no actuar con datos insuficientes
          2. Safety net individual     — thresholds duros ante degradación brusca
          3. Health score compuesto   — señal primaria para degradación sostenida
          4. Cooldowns direccionales  — anti-thrashing (reducción ≠ aumento)
          5. Slope guard              — no subir si la tendencia es creciente

        Orden de decisión: reduce > mantiene > sube (Fail-Fast primero).
        """
        # Guardia: ventana mínima insuficiente para señales estadísticas válidas
        if len(self._results) < _MIN_WINDOW_SAMPLES:
            return

        now        = time.monotonic()
        error_rate = self._error_rate()
        p95        = self._p95_latency()
        slope      = self._latency_slope()
        occ_rate   = self._occ_rate()
        health     = self._health_score()

        # ── Safety net: condiciones individuales duras ───────────────────────
        high_errors  = error_rate > self._error_threshold
        high_latency = p95 > 0 and p95 > self._latency_target_ms
        high_occ     = occ_rate > self._occ_threshold
        low_health   = health < _HEALTH_DOWN_THRESHOLD

        should_down = (
            (high_errors or high_latency or high_occ or low_health)
            and self.current > self._minimum
            and self._can_reduce(now)
        )

        # ── Scale_up: todas las condiciones deben cumplirse ──────────────────
        should_up = (
            health > _HEALTH_UP_THRESHOLD
            and error_rate < self._error_threshold / 2.0
            and (p95 == 0.0 or p95 < self._latency_target_ms * 0.7)
            and slope <= 0.0          # slope guard: no subir si tendencia creciente
            and occ_rate < self._occ_threshold / 2.0
            and self.current < self._maximum
            and self._can_increase(now)
        )

        if should_down:
            # Causa dominante: errores > latencia > occ > health_score_bajo
            if high_errors:
                factor = self._FACTOR_RATE_LIMIT
                cause  = "rate_limit_pressure"
            elif high_latency:
                factor = self._FACTOR_LATENCY
                cause  = "latency_pressure"
            elif high_occ:
                factor = self._FACTOR_OCC
                cause  = "occ_pressure"
            else:
                factor = self._FACTOR_LATENCY
                cause  = "health_score_low"

            self.current = max(self._minimum, int(self.current * factor))
            self._last_reduction_ts = now
            if self._limiter is not None:
                self._limiter.update_concurrency(self.current)
            logger.debug(
                "AdaptiveThrottle DOWN | exchange={} current={}/{} cause={} "
                "health={:.2f} error={:.0%} p95={:.0f}ms slope={:+.0f}ms occ={:.0%}",
                self._exchange_id, self.current, self._maximum, cause,
                health, error_rate, p95, slope, occ_rate,
            )

        elif should_up:
            self.current = min(self._maximum, self.current + 1)
            self._last_increase_ts = now
            if self._limiter is not None:
                self._limiter.update_concurrency(self.current)
            logger.debug(
                "AdaptiveThrottle UP | exchange={} current={}/{} "
                "health={:.2f} error={:.0%} p95={:.0f}ms slope={:+.0f}ms occ={:.0%}",
                self._exchange_id, self.current, self._maximum,
                health, error_rate, p95, slope, occ_rate,
            )


# ==========================================================
# Singleton registry + factories
# ==========================================================

_THROTTLES: Dict[str, AdaptiveThrottle] = {}


def get_or_create_throttle(
    exchange_id: str,
    market_type: str,
    dataset: str,
    initial: int,
    maximum: int,
    latency_target_ms: int = 500,
    occ_threshold: float = 0.30,
    limiter: Optional["AdaptiveLimiter"] = None,
) -> AdaptiveThrottle:
    """
    Singleton de AdaptiveThrottle por key "exchange:market:dataset".

    El probe (initial/maximum) define el starting point la primera vez.
    En runs subsiguientes del mismo proceso el estado persiste —
    el throttle recuerda la presión acumulada del exchange y del storage.

    Parameters
    ----------
    occ_threshold : fracción de OCC conflicts tolerable antes de scale_down suave.
                    Default conservador (0.30) — ajustar por exchange si necesario.
    limiter       : AdaptiveLimiter a sincronizar cuando cambia la concurrencia.
    """
    key = f"{exchange_id}:{market_type}:{dataset}"
    if key not in _THROTTLES:
        _THROTTLES[key] = AdaptiveThrottle(
            exchange_id=key,
            initial=initial,
            maximum=maximum,
            latency_target_ms=latency_target_ms,
            occ_threshold=occ_threshold,
            limiter=limiter,
        )
    return _THROTTLES[key]


def get_throttle_state(
    exchange_id: str,
    market_type: str = "spot",
    dataset: str = "ohlcv",
) -> dict:
    """
    Estado observable del throttle para logging y métricas externas.
    SafeOps: nunca lanza excepción. Todos los campos garantizados presentes.
    """
    key = f"{exchange_id}:{market_type}:{dataset}"
    _empty: dict = {
        "key":         key,
        "concurrent":  0,
        "maximum":     0,
        "error_rate":  0.0,
        "p95_ms":      0.0,
        "occ_rate":    0.0,
        "health":      1.0,
        "in_cooldown": False,
    }
    try:
        t = _THROTTLES.get(key)
        if t is None:
            return _empty
        return {
            "key":         key,
            "concurrent":  t.current,
            "maximum":     t._maximum,
            "error_rate":  t._error_rate(),
            "p95_ms":      t._p95_latency(),
            "occ_rate":    t._occ_rate(),
            "health":      t._health_score(),
            "in_cooldown": t.is_in_cooldown(),
        }
    except Exception:
        return _empty
