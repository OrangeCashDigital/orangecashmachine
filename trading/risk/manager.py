# -*- coding: utf-8 -*-
"""
trading/risk/manager.py
=======================

RiskManager — toda la lógica de validación de riesgo en un lugar.

Responsabilidad única (SRP)
---------------------------
Decide si una señal puede convertirse en orden dado:
  - el estado actual de posiciones abiertas
  - el drawdown acumulado
  - los límites de configuración

No ejecuta órdenes, no genera señales, no accede a exchanges.

SafeOps
-------
- Toda validación retorna RiskDecision (aprobado/rechazado + razón).
- Nunca lanza en validate() — errores se convierten en rechazos.
- Thread-safe: toda mutación de estado ocurre bajo _lock.
  La verificación de drawdown y el halt son atómicos — sin race conditions.

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Optional

from loguru import logger

from domain.boundaries import SignalProtocol  # DIP — risk depende de abstraccion
from trading.risk.models import RiskConfig


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

class RiskViolation(Exception):
    """Señal rechazada por riesgo — alternativa de excepción al patrón RiskDecision."""


@dataclass(frozen=True)
class RiskDecision:
    """
    Resultado inmutable de una validación de riesgo.

    Creado por RiskManager, consumido por OMS.
    """
    approved:  bool
    reason:    str   = ""
    size_pct:  float = 0.0    # % del capital a asignar (0.0 si rechazado)

    @property
    def rejected(self) -> bool:
        return not self.approved

    def __str__(self) -> str:
        status = "APPROVED" if self.approved else "REJECTED"
        return f"RiskDecision({status} | size={self.size_pct:.1%} | {self.reason})"


# ---------------------------------------------------------------------------
# RiskManager
# ---------------------------------------------------------------------------

class RiskManager:
    """
    Valida señales contra límites de riesgo y estado de posiciones.

    Parameters
    ----------
    config       : RiskConfig — límites de riesgo.
    capital_usd  : float      — capital total disponible (para sizing).
    """

    def __init__(
        self,
        config:      Optional[RiskConfig] = None,
        capital_usd: float = 10_000.0,
    ) -> None:
        self._config      = config or RiskConfig()
        self._capital_usd = capital_usd
        self._lock        = threading.Lock()

        # Estado mutable — SIEMPRE mutado bajo _lock
        self._open_positions:   int   = 0
        self._daily_pnl_pct:    float = 0.0
        self._total_pnl_pct:    float = 0.0
        self._halted:           bool  = False
        self._halt_reason:      str   = ""

        self._log = logger.bind(component="RiskManager")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(self, signal: SignalProtocol) -> RiskDecision:
        """
        Valida una señal. Retorna RiskDecision — nunca lanza.

        Checks en orden (fail-fast):
          1. Sistema no halteado
          2. Señal accionable
          3. Confianza mínima
          4. Máximo posiciones abiertas
          5. Drawdown diario
          6. Drawdown total
          7. Sizing válido en USD
        """
        try:
            return self._validate_internal(signal)
        except Exception as exc:
            self._log.error("validate: error inesperado | error={}", exc)
            return RiskDecision(approved=False, reason=f"internal_error:{exc}")

    def record_open(self) -> None:
        """Registra apertura de posición."""
        with self._lock:
            self._open_positions += 1

    def record_close(self, pnl_pct: float = 0.0) -> None:
        """
        Registra cierre de posición con P&L.

        pnl_pct > 0 → ganancia, < 0 → pérdida.
        Actualiza drawdown acumulado y verifica halt (atómico bajo lock).
        """
        with self._lock:
            self._open_positions = max(0, self._open_positions - 1)
            self._daily_pnl_pct += pnl_pct
            self._total_pnl_pct += pnl_pct
            # Verificación atómica — misma sección crítica que la mutación
            self._check_drawdown_halt_locked()

    def halt(self, reason: str) -> None:
        """Haltea el trading manualmente. Idempotente."""
        with self._lock:
            self._halt_locked(reason)

    def reset_daily(self) -> None:
        """Resetea contadores diarios (llamar al inicio de cada sesión)."""
        with self._lock:
            self._daily_pnl_pct = 0.0

    def reset_total(self) -> None:
        """
        Resetea todos los contadores de P&L y elimina el halt.

        Usar con precaución — solo para reinicio de sesión completa.
        """
        with self._lock:
            self._daily_pnl_pct = 0.0
            self._total_pnl_pct = 0.0
            self._halted        = False
            self._halt_reason   = ""
            self._log.info("RiskManager: contadores reseteados")

    @property
    def is_halted(self) -> bool:
        return self._halted

    @property
    def open_positions(self) -> int:
        return self._open_positions

    def state(self) -> dict:
        """Estado observable para logging y métricas. SafeOps: nunca lanza."""
        try:
            with self._lock:
                return {
                    "halted":           self._halted,
                    "halt_reason":      self._halt_reason,
                    "open_positions":   self._open_positions,
                    "daily_pnl_pct":    round(self._daily_pnl_pct, 4),
                    "total_pnl_pct":    round(self._total_pnl_pct, 4),
                    "capital_usd":      self._capital_usd,
                }
        except Exception:
            return {"halted": False, "halt_reason": "", "open_positions": 0}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_internal(self, signal: SignalProtocol) -> RiskDecision:
        cfg = self._config

        # Leer estado consistente bajo lock — atómico
        with self._lock:
            halted = self._halted
            halt_r = self._halt_reason
            opens  = self._open_positions
            daily  = self._daily_pnl_pct
            total  = self._total_pnl_pct

        # Checks de solo-lectura — fuera del lock (no mutan estado)
        # ----------------------------------------------------------------

        # 1 — halt
        if halted:
            return RiskDecision(False, f"system_halted:{halt_r}")

        # 2 — señal accionable
        if not signal.is_actionable:
            return RiskDecision(False, "signal_not_actionable")

        # 3 — confianza mínima
        min_conf = cfg.signal_filter.min_confidence
        if signal.confidence < min_conf:
            return RiskDecision(
                False,
                f"confidence_too_low:{signal.confidence:.2f}<{min_conf:.2f}",
            )

        # 4 — posiciones abiertas
        if opens >= cfg.position.max_open_positions:
            return RiskDecision(
                False,
                f"max_positions_reached:{opens}/{cfg.position.max_open_positions}",
            )

        # 5 & 6 — drawdown: verificar Y haltear atómicamente bajo lock
        if cfg.drawdown.halt_on_breach:
            with self._lock:
                if self._daily_pnl_pct <= -cfg.drawdown.max_daily_drawdown_pct:
                    reason = f"daily_drawdown_breached:{self._daily_pnl_pct:.2%}"
                    self._halt_locked(reason)
                    return RiskDecision(False, reason)
                if self._total_pnl_pct <= -cfg.drawdown.max_total_drawdown_pct:
                    reason = f"total_drawdown_breached:{self._total_pnl_pct:.2%}"
                    self._halt_locked(reason)
                    return RiskDecision(False, reason)

        # 7 — sizing
        size_pct  = cfg.position.max_position_pct
        order_usd = self._capital_usd * size_pct
        if order_usd < cfg.order.min_order_usd:
            return RiskDecision(
                False,
                f"order_too_small:{order_usd:.2f}<{cfg.order.min_order_usd}",
            )
        if order_usd > cfg.order.max_order_usd:
            size_pct = cfg.order.max_order_usd / self._capital_usd

        return RiskDecision(approved=True, reason="ok", size_pct=size_pct)

    def _halt_locked(self, reason: str) -> None:
        """
        Activa el halt. Debe llamarse DENTRO de self._lock.

        Idempotente — preserva la razón original si ya está halteado.
        DRY: único punto de mutación de _halted/_halt_reason.
        """
        if not self._halted:
            self._halted      = True
            self._halt_reason = reason
            self._log.warning("HALT activado | reason={}", reason)

    def _check_drawdown_halt_locked(self) -> None:
        """
        Verifica drawdown y haltea si es necesario.

        PRECONDICIÓN: llamar DENTRO de self._lock (desde record_close).
        DRY: delega a _halt_locked para mutación real.
        """
        cfg = self._config
        if not cfg.drawdown.halt_on_breach:
            return
        if self._daily_pnl_pct <= -cfg.drawdown.max_daily_drawdown_pct:
            self._halt_locked(f"daily_drawdown:{self._daily_pnl_pct:.2%}")
        if self._total_pnl_pct <= -cfg.drawdown.max_total_drawdown_pct:
            self._halt_locked(f"total_drawdown:{self._total_pnl_pct:.2%}")
