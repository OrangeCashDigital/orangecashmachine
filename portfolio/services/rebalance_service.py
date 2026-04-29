# -*- coding: utf-8 -*-
"""
portfolio/services/rebalance_service.py
=========================================

RebalanceService — lógica de rebalanceo de portfolio.

Responsabilidad única (SRP)
---------------------------
Dado el estado actual del portfolio y los targets deseados,
calcular qué ajustes son necesarios y emitir las señales correspondientes.

NO ejecuta órdenes — genera RebalanceSignal que el OMS puede consumir.
NO valida riesgo — eso es concern de trading/risk/.
NO persiste estado — eso es concern de PortfolioService.

Modelo de rebalanceo
--------------------
- target_weights: dict symbol → float (porcentaje deseado del capital)
- drift_threshold: si la desviación actual vs target supera este umbral,
  se genera una señal de ajuste
- Ejemplo: BTC target=40%, actual=30% → señal BUY BTC por 10%
           ETH target=20%, actual=25% → señal SELL ETH por 5%

Integración
-----------
  RebalanceService.rebalance(portfolio_state, targets)
    → list[RebalanceSignal]
    → caller envía señales al OMS

SafeOps
-------
- rebalance() nunca lanza — errores logueados, retorna lista vacía.
- Thresholds validados en construcción (Fail-Fast).

Principios: SOLID · DDD · SafeOps · KISS · DRY
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from portfolio.models.position import PortfolioState


# ---------------------------------------------------------------------------
# RebalanceSignal — value object de salida
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RebalanceSignal:
    """
    Señal de rebalanceo generada por RebalanceService.

    Representa un ajuste necesario para acercar el portfolio a los targets.
    El caller (use case) decide cómo enviarla al OMS.

    Campos
    ------
    symbol       : par a ajustar
    action       : "buy" (aumentar exposición) | "sell" (reducir)
    delta_pct    : magnitud del ajuste en % del capital
    current_pct  : exposición actual
    target_pct   : exposición objetivo
    generated_at : timestamp UTC
    """
    symbol:       str
    action:       str        # "buy" | "sell"
    delta_pct:    float      # magnitud del ajuste ∈ (0, 1]
    current_pct:  float
    target_pct:   float
    generated_at: datetime

    @property
    def is_increase(self) -> bool:
        return self.action == "buy"

    def __str__(self) -> str:
        return (
            f"RebalanceSignal({self.action.upper()} {self.symbol}"
            f" delta={self.delta_pct:+.1%}"
            f" current={self.current_pct:.1%} → target={self.target_pct:.1%})"
        )


# ---------------------------------------------------------------------------
# RebalanceService
# ---------------------------------------------------------------------------

class RebalanceService:
    """
    Calcula ajustes de portfolio para alcanzar target weights.

    Parameters
    ----------
    drift_threshold : float — desviación mínima para generar señal (default: 5%)
                              Evita micro-ajustes por ruido de mercado.
    min_delta_pct   : float — tamaño mínimo del ajuste en % del capital (default: 1%)
                              Evita órdenes demasiado pequeñas para ser ejecutadas.
    """

    _DEFAULT_DRIFT     = 0.05   # 5% de desviación mínima para actuar
    _DEFAULT_MIN_DELTA = 0.01   # 1% mínimo de ajuste

    def __init__(
        self,
        drift_threshold: float = _DEFAULT_DRIFT,
        min_delta_pct:   float = _DEFAULT_MIN_DELTA,
    ) -> None:
        # Fail-Fast: validar en construcción, no en cada llamada
        if not (0.0 < drift_threshold < 1.0):
            raise ValueError(
                f"drift_threshold debe estar en (0, 1), recibido: {drift_threshold}"
            )
        if not (0.0 < min_delta_pct < 1.0):
            raise ValueError(
                f"min_delta_pct debe estar en (0, 1), recibido: {min_delta_pct}"
            )
        if min_delta_pct >= drift_threshold:
            raise ValueError(
                f"min_delta_pct ({min_delta_pct}) debe ser < drift_threshold ({drift_threshold})"
            )

        self._drift     = drift_threshold
        self._min_delta = min_delta_pct
        self._log       = logger.bind(component="RebalanceService")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def rebalance(
        self,
        state:   PortfolioState,
        targets: dict[str, float],
        trigger: str = "manual",
    ) -> list[RebalanceSignal]:
        """
        Calcula señales de rebalanceo dado el estado actual y los targets.

        Parameters
        ----------
        state   : PortfolioState — snapshot actual del portfolio
        targets : dict symbol → target_pct — pesos objetivo (deben sumar ≤ 1.0)
        trigger : razón del rebalanceo ("scheduled" | "drift" | "manual")

        Returns
        -------
        list[RebalanceSignal] — ajustes necesarios, ordenados por |delta| desc.
        Lista vacía si no hay nada que ajustar o hay error.

        SafeOps: nunca lanza.
        """
        try:
            return self._compute(state, targets, trigger)
        except Exception as exc:
            self._log.error("rebalance error | trigger={} {}", trigger, exc)
            return []

    def validate_targets(self, targets: dict[str, float]) -> tuple[bool, str]:
        """
        Valida que los targets sean coherentes.

        Returns (valid, error_message). error_message vacío si valid=True.
        Usado por el use case antes de llamar a rebalance().
        """
        if not targets:
            return False, "targets vacío"

        for symbol, pct in targets.items():
            if not isinstance(pct, (int, float)):
                return False, f"target para {symbol} no es numérico: {pct!r}"
            if not (0.0 <= pct <= 1.0):
                return False, f"target para {symbol} fuera de rango [0, 1]: {pct}"

        total = sum(targets.values())
        if total > 1.0 + 1e-9:   # tolerancia float
            return False, f"targets suman {total:.2%} — deben sumar ≤ 100%"

        return True, ""

    # ------------------------------------------------------------------
    # Private
    # ------------------------------------------------------------------

    def _compute(
        self,
        state:   PortfolioState,
        targets: dict[str, float],
        trigger: str,
    ) -> list[RebalanceSignal]:
        """Lógica interna de cálculo de señales."""
        # Mapa actual: symbol → exposición total
        current: dict[str, float] = {}
        for pos in state.positions:
            current[pos.symbol] = current.get(pos.symbol, 0.0) + pos.size_pct

        now     = datetime.now(timezone.utc)
        signals = []

        # Calcular delta para cada símbolo en targets
        all_symbols = set(targets.keys()) | set(current.keys())
        for symbol in all_symbols:
            target_pct  = targets.get(symbol, 0.0)
            current_pct = current.get(symbol, 0.0)
            delta       = target_pct - current_pct   # positivo → comprar, negativo → vender

            if abs(delta) < self._drift:
                self._log.debug(
                    "skip {} | delta={:.2%} < drift={:.2%}",
                    symbol, abs(delta), self._drift,
                )
                continue

            if abs(delta) < self._min_delta:
                self._log.debug(
                    "skip {} | delta={:.2%} < min_delta={:.2%}",
                    symbol, abs(delta), self._min_delta,
                )
                continue

            action = "buy" if delta > 0 else "sell"
            signals.append(RebalanceSignal(
                symbol       = symbol,
                action       = action,
                delta_pct    = abs(delta),
                current_pct  = current_pct,
                target_pct   = target_pct,
                generated_at = now,
            ))
            self._log.info(
                "Señal rebalanceo | {} {} delta={:+.2%} ({:.2%} → {:.2%}) trigger={}",
                action.upper(), symbol, delta, current_pct, target_pct, trigger,
            )

        # Ordenar por magnitud descendente — ajustar primero los más desviados
        signals.sort(key=lambda s: s.delta_pct, reverse=True)

        self._log.info(
            "Rebalanceo calculado | trigger={} señales={} símbolos_evaluados={}",
            trigger, len(signals), len(all_symbols),
        )
        return signals
