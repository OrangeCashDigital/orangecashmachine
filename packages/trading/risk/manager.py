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

F2 — Exposición económica real (ADR-0025/0026/0027)
----------------------------------------------------
  - La cantidad económica canónica es la de la posición ejecutada real
    (Position.quantity, SSOT; el espejo se alimenta desde los fills del OMS).
    Risk NUNCA usa signal.quantity/requested/size_pct/capital×size_pct como
    sustitutos de la exposición de una posición ya existente (INV-F2-01/04).
  - `_positions[symbol] = (quantity, avg_entry)` es el espejo económico real:
    quantity es la ejecutada real y avg_entry el weighted average cost
    (ADR-0025/F4b). La exposición se computa como Σ quantity × avg_entry
    (cost basis) — el precio de valoración MARK/LAST es F9 (UNKNOWN, no
    implementado); aquí se usa el coste medio de entrada, la fuente válida
    existente (mismo concepto que PositionSnapshot.cost_basis).
  - POSITION COUNT ≠ POSITION QUANTITY ≠ POSITION NOTIONAL (INV-F2-03):
    `_open_positions` es el número de posiciones HELD (contado por apertura
    BUY), no la cantidad ni el notional. Un cierre parcial (remaining > 0)
    reduce la cantidad/exposición pero NO cierra la posición: el conteo solo
    decrementa en cierre completo (INV-F2-02) — ver record_close().
  - UNKNOWN ≠ ZERO (INV-F2-06): una cantidad/precio desconocido no se
    convierte silenciosamente en 0 para calcular exposición financiera. Las
    posiciones con precio UNKNOWN se contabilizan como `positions_unknown_price`
    y se EXCLUYEN de exposure_usd (su exposición no es 0: es indeterminada).
  - Reducción/cierre (SELL): las reglas de ENTRADA (max_open_positions,
    min/max_order_usd sobre capital×size_pct) aplican SOLO a aperturas. Una
    reducción no se rechaza por sizing de una nueva posición (INV-F2-05) — la
    cantidad económica la gobierna la posición (F1, clamp INV-08), no la
    señal. Un SELL de stop-loss nunca se trata como apertura.

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

from shared.contracts.boundaries import (
    SignalProtocol,
)  # DIP — risk depende de abstraccion
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

    approved: bool
    reason: str = ""
    size_pct: float = 0.0  # % del capital a asignar (0.0 si rechazado)

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
        config: Optional[RiskConfig] = None,
        capital_usd: float = 10_000.0,
    ) -> None:
        self._config = config or RiskConfig()
        self._capital_usd = capital_usd
        self._lock = threading.Lock()

        # Estado mutable — SIEMPRE mutado bajo _lock
        self._open_positions: int = 0
        self._daily_pnl_usd: float = 0.0
        self._total_pnl_usd: float = 0.0
        self._halted: bool = False
        self._halt_reason: str = ""

        # F2 — espejo económico real: symbol → (quantity, avg_entry|None).
        # quantity = cantidad ejecutada real; avg_entry = weighted average
        # cost (ADR-0025/F4b); None si el precio del fill es UNKNOWN
        # (INV-F2-06: la exposición de esa parte es indeterminada, no 0).
        # Lo alimenta el OMS desde los fills reales (mismo flujo que su
        # espejo local `_entry_positions`) — risk no importa execution ni
        # portfolio (BC-12/BC-13), solo recibe hechos por push.
        self._positions: dict[str, tuple[float, Optional[float]]] = {}

        self._pnl_unknown_closes: int = 0  # closures con P&L UNKNOWN (UNKNOWN ≠ ZERO)

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

    def record_position(
        self,
        symbol: str,
        quantity: Optional[float],
        avg_entry: Optional[float] = None,
    ) -> None:
        """
        F2 — actualiza la posición económica real (espejo) desde un fill.

        quantity : cantidad ejecutada real (Position.quantity, INV-F2-01).
                   None o ≤ 0 → la posición se elimina del espejo (cerrada).
        avg_entry: weighted average cost (ADR-0025); None o ≤ 0 → el precio
                   es UNKNOWN (INV-F2-06): la cantidad se mantiene, pero su
                   exposición no se computa (se reporta en
                   positions_unknown_price, nunca como 0).

        UNKNOWN ≠ ZERO: una cantidad desconocida NO se inventa (no se llama
        con un valor fabricado); un precio desconocido NO produce exposición 0.
        """
        with self._lock:
            if quantity is None or quantity <= 0.0:
                self._positions.pop(symbol, None)
            else:
                price = avg_entry if avg_entry is not None and avg_entry > 0 else None
                self._positions[symbol] = (quantity, price)

    def record_close(
        self,
        pnl_usd: Optional[float] = None,
        *,
        close_position: bool = True,
    ) -> None:
        """
        Registra cierre de posición con P&L monetario (USD).

        pnl_usd > 0 → ganancia, < 0 → pérdida.
        Si pnl_usd es None: P&L UNKNOWN (UNKNOWN ≠ ZERO) — no contribuye
        al drawdown acumulado, pero se cuenta en `_pnl_unknown_closes`.

        F2 (INV-F2-02/03) — close_position=False (cierre PARCIAL): el P&L y
        el drawdown se registran (o no, si es UNKNOWN), pero el conteo de
        posiciones HELD NO decrementa — la posición sigue abierta (remaining > 0).
        Solo un cierre COMPLETO (close_position=True, default) reduce el conteo.

        UNKNOWN ≠ ZERO: una pérdida/contribución desconocida no se convierte
        silenciosamente en 0 para calcular exposición financiera o drawdown.
        """
        with self._lock:
            if close_position:
                self._open_positions = max(0, self._open_positions - 1)
            if pnl_usd is None:
                self._pnl_unknown_closes += 1
            else:
                self._daily_pnl_usd += pnl_usd
                self._total_pnl_usd += pnl_usd
            # Verificación atómica — misma sección crítica que la mutación
            self._check_drawdown_halt_locked()

    def halt(self, reason: str) -> None:
        """Haltea el trading manualmente. Idempotente."""
        with self._lock:
            self._halt_locked(reason)

    def revert_open(self) -> None:
        """Decrementa el conteo de posiciones HELD sin registrar P&L
        (BUY rechazado/cancelado). No afecta a los contadores de drawdown."""
        with self._lock:
            self._open_positions = max(0, self._open_positions - 1)
            # No se toca _daily/_total_pnl_usd ni _pnl_unknown_closes

    def reset_daily(self) -> None:
        """Resetea contadores diarios (llamar al inicio de cada sesión)."""
        with self._lock:
            self._daily_pnl_usd = 0.0

    def reset_total(self) -> None:
        """
        Resetea todos los contadores de P&L y elimina el halt.

        Usar con precaución — solo para reinicio de sesión completa.
        """
        with self._lock:
            self._daily_pnl_usd = 0.0
            self._total_pnl_usd = 0.0
            self._halted = False
            self._halt_reason = ""
            self._pnl_unknown_closes = 0
            self._log.info("RiskManager: contadores reseteados")

    @property
    def is_halted(self) -> bool:
        return self._halted

    @property
    def open_positions(self) -> int:
        return self._open_positions

    def quantity(self, symbol: str) -> Optional[float]:
        """
        F2 — cantidad económica real de un símbolo (Position.quantity).

        None si el símbolo no está en el espejo (posible: sin posición,
        cantidad UNKNOWN o precio UNKNOWN con cantidad registrada — usar
        `positions_unknown_price` para distinguir).
        """
        with self._lock:
            entry = self._positions.get(symbol)
            return entry[0] if entry is not None else None

    @property
    def exposure_usd(self) -> float:
        """
        F2 — exposición económica = Σ quantity × avg_entry (cost basis).

        Solo posiciones con precio conocido (avg_entry). Las posiciones con
        precio UNKNOWN NO aportan 0 — quedan fuera y se reportan en
        `positions_unknown_price` (INV-F2-06: UNKNOWN ≠ ZERO). El precio de
        valoración es el coste medio de entrada (ADR-0025); MARK/LAST es F9.
        """
        with self._lock:
            return sum(qty * avg for qty, avg in self._positions.values() if avg is not None and avg > 0)

    @property
    def positions_unknown_price(self) -> int:
        """Nº de posiciones con cantidad real pero precio UNKNOWN (INV-F2-06)."""
        with self._lock:
            return sum(1 for _, avg in self._positions.values() if avg is None)

    def state(self) -> dict:
        """Estado observable para logging y métricas. SafeOps: nunca lanza."""
        try:
            with self._lock:
                # Cálculo inline bajo el lock ya tomado — `exposure_usd` y
                # `positions_unknown_price` son propiedades que re-adquieren el
                # lock (no reentrante): no llamarlas aquí dentro (deadlock).
                exposure = sum(qty * avg for qty, avg in self._positions.values() if avg is not None and avg > 0)
                unknown_price = sum(1 for _, avg in self._positions.values() if avg is None)
                return {
                    "halted": self._halted,
                    "halt_reason": self._halt_reason,
                    "open_positions": self._open_positions,
                    "daily_pnl_usd": round(self._daily_pnl_usd, 4),
                    "total_pnl_usd": round(self._total_pnl_usd, 4),
                    "capital_usd": self._capital_usd,
                    # F2 — exposición económica real (cantidad ejecutada × avg)
                    "exposure_usd": round(exposure, 2),
                    "positions_unknown_price": unknown_price,
                    "quantity_by_symbol": {symbol: round(qty, 8) for symbol, (qty, _) in self._positions.items()},
                }
        except Exception:
            return {"halted": False, "halt_reason": "", "open_positions": 0}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_internal(self, signal: SignalProtocol) -> RiskDecision:
        cfg = self._config

        # F2 — distinguir APERTURA (BUY) de REDUCCIÓN/CIERRE (SELL).
        # Las reglas de ENTRADA (max_open_positions, min/max_order_usd sobre
        # capital×size_pct) aplican SOLO a aperturas: una reducción reduce la
        # exposición económica real y no debe rechazarse por sizing de una
        # nueva posición (INV-F2-05). La cantidad económica de un SELL la
        # gobierna la posición (F1, clamp INV-08), no la señal — un SELL de
        # stop-loss nunca se trata como apertura (TEST 7).
        side = getattr(signal, "direction", None)
        is_reduction = side == "sell"

        # Leer estado consistente bajo lock — atómico
        with self._lock:
            halted = self._halted
            halt_r = self._halt_reason
            opens = self._open_positions

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

        # 4 — posiciones abiertas (solo aperturas)
        if not is_reduction and opens >= cfg.position.max_open_positions:
            return RiskDecision(
                False,
                f"max_positions_reached:{opens}/{cfg.position.max_open_positions}",
            )

        # 5 & 6 — drawdown: verificar Y haltear atómicamente bajo lock
        if cfg.drawdown.halt_on_breach:
            with self._lock:
                if (
                    self._capital_usd > 0
                    and self._daily_pnl_usd / self._capital_usd <= -cfg.drawdown.max_daily_drawdown_pct
                ):
                    reason = f"daily_drawdown_breached:{self._daily_pnl_usd / self._capital_usd:.2%}"
                    self._halt_locked(reason)
                    return RiskDecision(False, reason)
                if (
                    self._capital_usd > 0
                    and self._total_pnl_usd / self._capital_usd <= -cfg.drawdown.max_total_drawdown_pct
                ):
                    reason = f"total_drawdown_breached:{self._total_pnl_usd / self._capital_usd:.2%}"
                    self._halt_locked(reason)
                    return RiskDecision(False, reason)

        # 7 — sizing en USD (solo aperturas). Una reducción se exime del
        # min/max de orden de ENTRADA: su notional lo define la posición, no
        # capital×size_pct (INV-F2-05 / TEST 6). El size_pct retornado es el
        # de asignación de apertura; para SELL el executor usa order.quantity.
        size_pct = cfg.position.max_position_pct
        if not is_reduction:
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
            self._halted = True
            self._halt_reason = reason
            self._log.warning("HALT activado | reason={}", reason)

    def _check_drawdown_halt_locked(self) -> None:
        """
        Verifica drawdown y haltea si es necesario.

        PRECONDICIÓN: llamar DENTRO de self._lock (desde record_close).
        DRY: delega a _halt_locked para mutación real.

        El drawdown se calcula como P&L USD normalizado sobre capital:
            _daily_pnl_usd / capital_usd ≤ -max_daily_drawdown_pct
            _total_pnl_usd / capital_usd ≤ -max_total_drawdown_pct
        """
        cfg = self._config
        if not cfg.drawdown.halt_on_breach:
            return
        if self._capital_usd > 0 and self._daily_pnl_usd / self._capital_usd <= -cfg.drawdown.max_daily_drawdown_pct:
            self._halt_locked(f"daily_drawdown:{self._daily_pnl_usd / self._capital_usd:.2%}")
        if self._capital_usd > 0 and self._total_pnl_usd / self._capital_usd <= -cfg.drawdown.max_total_drawdown_pct:
            self._halt_locked(f"total_drawdown:{self._total_pnl_usd / self._capital_usd:.2%}")
