# -*- coding: utf-8 -*-
"""
shared/kafka/schemas/signals.py
================================

Wire payloads para el dominio de señales de trading.

Topología
---------
  StrategyConsumer → [signals.raw]    → SignalPayload
  RiskGateConsumer → [signals.approved] → ApprovedSignalPayload
  RiskGateConsumer → [signals.rejected] → RejectedSignalPayload

Separación dominio / wire
--------------------------
  shared/types/signal.py → Signal (value object interno)
  shared/kafka/schemas/signals.py → SignalPayload (wire, cruza BC boundary)

Routing key para signals
------------------------
  "{exchange}:{symbol}" — sin timeframe.
  Una señal BTC/USDT es la misma decisión sin importar el timeframe
  que la originó. Ordering guarantee por símbolo.

Retention policy (aplicar en Kafka admin)
-----------------------------------------
  signals.raw      → delete, retention.ms = 86400000   (1 día)
  signals.approved → delete, retention.ms = 86400000   (1 día)
  signals.rejected → delete, retention.ms = 604800000  (7 días, auditoría)

Schema version history
----------------------
  v1 (actual) — campos base del ciclo de vida de señales.

Principios: SSOT · DDD · Fail-Fast · KISS
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

from shared.kafka.schemas._base import BasePayload


SIGNAL_SCHEMA_VERSION:   int = 1
APPROVED_SCHEMA_VERSION: int = 1
REJECTED_SCHEMA_VERSION: int = 1

SignalDirection = Literal["buy", "sell", "hold"]


class SignalSchemaVersionError(ValueError):
    """Schema version incompatible en SignalPayload.from_dict()."""


# ---------------------------------------------------------------------------
# SignalPayload — señal cruda de estrategia → signals.raw
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SignalPayload(BasePayload):
    """
    Señal de trading generada por una estrategia.

    Publicado a: signals.raw
    Consumido por: RiskGateConsumer

    Campos
    ------
    exchange   : exchange de origen
    symbol     : par normalizado (e.g. "BTC/USDT")
    timeframe  : timeframe de la estrategia que generó la señal
    direction  : "buy" | "sell" | "hold"
    price      : precio al momento de la señal
    confidence : confianza ∈ [0.0, 1.0]
    strategy   : nombre de la estrategia emisora
    run_id     : correlación con el run
    meta       : datos adicionales de la estrategia
    """
    exchange:   str                      = ""
    symbol:     str                      = ""
    timeframe:  str                      = ""
    direction:  SignalDirection          = "hold"
    price:      float                    = 0.0
    confidence: float                    = 1.0
    strategy:   str                      = ""
    run_id:     str                      = ""
    meta:       Optional[Dict[str, Any]] = None

    @property
    def is_actionable(self) -> bool:
        """True si la señal requiere acción (buy o sell)."""
        return self.direction in ("buy", "sell")

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "event_version": SIGNAL_SCHEMA_VERSION,
            "exchange":      self.exchange,
            "symbol":        self.symbol,
            "timeframe":     self.timeframe,
            "direction":     self.direction,
            "price":         self.price,
            "confidence":    self.confidence,
            "strategy":      self.strategy,
            "run_id":        self.run_id,
            "meta":          self.meta,
        })
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SignalPayload":
        version = int(data.get("event_version", 1))
        if version != SIGNAL_SCHEMA_VERSION:
            raise SignalSchemaVersionError(
                f"SignalPayload schema v{version} incompatible "
                f"con v{SIGNAL_SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id      = str(data["event_id"]),
            event_version = version,
            occurred_at   = str(data.get("occurred_at", "")),
            exchange      = str(data["exchange"]),
            symbol        = str(data["symbol"]),
            timeframe     = str(data["timeframe"]),
            direction     = data["direction"],
            price         = float(data["price"]),
            confidence    = float(data.get("confidence", 1.0)),
            strategy      = str(data.get("strategy", "")),
            run_id        = str(data.get("run_id", "")),
            meta          = data.get("meta"),
        )


# ---------------------------------------------------------------------------
# ApprovedSignalPayload — señal aprobada por RiskGate → signals.approved
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ApprovedSignalPayload(BasePayload):
    """
    Señal aprobada por el RiskGate. Lista para ejecución.

    Publicado a: signals.approved
    Consumido por: ExecutionConsumer → OMS

    Campos adicionales vs SignalPayload
    ------------------------------------
    approved_size_pct : tamaño aprobado por risk (puede diferir del original)
    risk_score        : puntuación de riesgo asignada ∈ [0.0, 1.0]
    original_event_id : event_id del SignalPayload que originó esta aprobación
    """
    exchange:          str                      = ""
    symbol:            str                      = ""
    timeframe:         str                      = ""
    direction:         SignalDirection          = "hold"
    price:             float                    = 0.0
    confidence:        float                    = 1.0
    strategy:          str                      = ""
    approved_size_pct: float                    = 0.0
    risk_score:        float                    = 0.0
    original_event_id: str                      = ""
    run_id:            str                      = ""
    meta:              Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "event_version":     APPROVED_SCHEMA_VERSION,
            "exchange":          self.exchange,
            "symbol":            self.symbol,
            "timeframe":         self.timeframe,
            "direction":         self.direction,
            "price":             self.price,
            "confidence":        self.confidence,
            "strategy":          self.strategy,
            "approved_size_pct": self.approved_size_pct,
            "risk_score":        self.risk_score,
            "original_event_id": self.original_event_id,
            "run_id":            self.run_id,
            "meta":              self.meta,
        })
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ApprovedSignalPayload":
        version = int(data.get("event_version", 1))
        if version != APPROVED_SCHEMA_VERSION:
            raise SignalSchemaVersionError(
                f"ApprovedSignalPayload schema v{version} incompatible "
                f"con v{APPROVED_SCHEMA_VERSION} esperada."
            )
        return cls(
            event_id          = str(data["event_id"]),
            event_version     = version,
            occurred_at       = str(data.get("occurred_at", "")),
            exchange          = str(data["exchange"]),
            symbol            = str(data["symbol"]),
            timeframe         = str(data["timeframe"]),
            direction         = data["direction"],
            price             = float(data["price"]),
            confidence        = float(data.get("confidence", 1.0)),
            strategy          = str(data.get("strategy", "")),
            approved_size_pct = float(data.get("approved_size_pct", 0.0)),
            risk_score        = float(data.get("risk_score", 0.0)),
            original_event_id = str(data.get("original_event_id", "")),
            run_id            = str(data.get("run_id", "")),
            meta              = data.get("meta"),
        )


# ---------------------------------------------------------------------------
# RejectedSignalPayload — señal rechazada → signals.rejected
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RejectedSignalPayload(BasePayload):
    """
    Señal rechazada por el RiskGate. Para auditoría y alerting.

    Publicado a: signals.rejected
    Consumido por: observability, alerting
    """
    exchange:          str = ""
    symbol:            str = ""
    direction:         SignalDirection = "hold"
    reason:            str = ""
    original_event_id: str = ""
    run_id:            str = ""

    def to_dict(self) -> Dict[str, Any]:
        base = super().to_dict()
        base.update({
            "event_version":     REJECTED_SCHEMA_VERSION,
            "exchange":          self.exchange,
            "symbol":            self.symbol,
            "direction":         self.direction,
            "reason":            self.reason,
            "original_event_id": self.original_event_id,
            "run_id":            self.run_id,
        })
        return base

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RejectedSignalPayload":
        return cls(
            event_id          = str(data["event_id"]),
            event_version     = int(data.get("event_version", 1)),
            occurred_at       = str(data.get("occurred_at", "")),
            exchange          = str(data["exchange"]),
            symbol            = str(data["symbol"]),
            direction         = data.get("direction", "hold"),
            reason            = str(data.get("reason", "")),
            original_event_id = str(data.get("original_event_id", "")),
            run_id            = str(data.get("run_id", "")),
        )


__all__ = [
    "SIGNAL_SCHEMA_VERSION",
    "APPROVED_SCHEMA_VERSION",
    "REJECTED_SCHEMA_VERSION",
    "SignalDirection",
    "SignalSchemaVersionError",
    "SignalPayload",
    "ApprovedSignalPayload",
    "RejectedSignalPayload",
]
