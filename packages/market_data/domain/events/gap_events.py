# -*- coding: utf-8 -*-
"""
market_data/domain/events/gap_events.py
========================================

Eventos de ciclo de vida de gaps OHLCV — control plane del pipeline.

Responsabilidad
---------------
Modelar los tres estados posibles de un gap detectado en Silver:
  GapDetectedEvent  — hueco encontrado, requiere reparación
  GapHealedEvent    — reparado exitosamente (total o parcial)
  GapFailedEvent    — reparación fallida (transiente o permanente)

Arquitectura
------------
Estos son value objects de dominio puro.
No importan nada de infrastructure, Kafka, Redis ni adapters.
El dominio declara QUÉ ocurrió; el adapter decide CÓMO publicarlo.

Control plane vs data plane
----------------------------
Estos eventos NO son datos de mercado — son telemetría del sistema:
  DATA plane  → NormalizedTrade, OHLCV bars (market.trades.raw)
  STATE plane → Iceberg Silver/Gold (persistencia, no streaming)
  CONTROL plane → gap_events (market.gaps) ← ESTO

Semántica de event_id
---------------------
UUID4 generado en el site de emisión (RepairStrategy).
Garantiza idempotencia en el consumer Kafka (dedup por event_id).

Regla de invariante (DDD)
-------------------------
gap_event_id en GapHealedEvent / GapFailedEvent referencia el
event_id del GapDetectedEvent padre. Permite reconstruir el
ciclo de vida completo de cualquier gap sin necesidad de estado.

Principios
----------
DDD    — value objects inmutables, semántica explícita
SSOT   — definición única de los eventos del control plane
DIP    — sin imports de infrastructure
KISS   — frozen dataclasses, sin lógica de serialización aquí
"""

from __future__ import annotations

from dataclasses import dataclass

# ── GapDetectedEvent ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class GapDetectedEvent:
    """
    Hueco temporal detectado en Silver para un par (symbol, timeframe).

    Emitido por: RepairStrategy.execute_pair (post scan_gaps)
    Consumer:    KafkaGapPublisher → topic market.gaps

    Campos
    ------
    event_id     : UUID4 — identificador único de este evento.
    symbol       : par de trading normalizado (e.g. "BTC/USDT").
    timeframe    : intervalo canónico (e.g. "1h", "1m").
    exchange_id  : exchange origen (e.g. "bybit", "kucoin").
    start_ms     : epoch ms del último timestamp antes del hueco.
    end_ms       : epoch ms del primer timestamp después del hueco.
    expected     : velas esperadas en el rango [start_ms, end_ms].
    detected_at_ms : epoch ms de emisión del evento.
    """

    event_id: str
    symbol: str
    timeframe: str
    exchange_id: str
    start_ms: int
    end_ms: int
    expected: int
    detected_at_ms: int

    def duration_ms(self) -> int:
        """Duración del hueco en milisegundos."""
        return self.end_ms - self.start_ms


# ── GapHealedEvent ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class GapHealedEvent:
    """
    Gap reparado — total (fill_ratio >= 0.95) o parcial (< 0.95).

    Emitido por: RepairStrategy._heal_gap (post storage.save_ohlcv)
    Consumer:    KafkaGapPublisher → topic market.gaps

    Campos
    ------
    event_id      : UUID4 de este evento.
    gap_event_id  : event_id del GapDetectedEvent padre (FK lógica).
    rows_written  : velas efectivamente escritas en Silver.
    fill_ratio    : rows_written / expected — [0.0, 1.0+].
                    > 1.0 posible si el exchange entregó más velas.
    healed_at_ms  : epoch ms de escritura en Silver.
    """

    event_id: str
    gap_event_id: str
    rows_written: int
    fill_ratio: float
    healed_at_ms: int

    def is_full_heal(self, threshold: float = 0.95) -> bool:
        """True si la cobertura supera el umbral de sanación completa."""
        return self.fill_ratio >= threshold


# ── GapFailedEvent ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class GapFailedEvent:
    """
    Reparación fallida — sin datos disponibles o error transiente.

    Emitido por: RepairStrategy._heal_gap (rama except)
    Consumer:    KafkaGapPublisher → topic market.gaps

    Campos
    ------
    event_id      : UUID4 de este evento.
    gap_event_id  : event_id del GapDetectedEvent padre (FK lógica).
    reason        : mensaje de error corto (tipo de excepción).
    is_transient  : True → retry viable; False → marcar irrecuperable.
    failed_at_ms  : epoch ms del fallo.
    """

    event_id: str
    gap_event_id: str
    reason: str
    is_transient: bool
    failed_at_ms: int


# ── exports ───────────────────────────────────────────────────────────────────

__all__ = [
    "GapDetectedEvent",
    "GapHealedEvent",
    "GapFailedEvent",
]
