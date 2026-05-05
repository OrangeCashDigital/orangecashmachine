# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/candle.py
============================================

Candle — Value Object canónico de una vela OHLCV.

Principios
----------
DDD        — VO puro: inmutable, definido por valor, sin identidad de negocio
             Dos Candle con los mismos campos son equivalentes (frozen=True)
KISS       — solo los campos que el dominio necesita; sin lógica de infra
Fail-Fast  — is_valid() expone invariantes de dominio; adapters validan en ACL
No pandas  — el dominio no depende de infraestructura de análisis
No CCXT    — el dominio no conoce el wire format externo
"""
from __future__ import annotations

from dataclasses import dataclass


# ===========================================================================
# Candle — Value Object
# ===========================================================================

@dataclass(frozen=True)
class Candle:
    """
    Una vela OHLCV — unidad atómica de datos de mercado.

    Immutable (frozen=True): los precios históricos no se modifican.
    Equality por valor: dos Candle con los mismos campos son el mismo dato.

    Campos
    ------
    timestamp_ms : Unix epoch en milisegundos UTC — inicio del período
    open         : precio de apertura
    high         : precio máximo del período
    low          : precio mínimo del período
    close        : precio de cierre
    volume       : volumen negociado en el período

    Invariantes de dominio
    ----------------------
    low  ≤ open  ≤ high
    low  ≤ close ≤ high
    low  ≤ high  (trivialmente implícito)
    volume ≥ 0
    timestamp_ms > 0
    """
    timestamp_ms: int
    open:         float
    high:         float
    low:          float
    close:        float
    volume:       float

    # ----------------------------------------------------------
    # Domain invariants
    # ----------------------------------------------------------

    def is_valid(self) -> bool:
        """
        Verifica que la vela cumple las invariantes de dominio OHLCV.

        Checks
        ------
        • low ≤ open ≤ high
        • low ≤ close ≤ high
        • volume ≥ 0
        • timestamp_ms > 0

        Returns True si todos los checks pasan, False en cualquier violación.
        No lanza — el caller decide qué hacer con velas inválidas (QualityLabel).
        """
        if self.timestamp_ms <= 0:
            return False
        if self.volume < 0:
            return False
        if not (self.low <= self.open <= self.high):
            return False
        if not (self.low <= self.close <= self.high):
            return False
        return True

    def is_bullish(self) -> bool:
        """True si el precio de cierre supera el de apertura."""
        return self.close > self.open

    def is_bearish(self) -> bool:
        """True si el precio de cierre es inferior al de apertura."""
        return self.close < self.open

    @property
    def body_size(self) -> float:
        """Tamaño del cuerpo de la vela (|close - open|)."""
        return abs(self.close - self.open)

    @property
    def wick_upper(self) -> float:
        """Mecha superior (high - max(open, close))."""
        return self.high - max(self.open, self.close)

    @property
    def wick_lower(self) -> float:
        """Mecha inferior (min(open, close) - low)."""
        return min(self.open, self.close) - self.low

    # ----------------------------------------------------------
    # Conversions (ACL boundary — solo para adapters)
    # ----------------------------------------------------------

    def to_tuple(self) -> tuple[int, float, float, float, float, float]:
        """
        Serializa a tupla canónica (timestamp_ms, o, h, l, c, v).

        Usado por adapters y consumers que necesitan wire format.
        El dominio nunca consume este método internamente.
        """
        return (
            self.timestamp_ms,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
        )

    @classmethod
    def from_tuple(
        cls,
        t: tuple[int, float, float, float, float, float],
    ) -> "Candle":
        """
        Construye desde tupla canónica (timestamp_ms, o, h, l, c, v).

        Fail-Fast: lanza ValueError si la tupla no tiene exactamente 6 elementos.
        Usado en el ACL de adapters inbound (normalizer).
        """
        if len(t) != 6:
            raise ValueError(
                f"Candle.from_tuple esperaba 6 elementos, recibió {len(t)}: {t!r}"
            )
        return cls(
            timestamp_ms = int(t[0]),
            open         = float(t[1]),
            high         = float(t[2]),
            low          = float(t[3]),
            close        = float(t[4]),
            volume       = float(t[5]),
        )


# ===========================================================================
# __all__
# ===========================================================================

__all__ = ["Candle"]
