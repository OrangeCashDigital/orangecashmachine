# -*- coding: utf-8 -*-
"""
domain/entities/ohlcv.py
=========================

OHLCVBar — entidad de dominio para una vela OHLCV.

Por qué es entidad y no value object
--------------------------------------
Una barra OHLCV tiene identidad natural: el cuarteto
(exchange, symbol, timeframe, timestamp) la identifica unívocamente.
Dos barras con el mismo OHLCV pero timestamps distintos son entidades
distintas — la identidad importa para dedup y reconciliación.

Contraste con Signal (value object): dos señales buy/BTC/1h/@50k
con timestamps distintos son intercambiables si tienen los mismos valores.
Una barra OHLCV no lo es — el timestamp ES su identidad.

Invariantes de dominio (Fail-Fast en __post_init__)
----------------------------------------------------
- open, high, low, close > 0
- high >= max(open, close) >= min(open, close) >= low   (coherencia OHLC)
- volume >= 0
- timestamp tz-aware UTC
- symbol no vacío
- timeframe es Timeframe válido

Diseño
------
- frozen=True: entidad inmutable tras construcción (SafeOps)
- eq=True (default de frozen): igualdad por valor de todos los campos
  — para igualdad por identidad usar OHLCVBar.identity_key()
- No importa nada de infra ni frameworks — dominio puro

Principios: DDD · SSOT · Fail-Fast · SOLID · KISS
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from domain.value_objects.timeframe import Timeframe


@dataclass(frozen=True)
class OHLCVBar:
    """
    Una vela OHLCV con identidad de dominio.

    Campos obligatorios
    -------------------
    exchange   : nombre canónico del exchange  (e.g. "bybit")
    symbol     : par normalizado               (e.g. "BTC/USDT")
    timeframe  : intervalo temporal            (e.g. Timeframe.H1)
    timestamp  : apertura de la vela, tz-aware UTC
    open       : precio de apertura
    high       : precio máximo
    low        : precio mínimo
    close      : precio de cierre
    volume     : volumen transaccionado

    Campo opcional
    --------------
    quality_flag : "clean" | "suspect" | None — resultado de QualityPipeline

    Identidad
    ---------
    Usar identity_key() para comparar por identidad de negocio
    (exchange, symbol, timeframe, timestamp) sin considerar OHLCV.
    Útil para dedup en ingesta y reconciliación Silver.
    """

    exchange:     str
    symbol:       str
    timeframe:    Timeframe
    timestamp:    datetime
    open:         float
    high:         float
    low:          float
    close:        float
    volume:       float
    quality_flag: Optional[str] = None

    def __post_init__(self) -> None:
        """Fail-Fast: valida todas las invariantes de dominio en construcción."""

        # ── Identidad ─────────────────────────────────────────────────────────
        if not self.exchange:
            raise ValueError("OHLCVBar.exchange no puede estar vacío")
        if not self.symbol:
            raise ValueError("OHLCVBar.symbol no puede estar vacío")
        if not isinstance(self.timeframe, Timeframe):
            raise TypeError(
                f"OHLCVBar.timeframe debe ser Timeframe, recibido: {type(self.timeframe)}"
            )

        # ── Timestamp tz-aware ────────────────────────────────────────────────
        if self.timestamp.tzinfo is None:
            raise ValueError(
                f"OHLCVBar.timestamp debe ser tz-aware (UTC), recibido naive: {self.timestamp}"
            )

        # ── Precios positivos ─────────────────────────────────────────────────
        for field_name, value in (
            ("open",  self.open),
            ("high",  self.high),
            ("low",   self.low),
            ("close", self.close),
        ):
            if value <= 0:
                raise ValueError(
                    f"OHLCVBar.{field_name} debe ser > 0, recibido: {value}"
                )

        # ── Coherencia OHLC — invariante matemática de mercado ────────────────
        if not (self.low <= self.open <= self.high):
            raise ValueError(
                f"Incoherencia OHLC: low={self.low} <= open={self.open} <= high={self.high} "
                f"no se cumple | {self.symbol} {self.timeframe} {self.timestamp}"
            )
        if not (self.low <= self.close <= self.high):
            raise ValueError(
                f"Incoherencia OHLC: low={self.low} <= close={self.close} <= high={self.high} "
                f"no se cumple | {self.symbol} {self.timeframe} {self.timestamp}"
            )

        # ── Volumen no negativo ───────────────────────────────────────────────
        if self.volume < 0:
            raise ValueError(
                f"OHLCVBar.volume debe ser >= 0, recibido: {self.volume}"
            )

        # ── quality_flag — valores aceptados ──────────────────────────────────
        if self.quality_flag is not None and self.quality_flag not in (
            "clean", "suspect"
        ):
            raise ValueError(
                f"OHLCVBar.quality_flag debe ser 'clean', 'suspect' o None, "
                f"recibido: {self.quality_flag!r}"
            )

    # ------------------------------------------------------------------
    # Identidad de negocio
    # ------------------------------------------------------------------

    def identity_key(self) -> tuple[str, str, str, datetime]:
        """
        Clave de identidad de negocio para dedup y reconciliación.

        Retorna (exchange, symbol, timeframe.value, timestamp).
        Usar en sets y dicts cuando se quiere igualdad por identidad,
        no por valor completo de OHLCV.

        Ejemplo
        -------
        seen: set[tuple] = set()
        for bar in bars:
            key = bar.identity_key()
            if key not in seen:
                seen.add(key)
                process(bar)
        """
        return (self.exchange, self.symbol, self.timeframe.value, self.timestamp)

    # ------------------------------------------------------------------
    # Propiedades derivadas
    # ------------------------------------------------------------------

    @property
    def body(self) -> float:
        """Tamaño absoluto del cuerpo de la vela: |close - open|."""
        return abs(self.close - self.open)

    @property
    def upper_wick(self) -> float:
        """Mecha superior: high - max(open, close)."""
        return self.high - max(self.open, self.close)

    @property
    def lower_wick(self) -> float:
        """Mecha inferior: min(open, close) - low."""
        return min(self.open, self.close) - self.low

    @property
    def is_bullish(self) -> bool:
        """True si close > open (vela alcista)."""
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        """True si close < open (vela bajista)."""
        return self.close < self.open

    @property
    def is_doji(self, threshold: float = 0.001) -> bool:
        """
        True si el cuerpo es < 0.1% del precio — indecisión de mercado.

        threshold: fracción de close debajo de la cual se considera doji.
        """
        return self.body < self.close * threshold

    @property
    def is_suspect(self) -> bool:
        """True si quality_flag == 'suspect'."""
        return self.quality_flag == "suspect"

    # ------------------------------------------------------------------
    # Representación
    # ------------------------------------------------------------------

    def __str__(self) -> str:
        return (
            f"OHLCVBar({self.exchange}:{self.symbol}/{self.timeframe}"
            f" {self.timestamp.strftime('%Y-%m-%dT%H:%M')}Z"
            f" O={self.open:.4f} H={self.high:.4f}"
            f" L={self.low:.4f} C={self.close:.4f}"
            f" V={self.volume:.2f})"
        )

    def __repr__(self) -> str:
        return self.__str__()
