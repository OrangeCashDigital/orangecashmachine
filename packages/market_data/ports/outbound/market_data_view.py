# -*- coding: utf-8 -*-
"""
market_data/ports/outbound/market_data_view.py
===============================================

Puerto OUTBOUND: vista de lectura pública del order book L2 reconstruido
(ADR-0028 §3.6, D4).

Responsabilidad
---------------
Exponer la frontera PÚBLICA de lectura del BookBuilder sin convertir el
estado interno (``_BookState``) en API pública. Los consumidores externos
(trading/portfolio/adapters inbound) dependen de este contrato, NUNCA de
``market_data.application.processing.book_builder`` ni de
``market_data.infrastructure.kafka.book_builder_consumer`` (BC-56).

Este módulo define:
  MarketDataViewPort — Protocol de lectura: mid_price, best_bid, best_ask,
                       spread, depth, is_ready, stale_threshold_ms.

Frontera de dependencias
------------------------
Port outbound (driven) — BC-04: dependen solo del dominio propio y stdlib.
Implementado por: market_data.application.processing.book_builder.BookBuilder
  (el use case ES la proyección del estado; un adapter en adapters/outbound
  importaría application → violaría BC-06).
Consumido por:     B-MD-004 (market validity, Fase 2) desde trading/risk.

Semántica de readiness (ND1 + C1 del ADR-0028)
----------------------------------------------
  is_ready = has_snapshot AND not_stale
    - has_snapshot : snapshot válido aplicado (True tras on_snapshot).
    - not_stale    : ahora - last_update_at_ms <= stale_threshold_ms.
  Falta de snapshot o book ausente ⇒ is_ready=False. Sin segundo source
  of truth de gaps: un gap resetea BookState ⇒ has_snapshot=False.

Formato de valores
------------------
Los precios/mid/spread se devuelven como ``str`` en punto fijo (D-7c:
Decimal preservado, sin notación científica) — misma convención del wire
v2 y de BookBuilderOutcome. ``depth`` es el nº de niveles retenidos en el
estado interno. ``None`` si el libro no está disponible (sin snapshot).

Principios: DIP · ISP · SRP · BC-04 · BC-56 · contrato fuerte de tipos
"""

from __future__ import annotations

from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class MarketDataViewPort(Protocol):
    """
    Vista de lectura del order book L2 reconstruido (schema v2).

    Implementado por: market_data.application.processing.book_builder.BookBuilder
    Consumido por   : B-MD-004 (trading/risk, Fase 2) — sin consumidor en Fase 1.

    Todos los métodos devuelven ``None`` si el libro no existe o no tiene
    snapshot aplicado aún (fail-closed). ``is_ready`` es la compuerta que un
    consumidor debe consultar ANTES de usar mid/spread/bid/ask (C1).
    """

    @property
    def stale_threshold_ms(self) -> int:
        """Umbral de staleness configurado (ms) para evaluar not_stale."""
        ...

    def mid_price(self, exchange: str, symbol: str) -> Optional[str]: ...
    def best_bid(self, exchange: str, symbol: str) -> Optional[str]: ...
    def best_ask(self, exchange: str, symbol: str) -> Optional[str]: ...
    def spread(self, exchange: str, symbol: str) -> Optional[str]: ...
    def depth(self, exchange: str, symbol: str) -> Optional[int]: ...

    def is_ready(self, exchange: str, symbol: str, now_ms: int) -> bool:
        """has_snapshot AND not_stale (C1). now_ms se pasa explícito (testeable)."""
        ...


__all__ = [
    "MarketDataViewPort",
]
