# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/rest
==================================

Adapters REST inbound — fuentes de datos via HTTP/REST.

Landscape de fuentes REST
--------------------------

  ┌──────────────────────────────────────────────────────────────────┐
  │  Fuente                  │ TradeSource     │ Estado              │
  ├──────────────────────────┼─────────────────┼─────────────────────┤
  │  TradesBackfillFetcher   │ REST_BACKFILL   │ ✅ activo           │
  │  GapRecoveryFetcher      │ REST_RECOVERY   │ ✅ activo           │
  │  OHLCVFetcher            │ —               │ ✅ activo           │
  │  DerivativesFetcher      │ —               │ ✅ activo           │
  │  TradesFetcher           │ —               │ ✅ activo (storage) │
  └──────────────────────────┴─────────────────┴─────────────────────┘

Semántica SSOT de TradeSource
-------------------------------
  REST_BACKFILL → TradesBackfillFetcher  (bootstrap histórico acotado)
  REST_RECOVERY → GapRecoveryFetcher     (gap acotado — WS caído)
  WS            → WSTradesSource / GapAwareStream (live — fuente canónica)

Nota: RESTTradesPoller fue eliminado en Paso 5/Fase 3.
Migración: RESTTradesPoller → GapAwareStream(WSTradesSource, recovery_factory=...)
Ver: market_data/adapters/inbound/websocket/gap_aware_stream.py

Principios: DIP · ACL · Kappa · SSOT · SafeOps
"""

from market_data.adapters.inbound.rest.gap_recovery_fetcher import (
    GapRecoveryFetcher,
)
from market_data.adapters.inbound.rest.trades_backfill_fetcher import (
    TradesBackfillFetcher,
)

__all__ = [
    "TradesBackfillFetcher",
    "GapRecoveryFetcher",
]
