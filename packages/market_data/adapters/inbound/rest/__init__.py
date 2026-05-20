# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/rest
==================================

Adapters REST inbound — fuentes de datos via HTTP/REST.

Landscape de fuentes REST
--------------------------

  ┌──────────────────────────────────────────────────────────────────┐
  │  Fuente                  │ Source          │ Estado              │
  ├──────────────────────────┼─────────────────┼─────────────────────┤
  │  TradesBackfillFetcher   │ REST_BACKFILL   │ ✅ activo           │
  │  GapRecoveryFetcher      │ REST_RECOVERY   │ ✅ activo           │
  │  RESTTradesPoller        │ REST_POLLING    │ ⚠️  DEPRECATED      │
  │  OHLCVFetcher            │ —               │ ✅ activo           │
  │  DerivativesFetcher      │ —               │ ✅ activo           │
  │  TradesFetcher           │ —               │ ✅ activo (storage) │
  └──────────────────────────┴─────────────────┴─────────────────────┘

Ruta de migración de RESTTradesPoller
--------------------------------------
  RESTTradesPoller (live polling continuo) →
    GapAwareStream(WSTradesSource, recovery_factory=GapRecoveryFetcher, ...)
  Ver: market_data/adapters/inbound/websocket/gap_aware_stream.py

Semántica SSOT de TradeSource
-------------------------------
  REST_BACKFILL → TradesBackfillFetcher  (histórico acotado)
  REST_RECOVERY → GapRecoveryFetcher     (gap acotado)
  REST_POLLING  → RESTTradesPoller       (live continuo — DEPRECATED)
  WS            → WSTradesSource / GapAwareStream (live — preferido)

Principios: DIP · ACL · Kappa · SSOT · SafeOps
"""
from market_data.adapters.inbound.rest.trades_backfill_fetcher import (
    TradesBackfillFetcher,
)
from market_data.adapters.inbound.rest.gap_recovery_fetcher import (
    GapRecoveryFetcher,
)
from market_data.adapters.inbound.rest.rest_trades_poller import (
    RESTTradesPoller,  # DEPRECATED — ver docstring del módulo
)

__all__ = [
    # ── Activos ─────────────────────────────────────────────────────
    "TradesBackfillFetcher",
    "GapRecoveryFetcher",
    # ── Deprecated — remover tras producción de GapAwareStream ──────
    "RESTTradesPoller",
]
