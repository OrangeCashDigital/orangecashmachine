from __future__ import annotations

"""
core/observability
==================

Sistema de observabilidad centralizado para OrangeCashMachine.

Exports públicos
----------------
bootstrap_logging     — Fase 1, antes de AppConfig. Idempotente.
configure_logging     — Fase 2, con LoggingConfig validado. Hash-guarded.
bind_pipeline         — Logger con contexto de pipeline pre-enlazado.
is_logging_configured — True si el sistema está completamente inicializado.
InterceptHandler      — Bridge stdlib → loguru (útil para tests).
setup_logging         — DEPRECATED v0.2.0 — lanza RuntimeError siempre.

Uso
---
    from core.observability import bootstrap_logging, configure_logging, bind_pipeline

    bootstrap_logging(debug=True, run_id="abc123", env="development")
    configure_logging(cfg=app_config.observability.logging, env="production")

    log = bind_pipeline("ohlcv_fetcher", exchange="bybit", dataset="ohlcv")
    log.info("fetch_started", symbol="BTC/USDT")
"""

from core.observability.logger import (
    bootstrap_logging,
    configure_logging,
    bind_pipeline,
    is_logging_configured,
    InterceptHandler,
    setup_logging,       # deprecated — RuntimeError en v0.2.0+
)

__all__ = [
    "bootstrap_logging",
    "configure_logging",
    "bind_pipeline",
    "is_logging_configured",
    "InterceptHandler",
    "setup_logging",
]
