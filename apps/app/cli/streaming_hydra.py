# -*- coding: utf-8 -*-
"""
app/cli/streaming_hydra.py
==========================

Entrypoint CLI para el canary ORDERBOOK (F2.6b) — proceso independiente de
WebSocket realtime sobre cryptofeed, con observabilidad push existente.

F2.6c queda PREPARADA (no cerrada): la instrumentación de métricas ya vive
en OrderBookKafkaProducer (KafkaMetrics) y este proceso empuja el registro
Prometheus al Pushgateway usando exactamente el mecanismo oficial de OCM
(PrometheusPusher + RuntimeContext.pushgateway). Cerrar F2.6c requiere
medir el canary en ejecución y validar las métricas en Grafana/Pushgateway.

Cadena
------
  CryptofeedOrderBookStream (bybit, L2_BOOK)
      → OrderBookKafkaProducer.on_snapshot / on_delta
      → KafkaMetrics (ocm_kafka_events_*)             ← instrumentación F2.6c
      → PrometheusPusher.push({exchange: "orderbook",
                               gateway: ctx.pushgateway})  ← job ocm_pipeline_orderbook

Por qué no Hydra @main
----------------------
Usa load_appconfig_standalone() (ocm.config.hydra_loader) — mismo patrón que
live_hydra.py/paper_hydra.py — para cargar AppConfig validado sin el decorator
Hydra. Evita exponer DictConfig crudo fuera de ocm.config (BC-51).

Composition Root (BC-38)
------------------------
No se instancia ningún adapter aquí: CompositionRoot.build_ws_producers()
es el único punto de ensamblado de los producers WS. Se usa únicamente
WSProducerBundle.orderbook (canary ORDERBOOK — funding/oi/liquidations
permanecen sin runners hasta su fase).

Lifecycle (ADR-0022)
--------------------
  start:  bundle.start_all() → CryptofeedOrderBookStream.start()
  run:    loop de heartbeat — cada --push-interval segundos empuja el
          registro Prometheus al Pushgateway con exchange="orderbook"
          y gateway=ctx.pushgateway (SSOT: PUSHGATEWAY_URL).
  stop:   asyncio.Event seteado por SIGINT/SIGTERM → stream.stop()
          → bundle.close_all() (siempre en finally).

Uso
---
    uv run streaming                                   # development
    uv run streaming --env production --push-interval 15
    PUSHGATEWAY_URL=localhost:9091 uv run streaming

Exit codes
----------
    0 → shutdown limpio (SIGINT/SIGTERM) o ciclo completado
    1 → error fatal (config inválida, feeds no habilitados, arranque fallido)

Principios: SRP · SafeOps · Composition Root · ADR-0022 · BC-51
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from typing import Any

from loguru import logger

from shared.enums import DATASOURCE_REPLAY

# Identidad del job en el Pushgateway — por diseño distinto del batch
# (ocm_pipeline_local). Único para el canary: ocm_pipeline_orderbook.
_PUSH_EXCHANGE = "orderbook"


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="OrangeCashMachine — Streaming WS ORDERBOOK (canary F2.6b)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--env",
        default=None,
        help=(
            "Entorno Hydra (development|production|test). "
            "None -> cascada estandar (CLI > OCM_ENV > .env > settings.yaml)."
        ),
    )
    p.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help=(
            "Símbolos cryptofeed a subscribir (ej. BTC-USDT-PERP). "
            "Default: config.feeds.feeds.<exchange>.symbols (SSOT)."
        ),
    )
    p.add_argument(
        "--exchange",
        default="bybit",
        help="Slug del exchange WS (bybit|kucoin). Default: bybit.",
    )
    p.add_argument(
        "--push-interval",
        type=float,
        default=15.0,
        dest="push_interval",
        help="Segundos entre pushes de heartbeat al Pushgateway (F2.6c).",
    )
    p.add_argument("--debug", action="store_true", help="Log nivel DEBUG")
    return p


def _setup_logging(debug: bool) -> None:
    # R14/AUDIT-H8: scaffolding de CLI en SOLA fuente — app/cli/_bootstrap.py.
    # No re-inlinear logger.remove(); usar el setup compartido.
    from app.cli._bootstrap import setup_logging

    setup_logging(debug=debug, color="green")


def _load_config(env: str | None, *, run_id: str | None = None):
    """Carga AppConfig validado sin contexto Hydra (BC-51).

    Args:
        env:    Entorno activo (development|production|test).
        run_id: ID del run (SSOT RunConfig) — obligatorio en producción, donde
                load_appconfig_standalone escribe snapshot de auditoría y
                falla sin él (hydra_loader.py:263).
    """
    from ocm.config.hydra_loader import load_appconfig_standalone
    from ocm.config.loader.exceptions import (
        ConfigurationError,
        ConfigValidationError,
    )

    try:
        return load_appconfig_standalone(env=env, run_id=run_id)
    except (ConfigurationError, ConfigValidationError) as exc:
        logger.opt(exception=True).critical("config_load_failed | {}", exc)
        return None


def _build_pusher(config: Any):
    """PrometheusPusher si metrics.enabled, NoopPusher si no. Fail-Soft.

    Reutiliza exactamente el mecanismo de apps/app/cli/main.py — no se
    instancia MetricsRuntime, no se toca MetricsConfig (F2.6c).
    """
    from ocm.observability.pushers import NoopPusher, PrometheusPusher

    if config.observability.metrics.enabled:
        return PrometheusPusher()
    logger.warning("observability.metrics.enabled=false — push metrics disabled")
    return NoopPusher()


async def _heartbeat_loop(
    pusher: Any,
    gateway: str,
    stop: asyncio.Event,
    push_interval: float,
) -> None:
    """Empuja el registro Prometheus al Pushgateway cada push_interval.

    Etiquetas (F2.6c): exchange=_PUSH_EXCHANGE → job=ocm_pipeline_orderbook
    (prometheus.py construye job a partir del label exchange). gateway sale
    del contexto (PUSHGATEWAY_URL → RunConfig.pushgateway), nunca hardcodeado.
    """
    while not stop.is_set():
        pusher.push({"exchange": _PUSH_EXCHANGE, "gateway": gateway})
        try:
            await asyncio.wait_for(stop.wait(), timeout=push_interval)
        except asyncio.TimeoutError:
            continue


async def _run_streaming(
    config: Any,
    run_cfg: Any,
    *,
    exchange: str,
    symbols: list[str],
    push_interval: float,
) -> int:
    from datetime import datetime, timezone

    from market_data.adapters.inbound.websocket.cryptofeed_orderbook_stream import (
        CryptofeedOrderBookStream,
    )
    from market_data.infrastructure.bootstrap.composition_root import (
        CompositionRoot,
    )

    from ocm.runtime.context import RuntimeContext

    ctx = RuntimeContext(
        app_config=config,
        run_config=run_cfg,
        started_at=datetime.now(timezone.utc),
    )

    bootstrap_servers: str = config.integrations.kafka.bootstrap_servers
    gateway: str = ctx.pushgateway  # SSOT: PUSHGATEWAY_URL (RunConfig.from_env)

    pusher = _build_pusher(config)

    # Composition Root (BC-38) — único punto de ensamblado de producers WS.
    # source=DATASOURCE_REPLAY: este canary NO genera señales de trading
    # (F-008, docs/audits/2026-08-08-streaming-canary-audit.md). El default
    # DATASOURCE_LIVE queda reservado para live_hydra.py cuando lo adopte.
    bundle = CompositionRoot.build_ws_producers(bootstrap_servers, source=DATASOURCE_REPLAY)

    stream = CryptofeedOrderBookStream(
        exchange=exchange,
        symbols=symbols,
        on_snapshot=bundle.orderbook.on_snapshot,
        on_delta=bundle.orderbook.on_delta,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop(*_: Any) -> None:
        logger.info("shutdown_requested | signal received")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            # Fuera del hilo principal (tests) — fallback por asyncio sleep.
            logger.debug("signal_handler_unavailable | {}", sig)

    try:
        await bundle.start_all()
        await stream.start()
        logger.info(
            "streaming_started | exchange={} symbols={} job=ocm_pipeline_{} gateway={}",
            exchange,
            symbols,
            _PUSH_EXCHANGE,
            gateway,
        )
        await _heartbeat_loop(pusher, gateway, stop, push_interval)
        logger.info("streaming_stopped | shutdown limpio")
        return 0
    except Exception as exc:
        logger.opt(exception=True).critical("streaming_failed | {}", exc)
        return 1
    finally:
        # Shutdown ordenado (ADR-0022): stop event → stream.stop() → close_all().
        await stream.stop()
        await bundle.close_all()
        logger.info("streaming_closed | producers cerrados")


def main(argv: list[str] | None = None) -> int:
    from ocm.runtime.run_config import RunConfig

    args = _build_parser().parse_args(argv)
    _setup_logging(args.debug)

    run_cfg = RunConfig.from_env(explicit_env=args.env)
    config = _load_config(args.env, run_id=run_cfg.run_id)
    if config is None:
        return 1

    feeds = config.feeds.feeds
    entry = feeds.get(args.exchange)
    if entry is None or not entry.enabled or not entry.symbols:
        logger.critical(
            "feeds.feeds.{} not enabled or empty — canary ORDERBOOK requiere "
            "el exchange habilitado en config/market_data/feeds.yaml",
            args.exchange,
        )
        return 1

    symbols = args.symbols or list(entry.symbols)

    return asyncio.run(
        _run_streaming(
            config,
            run_cfg,
            exchange=args.exchange,
            symbols=symbols,
            push_interval=args.push_interval,
        )
    )


if __name__ == "__main__":
    sys.exit(main())
