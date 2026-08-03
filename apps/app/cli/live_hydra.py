# -*- coding: utf-8 -*-
"""
app/cli/live_hydra.py
=======================

Entrypoint CLI para live trading — variante Hydra/AppConfig (Fase 2).

⚠️  ADVERTENCIA: Este comando opera con capital real.

CLI oficial de live trading (ADR-0005) — reemplaza a app/cli/live.py.
app/cli/live.py permanece en el arbol hasta confirmar que ningun
consumidor externo (systemd, cron, scripts fuera de este repo) lo
invoca directamente; ver ADR-0005, seccion Consecuencias.

Qué cambia respecto a app/cli/live.py
---------------------------------------
    exchange              → config.portfolio.exchange
    max_risk_pct           → config.risk.position.max_position_pct
    max_positions             → config.risk.position.max_open_positions
    min_order_usd              → config.risk.order.min_order_usd
    redis_host/port/db/password → config.integrations.redis.*

Qué NO cambia — decisión deliberada de SafeOps
------------------------------------------------
--capital NO tiene default. AppConfig.portfolio.capital_usd (10000.0)
es SSOT compartido con paper_hydra.py, pero app/cli/live.py usaba a
propósito un default conservador distinto (1000.0) para capital real.
Adoptar el default de AppConfig aquí cambiaría la postura de riesgo en
el camino que mueve dinero real sin que nadie lo pida — se exige
--capital explícito y el proceso falla rápido si falta.

Parámetros de estrategia sin modelo en AppConfig (symbol, timeframe,
market_type, strategy, fast, slow, min_confidence, max_errors) se
mantienen como flags CLI — igual que en live.py.

Uso
---
    uv run live --capital 1000
    uv run live --capital 5000 --max-risk-pct 0.02
    uv run live --capital 1000 --env production

Exit codes
----------
    0 → ciclo completado (con o sin señales)
    1 → error fatal (config inválida, capital faltante, exchange no disponible)

Principios: SRP · SafeOps · Composition Root
"""

from __future__ import annotations

import argparse
import signal
import sys

from loguru import logger

# ---------------------------------------------------------------------------
# Argparse — solo parámetros sin equivalente en AppConfig, más --capital
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="OrangeCashMachine — Live Trading (Hydra/AppConfig) ⚠️  Capital Real",
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

    # SafeOps: capital real — sin default, obligatorio y explícito.
    p.add_argument(
        "--capital",
        type=float,
        default=None,
        required=True,
        help="Capital en USD — OBLIGATORIO, sin default (capital real, verificar antes de ejecutar)",
    )

    # Identidad — sin modelo en AppConfig aún
    p.add_argument("--symbol", default="BTC/USDT", help="Par de trading")
    p.add_argument("--timeframe", default="1h", help="Timeframe")
    p.add_argument(
        "--market-type",
        default="spot",
        dest="market_type",
        choices=["spot", "swap", "linear", "inverse"],
        help="Tipo de mercado",
    )

    # Estrategia — sin modelo en AppConfig aún
    p.add_argument("--strategy", default="ema_crossover", help="Nombre de estrategia")
    p.add_argument("--fast", type=int, default=9, help="EMA rápida")
    p.add_argument("--slow", type=int, default=21, help="EMA lenta")
    p.add_argument(
        "--min-confidence",
        type=float,
        default=0.9,
        dest="min_confidence",
        help="Confianza mínima de señal (live: más restrictivo)",
    )
    p.add_argument(
        "--max-errors",
        type=int,
        default=3,
        dest="max_errors",
        help="Errores consecutivos antes de activar guard/halt",
    )

    p.add_argument("--debug", action="store_true", help="Log nivel DEBUG")

    return p


# ---------------------------------------------------------------------------
# Puente AppConfig -> argparse.Namespace
# ---------------------------------------------------------------------------


def _merge_config_into_args(config, cli_args: argparse.Namespace) -> argparse.Namespace:
    """Construye el Namespace que espera execute_live.execute().

    SSOT: exchange/riesgo/redis vienen de AppConfig. capital es
    explícito por CLI (ver nota SafeOps en el docstring del módulo).
    Lo que AppConfig no modela (symbol, timeframe, strategy) viene de
    cli_args tal cual.

    Args:
        config:   AppConfig validado (pipeline L1-L5 completo).
        cli_args: Namespace ya parseado por _build_parser(), con
                   capital ya validado como no-None por main().

    Returns:
        argparse.Namespace con el mismo shape que produce app/cli/live.py.
    """
    merged = argparse.Namespace(**vars(cli_args))

    merged.exchange = config.portfolio.exchange
    merged.max_risk_pct = config.risk.position.max_position_pct
    merged.max_positions = config.risk.position.max_open_positions
    merged.min_order_usd = config.risk.order.min_order_usd

    redis_cfg = config.integrations.redis
    merged.redis_host = redis_cfg.host
    merged.redis_port = redis_cfg.port
    merged.redis_db = redis_cfg.db
    merged.redis_password = redis_cfg.password.get_secret_value() if redis_cfg.password else None

    return merged


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _handle_sigterm(signum, frame) -> None:
    """Traduce SIGTERM a SystemExit -- dispara los finally pendientes."""
    raise SystemExit(1)


def main(argv: list[str] | None = None) -> int:
    cli_args = _build_parser().parse_args(argv)

    # Fail-Fast explícito y con mensaje accionable — aunque argparse ya
    # marca --capital como required, este check documenta la razón de
    # negocio (SafeOps) en vez de depender solo del mensaje genérico
    # de argparse.
    if cli_args.capital is None or cli_args.capital <= 0:
        logger.critical(
            "live requiere --capital explícito y positivo — no hay default por diseño (capital real). Recibido: {}",
            cli_args.capital,
        )
        return 1

    signal.signal(signal.SIGTERM, _handle_sigterm)

    logger.remove()
    level = "DEBUG" if cli_args.debug else "INFO"
    logger.add(
        sys.stderr,
        level=level,
        colorize=True,
        format="<red>{time:HH:mm:ss}</red> | <level>{level:<8}</level> | {message}",
    )

    from ocm.config.hydra_loader import load_appconfig_standalone
    from ocm.config.loader.exceptions import ConfigurationError, ConfigValidationError

    try:
        config = load_appconfig_standalone(env=cli_args.env)
    except (ConfigurationError, ConfigValidationError) as exc:
        logger.opt(exception=True).critical("config_load_failed | {}", exc)
        return 1

    args = _merge_config_into_args(config, cli_args)

    logger.warning("=" * 52)
    logger.warning("⚠️   LIVE TRADING (HYDRA) — CAPITAL REAL")
    logger.warning(
        "    exchange={} symbol={} capital={:.0f} USD",
        args.exchange,
        args.symbol,
        args.capital,
    )
    logger.warning("=" * 52)

    logger.info(
        "Live trading iniciando (hydra) | exchange={} symbol={} tf={} market_type={} strategy={} capital={:.0f}",
        args.exchange,
        args.symbol,
        args.timeframe,
        args.market_type,
        args.strategy,
        args.capital,
    )

    # Fase 3: ensamblar PortfolioService vía CompositionRoot. capital_usd_override
    # es obligatorio aquí (no None) — args.capital ya fue validado como
    # explícito y positivo arriba. Sin este override, CompositionRoot usaría
    # config.portfolio.capital_usd (10000.0 default) y anularía el guard
    # SafeOps de --capital obligatorio que existe justamente para live.
    from portfolio.bootstrap.composition_root import (
        CompositionRoot as PortfolioCompositionRoot,
    )

    portfolio_root = PortfolioCompositionRoot.assemble(config, capital_usd_override=args.capital)

    from app.use_cases.execute_live import execute

    try:
        run_result = execute(args, portfolio_service=portfolio_root.portfolio_service)
    except (KeyboardInterrupt, SystemExit) as exc:
        logger.warning("Live trading interrumpido | {}", exc)
        return 1
    finally:
        portfolio_root.close()

    if not run_result.success:
        logger.error("Live use case fallido | {}", run_result.error)
        return run_result.exit_code

    assert run_result.engine_result is not None, (
        "engine_result is None with success=True — contrato roto en LiveRunResult"
    )
    result = run_result.engine_result

    logger.info(
        "Ciclo completado | status={} signals={} submitted={} filled={} rejected={}",
        result.status,
        result.signals_generated,
        result.orders_submitted,
        result.orders_filled,
        result.orders_rejected,
    )

    if result.skipped:
        logger.warning("Ciclo skipped | reason={}", result.skip_reason)
        return 0

    if not result.orders:
        logger.info("Sin señales accionables en este ciclo.")
        return 0

    for order in result.orders:
        logger.info(
            "  [LIVE] {} {} {} @ {:.4f} | size={:.1%} | status={}",
            order.order_id,
            order.side.value.upper(),
            order.symbol,
            order.signal.price,
            order.size_pct,
            order.status.value,
        )

    summary = run_result.performance
    if summary:
        logger.info("── Performance ─────────────────────────────────")
        logger.info("  Trades cerrados : {}", summary.total_trades)
        logger.info(
            "  Win rate        : {}",
            f"{summary.win_rate:.1%}" if summary.win_rate is not None else "N/A",
        )
        logger.info("  PnL total       : {:+.2%}", summary.total_pnl_pct)
        logger.info(
            "  PnL USD         : {:+.2f}",
            summary.pnl_usd if summary.pnl_usd is not None else 0.0,
        )
        logger.info("  Max drawdown    : {:.2%}", summary.max_drawdown)
        logger.info("────────────────────────────────────────────────")
    else:
        open_pos = run_result.open_positions
        if open_pos:
            logger.info("Posiciones abiertas | symbols={}", list(open_pos.keys()))

    logger.debug("OMS summary | {}", run_result.oms_summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
