# -*- coding: utf-8 -*-
"""
app/cli/paper_hydra.py
=======================

Entrypoint CLI para paper trading — variante Hydra/AppConfig (Fase 2).

Coexiste con app/cli/paper.py sin reemplazarlo ni modificarlo. Fase 2
del plan de integración CLI ↔ Hydra. Cuando ambos caminos demuestren
construir el mismo grafo de dependencias (Fase 4), este entrypoint
reemplaza a paper.py y paper.py se retira.

Qué cambia respecto a app/cli/paper.py
---------------------------------------
Parámetros que ya son SSOT de un bounded context — ahora vienen de
AppConfig (Hydra), no de argparse:

    capital, exchange   → config.portfolio.capital_usd / .exchange
    max_risk_pct         → config.risk.position.max_position_pct
    max_positions          → config.risk.position.max_open_positions
    dry_run (default)      → config.safety.dry_run

Parámetros de estrategia/ejecución sin modelo en AppConfig todavía —
se mantienen como flags CLI (no se fuerza su inclusión en Hydra solo
para cerrar esta fase; ver Fase 5 del plan):

    symbol, timeframe, market_type, fast, slow, min_confidence

Uso
---
    uv run paper-hydra
    uv run paper-hydra --symbol ETH/USDT --timeframe 4h
    uv run paper-hydra --env production
    uv run paper-hydra --dry-run

Nota sobre --dry-run
---------------------
config.safety.dry_run es el default SSOT. El flag --dry-run de este
CLI solo puede FORZAR dry-run a True — nunca lo desactiva
silenciosamente sobre lo que ya diga la config.

Exit codes
----------
    0 → ciclo completado (con o sin señales)
    1 → error fatal (config inválida, datos no disponibles)

Principios: SOLID · KISS · DRY · SSOT · SafeOps · Composition Root
"""

from __future__ import annotations

import argparse
import sys

from loguru import logger

# ---------------------------------------------------------------------------
# Argparse — solo parámetros sin equivalente en AppConfig todavía
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="OrangeCashMachine — Paper Trading (Hydra/AppConfig)",
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

    # Identidad del dataset — sin modelo en AppConfig aún
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
    p.add_argument("--fast", type=int, default=9, help="EMA rápida (períodos)")
    p.add_argument("--slow", type=int, default=21, help="EMA lenta (períodos)")
    p.add_argument(
        "--min-confidence",
        type=float,
        default=0.8,
        dest="min_confidence",
        help="Confianza mínima de señal para actuar (0.0–1.0)",
    )

    # Modo — dry_run tiene default en AppConfig; este flag solo fuerza True
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fuerza dry-run (datos sintéticos) aunque config.safety.dry_run sea false",
    )
    p.add_argument("--debug", action="store_true", help="Nivel de log DEBUG")

    return p


# ---------------------------------------------------------------------------
# Puente AppConfig -> argparse.Namespace
# ---------------------------------------------------------------------------
# execute_paper.execute() espera argparse.Namespace (Fase 3 lo reemplaza por
# AppConfig directo vía PortfolioCompositionRoot). Fase 2 no toca ese
# contrato — solo cambia de dónde vienen los valores que lo pueblan.


def _merge_config_into_args(config, cli_args: argparse.Namespace) -> argparse.Namespace:
    """Construye el Namespace que espera execute_paper.execute().

    SSOT: capital/exchange/riesgo vienen de AppConfig (Hydra). Lo que
    AppConfig todavía no modela (symbol, timeframe, estrategia) viene
    de cli_args tal cual.

    Args:
        config:   AppConfig validado (pipeline L1-L5 completo).
        cli_args: Namespace ya parseado por _build_parser().

    Returns:
        argparse.Namespace con el mismo shape que produce app/cli/paper.py.
    """
    merged = argparse.Namespace(**vars(cli_args))

    merged.capital = config.portfolio.capital_usd
    merged.exchange = config.portfolio.exchange
    merged.max_risk_pct = config.risk.position.max_position_pct
    merged.max_positions = config.risk.position.max_open_positions

    # dry_run: config.safety.dry_run es el default SSOT; --dry-run del CLI
    # solo puede forzarlo a True, nunca desactivarlo silenciosamente.
    merged.dry_run = config.safety.dry_run or cli_args.dry_run

    return merged


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """
    Punto de entrada principal.

    Returns
    -------
    int — exit code (0 = OK, 1 = error)
    """
    cli_args = _build_parser().parse_args(argv)

    logger.remove()
    level = "DEBUG" if cli_args.debug else "INFO"
    logger.add(
        sys.stderr,
        level=level,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
    )

    # Fail-Fast: config inválida detiene acá con mensaje accionable —
    # no en medio del ensamblaje del engine.
    from ocm.config.hydra_loader import load_appconfig_standalone
    from ocm.config.loader.exceptions import ConfigurationError, ConfigValidationError

    try:
        config = load_appconfig_standalone(env=cli_args.env)
    except (ConfigurationError, ConfigValidationError) as exc:
        logger.opt(exception=True).critical("config_load_failed | {}", exc)
        return 1

    args = _merge_config_into_args(config, cli_args)

    logger.info(
        "Paper trading iniciando (hydra) | exchange={} symbol={} tf={} "
        "market_type={} ema={}/{} capital={:.0f} dry_run={}",
        args.exchange,
        args.symbol,
        args.timeframe,
        args.market_type,
        args.fast,
        args.slow,
        args.capital,
        args.dry_run,
    )

    # Fase 3: ensamblar PortfolioService vía CompositionRoot en vez de
    # dejar que execute_paper.py lo construya a mano. capital_usd_override
    # no aplica aquí — config.portfolio.capital_usd ya es la fuente de
    # verdad para paper trading (sin el matiz SafeOps de live_hydra.py).
    from portfolio.bootstrap.composition_root import (
        CompositionRoot as PortfolioCompositionRoot,
    )

    portfolio_root = PortfolioCompositionRoot.assemble(config)

    from app.use_cases.execute_paper import execute

    run_result = execute(args, portfolio_service=portfolio_root.portfolio_service)

    if not run_result.success:
        logger.error("Use case fallido | {}", run_result.error)
        return run_result.exit_code

    assert run_result.engine_result is not None, (
        "engine_result is None with success=True — contrato roto en PaperRunResult"
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
            "  {} {} {} @ {:.4f} | size={:.1%} | status={}",
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
        logger.info(
            "  Sharpe ratio    : {}",
            (f"{summary.sharpe_ratio:.2f}" if summary.sharpe_ratio is not None else "N/A"),
        )
        logger.info("  Max drawdown    : {:.2%}", summary.max_drawdown)
        logger.info(
            "  Profit factor   : {}",
            (f"{summary.profit_factor:.2f}" if summary.profit_factor is not None else "N/A"),
        )
        logger.info("────────────────────────────────────────────────")
    else:
        open_pos = run_result.open_positions
        if open_pos:
            logger.info("Posiciones abiertas | symbols={}", list(open_pos.keys()))

    logger.debug("OMS summary | {}", run_result.oms_summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
