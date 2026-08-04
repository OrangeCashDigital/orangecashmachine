# -*- coding: utf-8 -*-
"""
app/cli/paper_hydra.py
=======================

Entrypoint CLI para paper trading — variante Hydra/AppConfig (Fase 2).

CLI oficial de paper trading (ADR-0005) — reemplaza a app/cli/paper.py.
app/cli/paper.py permanece en el arbol hasta confirmar que ningun
consumidor externo (systemd, cron, scripts fuera de este repo) lo
invoca directamente; ver ADR-0005, seccion Consecuencias.

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

    symbol, timeframe, market_type, strategy, fast, slow, min_confidence

H1 (AUDIT-apps-2026-08-03): el Namespace muere en el borde CLI —
assemble_cli_config() deriva TradingConfig/RiskConfig/RunParams tipados
(app/cli/_bootstrap.py). execute_paper ya no recibe argparse.Namespace.

H4 (SSOT de min_order_usd): paper respeta config.risk.order.min_order_usd —
antes usaba un fallback hardcodeado (10.0).

Uso
---
    uv run paper
    uv run paper --symbol ETH/USDT --timeframe 4h
    uv run paper --env production
    uv run paper --dry-run

Nota sobre --dry-run
---------------------
config.safety.dry_run es el default SSOT. El flag --dry-run de este
CLI solo puede FORZAR dry-run a True — nunca lo desactiva
silenciosamente sobre lo que ya diga la config.

Manejo de señales
------------------
SIGTERM se traduce a SystemExit (mismo patrón que app/cli/live_hydra.py)
para que execute_paper.execute() pueda cerrar recursos vía su bloque
finally en lugar de terminar el proceso en seco (docker stop, systemd
stop, kill). SIGINT usa el comportamiento default de Python
(KeyboardInterrupt) — no requiere handler explícito.

Exit codes
----------
    0 → ciclo completado (con o sin señales)
    1 → error fatal (config inválida, datos no disponibles)

Principios: SOLID · KISS · DRY · SSOT · SafeOps · Resiliencia · Composition Root
"""

from __future__ import annotations

import argparse
import signal
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
    p.add_argument("--strategy", default="ema_crossover", help="Nombre de estrategia")
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
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """
    Punto de entrada principal.

    Returns
    -------
    int — exit code (0 = OK, 1 = error)
    """
    from app.cli._bootstrap import (
        assemble_cli_config,
        handle_sigterm,
        log_cycle_result,
        setup_logging,
    )

    cli_args = _build_parser().parse_args(argv)

    # Resiliencia / SafeOps: registrar el handler apenas se conocen los args,
    # antes de abrir cualquier recurso (logger, config, engine) -- ver nota
    # "Manejo de señales" en el docstring del módulo.
    signal.signal(signal.SIGTERM, handle_sigterm)
    setup_logging(debug=cli_args.debug, color="green")

    # Fail-Fast: config inválida detiene acá con mensaje accionable —
    # no en medio del ensamblaje del engine.
    from ocm.config.hydra_loader import load_appconfig_standalone
    from ocm.config.loader.exceptions import ConfigurationError, ConfigValidationError

    try:
        config = load_appconfig_standalone(env=cli_args.env)
    except (ConfigurationError, ConfigValidationError) as exc:
        logger.opt(exception=True).critical("config_load_failed | {}", exc)
        return 1

    # dry_run: config.safety.dry_run es el default SSOT; --dry-run del CLI
    # solo puede forzarlo a True, nunca desactivarlo silenciosamente.
    trading_cfg, risk_cfg, params = assemble_cli_config(
        config,
        cli_args,
        capital=config.portfolio.capital_usd,
        dry_run=config.safety.dry_run or cli_args.dry_run,
    )
    symbol = trading_cfg.strategy_cfg["symbol"]
    timeframe = trading_cfg.strategy_cfg["timeframe"]

    logger.info(
        "Paper trading iniciando (hydra) | exchange={} symbol={} tf={} "
        "market_type={} ema={}/{} capital={:.0f} dry_run={}",
        trading_cfg.exchange,
        symbol,
        timeframe,
        trading_cfg.market_type,
        trading_cfg.strategy_cfg["fast_period"],
        trading_cfg.strategy_cfg["slow_period"],
        trading_cfg.capital_usd,
        params.dry_run,
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

    try:
        run_result = execute(
            trading_cfg,
            risk_cfg,
            portfolio_service=portfolio_root.portfolio_service,
            dry_run=params.dry_run,
            min_confidence=params.min_confidence,
        )
    except (KeyboardInterrupt, SystemExit) as exc:
        logger.warning("Paper trading interrumpido | {}", exc)
        return 1
    finally:
        portfolio_root.close()

    if not run_result.success:
        logger.error("Use case fallido | {}", run_result.error)
        return run_result.exit_code

    return log_cycle_result(run_result, extra_performance=True)


if __name__ == "__main__":
    sys.exit(main())
