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

H1 (AUDIT-apps-2026-08-03): el Namespace muere en el borde CLI —
assemble_cli_config() deriva TradingConfig/RiskConfig/RunParams tipados
(app/cli/_bootstrap.py). execute_live ya no recibe argparse.Namespace.

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
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    from app.cli._bootstrap import (
        assemble_cli_config,
        handle_sigterm,
        log_cycle_result,
        setup_logging,
    )

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

    signal.signal(signal.SIGTERM, handle_sigterm)
    setup_logging(debug=cli_args.debug, color="red")

    from ocm.config.hydra_loader import load_appconfig_standalone
    from ocm.config.loader.exceptions import ConfigurationError, ConfigValidationError

    try:
        config = load_appconfig_standalone(env=cli_args.env)
    except (ConfigurationError, ConfigValidationError) as exc:
        logger.opt(exception=True).critical("config_load_failed | {}", exc)
        return 1

    trading_cfg, risk_cfg, params = assemble_cli_config(
        config,
        cli_args,
        capital=cli_args.capital,
        max_errors=cli_args.max_errors,
    )
    symbol = trading_cfg.strategy_cfg["symbol"]
    timeframe = trading_cfg.strategy_cfg["timeframe"]

    logger.warning("=" * 52)
    logger.warning("⚠️   LIVE TRADING (HYDRA) — CAPITAL REAL")
    logger.warning(
        "    exchange={} symbol={} capital={:.0f} USD",
        trading_cfg.exchange,
        symbol,
        trading_cfg.capital_usd,
    )
    logger.warning("=" * 52)

    logger.info(
        "Live trading iniciando (hydra) | exchange={} symbol={} tf={} market_type={} strategy={} capital={:.0f}",
        trading_cfg.exchange,
        symbol,
        timeframe,
        trading_cfg.market_type,
        trading_cfg.strategy_name,
        trading_cfg.capital_usd,
    )

    # Fase 3: ensamblar PortfolioService vía CompositionRoot. capital_usd_override
    # es obligatorio aquí (no None) — cli_args.capital ya fue validado como
    # explícito y positivo arriba. Sin este override, CompositionRoot usaría
    # config.portfolio.capital_usd (10000.0 default) y anularía el guard
    # SafeOps de --capital obligatorio que existe justamente para live.
    from portfolio.bootstrap.composition_root import (
        CompositionRoot as PortfolioCompositionRoot,
    )

    portfolio_root = PortfolioCompositionRoot.assemble(config, capital_usd_override=cli_args.capital)

    from app.use_cases.execute_live import execute

    try:
        run_result = execute(
            trading_cfg,
            risk_cfg,
            portfolio_service=portfolio_root.portfolio_service,
            max_errors=params.max_errors,
            min_confidence=params.min_confidence,
        )
    except (KeyboardInterrupt, SystemExit) as exc:
        logger.warning("Live trading interrumpido | {}", exc)
        return 1
    finally:
        portfolio_root.close()

    if not run_result.success:
        logger.error("Live use case fallido | {}", run_result.error)
        return run_result.exit_code

    return log_cycle_result(run_result, order_tag="[LIVE]")


if __name__ == "__main__":
    sys.exit(main())
