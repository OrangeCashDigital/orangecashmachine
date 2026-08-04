# -*- coding: utf-8 -*-
"""
app/cli/_bootstrap.py
======================

Helpers compartidos de los entrypoints CLI Hydra (live_hydra.py / paper_hydra.py).

Centraliza la lógica que ambos CLIs duplicaban (~136 LOC cada uno):

  · assemble_cli_config()  — deriva TradingConfig/RiskConfig/RunParams tipados
                             desde AppConfig (SSOT) + flags CLI.
  · setup_logging()        — scaffolding loguru idéntico.
  · handle_sigterm()       — traducción SIGTERM → SystemExit.
  · log_cycle_result()     — rendering del resultado de un ciclo.

Por qué
-------
R1/R5 (AUDIT-apps-2026-08-03): los use cases no deben recibir
argparse.Namespace (sin tipar, sin validación). El Namespace muere en el
borde CLI — este módulo es el único puente que lo consume.

H1/R4 (max_order_usd derivado): max_order_usd = capital × max_position_pct
se calcula UNA vez aquí vía model_copy (config inmutable). No se repite la
fórmula dentro de cada use case.

H4 (SSOT de min_order_usd): ambos CLIs respetan config.risk.order.min_order_usd.
Antes paper_hydra usaba un fallback hardcodeado (10.0).

H8 (DRY live/paper): el scaffolding y el rendering del ciclo viven aquí.

Principios: DRY · SSOT · Composition Root
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

from loguru import logger

from ocm.config.schema import AppConfig, RiskConfig, TradingConfig

if TYPE_CHECKING:
    from app.use_cases.run_result import CycleRunResult

__all__ = [
    "RunParams",
    "assemble_cli_config",
    "handle_sigterm",
    "log_cycle_result",
    "setup_logging",
]


# ---------------------------------------------------------------------------
# Parámetros de ejecución — flags CLI que AppConfig no modela
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunParams:
    """Parámetros de ejecución de un ciclo (flags CLI, fuera de AppConfig).

    Cada CLI puebla solo los campos que aplica a su modo:
      · live  → min_confidence + max_errors
      · paper → min_confidence + dry_run
    """

    min_confidence: float
    dry_run: bool = False
    max_errors: int = 3


# ---------------------------------------------------------------------------
# Scaffolding compartido — setup de logging y señales
# ---------------------------------------------------------------------------


def setup_logging(*, debug: bool, color: str) -> None:
    """Configura loguru para los CLIs Hydra.

    Args:
        debug: nivel DEBUG si True, si no INFO.
        color: tag loguru para el timestamp (<red> live, <green> paper).
    """
    logger.remove()
    level = "DEBUG" if debug else "INFO"
    logger.add(
        sys.stderr,
        level=level,
        colorize=True,
        format=f"<{color}>{{time:HH:mm:ss}}</{color}> | <level>{{level:<8}}</level> | {{message}}",
    )


def handle_sigterm(signum, frame) -> None:
    """Traduce SIGTERM a SystemExit — dispara los finally pendientes."""
    raise SystemExit(1)


# ---------------------------------------------------------------------------
# Puente AppConfig + CLI → configs tipados (ADR-0003)
# ---------------------------------------------------------------------------


def assemble_cli_config(
    app_cfg: AppConfig,
    cli_args: argparse.Namespace,
    *,
    capital: float,
    dry_run: bool = False,
    max_errors: int = 3,
) -> tuple[TradingConfig, RiskConfig, RunParams]:
    """Deriva configs tipados desde AppConfig (SSOT) + flags CLI.

    SSOT: exchange y límites de riesgo vienen de AppConfig. Los flags que
    AppConfig no modela (symbol, timeframe, strategy, fast, slow) vienen de
    cli_args. El Namespace muere aquí — R5 del informe de auditoría.

    max_order_usd = capital × max_position_pct se deriva en el borde CLI
    (H1/R4): un único lugar, validado por model_copy (config inmutable).

    Args:
        app_cfg:   AppConfig validado (pipeline L1-L5 completo).
        cli_args:  Namespace ya parseado por _build_parser().
        capital:   capital_usd efectivo del ciclo (live: --capital explícito;
                   paper: config.portfolio.capital_usd).
        dry_run:   bandera paper (config.safety.dry_run or --dry-run).
        max_errors: guard de live (--max-errors).

    Returns:
        (TradingConfig, RiskConfig, RunParams) tipados para los use cases.
    """
    trading_cfg = app_cfg.trading.model_copy(
        update={
            "strategy_name": cli_args.strategy,
            "strategy_cfg": {
                "symbol": cli_args.symbol,
                "timeframe": cli_args.timeframe,
                "fast_period": cli_args.fast,
                "slow_period": cli_args.slow,
            },
            "capital_usd": capital,
            "exchange": app_cfg.portfolio.exchange,
            "market_type": cli_args.market_type,
        }
    )

    risk_cfg = app_cfg.risk.model_copy(
        update={
            "order": app_cfg.risk.order.model_copy(
                update={
                    "max_order_usd": capital * app_cfg.risk.position.max_position_pct,
                }
            )
        }
    )

    params = RunParams(
        min_confidence=cli_args.min_confidence,
        dry_run=dry_run,
        max_errors=max_errors,
    )
    return trading_cfg, risk_cfg, params


# ---------------------------------------------------------------------------
# Rendering compartido — resultado de un ciclo completado
# ---------------------------------------------------------------------------


def log_cycle_result(
    run_result: "CycleRunResult",
    *,
    order_tag: str = "",
    extra_performance: bool = False,
) -> int:
    """Renderiza un ciclo completado con éxito (H8 — DRY live/paper).

    Supone run_result.success is True — el fallo lo maneja cada CLI con su
    propio mensaje (diferencias de tono). Parámetros que diferencian live vs
    paper:

        order_tag:         "[LIVE]" en live, "" en paper.
        extra_performance:  Sharpe ratio y profit factor (solo paper).

    Returns:
        exit code (0 — ciclo completado, con o sin señales).
    """
    assert run_result.engine_result is not None, (
        "engine_result is None with success=True — contrato roto en CycleRunResult"
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
            "  {}{} {} {} @ {:.4f} | size={:.1%} | status={}",
            f"{order_tag} " if order_tag else "",
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
        if extra_performance:
            logger.info(
                "  Sharpe ratio    : {}",
                (f"{summary.sharpe_ratio:.2f}" if summary.sharpe_ratio is not None else "N/A"),
            )
        logger.info("  Max drawdown    : {:.2%}", summary.max_drawdown)
        if extra_performance:
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
