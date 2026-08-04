# -*- coding: utf-8 -*-
"""
app/use_cases/execute_paper.py
================================

Use case: ejecutar un ciclo de paper trading.

Responsabilidad
---------------
Ensamblar las dependencias y ejecutar un ciclo completo:
    GoldData → TradingEngine → TradeTracker → PerformanceEngine

Separación de concerns
-----------------------
    app/cli/paper_hydra.py        → CLI: Hydra, logging, exit codes
    app/use_cases/execute_paper.py → ensamblaje y ejecución (este módulo)

_SyntheticDataSource vive aquí — es un detalle de implementación
del use case (dry-run mode), no del CLI.

H1 (AUDIT-apps-2026-08-03): las firmas reciben TradingConfig/RiskConfig
tipados (no argparse.Namespace). El borde CLI deriva max_order_usd y
min_order_usd vía model_copy — este use case no repite fórmulas ni getattr.
H4: min_order_usd viene de config.risk.order.min_order_usd (SSOT), ya no
del fallback hardcodeado 10.0.

Principios: SRP · DIP · DRY · SSOT · SafeOps · Composition Root
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from portfolio.services.portfolio_service import PortfolioService

    from ocm.config.schema import RiskConfig, TradingConfig
    from shared.contracts.boundaries import FeatureSource

from loguru import logger

from app.use_cases.run_result import CycleRunResult

# Import estático — no dentro de closures (DRY · KISS · evita re-evaluación por fill)


# ---------------------------------------------------------------------------
# Synthetic data source — dry-run mode only
# ---------------------------------------------------------------------------


class SyntheticDataSource:
    """
    Fuente de datos sintética para dry-run y tests de integración.

    Genera un DataFrame OHLCV con tendencia alcista y cruce EMA garantizado.
    No requiere Iceberg ni conexión de red.

    Reproducibilidad
    ----------------
    Seed fijo (42) — garantiza que dos runs con los mismos args produzcan
    los mismos datos sintéticos. Esencial para comparar resultados
    en dry-run y para tests deterministas.

    SafeOps: NUNCA usar en producción — solo dry_run=True o tests.
    """

    _SEED: int = 42  # SSOT del seed — cambiar aquí y en tests/trading/

    def load_features(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        market_type: str = "spot",
        **kwargs,
    ):
        from datetime import datetime, timedelta, timezone

        import numpy as np
        import pandas as pd

        rng = np.random.default_rng(self._SEED)  # reproducible, no muta global state
        n = 100
        base = 50_000.0
        dates = [datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(hours=i) for i in range(n)]
        close = base + np.cumsum(rng.normal(50, 200, n))
        close = np.maximum(close, 1.0)

        df = pd.DataFrame(
            {
                "timestamp": dates,
                "open": close * 0.999,
                "high": close * 1.005,
                "low": close * 0.995,
                "close": close,
                "volume": rng.uniform(10, 100, n),
            }
        )

        df["return_1"] = df["close"].pct_change()
        df["log_return"] = np.log(df["close"] / df["close"].shift(1))
        df["volatility_20"] = df["log_return"].rolling(20, min_periods=5).std()
        df["high_low_spread"] = (df["high"] - df["low"]) / df["close"]
        tp = (df["high"] + df["low"] + df["close"]) / 3
        df["vwap"] = (tp * df["volume"]).rolling(20, min_periods=5).sum() / df["volume"].rolling(
            20, min_periods=5
        ).sum()

        logger.debug(
            "[DRY-RUN] SyntheticDataSource | rows={} symbol={} tf={} seed={}",
            len(df),
            symbol,
            timeframe,
            self._SEED,
        )
        return df


# ---------------------------------------------------------------------------
# Builder — ensamblaje de dependencias (Composition Root)
# ---------------------------------------------------------------------------


def build_paper_engine(
    trading: "TradingConfig",
    risk: "RiskConfig",
    portfolio_service: PortfolioService,
    *,
    dry_run: bool,
    min_confidence: float,
):
    """
    Ensambla TradingEngine + PortfolioService para paper trading vía Composition Root.

    Separado de execute() (SRP):
        build_paper_engine → sabe cómo construir
        execute()          → sabe cómo correr

    Fail-Fast: si dry_run=False y la FeatureSource Gold no puede cargar datos,
    lanza explícitamente en lugar de dejar que el engine corra con None
    y genere 0 señales sin diagnóstico.

    Parameters
    ----------
    trading : TradingConfig tipado — derivado por assemble_cli_config() en
        el borde CLI (AppConfig SSOT + flags CLI; ADR-0003).
    risk    : RiskConfig tipado — idem. max_order_usd/min_order_usd ya
        derivados en el CLI vía model_copy (H1/H4).
    portfolio_service : PortfolioService ya ensamblado por
        PortfolioCompositionRoot.assemble() (app/cli/paper_hydra.py).
    dry_run : True → datos sintéticos (SyntheticDataSource); False → Gold.
    min_confidence : confianza mínima de señal para actuar.

    Returns
    -------
    TradingRuntime — (engine, portfolio, tracker) ya ensamblado.
    """
    from trading.bootstrap.composition_root import TradingCompositionRoot

    root = TradingCompositionRoot(
        trading=trading,
        risk=risk,
        portfolio=portfolio_service,
        guard=None,
    )

    data_source: "FeatureSource"
    if dry_run:
        data_source = SyntheticDataSource()
        logger.info(
            "[DRY-RUN] Usando datos sintéticos — sin conexión a Iceberg | seed={}",
            SyntheticDataSource._SEED,
        )
    else:
        # Fail-Fast: verificar que Gold tiene datos antes de construir el engine.
        # Sin este check, engine.run_once() corre normalmente pero genera 0 señales
        # sin ningún diagnóstico — difícil de debuggear.
        data_source = root.build_gold_data_source()
        _probe_gold_data(
            data_source,
            exchange=trading.exchange,
            symbol=trading.strategy_cfg["symbol"],
            timeframe=trading.strategy_cfg["timeframe"],
            market_type=trading.market_type,
        )

    return root.assemble_paper(data_source=data_source, min_confidence=min_confidence)


# ---------------------------------------------------------------------------
# Helper privado — probe de disponibilidad Gold (Fail-Fast)
# ---------------------------------------------------------------------------


def _probe_gold_data(
    data_source: "FeatureSource",
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
    market_type: str,
) -> None:
    """Verifica que Gold tiene datos antes de construir el engine.

    Fail-Fast: lanza RuntimeError si no hay datos disponibles.
    Produce un mensaje accionable con exchange/symbol/tf para diagnóstico
    en lugar del silencioso 0-señales del engine.

    Solo ejecuta en dry_run=False. En dry-run SyntheticDataSource
    siempre devuelve datos — no necesita probe.

    Raises
    ------
    RuntimeError : si Gold no devuelve datos para el par solicitado.
                   Mensaje incluye exchange/symbol/timeframe para diagnóstico.
    """
    try:
        probe = data_source.load_features(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            market_type=market_type,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Gold data unavailable | exchange={exchange} "
            f"symbol={symbol} tf={timeframe} "
            f"market_type={market_type} | "
            f"Correr './run.sh ocm' para ingestar datos primero. "
            f"Error: {exc}"
        ) from exc

    if probe is None or (hasattr(probe, "empty") and probe.empty):
        raise RuntimeError(
            f"Gold data empty | exchange={exchange} "
            f"symbol={symbol} tf={timeframe} "
            f"market_type={market_type} | "
            f"Correr './run.sh ocm' para ingestar datos primero."
        )

    logger.info(
        "Gold data OK | exchange={} symbol={} tf={} rows={}",
        exchange,
        symbol,
        timeframe,
        len(probe) if hasattr(probe, "__len__") else "?",
    )


# ---------------------------------------------------------------------------
# Use case — ejecutar ciclo completo
# ---------------------------------------------------------------------------


def execute(
    trading: "TradingConfig",
    risk: "RiskConfig",
    portfolio_service: PortfolioService,
    *,
    dry_run: bool,
    min_confidence: float,
) -> CycleRunResult:
    """
    Ejecuta un ciclo completo de paper trading.

    Punto de entrada del use case — el CLI llama esto.
    Encapsula todo el flujo: build → run → analytics.

    SafeOps: nunca lanza — errores retornados en CycleRunResult.

    H7: analytics (closed_trades + summarize) dentro del try — un fallo en
    PerformanceEngine no pierde el resultado del ciclo.

    Returns
    -------
    CycleRunResult con todo lo necesario para que el CLI loguee y salga.
    """
    from trading.analytics.performance import PerformanceEngine

    try:
        runtime = build_paper_engine(
            trading,
            risk,
            portfolio_service=portfolio_service,
            dry_run=dry_run,
            min_confidence=min_confidence,
        )
    except Exception as exc:
        logger.error("Error construyendo engine | {} — {}", type(exc).__name__, exc)
        return CycleRunResult(success=False, error=str(exc))

    logger.info("Engine listo | {}", runtime.engine)
    logger.info("Portfolio | {}", runtime.portfolio)

    try:
        engine_result = runtime.engine.run_once()
    except Exception as exc:
        logger.error("Error en run_once | {} — {}", type(exc).__name__, exc)
        return CycleRunResult(success=False, error=str(exc))

    try:
        trades = runtime.tracker.closed_trades
        performance = PerformanceEngine.summarize(trades, capital_usd=trading.capital_usd) if trades else None
    except Exception as exc:
        logger.error(
            "Error calculando performance | {} — {}",
            type(exc).__name__,
            exc,
        )
        return CycleRunResult(success=False, error=str(exc))

    return CycleRunResult(
        success=True,
        engine_result=engine_result,
        performance=performance,
        open_positions=runtime.tracker.open_positions,
        oms_summary=runtime.engine.oms_summary,
    )
