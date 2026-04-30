"""
Gold Transformer — OrangeCashMachine
======================================

Responsabilidad
---------------
Transformar datos Silver (OHLCV limpio + quality_flag) en features
cuantitativos listos para estrategias de trading o modelos ML.

Capas del pipeline
------------------
RAW  → bytes/JSON del exchange
SILVER → OHLCV validado, tipado, sin corrupciones
GOLD → features derivados: retornos, volatilidad, VWAP, etc.

Features producidos (v1)
------------------------
• log_return        : log(close_t / close_{t-1})
• volatility_20     : std rolling 20 de log_return (annualizada)
• vwap              : (close * volume).cumsum() / volume.cumsum()
• volume_z          : z-score de volumen (ventana 20)
• price_range_pct   : (high - low) / open  — proxy de volatilidad intra-vela
• body_pct          : abs(close - open) / open — tamaño del cuerpo de la vela
• is_suspect        : bool desde quality_flag (para filtros en estrategia)

Principios
----------
SOLID  — SRP: solo features, sin I/O ni validación de schema
DRY    — _rolling_zscore() reutilizable para cualquier serie
KISS   — pandas vectorizado, sin loops explícitos
SafeOps — NaNs explícitos en las primeras N filas (ventana de calentamiento)
OCP    — nuevos features: añadir método _compute_X(), registrar en FEATURE_REGISTRY

Ref: Veerapaneni (2023) — separación estricta Silver/Gold evita
     feature leakage en backtesting.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from loguru import logger

from market_data.lineage.tracker import (
    LineageEvent, LineageStatus, PipelineLayer, lineage_tracker,
)


# ==========================================================
# Constants
# ==========================================================

# Ventana estándar para estadísticas rolling.
# Calibrar por timeframe si se necesita granularidad mayor.
_ROLLING_WINDOW    = 20
_ANNUALIZATION_MAP = {
    "1m":  525_600,   # minutos en un año
    "5m":  105_120,
    "15m": 35_040,
    "1h":  8_760,
    "4h":  2_190,
    "1d":  365,
}
_DEFAULT_ANNUALIZATION = 365  # fallback conservador


# ==========================================================
# GoldTransformer
# ==========================================================

class GoldTransformer:
    """
    Transforma un DataFrame Silver en features Gold.

    Invariantes del input (Silver)
    ------------------------------
    • timestamp: datetime64[ns, UTC], monotónico creciente
    • open, high, low, close: float > 0
    • volume: float >= 0
    • quality_flag: str ("clean" | "suspect")

    Invariantes del output (Gold)
    -----------------------------
    • Todas las columnas Silver preservadas (no destructivo)
    • Nuevas columnas: ver FEATURE_REGISTRY
    • Las primeras _ROLLING_WINDOW - 1 filas tienen NaN en features rolling
      (ventana de calentamiento — documentado, no silenciado)
    • is_suspect: bool derivado de quality_flag (facilita filtros downstream)
    """

    @classmethod
    def transform(
        cls,
        df:        pd.DataFrame,
        symbol:    str,
        timeframe: str,
        exchange:  str,
        run_id:    str | None = None,
    ) -> pd.DataFrame:
        """
        Pipeline principal Silver → Gold.

        Parameters
        ----------
        df        : DataFrame Silver validado
        symbol    : par de trading (BTC/USDT)
        timeframe : resolución temporal (1m, 1h, …)
        exchange  : exchange de origen
        run_id    : correlación de lineage (opcional)

        Returns
        -------
        pd.DataFrame con columnas Silver + features Gold.
        Vacío si el input es vacío (no lanza excepción — Fail-Soft).
        """
        if df is None or df.empty:
            logger.warning(
                "GoldTransformer: empty input | {}/{} exchange={}",
                symbol, timeframe, exchange,
            )
            return pd.DataFrame()

        rows_in = len(df)
        gold    = df.copy()

        gold = cls._compute_log_return(gold)
        gold = cls._compute_volatility(gold, timeframe)
        gold = cls._compute_vwap(gold)
        gold = cls._compute_volume_zscore(gold)
        gold = cls._compute_price_range(gold)
        gold = cls._compute_body_pct(gold)
        gold = cls._compute_is_suspect(gold)

        rows_out = len(gold)

        logger.info(
            "GoldTransformer | {}/{} exchange={} rows={} features={}",
            symbol, timeframe, exchange, rows_out,
            [c for c in gold.columns if c not in df.columns],
        )

        # Lineage
        if run_id is not None:
            lineage_tracker.record(LineageEvent(
                run_id        = run_id,
                layer         = PipelineLayer.GOLD,
                exchange      = exchange,
                symbol        = symbol,
                timeframe     = timeframe,
                rows_in       = rows_in,
                rows_out      = rows_out,
                status        = LineageStatus.SUCCESS,
                params        = {"rolling_window": _ROLLING_WINDOW, "timeframe": timeframe},
            ))

        return gold

    # ----------------------------------------------------------
    # Feature Computers — cada uno tiene SRP
    # ----------------------------------------------------------

    @staticmethod
    def _compute_log_return(df: pd.DataFrame) -> pd.DataFrame:
        """
        Retorno logarítmico: log(close_t / close_{t-1}).

        Preferido sobre retorno simple en análisis cuantitativo:
        aditivo en tiempo, simétrico, sin sesgo de compounding.
        La primera fila será NaN por diseño.
        """
        df = df.copy()
        df["log_return"] = np.log(df["close"] / df["close"].shift(1))
        return df

    @staticmethod
    def _compute_volatility(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        Volatilidad realizada rolling (std de log_returns, annualizada).

        Ann. factor = sqrt(periodos_por_año) según timeframe.
        Las primeras _ROLLING_WINDOW - 1 filas serán NaN.
        """
        df    = df.copy()
        ann   = _ANNUALIZATION_MAP.get(timeframe, _DEFAULT_ANNUALIZATION)
        df["volatility_20"] = (
            df["log_return"]
            .rolling(_ROLLING_WINDOW)
            .std()
            * np.sqrt(ann)
        )
        return df

    @staticmethod
    def _compute_vwap(df: pd.DataFrame) -> pd.DataFrame:
        """
        Volume-Weighted Average Price acumulado desde el inicio del DataFrame.

        Nota: VWAP sesión-based requiere agrupar por fecha. Esta implementación
        es cumulativa — apropiada para análisis de largo plazo.
        Para VWAP intra-day, resetear por sesión en el llamador.
        """
        df         = df.copy()
        cum_vol    = df["volume"].cumsum()
        cum_tpv    = (df["close"] * df["volume"]).cumsum()
        # Evitar división por cero en segmentos con volume=0
        df["vwap"] = np.where(cum_vol > 0, cum_tpv / cum_vol, np.nan)
        return df

    @staticmethod
    def _compute_volume_zscore(df: pd.DataFrame) -> pd.DataFrame:
        """
        Z-score de volumen en ventana rolling.

        volume_z > 2 o < -2 indica actividad anómala (spikes/vacíos).
        NaN en primeras _ROLLING_WINDOW - 1 filas.
        """
        df             = df.copy()
        roll           = df["volume"].rolling(_ROLLING_WINDOW)
        df["volume_z"] = (df["volume"] - roll.mean()) / roll.std().replace(0, np.nan)
        return df

    @staticmethod
    def _compute_price_range(df: pd.DataFrame) -> pd.DataFrame:
        """
        Rango de precio intra-vela como % del open.

        price_range_pct = (high - low) / open
        Proxy de volatilidad intra-bar. Siempre >= 0 por invariante OHLC.
        """
        df                    = df.copy()
        df["price_range_pct"] = (df["high"] - df["low"]) / df["open"]
        return df

    @staticmethod
    def _compute_body_pct(df: pd.DataFrame) -> pd.DataFrame:
        """
        Tamaño del cuerpo de la vela como % del open.

        body_pct = abs(close - open) / open
        Distingue velas de rango amplio (alta decisión) de dojis (indecisión).
        """
        df             = df.copy()
        df["body_pct"] = (df["close"] - df["open"]).abs() / df["open"]
        return df

    @staticmethod
    def _compute_is_suspect(df: pd.DataFrame) -> pd.DataFrame:
        """
        Flag booleano derivado de quality_flag Silver.

        Permite filtrar fácilmente velas sospechosas en estrategias:
            gold_df[~gold_df["is_suspect"]]
        """
        df             = df.copy()
        if "quality_flag" in df.columns:
            df["is_suspect"] = df["quality_flag"] == "suspect"
        else:
            df["is_suspect"] = False
        return df
