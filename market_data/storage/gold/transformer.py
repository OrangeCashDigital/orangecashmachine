"""
market_data/gold/transformer.py
================================
Silver DataFrame → features Gold.

Bugs corregidos en esta versión
---------------------------------
BUG-1  VWAP acumulado (desde t=0) → VWAP rolling-20 sobre typical price × volume.
       El acumulado produce señal falsa en series multi-sesión: confunde precio
       medio histórico con nivel de referencia intra-sesión.
       Fix: idéntico a FeatureEngineer v1.1 (que ya tenía la versión correcta).

BUG-2  close=0 en log_return producía -inf que se propagaba a volatility_20.
       Fix: replace(0, nan) antes del log.

BUG-3  rolling sin min_periods — primeras filas con <20 datos producían NaN
       inconsistente vs min_periods explícito.
       Fix: min_periods=5 (arranque suave, consistente con FeatureEngineer).

Añadido vs versión anterior
----------------------------
+ return_1         — retorno simple, complementa log_return
+ high_low_spread  — (high-low)/close, normalizado por close (no por open)
+ _sanitize()      — ±inf → NaN en todas las columnas numéricas al final

Nota: annualization NO fue tocada — el cálculo original era correcto.
_ANNUALIZATION_MAP almacena enteros (periodos/año), np.sqrt() se aplica
correctamente en _compute_volatility.

Principios
----------
- SSOT: esta es la única implementación de features Gold activa.
  storage/gold/feature_engineer.py está DEPRECATED y redirige aquí.
- SRP: cada _compute_* hace exactamente una feature.
- Fail-Soft: vacío in → vacío out, sin excepción.
- No destructivo: nunca muta el DataFrame de entrada.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from loguru import logger

from market_data.lineage.tracker import (
    LineageEvent,
    LineageStatus,
    PipelineLayer,
    lineage_tracker,
)

# ------------------------------------------------------------------
# Constantes — SSOT para toda la capa Gold
# ------------------------------------------------------------------

_ROLLING_WINDOW: int = 20
_MIN_PERIODS:    int = 5      # features disponibles desde barra 5

# Factores de anualización: periodos completos por año calendario (crypto 24/7).
# Se pasa a np.sqrt() en _compute_volatility — no almacenar la raíz aquí.
_ANNUALIZATION_MAP: dict[str, int] = {
    "1m":  525_600,
    "5m":  105_120,
    "15m":  35_040,
    "30m":  17_520,
    "1h":    8_760,
    "4h":    2_190,
    "8h":    1_095,
    "1d":      365,
    "1w":       52,
}
_DEFAULT_ANNUALIZATION: int = 365  # fallback conservador

# Columnas que este transformer añade — SSOT para validación downstream.
FEATURE_COLUMNS: list[str] = [
    "log_return",
    "return_1",
    "volatility_20",
    "vwap",
    "high_low_spread",
    "volume_z",
    "price_range_pct",
    "body_pct",
    "is_suspect",
]

VERSION: str = "2.0.0"
# 2.0.0 — VWAP rolling-20 tp×vol (elimina VWAP acumulado — bug semántico)
#          return_1, high_low_spread, _sanitize añadidos
#          min_periods=5 en todos los rolling
#          close=0 → NaN en log_return (era -inf)


class GoldTransformer:
    """
    Transforma un DataFrame Silver en features Gold.

    Invariantes del input (Silver)
    ------------------------------
    - timestamp : datetime64[ns, UTC], monotónico creciente
    - open, high, low, close : float > 0
    - volume : float >= 0
    - quality_flag : str ("clean" | "suspect")

    Invariantes del output (Gold)
    ------------------------------
    - Todas las columnas Silver preservadas (no destructivo).
    - Nuevas columnas: ver FEATURE_COLUMNS.
    - NaN en primeras _ROLLING_WINDOW-1 filas de features rolling (diseño).
    - ±inf → NaN al final (_sanitize).
    - is_suspect : bool derivado de quality_flag.
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

        Fail-Soft: vacío in → vacío out, nunca lanza excepción.
        """
        if df is None or df.empty:
            logger.warning(
                "GoldTransformer: empty input | {}/{} exchange={}",
                symbol, timeframe, exchange,
            )
            return pd.DataFrame()

        rows_in = len(df)
        gold    = df.copy().sort_values("timestamp").reset_index(drop=True)

        gold = cls._compute_log_return(gold)
        gold = cls._compute_return_1(gold)
        gold = cls._compute_volatility(gold, timeframe)
        gold = cls._compute_vwap(gold)
        gold = cls._compute_high_low_spread(gold)
        gold = cls._compute_volume_zscore(gold)
        gold = cls._compute_price_range(gold)
        gold = cls._compute_body_pct(gold)
        gold = cls._compute_is_suspect(gold)
        gold = cls._sanitize(gold)

        rows_out  = len(gold)
        new_cols  = [c for c in gold.columns if c not in df.columns]

        logger.info(
            "GoldTransformer | {}/{} exchange={} rows={} features={} v={}",
            symbol, timeframe, exchange, rows_out, new_cols, VERSION,
        )

        if run_id is not None:
            lineage_tracker.record(LineageEvent(
                run_id    = run_id,
                layer     = PipelineLayer.GOLD,
                exchange  = exchange,
                symbol    = symbol,
                timeframe = timeframe,
                rows_in   = rows_in,
                rows_out  = rows_out,
                status    = LineageStatus.SUCCESS,
                params    = {
                    "rolling_window": _ROLLING_WINDOW,
                    "min_periods":    _MIN_PERIODS,
                    "timeframe":      timeframe,
                    "version":        VERSION,
                },
            ))

        return gold

    # ------------------------------------------------------------------
    # Feature computers
    # SRP: un método = una feature. No mutan el input (df.copy()).
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_log_return(df: pd.DataFrame) -> pd.DataFrame:
        """
        log(close_t / close_{t-1}).

        close=0 → NaN (evita -inf que se propagaría a volatility_20).
        Primera fila: NaN por diseño (no hay t-1).
        Preferido sobre retorno simple: aditivo en tiempo, sin sesgo compounding.
        """
        df         = df.copy()
        safe_close = df["close"].replace(0, float("nan"))
        df["log_return"] = np.log(safe_close / safe_close.shift(1))
        return df

    @staticmethod
    def _compute_return_1(df: pd.DataFrame) -> pd.DataFrame:
        """Retorno simple período a período. Complementa log_return."""
        df             = df.copy()
        df["return_1"] = df["close"].pct_change()
        return df

    @staticmethod
    def _compute_volatility(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        Volatilidad realizada rolling, anualizada.

        std(log_returns, window=20) × sqrt(periodos_por_año).
        _ANNUALIZATION_MAP almacena enteros — np.sqrt() se aplica aquí.
        """
        df  = df.copy()
        ann = _ANNUALIZATION_MAP.get(timeframe, _DEFAULT_ANNUALIZATION)
        df["volatility_20"] = (
            df["log_return"]
            .rolling(_ROLLING_WINDOW, min_periods=_MIN_PERIODS)
            .std()
            * np.sqrt(ann)
        )
        return df

    @staticmethod
    def _compute_vwap(df: pd.DataFrame) -> pd.DataFrame:
        """
        VWAP rolling 20 periodos sobre typical price × volume.

        vwap = Σ(tp × vol, 20) / Σ(vol, 20)
        donde tp = (high + low + close) / 3

        Por qué NO acumulado
        --------------------
        VWAP acumulado desde t=0 produce señal falsa en series multi-sesión:
        el precio medio histórico total no es el nivel de referencia de la sesión.
        En backtesting diario/semanal genera look-ahead bias implícito.
        Typical-price rolling es la convención estándar (Bloomberg, QuantLib).
        """
        df   = df.copy()
        tp   = (df["high"] + df["low"] + df["close"]) / 3
        vol  = df["volume"].replace(0, float("nan"))
        tpv  = tp * vol

        roll_tpv = tpv.rolling(_ROLLING_WINDOW, min_periods=_MIN_PERIODS).sum()
        roll_vol = vol.rolling(_ROLLING_WINDOW, min_periods=_MIN_PERIODS).sum()

        df["vwap"] = np.where(
            roll_vol > 0,
            roll_tpv / roll_vol,
            float("nan"),
        )
        return df

    @staticmethod
    def _compute_high_low_spread(df: pd.DataFrame) -> pd.DataFrame:
        """
        (high - low) / close — rango normalizado por precio de cierre.

        Normalizar por close (no por open) es la convención estándar
        porque close es el precio de referencia de la sesión.
        """
        df              = df.copy()
        safe_close      = df["close"].replace(0, float("nan"))
        df["high_low_spread"] = (df["high"] - df["low"]) / safe_close
        return df

    @staticmethod
    def _compute_volume_zscore(df: pd.DataFrame) -> pd.DataFrame:
        """
        Z-score de volumen rolling.

        volume_z > 2  → spike de actividad.
        volume_z < -2 → vacío de liquidez.
        std=0 → NaN (no división por cero).
        """
        df   = df.copy()
        roll = df["volume"].rolling(_ROLLING_WINDOW, min_periods=_MIN_PERIODS)
        df["volume_z"] = (
            (df["volume"] - roll.mean())
            / roll.std().replace(0, float("nan"))
        )
        return df

    @staticmethod
    def _compute_price_range(df: pd.DataFrame) -> pd.DataFrame:
        """
        (high - low) / open — volatilidad intra-bar.

        Siempre >= 0 por invariante OHLC (high >= low).
        open=0 → NaN (no división por cero).
        """
        df              = df.copy()
        safe_open       = df["open"].replace(0, float("nan"))
        df["price_range_pct"] = (df["high"] - df["low"]) / safe_open
        return df

    @staticmethod
    def _compute_body_pct(df: pd.DataFrame) -> pd.DataFrame:
        """
        abs(close - open) / open — tamaño del cuerpo de la vela.

        Alto → decisión direccional clara.
        Bajo → doji / indecisión.
        """
        df          = df.copy()
        safe_open   = df["open"].replace(0, float("nan"))
        df["body_pct"] = (df["close"] - df["open"]).abs() / safe_open
        return df

    @staticmethod
    def _compute_is_suspect(df: pd.DataFrame) -> pd.DataFrame:
        """
        bool derivado de quality_flag Silver.

        Filtrado en estrategias: gold_df[~gold_df["is_suspect"]].
        False si quality_flag no existe — degradación segura.
        """
        df = df.copy()
        df["is_suspect"] = (
            df["quality_flag"] == "suspect"
            if "quality_flag" in df.columns
            else False
        )
        return df

    @staticmethod
    def _sanitize(df: pd.DataFrame) -> pd.DataFrame:
        """
        ±inf → NaN en todas las columnas numéricas.

        Semántica NaN de GoldTransformer:
          - close=0 o prev_close=0  → NaN  (log indefinido)
          - ventana rolling corta   → NaN  (min_periods no alcanzado)
          - ±inf por división       → NaN  (sanitizados aquí)

        Política NaN: los NaN se PROPAGAN. El caller (QualityPipeline,
        estrategia) decide si imputa o dropa. SSOT de política: caller.

        Fail-soft: nunca lanza excepción.
        """
        numeric  = df.select_dtypes(include="number")
        inf_mask = np.isinf(numeric)
        n_inf    = int(inf_mask.values.sum())

        if n_inf:
            logger.warning(
                "GoldTransformer._sanitize: ±inf→NaN | count={}", n_inf
            )
            df = df.copy()
            df[numeric.columns] = numeric.replace(
                [np.inf, -np.inf], float("nan")
            )
        return df
