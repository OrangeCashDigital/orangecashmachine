"""
Research Data Access Layer – OrangeCashMachine
==============================================

Responsabilidad
---------------
Proveer una API limpia para acceder a datos de mercado
desde el Data Lake dentro del entorno de research.

Esto desacopla:

Research Layer
      ↓
Data Platform

Principios aplicados
--------------------

• SOLID
• DRY
• KISS
• SafeOps

Ejemplo de uso
--------------

from research.data.data_access import get_ohlcv

df = get_ohlcv(
    symbol="BTC/USDT",
    timeframe="1m",
    start="2023-01-01",
    end="2023-03-01"
)
"""

from typing import Optional, List, Dict
import pandas as pd
from loguru import logger

from data_platform.loaders.market_data_loader import MarketDataLoader


# ---------------------------------------------------------
# Singleton loader
# ---------------------------------------------------------

_loader = MarketDataLoader()


# ---------------------------------------------------------
# Public APIs
# ---------------------------------------------------------

def get_ohlcv(
    symbol: str,
    timeframe: str,
    start: Optional[str] = None,
    end: Optional[str] = None
) -> pd.DataFrame:
    """
    Obtiene OHLCV desde el Data Lake.

    Parameters
    ----------
    symbol : str
        Ejemplo: BTC/USDT

    timeframe : str
        Ejemplo: 1m, 5m, 1h

    start : str, optional
        Fecha inicial

    end : str, optional
        Fecha final
    """

    try:

        if start or end:

            df = _loader.load_ohlcv_range(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start,
                end_date=end
            )

        else:

            df = _loader.load_ohlcv(
                symbol=symbol,
                timeframe=timeframe
            )

        logger.info(
            f"Research OHLCV loaded → {symbol} {timeframe} ({len(df)} rows)"
        )

        return df

    except Exception as e:

        logger.error(f"Error loading OHLCV for {symbol}: {e}")
        raise


def get_multiple_ohlcv(
    symbols: List[str],
    timeframe: str
) -> Dict[str, pd.DataFrame]:
    """
    Carga múltiples símbolos.

    Returns
    -------
    dict[symbol -> DataFrame]
    """

    try:

        data = _loader.load_multiple_symbols(
            symbols=symbols,
            timeframe=timeframe
        )

        logger.info(
            f"Research multi-symbol load → {len(data)} symbols"
        )

        return data

    except Exception as e:

        logger.error(f"Error loading multi-symbol data: {e}")
        raise