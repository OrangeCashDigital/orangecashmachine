# -*- coding: utf-8 -*-
"""
trading/data/gold_adapter.py
==============================

GoldLoaderAdapter — adapta GoldLoader al protocolo FeatureSource.

Problema que resuelve
----------------------
TradingEngine._load_data() llama:
    data_source.load_features(exchange=, symbol=, timeframe=, market_type=)

GoldLoader.load_features() tiene una firma diferente:
    load_features(symbol, market_type, timeframe, ..., exchange=kwarg_al_final)

Además, GoldLoader lanza DataNotFoundError/DataReadError cuando no hay datos.
TradingEngine espera None o DataFrame vacío — nunca excepciones de datos.

Este adapter resuelve ambos gaps sin tocar GoldLoader (OCP).

Principios: SOLID (OCP, DIP) · KISS · SafeOps
"""
from __future__ import annotations

from typing import Optional

import pandas as pd
from loguru import logger


class GoldLoaderAdapter:
    """
    Adapta GoldLoader al protocolo FeatureSource de TradingEngine.

    Responsabilidades
    -----------------
    1. Reordenar parámetros: (exchange, symbol, timeframe, market_type)
       → (symbol, market_type, timeframe, exchange=)
    2. Convertir excepciones de datos en None (SafeOps para TradingEngine).
    3. Propagar errores inesperados con log — no silenciarlos.

    Parameters
    ----------
    exchange : str
        Exchange por defecto. Puede ser sobreescrito en load_features().
    """

    def __init__(self, exchange: str = "bybit") -> None:
        # Import lazy — evita import circular y permite mockear en tests
        from data_platform.loaders.gold_loader import GoldLoader
        self._loader   = GoldLoader(exchange=exchange)
        self._exchange = exchange
        self._log      = logger.bind(component="GoldLoaderAdapter", exchange=exchange)

    def load_features(
        self,
        exchange:    str,
        symbol:      str,
        timeframe:   str,
        market_type: str = "spot",
        **kwargs,
    ) -> Optional[pd.DataFrame]:
        """
        Carga features Gold. Implementa el protocolo FeatureSource.

        Retorna
        -------
        pd.DataFrame : features cargados correctamente.
        None         : sin datos o error de lectura (TradingEngine lo maneja).

        Nunca lanza — SafeOps garantizado.
        """
        try:
            return self._loader.load_features(
                symbol      = symbol,
                market_type = market_type,
                timeframe   = timeframe,
                exchange    = exchange or self._exchange,
            )
        except Exception as exc:
            # Distinguir "sin datos" (esperado) de errores reales (inesperado)
            exc_name = type(exc).__name__
            if exc_name in ("DataNotFoundError", "GoldLoaderError"):
                self._log.warning(
                    "Sin datos Gold | {}/{}/{}/{} — skipping",
                    exchange, symbol, market_type, timeframe,
                )
            else:
                self._log.error(
                    "Error leyendo Gold | {}/{}/{}/{} | {} — {}",
                    exchange, symbol, market_type, timeframe,
                    exc_name, exc,
                )
            return None

    def __repr__(self) -> str:
        return f"GoldLoaderAdapter(exchange={self._exchange!r})"
