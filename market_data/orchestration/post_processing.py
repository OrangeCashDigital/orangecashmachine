from __future__ import annotations

"""
market_data/orchestration/post_processing.py
============================================

Post-procesado tras ingestión: Gold build.

Responsabilidad
---------------
Encapsula todo lo que ocurre DESPUÉS de market_data_flow.
El entrypoint llama PostProcessingService.execute() sin
conocer GoldStorage directamente.

Principios: SOLID · KISS · DIP (inyección de dependencias)
"""

from typing import Optional

from core.config.schema import AppConfig
from core.logging import bind_pipeline
from market_data.storage.gold.gold_storage import GoldStorage

_log = bind_pipeline("post_processing")


class PostProcessingService:
    """
    Ejecuta Gold build después de la ingestión.

    Diseño
    ------
    - Gold fallido no propaga excepción — best-effort.
    - Dependencias inyectables → testeable sin filesystem.

    Uso en producción
    -----------------
        PostProcessingService(config).execute()

    Uso en tests
    ------------
        PostProcessingService(config, gold_storage=mock_gold).execute()
    """

    def __init__(
        self,
        config: AppConfig,
        gold_storage: Optional[GoldStorage] = None,
        run_id: Optional[str] = None,
    ) -> None:
        self._config   = config
        self._gold     = gold_storage or GoldStorage()
        self._run_id   = run_id

    def execute(self) -> None:
        """Ejecuta Gold build. Nunca lanza excepción."""
        self._build_gold()

    # ----------------------------------------------------------
    # Private
    # ----------------------------------------------------------


    def _build_gold(self) -> None:
        try:
            for ex in self._config.exchanges:
                if ex.has_spot and ex.markets.spot_symbols:
                    self._gold.build_all(
                        exchange=ex.name.value,
                        symbols=ex.markets.spot_symbols,
                        market_type="spot",
                        timeframes=self._config.pipeline.historical.timeframes,
                        run_id=self._run_id,
                    )
                if ex.has_futures and ex.markets.futures_symbols:
                    self._gold.build_all(
                        exchange=ex.name.value,
                        symbols=ex.markets.futures_symbols,
                        market_type=ex.markets.futures_default_type or "swap",
                        timeframes=self._config.pipeline.historical.timeframes,
                        run_id=self._run_id,
                    )
            _log.info("gold_build_completed")
        except Exception as exc:
            _log.opt(exception=True).warning(
                "gold_build_failed",
                error_type=type(exc).__name__,
            )
