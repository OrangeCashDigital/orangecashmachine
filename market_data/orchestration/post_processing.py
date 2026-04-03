from __future__ import annotations

"""
market_data/orchestration/post_processing.py
============================================

Post-procesado tras ingestión: snapshot + Gold build.

Responsabilidad
---------------
Encapsula todo lo que ocurre DESPUÉS de market_data_flow.
El entrypoint llama PostProcessingService.execute() sin
conocer GoldStorage ni SnapshotManager directamente.

Principios: SOLID · KISS · DIP (inyección de dependencias)
"""

from typing import Optional

from core.config.schema import AppConfig
from core.logging.setup import bind_pipeline
from market_data.storage.gold.snapshot import SnapshotManager
from market_data.storage.gold.gold_storage import GoldStorage

_log = bind_pipeline("post_processing")


class PostProcessingService:
    """
    Ejecuta snapshot + Gold build después de la ingestión.

    Diseño
    ------
    - Snapshot fallido no cancela Gold build.
    - Gold fallido no propaga excepción — ambos son best-effort.
    - Dependencias inyectables → testeable sin filesystem.

    Uso en producción
    -----------------
        PostProcessingService(config).execute()

    Uso en tests
    ------------
        PostProcessingService(config, snapshot_manager=mock_snap, gold_storage=mock_gold).execute()
    """

    def __init__(
        self,
        config: AppConfig,
        snapshot_manager: Optional[SnapshotManager] = None,
        gold_storage: Optional[GoldStorage] = None,
    ) -> None:
        self._config   = config
        self._snapshot = snapshot_manager or SnapshotManager()
        self._gold     = gold_storage or GoldStorage()

    def execute(self) -> None:
        """Ejecuta snapshot y Gold build. Nunca lanza excepción."""
        self._create_snapshot()
        self._build_gold()

    # ----------------------------------------------------------
    # Private
    # ----------------------------------------------------------

    def _create_snapshot(self) -> None:
        try:
            snapshot_id = self._snapshot.create_snapshot()
            _log.info("snapshot_created", snapshot_id=snapshot_id)
        except Exception as exc:
            _log.opt(exception=True).warning(
                "snapshot_failed",
                error_type=type(exc).__name__,
            )

    def _build_gold(self) -> None:
        try:
            for ex in self._config.exchanges:
                if ex.has_spot and ex.markets.spot_symbols:
                    self._gold.build_all(
                        exchange=ex.name.value,
                        symbols=ex.markets.spot_symbols,
                        market_type="spot",
                        timeframes=self._config.pipeline.historical.timeframes,
                    )
                if ex.has_futures and ex.markets.futures_symbols:
                    self._gold.build_all(
                        exchange=ex.name.value,
                        symbols=ex.markets.futures_symbols,
                        market_type="swap",
                        timeframes=self._config.pipeline.historical.timeframes,
                    )
            _log.info("gold_build_completed")
        except Exception as exc:
            _log.opt(exception=True).warning(
                "gold_build_failed",
                error_type=type(exc).__name__,
            )
