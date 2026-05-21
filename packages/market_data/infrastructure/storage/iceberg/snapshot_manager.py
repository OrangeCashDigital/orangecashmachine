# -*- coding: utf-8 -*-
"""
infrastructure/storage/iceberg/snapshot_manager.py

Responsabilidad única: consultar metadatos de snapshots Iceberg
(versión semántica, timestamp de escritura, linaje).

No escribe datos — solo lectura de metadatos de Table.
Bounded context: Infrastructure / Storage / Iceberg.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from loguru import logger

if TYPE_CHECKING:
    from pyiceberg.table import Table


class SnapshotManager:
    """
    Proxy de solo-lectura sobre metadatos de snapshot Iceberg.

    Desacoplado de IcebergStorage (SRP): IcebergStorage escribe datos,
    SnapshotManager reporta el estado semántico de la tabla.
    """

    def __init__(self, exchange: str, market_type: str) -> None:
        self._exchange = exchange
        self._market_type = market_type

    def get_version(
        self,
        table: "Table",
        symbol: str,
        timeframe: str,
    ) -> Optional[dict]:
        """
        Retorna metadatos del snapshot actual como proxy de versión semántica.

        Returns None si la tabla no tiene snapshot o falla la consulta.
        Fail-soft: nunca propaga excepciones al caller.
        """
        try:
            snap = table.current_snapshot()
            if snap is None:
                logger.debug(
                    "SnapshotManager.get_version: sin snapshot | {}/{}",
                    symbol,
                    timeframe,
                )
                return None
            return {
                "version_id": str(snap.snapshot_id),
                "written_at": str(snap.timestamp_ms),
                "symbol": symbol,
                "timeframe": timeframe,
                "exchange": self._exchange,
                "market_type": self._market_type,
            }
        except Exception:
            logger.opt(exception=True).debug(
                "SnapshotManager.get_version failed | {}/{}",
                symbol,
                timeframe,
            )
            return None

    def is_fresh(
        self,
        table: "Table",
        symbol: str,
        timeframe: str,
        max_age_ms: int = 3_600_000,  # 1 hora por defecto
    ) -> bool:
        """
        Heurístico: ¿el snapshot fue escrito en los últimos max_age_ms ms?

        Útil para health-checks y decisiones de re-ingestión.
        Fail-soft: retorna False ante cualquier error.
        """
        meta = self.get_version(table, symbol, timeframe)
        if meta is None:
            return False
        try:
            written_at_ms = int(meta["written_at"])
            import time

            age_ms = int(time.time() * 1000) - written_at_ms
            return age_ms <= max_age_ms
        except Exception:
            logger.opt(exception=True).debug(
                "SnapshotManager.is_fresh error | {}/{}",
                symbol,
                timeframe,
            )
            return False
