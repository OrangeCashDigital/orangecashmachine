# -*- coding: utf-8 -*-
"""
market_data/lineage/tracker.py
================================

Repositorio de lineage — OrangeCashMachine.

Responsabilidad
---------------
Persistir eventos de trazabilidad (LineageEvent) en SQLite append-only
y exponerlos para consulta por run_id o por símbolo/timeframe.

No define tipos de dominio — los importa desde domain/events (DIP).

Garantías
---------
• Reproducibilidad: dado un run_id, qué datos entraron y salieron
• Auditoría:        quién transformó qué, cuándo, con qué parámetros
• Debugging:        localizar dónde se perdieron/alteraron datos
• Observabilidad:   throughput y pérdida por capa

Diseño
------
• SQLite append-only: cada evento es inmutable (solo INSERT, nunca UPDATE)
• run_id como clave de correlación entre capas (UUID v4)
• Fail-soft en escritura: lineage nunca bloquea el pipeline
• Separación entre LineageEvent (dominio) y LineageTracker (repositorio)

Principios
----------
SRP    — solo persiste y consulta; no define tipos de dominio
DIP    — importa PipelineLayer, LineageStatus, LineageEvent desde domain/events
OCP    — nuevas capas/eventos sin modificar el tracker
DRY    — _insert() centraliza toda escritura
SafeOps — fallos en lineage: loguear, nunca propagar al pipeline
SSOT   — tracker singleton a nivel de módulo, injectable para tests
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path
from typing import List

from loguru import logger

# Tipos de dominio — importar desde domain/events, nunca definir aquí (DIP · SSOT)
from market_data.domain.events import (  # noqa: F401
    LineageEvent,
    LineageStatus,
    PipelineLayer,
)


# ===========================================================================
# DDL
# ===========================================================================

_DDL = """
CREATE TABLE IF NOT EXISTS lineage (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        TEXT    NOT NULL,
    layer         TEXT    NOT NULL,
    exchange      TEXT    NOT NULL,
    symbol        TEXT    NOT NULL,
    timeframe     TEXT    NOT NULL,
    rows_in       INTEGER NOT NULL,
    rows_out      INTEGER NOT NULL,
    status        TEXT    NOT NULL,
    quality_score REAL,
    params        TEXT    NOT NULL DEFAULT '{}',
    ts            TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_lineage_run_id ON lineage(run_id);
CREATE INDEX IF NOT EXISTS idx_lineage_symbol ON lineage(exchange, symbol, timeframe);
CREATE INDEX IF NOT EXISTS idx_lineage_ts     ON lineage(ts);
"""

_INSERT = """
INSERT INTO lineage
    (run_id, layer, exchange, symbol, timeframe, rows_in, rows_out,
     status, quality_score, params, ts)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


# ===========================================================================
# LineageTracker — repositorio
# ===========================================================================

class LineageTracker:
    """
    Repositorio append-only de eventos de lineage.

    Uso típico
    ----------
    run_id = LineageTracker.new_run_id()

    lineage_tracker.record(LineageEvent(
        run_id=run_id, layer=PipelineLayer.RAW, ...
    ))
    lineage_tracker.record(LineageEvent(
        run_id=run_id, layer=PipelineLayer.SILVER, ...
    ))

    events = lineage_tracker.get_run(run_id)
    """

    def __init__(
        self,
        db_path: Path = Path("data/lineage/lineage.db"),
    ) -> None:
        self._db_path = db_path
        self._init_db()

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        """Crea directorio y schema DDL si no existen."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_DDL)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self._db_path), check_same_thread=False)

    # ── API pública ───────────────────────────────────────────────────────────

    @staticmethod
    def new_run_id() -> str:
        """Genera un run_id UUID v4. Llamar una vez por lote/chunk."""
        return str(uuid.uuid4())

    def record(self, event: LineageEvent) -> None:
        """
        Persiste un LineageEvent.

        Fail-soft: nunca interrumpe el pipeline si falla.
        """
        try:
            self._insert(event)
            logger.debug(
                "Lineage | run={} layer={} {}/{} exchange={} "
                "rows={}/{} loss={:.1%} status={}",
                event.run_id[:8], event.layer.value,
                event.symbol, event.timeframe, event.exchange,
                event.rows_out, event.rows_in,
                event.loss_rate, event.status.value,
            )
        except Exception as exc:
            logger.warning(
                "LineageTracker.record() failed (non-critical) | "
                "run={} layer={} err={}",
                event.run_id[:8], event.layer, exc,
            )

    def get_run(self, run_id: str) -> List[LineageEvent]:
        """
        Retorna todos los eventos de un run_id ordenados por ts.

        Permite reconstruir el flujo completo: RAW → SILVER → GOLD.
        """
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT run_id, layer, exchange, symbol, timeframe, "
                    "rows_in, rows_out, status, quality_score, params, ts "
                    "FROM lineage WHERE run_id = ? ORDER BY ts",
                    (run_id,),
                ).fetchall()
            return [self._row_to_event(r) for r in rows]
        except Exception as exc:
            logger.warning(
                "LineageTracker.get_run() failed | run={} err={}",
                run_id[:8], exc,
            )
            return []

    def get_symbol_history(
        self,
        exchange:  str,
        symbol:    str,
        timeframe: str,
        limit:     int = 100,
    ) -> List[LineageEvent]:
        """
        Historial de procesamiento de un par/timeframe.

        Útil para análisis longitudinal de pérdida de datos.
        """
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT run_id, layer, exchange, symbol, timeframe, "
                    "rows_in, rows_out, status, quality_score, params, ts "
                    "FROM lineage "
                    "WHERE exchange=? AND symbol=? AND timeframe=? "
                    "ORDER BY ts DESC LIMIT ?",
                    (exchange, symbol, timeframe, limit),
                ).fetchall()
            return [self._row_to_event(r) for r in rows]
        except Exception as exc:
            logger.warning(
                "LineageTracker.get_symbol_history() failed | err={}", exc,
            )
            return []

    # ── Helpers privados ──────────────────────────────────────────────────────

    def _insert(self, event: LineageEvent) -> None:
        with self._connect() as conn:
            conn.execute(_INSERT, (
                event.run_id,
                event.layer.value,
                event.exchange,
                event.symbol,
                event.timeframe,
                event.rows_in,
                event.rows_out,
                event.status.value,
                event.quality_score,
                json.dumps(event.params),
                event.ts,
            ))

    @staticmethod
    def _row_to_event(row: tuple) -> LineageEvent:
        return LineageEvent(
            run_id        = row[0],
            layer         = PipelineLayer(row[1]),
            exchange      = row[2],
            symbol        = row[3],
            timeframe     = row[4],
            rows_in       = row[5],
            rows_out      = row[6],
            status        = LineageStatus(row[7]),
            quality_score = row[8],
            params        = json.loads(row[9]) if row[9] else {},
            ts            = row[10],
        )


# ===========================================================================
# Singleton de módulo — SSOT, injectable para tests
# ===========================================================================

lineage_tracker: LineageTracker = LineageTracker()
"""
Instancia compartida para uso en producción.

Injectable para tests:
    tracker = LineageTracker(db_path=tmp_path / "lineage.db")
"""

__all__ = [
    # Re-exports de domain/events para backward-compat
    "PipelineLayer",
    "LineageStatus",
    "LineageEvent",
    # Repositorio
    "LineageTracker",
    "lineage_tracker",
]
