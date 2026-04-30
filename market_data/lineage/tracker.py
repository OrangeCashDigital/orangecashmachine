"""
Lineage Tracker — OrangeCashMachine
=====================================

Responsabilidad
---------------
Registrar la trazabilidad completa de cada lote de datos a través
de las capas del pipeline: RAW → SILVER → GOLD.

Garantiza
---------
• Reproducibilidad: dado un run_id, qué datos entraron y salieron
• Auditoría: quién transformó qué, cuándo, con qué parámetros
• Debugging: localizar dónde se perdieron/alteraron datos
• Observabilidad: métricas de throughput y pérdida por capa

Diseño (Ref: Owen 2023, Tamla 2025)
-------------------------------
• SQLite append-only — cada evento es inmutable (no UPDATE, solo INSERT)
• run_id como clave de correlación entre capas (UUID v4)
• Fail-soft en escritura: lineage no debe bloquear el pipeline
• Separación entre LineageEvent (dato) y LineageTracker (repositorio)

Principios
----------
SOLID  — OCP: nuevas capas/eventos sin modificar el tracker
DRY    — _insert() centraliza toda escritura
SafeOps — fallos en lineage: loguear, nunca propagar al pipeline
SSOT   — tracker singleton a nivel de módulo, injectable para tests
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from loguru import logger


# ==========================================================
# Types & Enums
# ==========================================================

class PipelineLayer(str, Enum):
    RAW    = "raw"
    SILVER = "silver"
    GOLD   = "gold"


class LineageStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"   # algunos registros descartados
    FAILED  = "failed"    # lote rechazado completamente


# ==========================================================
# LineageEvent — Value Object (inmutable)
# ==========================================================

@dataclass(frozen=True)
class LineageEvent:
    """
    Snapshot inmutable de una transición entre capas.

    Campos
    ------
    run_id      : correlación entre capas del mismo lote
    layer       : capa de destino (SILVER, GOLD, …)
    exchange    : origen del dato
    symbol      : par de trading (e.g. BTC/USDT)
    timeframe   : resolución temporal (e.g. 1m, 1h)
    rows_in     : filas que entraron a la transformación
    rows_out    : filas que salieron (después de filtros/drops)
    status      : resultado del procesamiento
    quality_score: puntuación de calidad [0.0–1.0], None si no aplica
    params      : dict con parámetros relevantes (overlap, chunk_limit, …)
    ts          : timestamp UTC del evento
    """
    run_id:        str
    layer:         PipelineLayer
    exchange:      str
    symbol:        str
    timeframe:     str
    rows_in:       int
    rows_out:      int
    status:        LineageStatus
    quality_score: Optional[float]        = None
    params:        dict[str, Any]         = field(default_factory=dict)
    ts:            str                    = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def loss_rate(self) -> float:
        """Fracción de filas descartadas. 0.0 = ninguna pérdida."""
        if self.rows_in == 0:
            return 0.0
        return (self.rows_in - self.rows_out) / self.rows_in


# ==========================================================
# DDL
# ==========================================================

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
CREATE INDEX IF NOT EXISTS idx_lineage_run_id   ON lineage(run_id);
CREATE INDEX IF NOT EXISTS idx_lineage_symbol   ON lineage(exchange, symbol, timeframe);
CREATE INDEX IF NOT EXISTS idx_lineage_ts       ON lineage(ts);
"""

_INSERT = """
INSERT INTO lineage
    (run_id, layer, exchange, symbol, timeframe, rows_in, rows_out,
     status, quality_score, params, ts)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


# ==========================================================
# LineageTracker
# ==========================================================

class LineageTracker:
    """
    Repositorio append-only de eventos de lineage.

    Uso típico
    ----------
    run_id = lineage_tracker.new_run_id()

    # En el fetcher (RAW):
    lineage_tracker.record(LineageEvent(
        run_id=run_id, layer=PipelineLayer.RAW, ...
    ))

    # En el transformer (SILVER):
    lineage_tracker.record(LineageEvent(
        run_id=run_id, layer=PipelineLayer.SILVER, ...
    ))

    # Consultar trazabilidad de un run:
    events = lineage_tracker.get_run(run_id)
    """

    def __init__(self, db_path: Path = Path("data/lineage/lineage.db")) -> None:
        self._db_path = db_path
        self._init_db()

    # ----------------------------------------------------------
    # Setup
    # ----------------------------------------------------------

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_DDL)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self._db_path), check_same_thread=False)

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    @staticmethod
    def new_run_id() -> str:
        """Genera un run_id UUID v4. Llama esto una vez por lote/chunk."""
        return str(uuid.uuid4())

    def record(self, event: LineageEvent) -> None:
        """
        Persiste un LineageEvent.

        Fail-soft: un fallo aquí nunca debe interrumpir el pipeline.
        El pipeline sigue funcionando aunque el registro de lineage falle.
        """
        try:
            self._insert(event)
            logger.debug(
                "Lineage | run={} layer={} {}/{} exchange={} rows={}/{} "
                "loss={:.1%} status={}",
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

    def get_run(self, run_id: str) -> list[LineageEvent]:
        """
        Retorna todos los eventos de un run_id ordenados por capa/ts.

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
            logger.warning("LineageTracker.get_run() failed | run={} err={}", run_id[:8], exc)
            return []

    def get_symbol_history(
        self,
        exchange:  str,
        symbol:    str,
        timeframe: str,
        limit:     int = 100,
    ) -> list[LineageEvent]:
        """
        Historial de procesamiento de un symbol/timeframe.

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
            logger.warning("LineageTracker.get_symbol_history() failed | err={}", exc)
            return []

    # ----------------------------------------------------------
    # Private Helpers
    # ----------------------------------------------------------

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


# ==========================================================
# Module-level singleton — SSOT
# ==========================================================

lineage_tracker = LineageTracker()
