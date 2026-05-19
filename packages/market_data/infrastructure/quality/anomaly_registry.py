"""
Anomaly Registry — OrangeCashMachine
=====================================

Responsabilidad
---------------
Registro persistente de anomalías de calidad de datos.
Evita log-spam de la misma anomalía en runs incrementales
sin perder historial entre reinicios de proceso.

Diseño
------
• SQLite (stdlib) — cero dependencias extra, ACID, append-only
• In-memory cache como L1: evita RTT a disco en hot path
• Thread-safe: Lock local por instancia
• Fail-soft: si SQLite falla en write, loguea y retorna True
  (preferir log duplicado a silenciar una anomalía real — SafeOps)
• Singleton a nivel de módulo (SSOT); injectable en tests

Principios
----------
SOLID  — SRP: solo registra anomalías, nada más
DRY    — _persist_upsert reutilizado en is_new e _increment
KISS   — SQLite sobre Redis/Postgres; la complejidad no se justifica aún
SafeOps — fallos explícitos en escritura, nunca swallow silencioso
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger


# ==========================================================
# Types
# ==========================================================

_AnomalyKey = tuple[str, str, str, str]  # (exchange, symbol, timeframe, reason_key)


# ==========================================================
# Constants
# ==========================================================

_DEFAULT_DB_PATH = Path("data/quality/anomaly_registry.db")

_DDL = """
CREATE TABLE IF NOT EXISTS anomalies (
    exchange    TEXT    NOT NULL,
    symbol      TEXT    NOT NULL,
    timeframe   TEXT    NOT NULL,
    reason_key  TEXT    NOT NULL,
    first_seen  TEXT    NOT NULL,
    last_seen   TEXT    NOT NULL,
    count       INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (exchange, symbol, timeframe, reason_key)
);
"""

_UPSERT = """
INSERT INTO anomalies (exchange, symbol, timeframe, reason_key, first_seen, last_seen, count)
VALUES (?, ?, ?, ?, ?, ?, 1)
ON CONFLICT (exchange, symbol, timeframe, reason_key)
DO UPDATE SET
    last_seen = excluded.last_seen,
    count     = count + 1;
"""


# ==========================================================
# AnomalyRegistry
# ==========================================================

class AnomalyRegistry:
    """
    Registro thread-safe y persistente de anomalías de calidad.

    Ciclo de vida típico
    --------------------
    1. __init__: crea DB + tabla si no existen, carga cache L1 desde disco.
    2. is_new():  consulta L1 (lock), si nueva → escribe a disco (fail-soft).
    3. Al reiniciar: warm_cache() recarga el estado desde SQLite.

    Nota sobre concurrencia
    -----------------------
    check_same_thread=False permite reutilizar la conexión entre threads,
    pero el Lock garantiza que solo un thread escriba a la vez.
    Para escrituras de alta frecuencia, migrar a WAL mode (PRAGMA journal_mode=WAL).
    """

    def __init__(self, db_path: Path = _DEFAULT_DB_PATH) -> None:
        self._db_path = db_path
        self._lock    = threading.Lock()
        self._cache:  set[_AnomalyKey] = set()
        self._init_db()
        self._warm_cache()

    # ----------------------------------------------------------
    # Setup
    # ----------------------------------------------------------

    def _init_db(self) -> None:
        """Crea directorio y tabla DDL si no existen. Fail-fast en error de FS."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")   # escrituras concurrentes más seguras
            conn.execute(_DDL)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self._db_path), check_same_thread=False)

    def _warm_cache(self) -> None:
        """
        Carga todas las claves conocidas en L1 al arrancar.

        O(n) al inicio, O(1) en el hot path posterior.
        Aceptable: el número de anomalías únicas es acotado.
        """
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT exchange, symbol, timeframe, reason_key FROM anomalies"
                ).fetchall()
            with self._lock:
                self._cache = {(r[0], r[1], r[2], r[3]) for r in rows}
            logger.debug(
                "AnomalyRegistry: warm cache loaded | known_anomalies={}",
                len(self._cache),
            )
        except Exception as exc:
            # Fail-soft: arrancar con cache vacío es preferible a crashear
            logger.warning(
                "AnomalyRegistry: warm_cache failed (starting empty) | err={}",
                exc,
            )
            with self._lock:
                self._cache = set()

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    def is_new(
        self,
        exchange:  str,
        symbol:    str,
        timeframe: str,
        reason:    str,
    ) -> bool:
        """
        Retorna True si esta anomalía no había sido vista antes y la registra.

        Normalización del reason
        ------------------------
        "price_outliers_mad (n=3)" → "price_outliers_mad"
        Agrupa el mismo tipo de anomalía independiente del conteo.

        Thread-safety
        -------------
        Lock solo en el check/add del cache L1.
        La escritura a SQLite es fuera del lock para no bloquear otros threads.
        Race condition teórica (dos threads ven is_new=True para la misma clave)
        es aceptable: produce un log extra pero no corrupción de datos.
        """
        reason_key = reason.split("(")[0].strip()
        key: _AnomalyKey = (exchange, symbol, timeframe, reason_key)

        with self._lock:
            already_known = key in self._cache
            if not already_known:
                self._cache.add(key)

        if already_known:
            self._upsert(key)   # incrementa count en background
            return False

        self._upsert(key)
        return True

    def clear(self) -> None:
        """Limpia solo el cache L1. No toca la DB. Para tests que necesitan re-evaluar."""
        with self._lock:
            self._cache.clear()

    def wipe(self) -> None:
        """
        Borra cache L1 y tabla DB completa.

        ⚠️  Solo para tests o reset manual. No llamar en producción.
        """
        with self._lock:
            self._cache.clear()
        try:
            with self._connect() as conn:
                conn.execute("DELETE FROM anomalies;")
            logger.warning("AnomalyRegistry: DB wiped (all history lost)")
        except Exception as exc:
            logger.error("AnomalyRegistry: wipe failed | err={}", exc)

    def stats(self) -> list[dict]:
        """
        Retorna lista de anomalías conocidas con conteos para observabilidad.

        Útil para dashboards o auditorías periódicas.
        """
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT exchange, symbol, timeframe, reason_key, first_seen, last_seen, count "
                    "FROM anomalies ORDER BY count DESC"
                ).fetchall()
            return [
                {
                    "exchange":   r[0], "symbol":     r[1],
                    "timeframe":  r[2], "reason_key": r[3],
                    "first_seen": r[4], "last_seen":  r[5],
                    "count":      r[6],
                }
                for r in rows
            ]
        except Exception as exc:
            logger.warning("AnomalyRegistry.stats() failed | err={}", exc)
            return []

    # ----------------------------------------------------------
    # Private Helpers
    # ----------------------------------------------------------

    def _upsert(self, key: _AnomalyKey) -> None:
        """
        INSERT OR UPDATE en SQLite.

        Fail-soft: loguea el error pero no propaga — la calidad del pipeline
        no debe depender de la disponibilidad del registro de anomalías.
        """
        now = datetime.now(timezone.utc).isoformat()
        try:
            with self._connect() as conn:
                conn.execute(_UPSERT, (*key, now, now))
        except Exception as exc:
            logger.warning(
                "AnomalyRegistry: _upsert failed (non-critical) | key={} err={}",
                key, exc,
            )


# ==========================================================
# Module-level singleton — SSOT
# ==========================================================
# Injectable en tests via QualityPipeline(registry=AnomalyRegistry(db_path=tmp_path))

_registry = AnomalyRegistry()

# Alias público del singleton — SSOT, injectable en tests.
# _registry se mantiene por compatibilidad interna; callers externos usan default_registry.
default_registry: AnomalyRegistry = _registry
