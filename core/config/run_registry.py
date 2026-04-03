from __future__ import annotations

"""
core/config/run_registry.py
============================

Persistencia de metadata de ejecución (auditabilidad).

Storage
-------
Primario : SQLite  — logs/run_registry.db  (consultable, indexado)
Fallback : JSONL   — logs/run_registry.jsonl (grep-able, append-only)

Si SQLite falla (permisos, disco lleno), escribe en JSONL y sigue.
Ambos coexisten — no son mutuamente excluyentes.

Uso
---
    from core.config.run_registry import record_run, query_runs

    run_id = record_run(
        env="development",
        git_hash="abc1234",
        config_hash="73ec1637ab12",
        exchanges=["kucoin", "bybit"],
        result="success",
        duration_s=42.3,
    )

    runs = query_runs(limit=10)
    runs = query_runs(env="production", result="error", limit=5)

Schema SQLite
-------------
runs(
  run_id        TEXT PRIMARY KEY,
  timestamp     TEXT NOT NULL,
  env           TEXT NOT NULL,
  git_hash      TEXT NOT NULL,
  config_hash   TEXT NOT NULL,
  exchanges     TEXT NOT NULL,   -- JSON array
  result        TEXT NOT NULL,
  duration_s    REAL NOT NULL,
  extra         TEXT             -- JSON dict, nullable
)
"""

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


_DB_PATH      = Path("logs/run_registry.db")
_JSONL_PATH   = Path("logs/run_registry.jsonl")

_DDL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id       TEXT PRIMARY KEY,
    timestamp    TEXT NOT NULL,
    env          TEXT NOT NULL,
    git_hash     TEXT NOT NULL,
    config_hash  TEXT NOT NULL,
    exchanges    TEXT NOT NULL,
    result       TEXT NOT NULL,
    duration_s   REAL NOT NULL,
    extra        TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_env    ON runs(env);
CREATE INDEX IF NOT EXISTS idx_runs_result ON runs(result);
CREATE INDEX IF NOT EXISTS idx_runs_ts     ON runs(timestamp);
"""


def _ensure_db() -> sqlite3.Connection:
    """Abre/crea la base de datos y aplica el DDL. No cachea conexión (thread-safe)."""
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), timeout=5)
    conn.executescript(_DDL)
    conn.commit()
    return conn


def _make_run_id() -> str:
    ts  = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return f"{ts}-{uid}"


def record_run(
    *,
    env:         str,
    git_hash:    str,
    config_hash: str,
    exchanges:   List[str],
    result:      str,
    duration_s:  float,
    run_id:      Optional[str] = None,
    extra:       Optional[dict] = None,
) -> str:
    """
    Persiste metadata de un run.

    SafeOps: nunca lanza — un fallo de escritura no debe abortar el pipeline.
    Retorna el run_id para correlación con logs y manifests de Silver/Gold.
    """
    if run_id is None:
        run_id = _make_run_id()

    timestamp  = datetime.now(timezone.utc).isoformat()
    duration_r = round(duration_s, 3)
    exc_json   = json.dumps(exchanges)
    extra_json = json.dumps(extra) if extra else None

    # ── SQLite (primario) ────────────────────────────────────────────────
    _sqlite_ok = False
    try:
        conn = _ensure_db()
        conn.execute(
            """
            INSERT OR REPLACE INTO runs
              (run_id, timestamp, env, git_hash, config_hash, exchanges, result, duration_s, extra)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, timestamp, env, git_hash, config_hash, exc_json, result, duration_r, extra_json),
        )
        conn.commit()
        conn.close()
        _sqlite_ok = True
    except Exception:
        pass  # SafeOps — caer a JSONL

    # ── JSONL (fallback + grep-ability) ─────────────────────────────────
    record: Dict[str, Any] = {
        "run_id":      run_id,
        "timestamp":   timestamp,
        "env":         env,
        "git_hash":    git_hash,
        "config_hash": config_hash,
        "exchanges":   exchanges,
        "result":      result,
        "duration_s":  duration_r,
    }
    if extra:
        record["extra"] = extra
    if not _sqlite_ok:
        record["_sqlite_failed"] = True  # señal de diagnóstico

    try:
        _JSONL_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _JSONL_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
    except Exception:
        pass  # SafeOps — auditabilidad no rompe el pipeline

    return run_id


def query_runs(
    *,
    env:    Optional[str] = None,
    result: Optional[str] = None,
    limit:  int           = 20,
) -> List[Dict[str, Any]]:
    """
    Consulta runs recientes desde SQLite.

    Parameters
    ----------
    env    : filtrar por entorno ("development", "production")
    result : filtrar por resultado ("success", "error", "interrupted")
    limit  : máximo de filas a devolver (orden descendente por timestamp)

    Returns
    -------
    Lista de dicts, o [] si SQLite no existe o falla.

    Ejemplo
    -------
        for r in query_runs(result="error", limit=5):
            print(r["run_id"], r["duration_s"])
    """
    try:
        if not _DB_PATH.exists():
            return []
        conn = _ensure_db()

        clauses = []
        params: list = []
        if env:
            clauses.append("env = ?")
            params.append(env)
        if result:
            clauses.append("result = ?")
            params.append(result)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)

        rows = conn.execute(
            f"SELECT * FROM runs {where} ORDER BY timestamp DESC LIMIT ?",
            params,
        ).fetchall()
        cols = [d[0] for d in conn.description]
        conn.close()

        result_list = []
        for row in rows:
            d = dict(zip(cols, row))
            d["exchanges"] = json.loads(d["exchanges"])
            if d.get("extra"):
                d["extra"] = json.loads(d["extra"])
            result_list.append(d)
        return result_list

    except Exception:
        return []


__all__ = ["record_run", "query_runs"]
