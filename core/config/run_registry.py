from __future__ import annotations

"""
core/config/run_registry.py
============================

Persistencia de metadata de ejecución (auditabilidad).

Escribe un registro JSON por cada run en:
    logs/run_registry.jsonl   (append-only, una línea JSON por run)

Uso
---
    from core.config.run_registry import record_run

    record_run(
        env=run_cfg.env,
        git_hash=git_hash,
        config_hash=config_hash,
        exchanges=["kucoin", "bybit"],
        result="success",
        duration_s=42.3,
    )

Formato de cada línea
---------------------
{
  "run_id":    "20260403T154201-a1b2c3d4",
  "timestamp": "2026-04-03T15:42:01.123456+00:00",
  "env":        "development",
  "git_hash":   "dbfbf99",
  "config_hash": "73ec1637ab12",
  "exchanges":  ["kucoin", "kucoinfutures"],
  "result":     "success",
  "duration_s": 42.3
}
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


_REGISTRY_PATH = Path("logs/run_registry.jsonl")


def record_run(
    *,
    env:         str,
    git_hash:    str,
    config_hash: str,
    exchanges:   List[str],
    result:      str,
    duration_s:  float,
    run_id:      Optional[str] = None,
) -> str:
    """
    Persiste metadata de un run en el registry JSONL.

    SafeOps: nunca lanza — un fallo de escritura no debe abortar el pipeline.
    Retorna el run_id para correlación con logs.
    """
    if run_id is None:
        ts  = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        uid = uuid.uuid4().hex[:8]
        run_id = f"{ts}-{uid}"

    record = {
        "run_id":      run_id,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "env":         env,
        "git_hash":   git_hash,
        "config_hash": config_hash,
        "exchanges":  exchanges,
        "result":     result,
        "duration_s": round(duration_s, 3),
    }

    try:
        _REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _REGISTRY_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "
")
    except Exception:
        pass  # SafeOps — auditabilidad no debe romper el pipeline

    return run_id


__all__ = ["record_run"]
