"""
core/config/loader/snapshot.py
================================
Config snapshot inmutable por run_id.

Propósito
---------
Registrar en disco la configuración exacta que usó cada run.
Permite reproducibilidad total y auditoría post-mortem.

Formato del archivo
-------------------
logs/config_snapshots/{run_id}_{hash8}.json

{
  "run_id":      "abc123def456",
  "config_hash": "370676ed",
  "env":         "development",
  "snapshot_at": "2026-04-05T12:00:00Z",
  "config":      { ... AppConfig completo serializado ... }
}

Garantías
---------
- Escritura atómica: tmp → rename (nunca archivo parcial)
- Fail-soft: nunca propaga excepciones al pipeline
- Idempotente: mismo run_id + hash → mismo archivo (no sobreescribe)
"""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger


_SNAPSHOT_DIR = Path("logs/config_snapshots")


def write_config_snapshot(
    config,
    run_id:      str,
    config_hash: str,
    env:         str,
    snapshot_dir: Optional[Path] = None,
) -> Optional[Path]:
    """
    Serializa AppConfig a JSON y lo escribe atómicamente en disco.

    Retorna el Path del snapshot escrito, o None si falló (fail-soft).

    Parámetros
    ----------
    config      : AppConfig validado (Pydantic model)
    run_id      : identificador único del run (hex[:12])
    config_hash : hash del config mergeado (primeros 8 chars)
    env         : nombre del entorno ("development", etc.)
    snapshot_dir: override del directorio — útil en tests
    """
    out_dir = Path(snapshot_dir) if snapshot_dir else _SNAPSHOT_DIR

    try:
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"{run_id}_{config_hash[:8]}.json"

        # Idempotente: si ya existe con mismo run_id+hash, no reescribir
        if out_path.exists():
            logger.debug(
                "config_snapshot_exists | run_id={} path={}",
                run_id, out_path,
            )
            return out_path

        # Serializar AppConfig — model_dump() en Pydantic v2, dict() en v1
        try:
            config_dict = config.model_dump(mode="json")
        except AttributeError:
            config_dict = config.dict()

        snapshot = {
            "run_id":      run_id,
            "config_hash": config_hash,
            "env":         env,
            "snapshot_at": datetime.now(timezone.utc).isoformat(),
            "config":      config_dict,
        }

        # Escritura atómica: tmp en mismo directorio → rename
        tmp = out_dir / f".{run_id}_{config_hash[:8]}.tmp"
        tmp.write_text(json.dumps(snapshot, indent=2, default=str), encoding="utf-8")
        tmp.replace(out_path)

        logger.info(
            "config_snapshot_written | run_id={} env={} hash={} path={}",
            run_id, env, config_hash[:8], out_path,
        )
        return out_path

    except Exception as exc:
        # Fail-soft: el snapshot es auditoría, nunca debe romper el pipeline
        logger.warning(
            "config_snapshot_failed | run_id={} error={} type={}",
            run_id, exc, type(exc).__name__,
        )
        return None
