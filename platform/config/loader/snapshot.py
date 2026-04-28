from __future__ import annotations

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
``logs/config_snapshots/{run_id}_{hash8}.json``::

    {
      "run_id":      "abc123def456",
      "config_hash": "370676ed",
      "env":         "development",
      "snapshot_at": "2026-04-05T12:00:00+00:00",
      "config":      { ... AppConfig completo serializado ... }
    }

Garantías
---------
- Escritura atómica: ``tmp → rename`` (nunca archivo parcial).
- Fail-soft: nunca propaga excepciones al pipeline principal.
- Idempotente: mismo ``run_id`` + ``hash`` → no sobreescribe.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from loguru import logger


_SNAPSHOT_DIR: Path = Path("logs/config_snapshots")


def write_config_snapshot(
    config: Any,
    run_id: str,
    config_hash: str,
    env: str,
    snapshot_dir: Optional[Path] = None,
) -> Optional[Path]:
    """Serializa AppConfig a JSON y lo escribe atómicamente en disco.

    Fail-soft: captura cualquier excepción y la loggea como warning.

    Args:
        config: AppConfig validado (Pydantic v2 con ``model_dump``).
        run_id: Identificador único del run (hex de 12 chars).
        config_hash: Hash SHA-256 del config mergeado.
        env: Nombre del entorno activo.
        snapshot_dir: Override del directorio de salida. Útil en tests.

    Returns:
        Path del snapshot escrito, o None si falló.
    """
    out_dir = Path(snapshot_dir) if snapshot_dir else _SNAPSHOT_DIR

    try:
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"{run_id}_{config_hash[:8]}.json"

        if out_path.exists():
            logger.debug(
                "config_snapshot_exists | run_id={} path={}",
                run_id, out_path,
            )
            return out_path

        try:
            config_dict: dict[str, Any] = config.model_dump(mode="json")
        except AttributeError:
            config_dict = config.dict()  # type: ignore[attr-defined]

        snapshot: dict[str, Any] = {
            "run_id": run_id,
            "config_hash": config_hash,
            "env": env,
            "snapshot_at": datetime.now(timezone.utc).isoformat(),
            "config": config_dict,
        }

        tmp = out_dir / f".{run_id}_{config_hash[:8]}.tmp"
        tmp.write_text(
            json.dumps(snapshot, indent=2, default=str),
            encoding="utf-8",
        )
        tmp.replace(out_path)

        logger.info(
            "config_snapshot_written | run_id={} env={} hash={} path={}",
            run_id, env, config_hash[:8], out_path,
        )
        return out_path

    except Exception as exc:
        logger.warning(
            "config_snapshot_failed | run_id={} type={} error={}",
            run_id, type(exc).__name__, exc,
        )
        return None
