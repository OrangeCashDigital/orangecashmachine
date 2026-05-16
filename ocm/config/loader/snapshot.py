from __future__ import annotations

"""
ocm/config/loader/snapshot.py
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
- Escritura atómica: ``tmp → rename`` (nunca archivo parcial en disco).
- Fail-soft: nunca propaga excepciones al pipeline principal.
- Idempotente: mismo ``run_id`` + ``hash`` → no sobreescribe.

Principios: SafeOps · Fail-Soft · SSOT · KISS.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Optional

from loguru import logger

__all__ = ["write_config_snapshot"]

# Longitud del prefijo de hash visible en el nombre de archivo.
# 8 hex chars = 32 bits de entropía — suficiente para colisiones en logs.
_HASH_PREFIX_LEN: Final[int] = 8

# Directorio por defecto para snapshots — Final para evitar mutación accidental.
_SNAPSHOT_DIR: Final[Path] = Path("logs/config_snapshots")


def write_config_snapshot(
    config: Any,
    run_id: str,
    config_hash: str,
    env: str,
    snapshot_dir: Optional[Path] = None,
) -> Optional[Path]:
    """Serializa AppConfig a JSON y lo escribe atómicamente en disco.

    Fail-soft: captura cualquier excepción y la loggea como warning.
    El pipeline principal nunca se interrumpe por un fallo de snapshot.

    Args:
        config: AppConfig validado (Pydantic v2, debe implementar
            ``model_dump(mode="json")``).
        run_id: Identificador único del run (hex de 12 chars).
        config_hash: Hash SHA-256 del config mergeado (64 chars).
        env: Nombre del entorno activo (e.g. ``"development"``).
        snapshot_dir: Override del directorio de salida. Útil en tests.
            Si es ``None``, usa ``_SNAPSHOT_DIR``.

    Returns:
        Path del snapshot escrito, o ``None`` si falló.
    """
    out_dir: Path = snapshot_dir if snapshot_dir is not None else _SNAPSHOT_DIR
    hash_prefix = config_hash[:_HASH_PREFIX_LEN]

    try:
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"{run_id}_{hash_prefix}.json"

        if out_path.exists():
            logger.debug(
                "config_snapshot_exists | run_id={} path={}",
                run_id, out_path,
            )
            return out_path

        config_dict: dict[str, Any] = config.model_dump(mode="json")

        snapshot: dict[str, Any] = {
            "run_id":      run_id,
            "config_hash": config_hash,
            "env":         env,
            "snapshot_at": datetime.now(timezone.utc).isoformat(),
            "config":      config_dict,
        }

        tmp = out_dir / f".{run_id}_{hash_prefix}.tmp"
        tmp.write_text(
            json.dumps(snapshot, indent=2, default=str),
            encoding="utf-8",
        )
        tmp.replace(out_path)

        logger.info(
            "config_snapshot_written | run_id={} env={} hash={} path={}",
            run_id, env, hash_prefix, out_path,
        )
        return out_path

    except Exception as exc:  # noqa: BLE001 — fail-soft intencional
        logger.warning(
            "config_snapshot_failed | run_id={} type={} error={}",
            run_id, type(exc).__name__, exc,
        )
        return None
