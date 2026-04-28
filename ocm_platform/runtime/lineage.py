from __future__ import annotations

"""
core/runtime/lineage.py
========================

Trazabilidad de datos — Single Source of Truth para lineage.

Ubicación canónica: core/runtime/ (no core/config/)
Razón: lineage describe qué run_id produjo qué datos — es metadata
de ejecución, no configuración declarativa. Separación por SRP.

Motivación
----------
Reproducibilidad experimental, auditoría de pipelines y rollback seguro
requieren trazabilidad completa: qué run produjo qué partición, con qué
código, en qué momento.

Diseño
------
  get_git_hash()   — stdlib pura, sin dependencias
  LineageRecord    — dataclass inmutable con todos los campos de trazabilidad
  build_lineage()  — constructor conveniente para uso en storages

Uso
---
    from ocm_platform.runtime.lineage import get_git_hash, build_lineage

    rec = build_lineage(run_id=run_id, version_id=version_id)
    manifest["git_hash"]   = rec.git_hash
    manifest["written_at"] = rec.written_at
"""

import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


# ==========================================================
# Git hash — stdlib pura
# ==========================================================

def get_git_hash() -> str:
    """Hash corto del commit actual para trazabilidad de lineage.

    Devuelve 'unknown' si git no está disponible o el repo no tiene commits.
    Nunca lanza — el lineage parcial es mejor que un fallo de escritura.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        if result.returncode != 0:
            return "unknown"
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


# ==========================================================
# LineageRecord — dataclass inmutable de trazabilidad
# ==========================================================

@dataclass(frozen=True)
class LineageRecord:
    """Registro de trazabilidad para una operación de escritura en el lake.

    Campos
    ------
    run_id      : ID del run de ingestión (correlaciona bronze ↔ silver ↔ gold)
    version_id  : ID de versión del dataset (e.g. "v000042")
    git_hash    : Hash del commit que produjo los datos
    written_at  : Timestamp UTC de escritura (ISO 8601)
    as_of       : Timestamp de referencia temporal para reproducibilidad
    exchange    : Exchange origen de los datos
    layer       : Capa del lake ("bronze" | "silver" | "gold")
    """

    run_id:     str
    version_id: str
    git_hash:   str           = field(default_factory=get_git_hash)
    written_at: str           = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    as_of:      Optional[str] = None
    exchange:   Optional[str] = None
    layer:      str           = "silver"

    def to_manifest(self) -> dict:
        """Serializa a dict compatible con los manifests de versión."""
        d = {
            "run_id":     self.run_id,
            "version_id": self.version_id,
            "git_hash":   self.git_hash,
            "written_at": self.written_at,
            "layer":      self.layer,
        }
        if self.as_of is not None:
            d["as_of"] = self.as_of
        if self.exchange is not None:
            d["exchange"] = self.exchange
        return d


def build_lineage(
    run_id:     str,
    version_id: str,
    *,
    as_of:    Optional[str] = None,
    exchange: Optional[str] = None,
    layer:    str           = "silver",
) -> LineageRecord:
    """Constructor conveniente para LineageRecord.

    git_hash y written_at se capturan automáticamente en el momento
    de la llamada — no hay que pasarlos explícitamente.
    """
    return LineageRecord(
        run_id=run_id,
        version_id=version_id,
        as_of=as_of,
        exchange=exchange,
        layer=layer,
    )


__all__ = [
    "get_git_hash",
    "LineageRecord",
    "build_lineage",
]
