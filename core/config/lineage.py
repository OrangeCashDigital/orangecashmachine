from __future__ import annotations

"""
core/config/lineage.py
======================

Trazabilidad de datos — Single Source of Truth para lineage.

Motivación
----------
La literatura sobre data lakehouses modernos orientados a ML/research
(Nguyen 2024, He & Fang 2025, Thomas et al. 2025) identifica el lineage
completo como requisito no negociable para:

  • Reproducibilidad experimental: reconstruir exactamente qué datos
    existían en el momento T en que se entrenó un modelo.
  • Auditoría de pipelines: rastrear qué run_id produjo cada partición.
  • Rollback seguro: identificar la versión exacta antes de una regresión.

Diseño
------
  get_git_hash()   — stdlib pura, sin dependencias
  LineageRecord    — dataclass inmutable con todos los campos de trazabilidad
  build_lineage()  — constructor conveniente para uso en storages

Uso
---
    from core.config.lineage import get_git_hash, build_lineage

    # En _write_version() de silver/gold:
    rec = build_lineage(run_id=run_id, version_id=version_id)
    manifest["git_hash"]  = rec.git_hash
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
    """
    Hash corto del commit actual para trazabilidad de lineage.

    Devuelve 'unknown' si git no está disponible o el repo no tiene commits.
    Nunca lanza — el lineage parcial es mejor que un fallo de escritura.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        # returncode != 0 indica repo sin commits o git no inicializado.
        # stdout estará vacío y stderr tendrá el mensaje de error — no usar.
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
    """
    Registro de trazabilidad para una operación de escritura en el lake.

    Campos
    ------
    run_id      : ID del run de ingestión (correlaciona bronze ↔ silver ↔ gold)
    version_id  : ID de versión del dataset (e.g. "v000042")
    git_hash    : Hash del commit que produjo los datos
    written_at  : Timestamp UTC de escritura (ISO 8601)
    as_of       : Timestamp de referencia temporal para reproducibilidad
                  Si se pide versión "¿qué datos existían el 2026-03-01?",
                  as_of permite resolver la versión vigente en ese momento.
    exchange    : Exchange origen de los datos
    layer       : Capa del lake ("bronze" | "silver" | "gold")

    Uso en manifests
    ----------------
    El dict .to_manifest() produce exactamente los campos que silver_storage
    y gold_storage ya escriben en sus version JSONs — centraliza el schema.
    """

    run_id:     str
    version_id: str
    git_hash:   str                  = field(default_factory=get_git_hash)
    written_at: str                  = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    as_of:      Optional[str]        = None
    exchange:   Optional[str]        = None
    layer:      str                  = "silver"

    def to_manifest(self) -> dict:
        """
        Serializa a dict compatible con los manifests de versión existentes.

        Produce los mismos campos que silver_storage._write_version() y
        gold_storage ya escriben — reemplaza la lógica dispersa por un
        schema centralizado.
        """
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
    """
    Constructor conveniente para LineageRecord.

    git_hash y written_at se capturan automáticamente en el momento
    de la llamada — no hay que pasarlos explícitamente.

    Ejemplo
    -------
        rec = build_lineage(run_id=run_id, version_id="v000042", layer="silver")
        manifest.update(rec.to_manifest())
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
