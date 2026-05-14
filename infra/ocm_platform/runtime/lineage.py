from __future__ import annotations

"""
ocm_platform/runtime/lineage.py
================================

Trazabilidad de datos — Single Source of Truth para lineage.

Ubicación canónica: ocm_platform/runtime/
Razón: lineage describe qué run_id produjo qué datos — es metadata
de ejecución, no configuración declarativa. Separación por SRP.

Motivación
----------
Reproducibilidad experimental, auditoría de pipelines y rollback seguro
requieren trazabilidad completa: qué run produjo qué partición, con qué
código, en qué momento.

Diseño
------
  VALID_LAYERS    — frozenset de capas permitidas (SSOT / Fail-Fast)
  get_git_hash()  — stdlib pura, sin dependencias, nunca lanza
  LineageRecord   — dataclass inmutable con validación en construcción
  build_lineage() — constructor conveniente para uso en storages

Uso
---
    from ocm_platform.runtime.lineage import build_lineage

    rec = build_lineage(run_id=run_id, version_id=version_id, layer="silver")
    manifest["git_hash"]   = rec.git_hash
    manifest["written_at"] = rec.written_at
"""

import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Constantes públicas — SSOT para validación en toda la plataforma
# ---------------------------------------------------------------------------

VALID_LAYERS: frozenset[str] = frozenset({"bronze", "silver", "gold"})


# ---------------------------------------------------------------------------
# Git hash — stdlib pura, nunca lanza
# ---------------------------------------------------------------------------

def get_git_hash() -> str:
    """Hash corto del commit actual para trazabilidad de lineage.

    Devuelve ``'unknown'`` si git no está disponible o el repo no tiene
    commits. Nunca lanza — el lineage parcial es mejor que un fallo de
    escritura (Fail-Soft).
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
        )
        if result.returncode != 0:
            return "unknown"
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# LineageRecord — dataclass inmutable de trazabilidad
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LineageRecord:
    """Registro de trazabilidad para una operación de escritura en el lake.

    Campos
    ------
    run_id      : ID del run de ingestión (correlaciona bronze ↔ silver ↔ gold).
    version_id  : ID de versión del dataset (e.g. ``"v000042"``).
    git_hash    : Hash del commit que produjo los datos (auto-capturado).
    written_at  : Timestamp UTC de escritura ISO 8601 (auto-capturado).
    as_of       : Timestamp de referencia temporal para reproducibilidad.
    exchange    : Exchange origen de los datos.
    layer       : Capa del lake — debe pertenecer a :data:`VALID_LAYERS`.
    """

    run_id:     str
    version_id: str
    git_hash:   str        = field(default_factory=get_git_hash)
    written_at: str        = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    as_of:      str | None = None
    exchange:   str | None = None
    layer:      str        = "silver"

    # ------------------------------------------------------------------
    # Fail-Fast — validación en construcción, no en uso
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        if self.layer not in VALID_LAYERS:
            raise ValueError(
                f"layer={self.layer!r} no es válido. "
                f"Valores permitidos: {sorted(VALID_LAYERS)}"
            )
        if not self.run_id:
            raise ValueError("run_id no puede ser vacío.")
        if not self.version_id:
            raise ValueError("version_id no puede ser vacío.")

    def to_manifest(self) -> dict[str, Any]:
        """Serializa a dict compatible con los manifests de versión.

        Omite claves opcionales cuando son ``None`` para mantener
        los manifests mínimos y deterministas.
        """
        d: dict[str, Any] = {
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


# ---------------------------------------------------------------------------
# Constructor conveniente
# ---------------------------------------------------------------------------

def build_lineage(
    run_id:     str,
    version_id: str,
    *,
    as_of:    str | None = None,
    exchange: str | None = None,
    layer:    str        = "silver",
) -> LineageRecord:
    """Constructor conveniente para :class:`LineageRecord`.

    ``git_hash`` y ``written_at`` se capturan automáticamente en el momento
    de la llamada — no se pasan explícitamente.

    Args:
        run_id:     ID del run de ingestión.
        version_id: ID de versión del dataset.
        as_of:      Timestamp de referencia temporal (opcional).
        exchange:   Exchange origen (opcional).
        layer:      Capa del lake. Default ``"silver"``.

    Returns:
        :class:`LineageRecord` inmutable con trazabilidad completa.

    Raises:
        ValueError: Si ``layer`` no pertenece a :data:`VALID_LAYERS`.
    """
    return LineageRecord(
        run_id=run_id,
        version_id=version_id,
        as_of=as_of,
        exchange=exchange,
        layer=layer,
    )


__all__ = [
    "VALID_LAYERS",
    "get_git_hash",
    "LineageRecord",
    "build_lineage",
]
