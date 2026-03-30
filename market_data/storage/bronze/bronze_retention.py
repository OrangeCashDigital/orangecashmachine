"""
bronze_retention.py
===================

Política de retención para la capa Bronze.

Principio
---------
Bronze es la "caja negra" de ingestión — append-only e inmutable.
Pero sin retención, crece indefinidamente: cada run genera ~1100 archivos.
En producción con runs cada hora = ~26,000 archivos/día solo en Bronze.

Estrategia
----------
Eliminar parts Bronze con written_at > RETENTION_DAYS días.
Cualquier dato de más de RETENTION_DAYS días ya fue:
  1. Procesado por Silver (merge + dedup)
  2. Versionado en Silver _versions/
  3. Disponible para reproducibilidad via Silver

SafeOps
-------
• Nunca toca Silver — solo Bronze
• Solo elimina parts con escrita > RETENTION_DAYS días (default: 7)
• Siempre preserva al menos MIN_KEEP_DAYS días (default: 2)
• Dry-run mode por defecto — nada se elimina sin --execute
• Loguea todo antes de eliminar

Uso
---
    # Ver qué se eliminaría (sin borrar nada)
    python -m market_data.storage.bronze_retention

    # Ejecutar limpieza real
    python -m market_data.storage.bronze_retention --execute

    # Retención agresiva (3 días)
    python -m market_data.storage.bronze_retention --days 3 --execute
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import NamedTuple

from loguru import logger


# ==========================================================
# Constants
# ==========================================================

RETENTION_DAYS_DEFAULT = 7
MIN_KEEP_DAYS          = 2   # nunca borrar partes de los últimos N días

_BRONZE_SUBPATH = ("data_platform", "data_lake", "bronze", "ohlcv")


# ==========================================================
# Data classes
# ==========================================================

class RetentionResult(NamedTuple):
    scanned:  int
    eligible: int
    deleted:  int
    errors:   int
    freed_mb: float


# ==========================================================
# Core
# ==========================================================

def run_retention(
    base_path: Path | None = None,
    retention_days: int = RETENTION_DAYS_DEFAULT,
    dry_run: bool = True,
) -> RetentionResult:
    """
    Escanea Bronze y elimina parts con written_at > retention_days.

    Parameters
    ----------
    base_path : Path, optional
        Ruta al directorio bronze/ohlcv. Por defecto auto-detecta.
    retention_days : int
        Parts con más de esta edad (días) son elegibles.
    dry_run : bool
        Si True, solo reporta sin eliminar. Default: True.
    """
    bronze_path = _resolve_bronze_path(base_path)
    cutoff      = datetime.now(timezone.utc) - timedelta(days=retention_days)
    safe_cutoff = datetime.now(timezone.utc) - timedelta(days=MIN_KEEP_DAYS)

    # Usar el más conservador
    effective_cutoff = min(cutoff, safe_cutoff)

    logger.info(
        "Bronze retention | path={} retention_days={} cutoff={} dry_run={}",
        bronze_path, retention_days,
        effective_cutoff.strftime("%Y-%m-%d %H:%M UTC"), dry_run,
    )

    meta_files = list(bronze_path.rglob("*.meta.json"))
    logger.info("Scanned {} meta files", len(meta_files))

    scanned  = len(meta_files)
    eligible = 0
    deleted  = 0
    errors   = 0
    freed_b  = 0

    for meta_path in meta_files:
        try:
            meta = json.loads(meta_path.read_text())
            written_at_str = meta.get("written_at", "")
            if not written_at_str:
                continue

            written_at = datetime.fromisoformat(written_at_str)
            if written_at > effective_cutoff:
                continue  # reciente — conservar

            eligible += 1
            part_path = meta_path.with_suffix(".parquet")
            part_size = part_path.stat().st_size if part_path.exists() else 0

            if dry_run:
                logger.debug(
                    "DRY-RUN would delete | {} (written {})",
                    part_path.name, written_at.strftime("%Y-%m-%d %H:%M")
                )
                freed_b += part_size
            else:
                if part_path.exists():
                    part_path.unlink()
                    freed_b += part_size
                meta_path.unlink()
                deleted += 1
                logger.debug("Deleted | {}", part_path.name)

        except Exception as exc:
            errors += 1
            logger.warning("Retention error | {} | {}", meta_path, exc)

    # Limpiar directorios vacíos
    if not dry_run:
        _cleanup_empty_dirs(bronze_path)

    freed_mb = freed_b / (1024 * 1024)

    logger.info(
        "Bronze retention complete | scanned={} eligible={} deleted={} errors={} freed={:.1f}MB dry_run={}",
        scanned, eligible, deleted, errors, freed_mb, dry_run,
    )

    return RetentionResult(
        scanned=scanned,
        eligible=eligible,
        deleted=deleted,
        errors=errors,
        freed_mb=freed_mb,
    )


# ==========================================================
# Helpers
# ==========================================================

def _resolve_bronze_path(base_path: Path | None) -> Path:
    if base_path:
        return Path(base_path).resolve()
    return Path(__file__).resolve().parents[3].joinpath(*_BRONZE_SUBPATH)


def _cleanup_empty_dirs(root: Path) -> int:
    """Elimina directorios vacíos de hojas hacia arriba. Retorna count."""
    removed = 0
    # Iterar en orden inverso (hojas primero)
    for d in sorted(root.rglob("*"), reverse=True):
        if d.is_dir() and not any(d.iterdir()):
            try:
                d.rmdir()
                removed += 1
            except OSError:
                pass
    return removed


# ==========================================================
# CLI
# ==========================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze retention policy")
    parser.add_argument(
        "--days", type=int, default=RETENTION_DAYS_DEFAULT,
        help=f"Retener N días (default: {RETENTION_DAYS_DEFAULT})"
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Ejecutar borrado real (sin este flag es dry-run)"
    )
    args = parser.parse_args()

    result = run_retention(
        retention_days=args.days,
        dry_run=not args.execute,
    )

    print(f"\n{'DRY-RUN' if not args.execute else 'EXECUTED'} SUMMARY")
    print(f"  Scanned:  {result.scanned:,} meta files")
    print(f"  Eligible: {result.eligible:,} parts to delete")
    print(f"  Deleted:  {result.deleted:,} parts")
    print(f"  Errors:   {result.errors}")
    print(f"  Freed:    {result.freed_mb:.1f} MB")
    if not args.execute and result.eligible > 0:
        print(f"\n  → Run with --execute to delete {result.eligible:,} files")
