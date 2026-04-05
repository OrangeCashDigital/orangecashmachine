#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/iceberg_compact_and_maintain.py
========================================

Mantenimiento de la tabla Iceberg silver.ohlcv.

Qué hace
--------
1. Compaction (rewrite_data_files): fusiona archivos pequeños en archivos
   de ~128MB. Crítico después de ingestión incremental que genera muchos
   archivos pequeños (actualmente 314 archivos × 139KB promedio).

2. Expire snapshots: elimina snapshots antiguos del historial.
   Retiene los últimos N días para time-travel y rollback.
   Sin esto, el catálogo crece indefinidamente.

3. Remove orphan files: elimina archivos de datos huérfanos
   (escrituras abortadas, bugs de concurrencia, etc.).

Cuándo ejecutar
---------------
  - Post-backfill masivo (inmediatamente después)
  - Cron semanal en producción incremental
  - Manualmente: uv run python scripts/iceberg_compact_and_maintain.py

Estado actual (por qué es urgente)
-----------------------------------
  314 archivos × 139 KB promedio = 42.6 MB fragmentado
  Target post-compaction: ~5-10 archivos × 4-8 MB cada uno

Diseño
------
  pyiceberg 0.8 soporta rewrite_data_files() nativo.
  No requiere Spark — funciona con el catalog SQLite local.
  Target file size: 128MB (configurable).

Usage
-----
  uv run python scripts/iceberg_compact_and_maintain.py
  uv run python scripts/iceberg_compact_and_maintain.py --dry-run
  uv run python scripts/iceberg_compact_and_maintain.py --expire-days 14
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone, timedelta

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.dirname(__file__)))

from loguru import logger


# ==========================================================
# Constantes
# ==========================================================

TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024   # 128 MB
DEFAULT_EXPIRE_DAYS    = 7                    # retener snapshots de los últimos 7 días
DEFAULT_ORPHAN_DAYS    = 3                    # archivos sin referencia desde hace 3+ días


# ==========================================================
# Helpers
# ==========================================================

def _table_stats(table) -> dict:
    snap = table.current_snapshot()
    if snap is None:
        return {}
    s = snap.summary or {}
    return {
        "snapshots":   len(table.snapshots()),
        "data_files":  int(s.get("total-data-files", 0)),
        "records":     int(s.get("total-records", 0)),
        "size_mb":     int(s.get("total-files-size", 0)) / (1024 * 1024),
        "avg_file_kb": int(s.get("total-files-size", 0)) / max(int(s.get("total-data-files", 1)), 1) / 1024,
    }


def _print_stats(label: str, stats: dict) -> None:
    print(f"\n  {label}")
    print(f"    Snapshots     : {stats.get('snapshots', '?')}")
    print(f"    Data files    : {stats.get('data_files', '?')}")
    print(f"    Total records : {stats.get('records', '?'):,}")
    print(f"    Total size    : {stats.get('size_mb', 0):.1f} MB")
    print(f"    Avg file size : {stats.get('avg_file_kb', 0):.0f} KB")


# ==========================================================
# Operaciones
# ==========================================================

def compact(table, dry_run: bool = False) -> bool:
    """
    Reescribe archivos pequeños en archivos de ~128MB.

    pyiceberg rewrite_data_files() agrupa archivos por partición
    y los fusiona respetando el partition spec. Genera un nuevo
    snapshot con los archivos compactados.
    """
    print("\n  ── Compaction (rewrite_data_files) ──────────────────────")
    if dry_run:
        print("  [DRY RUN] Compaction saltada")
        return True

    try:
        from pyiceberg.table.rewrite import RewriteDataFilesAction
        _t0 = time.monotonic()

        result = table.rewrite_data_files(
            options={"target-file-size-bytes": str(TARGET_FILE_SIZE_BYTES)},
        )

        elapsed = time.monotonic() - _t0
        rewritten = getattr(result, "rewritten_data_files_count", "?")
        added     = getattr(result, "added_data_files_count", "?")
        print(f"  ✅ Compaction completada en {elapsed:.1f}s")
        print(f"     Archivos reescritos : {rewritten}")
        print(f"     Archivos nuevos     : {added}")
        return True

    except ImportError:
        # pyiceberg < 0.7 — intentar API alternativa
        try:
            _t0 = time.monotonic()
            result = table.rewrite_data_files()
            elapsed = time.monotonic() - _t0
            print(f"  ✅ Compaction completada en {elapsed:.1f}s")
            return True
        except Exception as exc:
            logger.error("Compaction failed | error={}", exc)
            print(f"  ❌ Compaction falló: {exc}")
            return False
    except Exception as exc:
        logger.error("Compaction failed | error={}", exc)
        print(f"  ❌ Compaction falló: {exc}")
        return False


def expire_snapshots(table, expire_days: int, dry_run: bool = False) -> bool:
    """
    Elimina snapshots más antiguos que expire_days.

    Retiene siempre al menos el snapshot actual.
    Los datos en snapshots expirados se vuelven irrecuperables
    para time-travel, pero los datos físicos siguen accesibles
    hasta que remove_orphan_files() los limpie.
    """
    print(f"\n  ── Expire snapshots (retener {expire_days} días) ────────────")
    if dry_run:
        cutoff = datetime.now(timezone.utc) - timedelta(days=expire_days)
        old_snaps = [s for s in table.snapshots()
                     if datetime.fromtimestamp(s.timestamp_ms / 1000, tz=timezone.utc) < cutoff]
        print(f"  [DRY RUN] Se eliminarían {len(old_snaps)} snapshots anteriores a {cutoff.date()}")
        return True

    try:
        cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=expire_days)).timestamp() * 1000)
        _t0 = time.monotonic()

        result = table.expire_snapshots().expire_older_than(cutoff_ms).commit()

        elapsed = time.monotonic() - _t0
        expired = getattr(result, "expired_snapshots", "?")
        print(f"  ✅ Snapshots expirados en {elapsed:.1f}s | eliminados={expired}")
        return True

    except Exception as exc:
        logger.warning("expire_snapshots failed (non-critical) | error={}", exc)
        print(f"  ⚠️  expire_snapshots falló (no crítico): {exc}")
        return False


def remove_orphan_files(table, orphan_days: int, dry_run: bool = False) -> bool:
    """
    Elimina archivos de datos sin referencia en el catálogo.

    Archivos creados hace más de orphan_days sin estar referenciados
    por ningún snapshot son candidatos a eliminación.
    """
    print(f"\n  ── Remove orphan files (>{orphan_days} días) ─────────────────")
    if dry_run:
        print("  [DRY RUN] Orphan cleanup saltado")
        return True

    try:
        cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=orphan_days)).timestamp() * 1000)
        _t0 = time.monotonic()

        result = table.remove_orphan_files(older_than=cutoff_ms).execute()

        elapsed = time.monotonic() - _t0
        removed = getattr(result, "orphan_file_locations", [])
        print(f"  ✅ Orphan cleanup en {elapsed:.1f}s | eliminados={len(removed)}")
        return True

    except Exception as exc:
        logger.warning("remove_orphan_files failed (non-critical) | error={}", exc)
        print(f"  ⚠️  orphan cleanup falló (no crítico): {exc}")
        return False


# ==========================================================
# Main
# ==========================================================

def maintain(
    expire_days:  int  = DEFAULT_EXPIRE_DAYS,
    orphan_days:  int  = DEFAULT_ORPHAN_DAYS,
    dry_run:      bool = False,
    skip_compact: bool = False,
    skip_expire:  bool = False,
    skip_orphan:  bool = False,
) -> None:
    from market_data.storage.iceberg.iceberg_storage import _get_catalog

    print("\n" + "═" * 60)
    print("  Iceberg Maintenance — silver.ohlcv")
    if dry_run:
        print("  MODE: DRY RUN — ninguna operación escribe")
    print("═" * 60)

    catalog = _get_catalog()
    table   = catalog.load_table("silver.ohlcv")

    stats_before = _table_stats(table)
    _print_stats("ANTES", stats_before)

    results = {}

    if not skip_compact:
        results["compact"] = compact(table, dry_run=dry_run)
        if results["compact"] and not dry_run:
            table = catalog.load_table("silver.ohlcv")  # recargar post-compaction

    if not skip_expire:
        results["expire"] = expire_snapshots(table, expire_days, dry_run=dry_run)
        if results["expire"] and not dry_run:
            table = catalog.load_table("silver.ohlcv")

    if not skip_orphan:
        results["orphan"] = remove_orphan_files(table, orphan_days, dry_run=dry_run)

    if not dry_run:
        table = catalog.load_table("silver.ohlcv")
        stats_after = _table_stats(table)
        _print_stats("DESPUÉS", stats_after)

        size_saved = stats_before.get("size_mb", 0) - stats_after.get("size_mb", 0)
        files_saved = stats_before.get("data_files", 0) - stats_after.get("data_files", 0)
        print(f"\n  Reducción: {files_saved} archivos | {size_saved:.1f} MB liberados")

    print("\n" + "═" * 60)
    all_ok = all(v for v in results.values())
    print(f"  {'✅ Mantenimiento completado' if all_ok else '⚠️  Mantenimiento con advertencias'}")
    print("═" * 60 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Mantenimiento Iceberg — silver.ohlcv")
    parser.add_argument("--dry-run",      action="store_true", help="Solo reportar, no modificar")
    parser.add_argument("--expire-days",  type=int, default=DEFAULT_EXPIRE_DAYS,
                        help=f"Días a retener en historial de snapshots (default: {DEFAULT_EXPIRE_DAYS})")
    parser.add_argument("--orphan-days",  type=int, default=DEFAULT_ORPHAN_DAYS,
                        help=f"Días mínimos de antigüedad para orphan files (default: {DEFAULT_ORPHAN_DAYS})")
    parser.add_argument("--skip-compact", action="store_true", help="Saltar compaction")
    parser.add_argument("--skip-expire",  action="store_true", help="Saltar expire_snapshots")
    parser.add_argument("--skip-orphan",  action="store_true", help="Saltar remove_orphan_files")
    args = parser.parse_args()

    maintain(
        expire_days  = args.expire_days,
        orphan_days  = args.orphan_days,
        dry_run      = args.dry_run,
        skip_compact = args.skip_compact,
        skip_expire  = args.skip_expire,
        skip_orphan  = args.skip_orphan,
    )


if __name__ == "__main__":
    main()
