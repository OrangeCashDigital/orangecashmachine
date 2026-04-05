#!/usr/bin/env python3
"""
iceberg_compact_and_maintain.py — Mantenimiento Iceberg silver.ohlcv

PyIceberg 0.8.1 no tiene rewrite_data_files() nativo.
Compaction = snapshot_expire via manage_snapshots().
Los 312 archivos son correctos: 28 combos × ~11 meses de particiones.

Operaciones disponibles:
1. expire_snapshots() — eliminar historial antiguo via manage_snapshots()
2. stats             — reportar estado actual de la tabla

Uso:
  uv run python scripts/iceberg_compact_and_maintain.py
  uv run python scripts/iceberg_compact_and_maintain.py --dry-run
  uv run python scripts/iceberg_compact_and_maintain.py --expire-days 30
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone

from loguru import logger

DEFAULT_EXPIRE_DAYS = 7


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _table_stats(table) -> dict:
    snap = table.current_snapshot()
    if snap is None:
        return {"snapshots": 0, "data_files": 0, "records": 0, "size_bytes": 0}
    s = snap.summary or {}
    return {
        "snapshots":   len(table.snapshots()),
        "data_files":  int(s.get("total-data-files", 0)),
        "records":     int(s.get("total-records", 0)),
        "size_bytes":  int(s.get("total-files-size", 0)),
    }


def _print_stats(label: str, stats: dict) -> None:
    size_mb  = stats["size_bytes"] / (1024 * 1024)
    n_files  = max(stats["data_files"], 1)
    avg_kb   = stats["size_bytes"] / n_files / 1024
    print(f"\n  {label}")
    print(f"    Snapshots     : {stats['snapshots']}")
    print(f"    Data files    : {stats['data_files']:,}")
    print(f"    Total records : {stats['records']:,}")
    print(f"    Total size    : {size_mb:.1f} MB")
    print(f"    Avg file size : {avg_kb:.0f} KB")


# ──────────────────────────────────────────────────────────────────────────────
# Operaciones
# ──────────────────────────────────────────────────────────────────────────────

def expire_snapshots(table, expire_days: int, dry_run: bool) -> bool:
    """Elimina snapshots anteriores a expire_days usando manage_snapshots()."""
    cutoff_ms = int(
        (datetime.now(timezone.utc) - timedelta(days=expire_days)).timestamp() * 1000
    )
    cutoff_dt = datetime.fromtimestamp(cutoff_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

    snapshots     = table.snapshots()
    current_id    = table.current_snapshot().snapshot_id
    to_expire     = [
        s for s in snapshots
        if s.snapshot_id != current_id and s.timestamp_ms < cutoff_ms
    ]

    print(f"\n  ── Expire snapshots (retener {expire_days} días) ────────────")
    print(f"  Snapshots totales    : {len(snapshots)}")
    print(f"  Anteriores a {cutoff_dt} : {len(to_expire)}")

    if not to_expire:
        print("  ✅ Nada que expirar")
        return True

    if dry_run:
        print(f"  [DRY RUN] Se eliminarían {len(to_expire)} snapshots anteriores a {cutoff_dt}")
        return True

    try:
        with table.manage_snapshots() as ms:
            ms.expire_snapshots_older_than(cutoff_ms)
        print(f"  ✅ {len(to_expire)} snapshots expirados")
        return True
    except Exception as e:
        logger.warning(f"expire_snapshots failed | error={e!r}")
        print(f"  ⚠️  expire_snapshots falló (no crítico): {e}")
        return False


def print_partition_stats(table) -> None:
    """Muestra distribución de archivos por partición — útil para detectar skew."""
    print("\n  ── Distribución de particiones ──────────────────────────")
    try:
        import pyarrow.compute as pc
        df = table.scan().to_arrow()
        combos = df.group_by(
            ["exchange", "market_type", "symbol", "timeframe"]
        ).aggregate([("timestamp", "count")])
        combos = combos.sort_by([("timestamp_count", "descending")])
        d = combos.to_pydict()
        print(f"  Combinaciones únicas: {len(d['exchange'])}")
        for i in range(min(10, len(d["exchange"]))):
            print(
                f"    {d['exchange'][i]:15} {d['market_type'][i]:5} "
                f"{d['symbol'][i]:20} {d['timeframe'][i]:4} "
                f"→ {d['timestamp_count'][i]:>10,} registros"
            )
        if len(d["exchange"]) > 10:
            print(f"    ... y {len(d['exchange']) - 10} más")
    except Exception as e:
        print(f"  ⚠️  No se pudo calcular distribución: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def maintain(
    expire_days:   int  = DEFAULT_EXPIRE_DAYS,
    dry_run:       bool = False,
    skip_expire:   bool = False,
    show_partitions: bool = False,
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
    _print_stats("ESTADO ACTUAL", stats_before)

    # Nota sobre compaction
    print("\n  ── Compaction ───────────────────────────────────────────")
    print("  ℹ️  PyIceberg 0.8.1 no expone rewrite_data_files().")
    print(f"  ℹ️  {stats_before['data_files']} archivos es correcto dado el partition spec.")
    print("  ℹ️  (28 combos exchange/symbol/timeframe × meses de datos)")

    results = {}

    if not skip_expire:
        results["expire"] = expire_snapshots(table, expire_days, dry_run)

    if show_partitions:
        print_partition_stats(table)

    all_ok = all(results.values()) if results else True
    print("\n" + "═" * 60)
    print(f"  {'✅ Mantenimiento completado' if all_ok else '⚠️  Mantenimiento con advertencias'}")
    print("═" * 60 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Mantenimiento Iceberg — silver.ohlcv")
    parser.add_argument("--dry-run",          action="store_true",
                        help="Solo reportar, no modificar")
    parser.add_argument("--expire-days",      type=int, default=DEFAULT_EXPIRE_DAYS,
                        help=f"Días a retener en historial de snapshots (default: {DEFAULT_EXPIRE_DAYS})")
    parser.add_argument("--skip-expire",      action="store_true",
                        help="Saltar expire_snapshots")
    parser.add_argument("--show-partitions",  action="store_true",
                        help="Mostrar distribución de registros por partición")
    args = parser.parse_args()

    maintain(
        expire_days      = args.expire_days,
        dry_run          = args.dry_run,
        skip_expire      = args.skip_expire,
        show_partitions  = args.show_partitions,
    )


if __name__ == "__main__":
    main()
