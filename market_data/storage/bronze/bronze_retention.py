"""
market_data/storage/bronze/bronze_retention.py
===============================================

Política de retención para la capa Bronze (Iceberg).

Estrategia
----------
Bronze es append-only — crece con cada run. La retención elimina
snapshots Iceberg anteriores al cutoff mediante expire_snapshots(),
liberando los archivos Parquet físicos que ya no son referenciados.

Cualquier dato expirado ya fue:
  1. Procesado por Silver (append + dedup en silver.ohlcv)
  2. Versionado en Silver via snapshots Iceberg
  3. Accesible para reproducibilidad via IcebergStorage

SafeOps
-------
• Nunca toca Silver ni Gold — solo bronze.ohlcv.
• Solo expira snapshots con timestamp_ms anterior al cutoff.
• Preserva siempre al menos MIN_KEEP_DAYS días.
• dry_run=True (default) — reporta sin modificar nada.
• Loguea snapshot_ids candidatos antes de expirar.

Uso
---
    # Ver qué se expiraría (sin modificar nada)
    python -m market_data.storage.bronze.bronze_retention

    # Ejecutar expiración real
    python -m market_data.storage.bronze.bronze_retention --execute

    # Retención agresiva (3 días)
    python -m market_data.storage.bronze.bronze_retention --days 3 --execute
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta
from typing import NamedTuple

from loguru import logger

from market_data.storage.iceberg.catalog import get_catalog, ensure_bronze_table


# =============================================================================
# Constantes
# =============================================================================

RETENTION_DAYS_DEFAULT: int = 7
MIN_KEEP_DAYS:          int = 2   # nunca expirar snapshots de los últimos N días


# =============================================================================
# Data class resultado
# =============================================================================

class RetentionResult(NamedTuple):
    scanned:  int   # snapshots inspeccionados
    eligible: int   # snapshots anteriores al cutoff
    expired:  int   # snapshots efectivamente expirados (0 en dry_run)
    errors:   int


# =============================================================================
# Core
# =============================================================================

def run_retention(
    retention_days: int  = RETENTION_DAYS_DEFAULT,
    dry_run:        bool = True,
) -> RetentionResult:
    """
    Expira snapshots Bronze con timestamp_ms anterior al cutoff.

    Parameters
    ----------
    retention_days : int
        Snapshots con más de esta edad (días) son elegibles.
    dry_run : bool
        Si True, solo reporta sin expirar. Default: True.

    Returns
    -------
    RetentionResult
    """
    ensure_bronze_table()
    table = get_catalog().load_table("bronze.ohlcv")

    cutoff      = datetime.now(timezone.utc) - timedelta(days=retention_days)
    safe_cutoff = datetime.now(timezone.utc) - timedelta(days=MIN_KEEP_DAYS)
    effective   = min(cutoff, safe_cutoff)
    cutoff_ms   = int(effective.timestamp() * 1000)

    logger.info(
        "Bronze retention | retention_days={} cutoff={} dry_run={}",
        retention_days,
        effective.strftime("%Y-%m-%d %H:%M UTC"),
        dry_run,
    )

    try:
        history = list(table.history())
    except Exception as exc:
        logger.error("Bronze retention: cannot read snapshot history | {}", exc)
        return RetentionResult(scanned=0, eligible=0, expired=0, errors=1)

    scanned  = len(history)
    eligible = [s for s in history if s.timestamp_ms < cutoff_ms]
    errors   = 0

    logger.info(
        "Bronze retention | scanned={} snapshots eligible={}",
        scanned, len(eligible),
    )

    for snap in eligible:
        snap_dt = datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc)
        logger.debug(
            "Bronze retention candidate | snapshot_id={} ts={}",
            snap.snapshot_id,
            snap_dt.strftime("%Y-%m-%d %H:%M UTC"),
        )

    if dry_run:
        logger.info(
            "Bronze retention DRY-RUN complete | would expire={} snapshots",
            len(eligible),
        )
        return RetentionResult(
            scanned  = scanned,
            eligible = len(eligible),
            expired  = 0,
            errors   = 0,
        )

    # Expiración real vía Iceberg expire_snapshots
    expired = 0
    try:
        (
            table.expire_snapshots()
            .expire_older_than(effective)
            .commit()
        )
        expired = len(eligible)
        logger.info(
            "Bronze retention complete | expired={} snapshots",
            expired,
        )
    except Exception as exc:
        errors += 1
        logger.error("Bronze retention: expire_snapshots failed | {}", exc)

    return RetentionResult(
        scanned  = scanned,
        eligible = len(eligible),
        expired  = expired,
        errors   = errors,
    )


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Iceberg retention policy")
    parser.add_argument(
        "--days", type=int, default=RETENTION_DAYS_DEFAULT,
        help=f"Expirar snapshots con más de N días (default: {RETENTION_DAYS_DEFAULT})",
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Ejecutar expiración real (sin este flag es dry-run)",
    )
    args = parser.parse_args()

    result = run_retention(
        retention_days = args.days,
        dry_run        = not args.execute,
    )

    label = "DRY-RUN" if not args.execute else "EXECUTED"
    print(f"\n{label} SUMMARY")
    print(f"  Snapshots scanned:  {result.scanned:,}")
    print(f"  Eligible to expire: {result.eligible:,}")
    print(f"  Expired:            {result.expired:,}")
    print(f"  Errors:             {result.errors}")
    if not args.execute and result.eligible > 0:
        print(f"\n  → Run with --execute to expire {result.eligible:,} snapshots")
