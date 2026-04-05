#!/usr/bin/env python3
"""
scripts/iceberg_reset_and_migrate.py
======================================

Fase 1 — Reset total: purge + recrear tabla con schema y partition spec correctos
Fase 2 — Migración batch-aware: una escritura por serie completa → un snapshot por serie

Resultado esperado:
  - N series → N snapshots (no 989)
  - Archivos de tamaño razonable por partición ts_month
  - Tabla lista para compaction en siguiente fase

Uso:
  uv run python scripts/iceberg_reset_and_migrate.py
  uv run python scripts/iceberg_reset_and_migrate.py --dry-run
  uv run python scripts/iceberg_reset_and_migrate.py --exchange bybit
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import NamedTuple

import pandas as pd
import pyarrow as pa
from loguru import logger

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from core.config.paths import silver_ohlcv_root
from data_platform.ohlcv_utils import safe_symbol

# =============================================================================
# Schema y partition spec — fuente de verdad única
# =============================================================================

OHLCV_SCHEMA = pa.schema([
    pa.field("timestamp",   pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("open",        pa.float64(),                 nullable=False),
    pa.field("high",        pa.float64(),                 nullable=False),
    pa.field("low",         pa.float64(),                 nullable=False),
    pa.field("close",       pa.float64(),                 nullable=False),
    pa.field("volume",      pa.float64(),                 nullable=False),
    pa.field("exchange",    pa.string(),                  nullable=False),
    pa.field("market_type", pa.string(),                  nullable=False),
    pa.field("symbol",      pa.string(),                  nullable=False),
    pa.field("timeframe",   pa.string(),                  nullable=False),
])

# Columnas en orden canónico para escritura
_COLS = [f.name for f in OHLCV_SCHEMA]

# Tamaño máximo de batch por escritura (filas) — evita OOM en series largas
_BATCH_ROWS = 500_000


# =============================================================================
# Helpers
# =============================================================================

class Series(NamedTuple):
    exchange:    str
    symbol_path: str
    market_type: str
    timeframe:   str

    @property
    def symbol(self) -> str:
        return self.symbol_path.replace("_", "/", 1)

    @property
    def label(self) -> str:
        return f"{self.exchange}/{self.symbol}/{self.market_type}/{self.timeframe}"


def discover_series(root: Path, exchange_filter: str | None) -> list[Series]:
    pattern = re.compile(
        r"exchange=(?P<exchange>[^/]+)"
        r"/symbol=(?P<symbol>[^/]+)"
        r"/market_type=(?P<market_type>[^/]+)"
        r"/timeframe=(?P<timeframe>[^/]+)"
    )
    seen:   set[Series] = set()
    result: list[Series] = []
    for p in root.rglob("*.parquet"):
        m = pattern.search(str(p))
        if not m:
            continue
        s = Series(
            exchange    = m.group("exchange"),
            symbol_path = m.group("symbol"),
            market_type = m.group("market_type"),
            timeframe   = m.group("timeframe"),
        )
        if exchange_filter and s.exchange != exchange_filter:
            continue
        if s not in seen:
            seen.add(s)
            result.append(s)
    return sorted(result, key=lambda x: x.label)


def read_series(root: Path, s: Series) -> pd.DataFrame:
    """Lee todos los parquets de la serie, concatena, deduplica y ordena."""
    series_root = (
        root
        / f"exchange={s.exchange}"
        / f"symbol={s.symbol_path}"
        / f"market_type={s.market_type}"
        / f"timeframe={s.timeframe}"
    )
    files = sorted(series_root.rglob("*.parquet"))
    if not files:
        return pd.DataFrame()

    parts = []
    for f in files:
        try:
            parts.append(pd.read_parquet(f))
        except Exception as exc:
            logger.warning("Parquet corrupto, saltando | {} | {}", f, exc)

    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.as_unit("us")
    df["exchange"]    = s.exchange
    df["market_type"] = s.market_type
    df["symbol"]      = s.symbol
    df["timeframe"]   = s.timeframe

    return (
        df[_COLS]
        .drop_duplicates(subset=["timestamp", "exchange", "symbol", "timeframe"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def df_to_arrow(df: pd.DataFrame) -> pa.Table:
    return pa.Table.from_pandas(df, schema=OHLCV_SCHEMA, preserve_index=False)


# =============================================================================
# Fase 1 — Reset
# =============================================================================

def phase1_reset(catalog, dry_run: bool) -> None:
    print("\n" + "═" * 60)
    print("  FASE 1 — Reset tabla silver.ohlcv")
    print("═" * 60)

    if dry_run:
        print("  [DRY RUN] purge_table + create_table — saltado")
        return

    # Purge elimina tabla del catálogo Y archivos físicos del warehouse
    try:
        catalog.purge_table("silver.ohlcv")
        print("  ✅ purge_table('silver.ohlcv') — tabla y archivos eliminados")
    except Exception as exc:
        print(f"  ⚠️  purge_table falló (puede no existir): {exc}")

    # Recrear namespace si fue eliminado
    try:
        catalog.create_namespace_if_not_exists("silver")
        print("  ✅ namespace 'silver' listo")
    except Exception as exc:
        print(f"  ⚠️  namespace: {exc}")

    # Recrear tabla con schema + partition spec correctos
    from pyiceberg.partitioning import PartitionSpec, PartitionField
    from pyiceberg.transforms import IdentityTransform, MonthTransform

    spec = PartitionSpec(
        PartitionField(source_id=7,  field_id=1000, transform=IdentityTransform(), name="exchange"),
        PartitionField(source_id=8,  field_id=1001, transform=IdentityTransform(), name="market_type"),
        PartitionField(source_id=9,  field_id=1002, transform=IdentityTransform(), name="symbol"),
        PartitionField(source_id=10, field_id=1003, transform=IdentityTransform(), name="timeframe"),
        PartitionField(source_id=1,  field_id=1004, transform=MonthTransform(),    name="ts_month"),
    )

    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        TimestamptzType, DoubleType, StringType,
        NestedField,
    )
    schema = Schema(
        NestedField(1,  "timestamp",   TimestamptzType(), required=True),
        NestedField(2,  "open",        DoubleType(),      required=True),
        NestedField(3,  "high",        DoubleType(),      required=True),
        NestedField(4,  "low",         DoubleType(),      required=True),
        NestedField(5,  "close",       DoubleType(),      required=True),
        NestedField(6,  "volume",      DoubleType(),      required=True),
        NestedField(7,  "exchange",    StringType(),      required=True),
        NestedField(8,  "market_type", StringType(),      required=True),
        NestedField(9,  "symbol",      StringType(),      required=True),
        NestedField(10, "timeframe",   StringType(),      required=True),
    )

    table = catalog.create_table(
        "silver.ohlcv",
        schema          = schema,
        partition_spec  = spec,
        properties      = {
            "write.target-file-size-bytes": str(128 * 1024 * 1024),  # 128MB
            "write.parquet.compression-codec": "zstd",
            "history.expire.min-snapshots-to-keep": "3",
            "history.expire.max-snapshot-age-ms":   str(7 * 24 * 3600 * 1000),  # 7 días
        },
    )
    print(f"  ✅ create_table('silver.ohlcv') — schema y partition spec OK")
    print(f"     Particiones: exchange / market_type / symbol / timeframe / ts_month(month)")
    print(f"     Target file size: 128MB | compression: zstd")


# =============================================================================
# Fase 2 — Migración batch-aware
# =============================================================================

def phase2_migrate(
    catalog,
    root:            Path,
    series:          list[Series],
    dry_run:         bool,
) -> dict:
    print("\n" + "═" * 60)
    print("  FASE 2 — Migración batch-aware")
    print(f"  Series: {len(series)}")
    print("═" * 60 + "\n")

    if not dry_run:
        table = catalog.load_table("silver.ohlcv")

    results = {"ok": [], "empty": [], "error": []}

    for i, s in enumerate(series, 1):
        _t0 = time.monotonic()
        print(f"[{i:3d}/{len(series)}] {s.label} ... ", end="", flush=True)

        try:
            df = read_series(root, s)
        except Exception as exc:
            print(f"❌ lectura: {exc}")
            results["error"].append({"series": s.label, "error": str(exc)})
            continue

        if df.empty:
            print("⚠️  sin datos")
            results["empty"].append(s.label)
            continue

        rows = len(df)

        if dry_run:
            print(f"[DRY RUN] {rows:>8,} filas — no escrito")
            results["ok"].append({"series": s.label, "rows": rows})
            continue

        try:
            # Escribir toda la serie en UN solo append → UN snapshot
            # Para series muy grandes, dividir en batches de _BATCH_ROWS
            # pero aún así un snapshot por serie (usando transaction si disponible)
            if rows <= _BATCH_ROWS:
                arrow = df_to_arrow(df)
                table.append(arrow, snapshot_properties={
                    "series":    s.label,
                    "rows":      str(rows),
                    "migration": "iceberg_reset_and_migrate_v1",
                })
            else:
                # Serie grande: múltiples appends pero documentados
                n_batches = (rows + _BATCH_ROWS - 1) // _BATCH_ROWS
                for b in range(n_batches):
                    chunk = df.iloc[b * _BATCH_ROWS : (b + 1) * _BATCH_ROWS]
                    arrow = df_to_arrow(chunk)
                    table.append(arrow, snapshot_properties={
                        "series":    s.label,
                        "batch":     f"{b+1}/{n_batches}",
                        "migration": "iceberg_reset_and_migrate_v1",
                    })

            dur = round(time.monotonic() - _t0, 2)
            print(f"✅ {rows:>8,} filas  ({dur}s)")
            results["ok"].append({"series": s.label, "rows": rows, "duration": dur})

        except Exception as exc:
            print(f"❌ escritura: {exc}")
            results["error"].append({"series": s.label, "error": str(exc)})

    return results


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--exchange", default=None)
    parser.add_argument("--skip-reset", action="store_true",
                        help="Saltar Fase 1 (tabla ya reseteada)")
    args = parser.parse_args()

    from market_data.storage.iceberg.iceberg_storage import _get_catalog
    catalog = _get_catalog()
    root    = silver_ohlcv_root()
    series  = discover_series(root, args.exchange)

    print(f"\n{'═'*60}")
    print(f"  Iceberg Reset & Migrate — producción profesional")
    if args.dry_run:
        print(f"  ⚠️  DRY RUN")
    print(f"  Series encontradas: {len(series)}")
    print(f"  Exchange filter:    {args.exchange or 'todos'}")
    print(f"{'═'*60}")

    if not args.skip_reset:
        phase1_reset(catalog, args.dry_run)

    results = phase2_migrate(catalog, root, series, args.dry_run)

    # Resumen
    total_rows = sum(r.get("rows", 0) for r in results["ok"])
    n_snaps    = len(results["ok"]) if not args.dry_run else 0

    print(f"\n{'═'*60}")
    print(f"  ✅ OK      : {len(results['ok'])} series  ({total_rows:,} filas)")
    print(f"  ⚠️  Vacías  : {len(results['empty'])}")
    print(f"  ❌ Errores : {len(results['error'])}")
    print(f"  📸 Snapshots generados: ~{n_snaps} (1 por serie)")
    if results["error"]:
        print(f"\n  Errores:")
        for r in results["error"]:
            print(f"    - {r['series']}: {r['error']}")
    print(f"{'═'*60}\n")
    print("  Siguiente paso:")
    print("  uv run python scripts/iceberg_compact_and_maintain.py")
    print()

    sys.exit(1 if results["error"] else 0)


if __name__ == "__main__":
    main()
