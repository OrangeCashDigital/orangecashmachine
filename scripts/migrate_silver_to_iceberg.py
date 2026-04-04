#!/usr/bin/env python3
"""
scripts/migrate_silver_to_iceberg.py
=====================================

Migra todos los parquets de SilverStorage a la tabla Iceberg silver.ohlcv.

Estrategia
----------
- Descubre series únicas desde el filesystem (exchange/symbol/market_type/timeframe)
- Por cada serie: lee todos sus parquets, dedup global, escribe en Iceberg
- Idempotente: si una serie ya existe en Iceberg con el mismo row count, la salta
- Progreso visible por serie con conteo de filas y duración
- Reporte final: series OK / saltadas / fallidas

Reconstrucción de símbolo desde path
-------------------------------------
safe_symbol() convierte BTC/USDT → BTC_USDT y BTC/USDT:USDT → BTC_USDT:USDT.
La inversa: primer '_' → '/' (solo el primero).
  BTC_USDT       → BTC/USDT
  BTC_USDT:USDT  → BTC/USDT:USDT
  BTC_USD:BTC    → BTC/USD:BTC  (sin cambio porque no hay '_' antes de ':')

Uso
---
  uv run python scripts/migrate_silver_to_iceberg.py
  uv run python scripts/migrate_silver_to_iceberg.py --dry-run
  uv run python scripts/migrate_silver_to_iceberg.py --exchange bybit
  uv run python scripts/migrate_silver_to_iceberg.py --force   # re-migra aunque ya exista
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import NamedTuple

import pandas as pd
import pyarrow.compute as pc
from loguru import logger

# Añadir raíz del proyecto al path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from core.config.paths import silver_ohlcv_root
from market_data.storage.iceberg.iceberg_storage import IcebergStorage


# =============================================================================
# Tipos
# =============================================================================

class Series(NamedTuple):
    exchange:    str
    symbol_path: str   # tal como aparece en el path (BTC_USDT, BTC_USDT:USDT)
    market_type: str
    timeframe:   str

    @property
    def symbol(self) -> str:
        """Reconstruye símbolo ccxt desde el path: primer '_' → '/'."""
        return self.symbol_path.replace("_", "/", 1)

    @property
    def label(self) -> str:
        return f"{self.exchange}/{self.symbol}/{self.market_type}/{self.timeframe}"


# =============================================================================
# Descubrimiento
# =============================================================================

def discover_series(root: Path, exchange_filter: str | None = None) -> list[Series]:
    """
    Descubre todas las series únicas leyendo el filesystem.
    Patrón: exchange=X/symbol=Y/market_type=Z/timeframe=T/
    """
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

    return sorted(result, key=lambda s: s.label)


# =============================================================================
# Lectura de parquets de una serie
# =============================================================================

def read_series_parquets(root: Path, s: Series) -> pd.DataFrame:
    """
    Lee todos los parquets de una serie, los concatena y deduplica.
    """
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
            df = pd.read_parquet(f)
            parts.append(df)
        except Exception as exc:
            logger.warning("Parquet corrupto, saltando | {} | {}", f, exc)

    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True)

    # Normalizar timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Dedup global por timestamp (last-write-wins ya garantizado por Silver)
    df = (
        df.drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    return df


# =============================================================================
# Verificar si serie ya migrada
# =============================================================================

def iceberg_row_count(storage: IcebergStorage, symbol: str, timeframe: str) -> int:
    """Cuenta filas en Iceberg para esta serie. Retorna 0 si no hay datos."""
    try:
        from pyiceberg.expressions import And, EqualTo
        result = (
            storage._table
            .scan(row_filter=storage._base_filter(symbol, timeframe),
                  selected_fields=("timestamp",))
            .to_arrow()
        )
        return result.num_rows
    except Exception:
        return 0


# =============================================================================
# Migración de una serie
# =============================================================================

def migrate_series(
    s:       Series,
    root:    Path,
    force:   bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Migra una serie completa. Retorna dict con resultado.
    """
    _t0 = time.monotonic()

    storage = IcebergStorage(
        exchange    = s.exchange,
        market_type = s.market_type,
        dry_run     = dry_run,
    )

    # Verificar si ya existe (idempotencia)
    if not force and not dry_run:
        existing = iceberg_row_count(storage, s.symbol, s.timeframe)
        if existing > 0:
            return {
                "status":   "skipped",
                "series":   s.label,
                "rows_src": "?",
                "rows_ice": existing,
                "duration": round(time.monotonic() - _t0, 2),
            }

    # Leer parquets
    df = read_series_parquets(root, s)
    if df.empty:
        return {
            "status":   "empty",
            "series":   s.label,
            "rows_src": 0,
            "rows_ice": 0,
            "duration": round(time.monotonic() - _t0, 2),
        }

    rows_src = len(df)

    # Escribir en Iceberg
    try:
        storage.save_ohlcv(df, symbol=s.symbol, timeframe=s.timeframe)
    except Exception as exc:
        return {
            "status":   "error",
            "series":   s.label,
            "rows_src": rows_src,
            "rows_ice": 0,
            "error":    str(exc),
            "duration": round(time.monotonic() - _t0, 2),
        }

    # Validar conteo post-escritura
    rows_ice = 0 if dry_run else iceberg_row_count(storage, s.symbol, s.timeframe)

    return {
        "status":   "ok",
        "series":   s.label,
        "rows_src": rows_src,
        "rows_ice": rows_ice,
        "duration": round(time.monotonic() - _t0, 2),
    }


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Migra SilverStorage → Iceberg")
    parser.add_argument("--dry-run",  action="store_true", help="No escribe en Iceberg")
    parser.add_argument("--force",    action="store_true", help="Re-migra aunque ya exista")
    parser.add_argument("--exchange", default=None,        help="Filtrar por exchange")
    args = parser.parse_args()

    root   = silver_ohlcv_root()
    series = discover_series(root, exchange_filter=args.exchange)

    print(f"\n{'═'*60}")
    print(f"  Migración Silver → Iceberg")
    if args.dry_run:
        print(f"  ⚠️  DRY RUN — no se escribirá nada")
    print(f"  Series encontradas : {len(series)}")
    print(f"  Exchange filter    : {args.exchange or 'todos'}")
    print(f"{'═'*60}\n")

    results = {"ok": [], "skipped": [], "empty": [], "error": []}

    for i, s in enumerate(series, 1):
        print(f"[{i:2d}/{len(series)}] {s.label} ... ", end="", flush=True)
        r = migrate_series(s, root, force=args.force, dry_run=args.dry_run)
        status = r["status"]
        results[status].append(r)

        if status == "ok":
            print(f"✅  {r['rows_src']:>8,} filas → Iceberg {r['rows_ice']:>8,}  ({r['duration']}s)")
        elif status == "skipped":
            print(f"⏭️   ya existe  ({r['rows_ice']:>8,} filas en Iceberg)")
        elif status == "empty":
            print(f"⚠️   sin datos")
        else:
            print(f"❌  ERROR: {r.get('error', '?')}")

    # Reporte final
    print(f"\n{'═'*60}")
    print(f"  ✅  OK       : {len(results['ok'])}")
    print(f"  ⏭️   Saltadas : {len(results['skipped'])}")
    print(f"  ⚠️   Vacías   : {len(results['empty'])}")
    print(f"  ❌  Errores  : {len(results['error'])}")

    if results["error"]:
        print(f"\n  Series con error:")
        for r in results["error"]:
            print(f"    - {r['series']}: {r.get('error', '?')}")

    total_src = sum(r["rows_src"] for r in results["ok"] if isinstance(r["rows_src"], int))
    total_ice = sum(r["rows_ice"] for r in results["ok"] if isinstance(r["rows_ice"], int))
    print(f"\n  Filas migradas : {total_src:,} → {total_ice:,}")
    print(f"{'═'*60}\n")

    sys.exit(1 if results["error"] else 0)


if __name__ == "__main__":
    main()
