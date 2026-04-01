#!/usr/bin/env python3
"""
data_platform/validate_silver.py
=================================

Validador físico de datos OHLCV en la capa Silver.

Responsabilidad
---------------
Verificar la integridad y continuidad temporal de todas las series
almacenadas en silver/ohlcv, agrupando por serie completa
(exchange, symbol, market_type, timeframe) independientemente de
la partición año/mes/día.

Estrategia de lectura
---------------------
1. Lee _versions/latest.json para obtener rangos por partición en O(1).
2. Fallback a escaneo de parquets si no existe manifest.
3. Solo abre parquets cuando el manifest no es suficiente.

Detección de anomalías
----------------------
- Gap: diferencia entre max_ts de una partición y min_ts de la siguiente
  mayor que el timeframe esperado + tolerancia.
- Solapamiento: min_ts de una partición anterior al max_ts de la anterior.
- Partición huérfana: path en manifest pero parquet no existe en disco.
- Partición sin manifest: parquet en disco pero no listado en manifest.
- Rows insuficientes: serie con menos de 2 filas (no validable).

Uso
---
    uv run python3 data_platform/validate_silver.py
    uv run python3 data_platform/validate_silver.py --exchange kucoin
    uv run python3 data_platform/validate_silver.py --fail-only
    uv run python3 data_platform/validate_silver.py --verbose
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ==========================================================
# Configuración
# ==========================================================

_SILVER_ROOT = Path("data_platform/local_data_lake/silver/ohlcv")

_TIMEFRAME_SECONDS: Dict[str, int] = {
    "1m":  60,
    "3m":  180,
    "5m":  300,
    "15m": 900,
    "30m": 1800,
    "1h":  3600,
    "2h":  7200,
    "4h":  14400,
    "6h":  21600,
    "8h":  28800,
    "12h": 43200,
    "1d":  86400,
    "3d":  259200,
    "1w":  604800,
}

_TOLERANCE_S = 1.0


# ==========================================================
# Estructuras de datos
# ==========================================================

@dataclass
class PartitionInfo:
    path:     str
    rows:     int
    min_ts:   pd.Timestamp
    max_ts:   pd.Timestamp
    checksum: Optional[str] = None
    exists:   bool = True


@dataclass
class GapInfo:
    prev_max:    pd.Timestamp
    next_min:    pd.Timestamp
    gap_seconds: float
    expected_s:  int

    @property
    def gap_candles(self) -> float:
        return self.gap_seconds / self.expected_s

    def __str__(self) -> str:
        return (
            f"{self.prev_max} -> {self.next_min} "
            f"({self.gap_seconds:.0f}s = {self.gap_candles:.0f}x tf)"
        )


@dataclass
class OverlapInfo:
    prev_max:        pd.Timestamp
    next_min:        pd.Timestamp
    overlap_seconds: float

    def __str__(self) -> str:
        return (
            f"{self.prev_max} <> {self.next_min} "
            f"({self.overlap_seconds:.0f}s solapamiento)"
        )


@dataclass
class SeriesResult:
    exchange:    str
    symbol:      str
    market_type: str
    timeframe:   str

    total_rows:      int = 0
    first_ts:        Optional[pd.Timestamp] = None
    last_ts:         Optional[pd.Timestamp] = None
    partition_count: int = 0

    gaps:            List[GapInfo]     = field(default_factory=list)
    overlaps:        List[OverlapInfo] = field(default_factory=list)
    orphan_paths:    List[str]         = field(default_factory=list)
    untracked_paths: List[str]         = field(default_factory=list)
    error:           Optional[str]     = None
    skipped:         bool = False
    skip_reason:     Optional[str] = None

    @property
    def key(self) -> str:
        return f"{self.exchange}/{self.symbol}/{self.market_type}/{self.timeframe}"

    @property
    def ok(self) -> bool:
        return (
            self.error is None
            and not self.skipped
            and not self.gaps
            and not self.overlaps
            and not self.orphan_paths
        )

    @property
    def status(self) -> str:
        if self.error:
            return "ERROR"
        if self.skipped:
            return "SKIP"
        if self.gaps or self.overlaps or self.orphan_paths:
            return "FAIL"
        return "OK"


# ==========================================================
# Lectura de manifests
# ==========================================================

def _load_manifest(dataset_root: Path) -> Optional[List[PartitionInfo]]:
    """Lee particiones desde _versions/latest.json en O(1)."""
    latest = dataset_root / "_versions" / "latest.json"
    if not latest.exists():
        return None
    try:
        manifest = json.loads(latest.read_text(encoding="utf-8"))
    except Exception:
        return None

    partitions: List[PartitionInfo] = []
    for p in manifest.get("partitions", []):
        if "min_ts" not in p or "max_ts" not in p:
            continue
        abs_path = _SILVER_ROOT / p["path"]
        partitions.append(PartitionInfo(
            path     = p["path"],
            rows     = p.get("rows", 0),
            min_ts   = pd.Timestamp(p["min_ts"], tz="UTC"),
            max_ts   = pd.Timestamp(p["max_ts"], tz="UTC"),
            checksum = p.get("checksum"),
            exists   = abs_path.exists(),
        ))
    return partitions if partitions else None


def _load_from_parquets(dataset_root: Path) -> Optional[List[PartitionInfo]]:
    """Fallback: construye particiones escaneando parquets en disco."""
    files = sorted(dataset_root.rglob("*.parquet"))
    if not files:
        return None
    partitions: List[PartitionInfo] = []
    for f in files:
        try:
            df = pd.read_parquet(f, columns=["timestamp"])
            partitions.append(PartitionInfo(
                path   = str(f.relative_to(_SILVER_ROOT)),
                rows   = len(df),
                min_ts = df["timestamp"].min(),
                max_ts = df["timestamp"].max(),
                exists = True,
            ))
        except Exception as exc:
            partitions.append(PartitionInfo(
                path     = str(f.relative_to(_SILVER_ROOT)),
                rows     = 0,
                min_ts   = pd.Timestamp("1970-01-01", tz="UTC"),
                max_ts   = pd.Timestamp("1970-01-01", tz="UTC"),
                exists   = True,
                checksum = f"READ_ERROR:{exc}",
            ))
    return partitions if partitions else None


# ==========================================================
# Detección de gaps y solapamientos
# ==========================================================

def _detect_inter_partition_issues(
    partitions: List[PartitionInfo],
    expected_s: int,
) -> Tuple[List[GapInfo], List[OverlapInfo]]:
    valid = [p for p in partitions if p.exists and p.rows > 0]
    valid.sort(key=lambda p: p.min_ts)

    gaps:     List[GapInfo]     = []
    overlaps: List[OverlapInfo] = []

    for i in range(1, len(valid)):
        prev = valid[i - 1]
        curr = valid[i]
        diff_s = (curr.min_ts - prev.max_ts).total_seconds()
        if diff_s > expected_s + _TOLERANCE_S:
            gaps.append(GapInfo(
                prev_max    = prev.max_ts,
                next_min    = curr.min_ts,
                gap_seconds = diff_s,
                expected_s  = expected_s,
            ))
        elif diff_s < -_TOLERANCE_S:
            overlaps.append(OverlapInfo(
                prev_max        = prev.max_ts,
                next_min        = curr.min_ts,
                overlap_seconds = -diff_s,
            ))
    return gaps, overlaps


# ==========================================================
# Validación de una serie
# ==========================================================

def validate_series(
    exchange:    str,
    symbol:      str,
    market_type: str,
    timeframe:   str,
) -> SeriesResult:

    result = SeriesResult(
        exchange=exchange, symbol=symbol,
        market_type=market_type, timeframe=timeframe,
    )

    expected_s = _TIMEFRAME_SECONDS.get(timeframe)
    if expected_s is None:
        result.error = f"Timeframe desconocido: {timeframe}"
        return result

    safe_sym = symbol.replace("/", "_")
    dataset_root = (
        _SILVER_ROOT
        / f"exchange={exchange}"
        / f"symbol={safe_sym}"
        / f"market_type={market_type}"
        / f"timeframe={timeframe}"
    )

    if not dataset_root.exists():
        result.skipped = True
        result.skip_reason = "directorio no existe"
        return result

    partitions = _load_manifest(dataset_root)
    used_fallback = partitions is None
    if used_fallback:
        partitions = _load_from_parquets(dataset_root)

    if not partitions:
        result.skipped = True
        result.skip_reason = "sin particiones"
        return result

    result.orphan_paths = [p.path for p in partitions if not p.exists]

    if not used_fallback:
        tracked = {p.path for p in partitions}
        for f in sorted(dataset_root.rglob("*.parquet")):
            rel = str(f.relative_to(_SILVER_ROOT))
            if rel not in tracked:
                result.untracked_paths.append(rel)

    valid_partitions = [p for p in partitions if p.exists and p.rows > 0]

    if len(valid_partitions) < 1:
        result.skipped = True
        result.skip_reason = (
            f"0 particiones válidas en disco "
            f"({len(result.orphan_paths)} huérfanas)"
        )
        return result

    result.partition_count = len(valid_partitions)
    result.total_rows      = sum(p.rows for p in valid_partitions)
    result.first_ts        = min(p.min_ts for p in valid_partitions)
    result.last_ts         = max(p.max_ts for p in valid_partitions)

    if len(valid_partitions) < 2:
        result.skipped = True
        result.skip_reason = (
            f"1 partición ({result.total_rows} filas) "
            f"— insuficiente para validar continuidad"
        )
        return result

    result.gaps, result.overlaps = _detect_inter_partition_issues(
        valid_partitions, expected_s
    )
    return result


# ==========================================================
# Descubrimiento de series
# ==========================================================

def discover_series(
    root:     Path = _SILVER_ROOT,
    exchange: Optional[str] = None,
) -> List[Tuple[str, str, str, str]]:
    series = set()
    for f in root.rglob("*.parquet"):
        rel   = f.relative_to(root)
        parts = rel.parts
        if len(parts) < 5:
            continue
        exc = parts[0].removeprefix("exchange=")
        sym = parts[1].removeprefix("symbol=").replace("_", "/", 1)
        mkt = parts[2].removeprefix("market_type=")
        tf  = parts[3].removeprefix("timeframe=")
        series.add((exc, sym, mkt, tf))
    result = sorted(series)
    if exchange:
        result = [(e, s, m, t) for e, s, m, t in result if e == exchange]
    return result


# ==========================================================
# Report
# ==========================================================

def _print_result(r: SeriesResult, verbose: bool = False) -> None:
    status = f"[{r.status}]"

    if r.skipped:
        print(f"{status:<7} {r.key} -- {r.skip_reason}")
        return
    if r.error:
        print(f"{status:<7} {r.key} -- {r.error}")
        return

    summary = (
        f"rows={r.total_rows} "
        f"partitions={r.partition_count} "
        f"rango={r.first_ts} -> {r.last_ts} "
        f"gaps={len(r.gaps)} "
        f"overlaps={len(r.overlaps)}"
    )
    if r.orphan_paths:
        summary += f" orphans={len(r.orphan_paths)}"

    print(f"{status:<7} {r.key} | {summary}")

    if not r.ok or verbose:
        for g in r.gaps[:5]:
            print(f"        GAP:     {g}")
        if len(r.gaps) > 5:
            print(f"        ... y {len(r.gaps) - 5} gaps mas")
        for o in r.overlaps[:3]:
            print(f"        OVERLAP: {o}")
        for p in r.orphan_paths[:3]:
            print(f"        ORPHAN:  {p}")
        for p in r.untracked_paths[:3]:
            print(f"        UNTRACK: {p}")


def _print_summary(results: List[SeriesResult]) -> None:
    total      = len(results)
    ok         = sum(1 for r in results if r.ok)
    failed     = sum(1 for r in results if r.status == "FAIL")
    errors     = sum(1 for r in results if r.status == "ERROR")
    skipped    = sum(1 for r in results if r.skipped)
    total_gaps = sum(len(r.gaps) for r in results)

    print()
    print("=" * 70)
    print(
        f"RESUMEN | series={total} ok={ok} fail={failed} "
        f"error={errors} skip={skipped} gaps_totales={total_gaps}"
    )

    failures = [r for r in results if not r.ok and not r.skipped]
    if failures:
        print()
        print("Series con problemas:")
        for r in failures:
            print(f"  {r.status:<6} {r.key}")
            for g in r.gaps[:3]:
                print(f"         gap: {g}")
            if r.error:
                print(f"         error: {r.error}")


# ==========================================================
# Entry point
# ==========================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validador fisico de datos OHLCV Silver"
    )
    parser.add_argument(
        "--exchange", "-e",
        help="Filtrar por exchange (ej: kucoin, bybit)",
        default=None,
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Mostrar detalle de gaps incluso en series OK",
    )
    parser.add_argument(
        "--fail-only",
        action="store_true",
        help="Mostrar solo series con problemas",
    )
    args = parser.parse_args()

    series_list = discover_series(exchange=args.exchange)
    if not series_list:
        print(f"No se encontraron series en {_SILVER_ROOT}")
        return 1

    print(f"Validando {len(series_list)} series en {_SILVER_ROOT}")
    print("=" * 70)

    results: List[SeriesResult] = []
    for exc, sym, mkt, tf in series_list:
        r = validate_series(exc, sym, mkt, tf)
        results.append(r)
        if not args.fail_only or not r.ok:
            _print_result(r, verbose=args.verbose)

    _print_summary(results)

    has_failures = any(not r.ok and not r.skipped for r in results)
    return 1 if has_failures else 0


if __name__ == "__main__":
    sys.exit(main())
