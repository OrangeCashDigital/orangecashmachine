#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/calibrate_lateness.py
==============================

Job de calibración de lateness empírico.

Qué hace
--------
1. Consulta Prometheus/Pushgateway para obtener p95 y p99 de
   ocm_candle_delay_ms{mode="incremental"} por exchange+timeframe.
2. Aplica guardrails (floor/cap definidos en LatenessCalibrationStore).
3. Persiste en Redis via LatenessCalibrationStore.
4. overlap_for_timeframe() consumirá estos valores en el próximo run.

Cuándo ejecutar
---------------
  - Cron diario (ej: 02:00 UTC) una vez que haya ≥7 días de datos incrementales.
  - Manualmente: uv run python scripts/calibrate_lateness.py --dry-run

Dependencias externas
---------------------
  - Prometheus accesible en PROMETHEUS_URL (default: http://localhost:9090)
  - Redis accesible (igual que el pipeline principal)
  - ≥ MIN_SAMPLE_COUNT observaciones para confiar en el p99

Diseño
------
  El p99 de un Histogram de Prometheus requiere histogram_quantile() sobre
  los buckets acumulados. Este script usa la API HTTP de Prometheus
  (instant query) — no requiere librería adicional, solo requests/urllib.

  Ventana de 24h para p99: suficiente para capturar variación diaria
  sin mezclar con patrones de días anteriores.

Usage
-----
  uv run python scripts/calibrate_lateness.py
  uv run python scripts/calibrate_lateness.py --dry-run
  uv run python scripts/calibrate_lateness.py --prometheus-url http://prometheus:9090
  uv run python scripts/calibrate_lateness.py --window-hours 48 --min-samples 100
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
import urllib.parse
from typing import Optional

# Añadir raíz del proyecto al path para imports
sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.dirname(__file__)))

from loguru import logger


# ==========================================================
# Constantes
# ==========================================================

DEFAULT_PROMETHEUS_URL = "http://localhost:9090"
DEFAULT_WINDOW_HOURS   = 24
MIN_SAMPLE_COUNT       = 50   # mínimo de observaciones para confiar en p99

# Exchanges y timeframes a calibrar.
# Solo los que tienen tráfico incremental real.
_EXCHANGES  = ["bybit", "kucoin", "kucoinfutures"]
_TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d"]


# ==========================================================
# Prometheus query
# ==========================================================

def _query_prometheus(prometheus_url: str, promql: str) -> Optional[float]:
    """
    Ejecuta una instant query en Prometheus y retorna el primer valor escalar.
    Retorna None si no hay resultado o hay error.
    """
    endpoint = f"{prometheus_url}/api/v1/query"
    params   = urllib.parse.urlencode({"query": promql})
    url      = f"{endpoint}?{params}"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        if data.get("status") != "success":
            logger.warning("Prometheus query failed | promql={} status={}", promql, data.get("status"))
            return None
        results = data.get("data", {}).get("result", [])
        if not results:
            return None
        # instant query retorna [{metric: {}, value: [timestamp, "value"]}]
        value = results[0].get("value", [None, None])[1]
        return float(value) if value is not None else None
    except Exception as exc:
        logger.warning("Prometheus HTTP error | url={} error={}", url, exc)
        return None


def get_p99_lateness_ms(
    prometheus_url: str,
    exchange:       str,
    timeframe:      str,
    window_hours:   int,
) -> Optional[float]:
    """
    Calcula p99 de ocm_candle_delay_ms{mode="incremental"} para exchange+timeframe.
    Retorna ms como float, o None si no hay datos suficientes.
    """
    window = f"{window_hours}h"
    promql = (
        f'histogram_quantile(0.99, '
        f'rate(ocm_candle_delay_ms_bucket{{'
        f'exchange="{exchange}",timeframe="{timeframe}",mode="incremental"'
        f'}}[{window}]))'
    )
    return _query_prometheus(prometheus_url, promql)


def get_p95_lateness_ms(
    prometheus_url: str,
    exchange:       str,
    timeframe:      str,
    window_hours:   int,
) -> Optional[float]:
    """Calcula p95 de ocm_candle_delay_ms{mode="incremental"}."""
    window = f"{window_hours}h"
    promql = (
        f'histogram_quantile(0.95, '
        f'rate(ocm_candle_delay_ms_bucket{{'
        f'exchange="{exchange}",timeframe="{timeframe}",mode="incremental"'
        f'}}[{window}]))'
    )
    return _query_prometheus(prometheus_url, promql)


def get_sample_count(
    prometheus_url: str,
    exchange:       str,
    timeframe:      str,
    window_hours:   int,
) -> int:
    """Cuenta observaciones incrementales en la ventana."""
    window = f"{window_hours}h"
    promql = (
        f'increase(ocm_candle_delay_ms_count{{'
        f'exchange="{exchange}",timeframe="{timeframe}",mode="incremental"'
        f'}}[{window}])'
    )
    val = _query_prometheus(prometheus_url, promql)
    return int(val) if val is not None else 0


# ==========================================================
# Main
# ==========================================================

def calibrate(
    prometheus_url: str  = DEFAULT_PROMETHEUS_URL,
    window_hours:   int  = DEFAULT_WINDOW_HOURS,
    min_samples:    int  = MIN_SAMPLE_COUNT,
    dry_run:        bool = False,
) -> dict:
    """
    Ejecuta calibración para todos los exchanges y timeframes configurados.

    Returns dict con resultados por exchange+timeframe:
      {"bybit:1m": {"status": "ok", "lateness_ms": 5000, ...}, ...}
    """
    from infra.state.factories import build_lateness_calibration_store

    cal_store = None
    if not dry_run:
        cal_store = build_lateness_calibration_store()
        if cal_store is None:
            logger.error("Redis no disponible — abortando calibración")
            return {}

    results = {}

    for exchange in _EXCHANGES:
        for timeframe in _TIMEFRAMES:
            key = f"{exchange}:{timeframe}"

            samples = get_sample_count(prometheus_url, exchange, timeframe, window_hours)
            if samples < min_samples:
                logger.info(
                    "Calibración skipped — muestras insuficientes | "
                    "exchange={} timeframe={} samples={} min={}",
                    exchange, timeframe, samples, min_samples,
                )
                results[key] = {
                    "status":   "skipped",
                    "reason":   "insufficient_samples",
                    "samples":  samples,
                }
                continue

            p99 = get_p99_lateness_ms(prometheus_url, exchange, timeframe, window_hours)
            p95 = get_p95_lateness_ms(prometheus_url, exchange, timeframe, window_hours)

            if p99 is None:
                logger.warning(
                    "Calibración skipped — p99 no disponible | exchange={} timeframe={}",
                    exchange, timeframe,
                )
                results[key] = {"status": "skipped", "reason": "p99_unavailable"}
                continue

            lateness_ms = int(p99)
            p95_ms      = int(p95) if p95 is not None else 0

            logger.info(
                "Calibración calculada | exchange={} timeframe={} "
                "p99_ms={} p95_ms={} samples={} dry_run={}",
                exchange, timeframe, lateness_ms, p95_ms, samples, dry_run,
            )

            if not dry_run and cal_store is not None:
                ok = cal_store.set(
                    exchange     = exchange,
                    timeframe    = timeframe,
                    lateness_ms  = lateness_ms,
                    p95_ms       = p95_ms,
                    sample_count = samples,
                    window_hours = window_hours,
                    source       = "prometheus",
                )
                results[key] = {
                    "status":      "ok" if ok else "write_error",
                    "lateness_ms": lateness_ms,
                    "p95_ms":      p95_ms,
                    "samples":     samples,
                }
            else:
                results[key] = {
                    "status":      "dry_run",
                    "lateness_ms": lateness_ms,
                    "p95_ms":      p95_ms,
                    "samples":     samples,
                }

    # Resumen
    ok_count      = sum(1 for v in results.values() if v.get("status") == "ok")
    skipped_count = sum(1 for v in results.values() if v.get("status") == "skipped")
    dryrun_count  = sum(1 for v in results.values() if v.get("status") == "dry_run")
    logger.info(
        "Calibración completada | ok={} skipped={} dry_run={} total={}",
        ok_count, skipped_count, dryrun_count, len(results),
    )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrar lateness desde Prometheus → Redis")
    parser.add_argument(
        "--prometheus-url", default=DEFAULT_PROMETHEUS_URL,
        help=f"URL de Prometheus (default: {DEFAULT_PROMETHEUS_URL})",
    )
    parser.add_argument(
        "--window-hours", type=int, default=DEFAULT_WINDOW_HOURS,
        help=f"Ventana de datos en horas (default: {DEFAULT_WINDOW_HOURS})",
    )
    parser.add_argument(
        "--min-samples", type=int, default=MIN_SAMPLE_COUNT,
        help=f"Mínimo de observaciones para confiar en p99 (default: {MIN_SAMPLE_COUNT})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Calcular sin escribir en Redis",
    )
    args = parser.parse_args()

    results = calibrate(
        prometheus_url = args.prometheus_url,
        window_hours   = args.window_hours,
        min_samples    = args.min_samples,
        dry_run        = args.dry_run,
    )

    if args.dry_run:
        print("\n=== DRY RUN — no se escribió en Redis ===")
        for key, val in sorted(results.items()):
            print(f"  {key}: {val}")


if __name__ == "__main__":
    main()
