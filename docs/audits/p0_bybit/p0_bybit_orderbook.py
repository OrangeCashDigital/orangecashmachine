#!/usr/bin/env python3
"""
P0 EXPERIMENTAL — BYBIT PUBLIC ORDERBOOK WEBSOCKET (READ-ONLY)
=============================================================

Autor: Lead Market-Data Engineer + SRE + Software Architect (OCM)
Fecha: 2026-08-28

PROPÓSITO
---------
Obtener evidencia EMPÍRICA sobre el comportamiento real del WebSocket
público de orderbook de Bybit, para validar/refinar ADR-0028 (BookBuilder).

SOLO MARKET-DATA PÚBLICO. SIN AUTH. SIN CREDENCIALES. SIN TRADING.

Seguridad / alcance (NO AUTORIZADO):
  * NO usa API keys/secrets (solo conexión pública).
  * NO envía órdenes. NO toca cuentas/balances/posiciones.
  * NO modifica producción, systemd, config ni trading.
  * NO habilita exchanges en producción.

Endpoints oficiales Bybit (public, sin auth):
  * wss://stream.bybit.com/v5/public/linear   (USDT/USDC perpetual & USDT futures)

Dependencias: websockets (presente en el venv OCM).

USO
---
  .venv/bin/python docs/audits/p0_bybit/p0_bybit_orderbook.py --duration 60
  (escribe evidencia en docs/audits/p0_bybit/evidence/<ts>/ )
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter, deque
from datetime import datetime, timezone
from pathlib import Path

try:
    import websockets
except ImportError as exc:  # pragma: no cover
    sys.exit(f"Falta 'websockets' en el entorno: {exc}")


PUBLIC_WS = "wss://stream.bybit.com/v5/public/linear"
DEFAULT_SYMBOL = "BTCUSDT"
DEFAULT_DEPTH = 50
HEARTBEAT_INTERVAL = 20.0  # Bybit recomienda ping cada 20s
IP_LIMIT_HINT = 500  # conexiones / 5 min por dominio (oficial)


class P0OrderBookObserver:
    """Observa el WS público de orderbook de Bybit y mide semántica."""

    def __init__(self, symbol: str, depth: int, duration: float, outdir: Path) -> None:
        self.symbol = symbol
        self.depth = depth
        self.duration = duration
        self.outdir = outdir
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.raw_path = outdir / "raw.jsonl"
        self.rawf = None

        # acumuladores
        self.msg_count = 0
        self.type_counts: Counter[str] = Counter()
        self.levels_per_delta: list[int] = []
        self.levels_per_snapshot: list[int] = []
        self.u_series: list[int] = []
        self.seq_series: list[int] = []
        self.u_deltas: list[int] = []
        self.seq_deltas: list[int] = []
        self.duplicate_u = 0
        self.duplicate_seq = 0
        self.u_eq_1 = 0
        self.nowish = 0
        self.reconnects = 0
        self.latencies_ms: list[int] = []
        self.ts_series: list[int] = []
        self.cts_series: list[int] = []
        self.timestamps = deque(maxlen=100000)
        self.crossed_books = 0

    # -- utilidades -------------------------------------------------------
    @staticmethod
    def _ts_ms() -> int:
        return int(time.time() * 1000)

    def _log(self, msg: str) -> None:
        line = f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}"
        print(line, flush=True)

    # -- hooks WS ---------------------------------------------------------
    async def on_message(self, text: str, local_recv_ms: int) -> None:
        self.msg_count += 1
        if self.rawf:
            self.rawf.write(text + "\n")
            self.rawf.flush()
        try:
            msg = json.loads(text)
        except Exception:
            return

        # heartbeat responses
        if msg.get("op") in ("pong", "ping"):
            return

        if msg.get("type") == "COMMAND_RESP":
            self._log(f"subscription resp: {text}")
            return

        topic = msg.get("topic", "")
        if topic.startswith("orderbook."):
            self._handle_orderbook(msg, local_recv_ms)

    def _handle_orderbook(self, msg: dict, local_recv_ms: int) -> None:
        mtype = msg.get("type")
        self.type_counts[mtype] += 1
        data = msg.get("data", {})
        ts = msg.get("ts")
        cts = data.get("cts")
        u = data.get("u")
        seq = data.get("seq")
        bids = data.get("b", [])
        asks = data.get("a", [])

        if ts is not None:
            self.ts_series.append(ts)
            self.latencies_ms.append(local_recv_ms - ts)
        if cts is not None:
            self.cts_series.append(cts)

        if mtype == "snapshot":
            self.levels_per_snapshot.append(len(bids) + len(asks))
            # sanity: bids desc, asks asc
            if not self._is_sorted(bids, desc=True) or not self._is_sorted(asks, desc=False):
                self.crossed_books += 1
        elif mtype == "delta":
            self.levels_per_delta.append(len(bids) + len(asks))

        # secuencia
        if u is not None:
            prev = self.u_series[-1] if self.u_series else None
            self.u_series.append(u)
            if u == 1:
                self.u_eq_1 += 1
            if prev is not None:
                self.u_deltas.append(u - prev)
                if u == prev:
                    self.duplicate_u += 1
        if seq is not None:
            prev = self.seq_series[-1] if self.seq_series else None
            self.seq_series.append(seq)
            if prev is not None:
                self.seq_deltas.append(seq - prev)
                if seq == prev:
                    self.duplicate_seq += 1

    @staticmethod
    def _is_sorted(levels: list, *, desc: bool) -> bool:
        prices = [float(lvl[0]) for lvl in levels if len(lvl) >= 2]
        if desc:
            return all(prices[i] >= prices[i + 1] for i in range(len(prices) - 1))
        return all(prices[i] <= prices[i + 1] for i in range(len(prices) - 1))

    # -- loop principal ----------------------------------------------------
    async def run(self) -> None:
        topic = f"orderbook.{self.depth}.{self.symbol}"
        self._log(f"Conectando (sin auth) a {PUBLIC_WS} | topic={topic}")
        deadline = time.monotonic() + self.duration

        with open(self.raw_path, "w", encoding="utf-8") as self.rawf:
            while time.monotonic() < deadline:
                try:
                    async with websockets.connect(
                        PUBLIC_WS,
                        max_size=8 * 1024 * 1024,
                        open_timeout=15,
                    ) as ws:
                        await ws.send(json.dumps({"op": "subscribe", "args": [topic]}))
                        self._log(f"Suscrito: {topic}")
                        last_ping = time.monotonic()
                        while time.monotonic() < deadline:
                            # heartbeat
                            if time.monotonic() - last_ping >= HEARTBEAT_INTERVAL:
                                await ws.send(json.dumps({"op": "ping"}))
                                last_ping = time.monotonic()
                            try:
                                text = await asyncio.wait_for(ws.recv(), timeout=HEARTBEAT_INTERVAL + 5)
                                await self.on_message(text, self._ts_ms())
                            except asyncio.TimeoutError:
                                # sin mensajes -> enviar ping preventivo
                                await ws.send(json.dumps({"op": "ping"}))
                                last_ping = time.monotonic()
                except Exception as exc:
                    self.reconnects += 1
                    self._log(f"reconnect #{self.reconnects} tras error: {type(exc).__name__}: {exc}")
                    await asyncio.sleep(1.0)

        self._log("duración alcanzada; finalizando")

    # -- reporte ------------------------------------------------------------
    def summarize(self) -> dict:
        def gap_stats(deltas: list[int]) -> dict:
            if not deltas:
                return {"n": 0, "eq1": 0, "neq1": 0, "min": None, "max": None}
            return {
                "n": len(deltas),
                "eq1": sum(1 for d in deltas if d == 1),
                "neq1": sum(1 for d in deltas if d != 1),
                "min": min(deltas),
                "max": max(deltas),
            }

        return {
            "messages": self.msg_count,
            "type_counts": dict(self.type_counts),
            "reconnects": self.reconnects,
            "levels_per_delta": {
                "n": len(self.levels_per_delta),
                "min": min(self.levels_per_delta) if self.levels_per_delta else None,
                "max": max(self.levels_per_delta) if self.levels_per_delta else None,
                "dist": dict(Counter(self.levels_per_delta)),
            },
            "levels_per_snapshot": {
                "n": len(self.levels_per_snapshot),
                "min": min(self.levels_per_snapshot) if self.levels_per_snapshot else None,
                "max": max(self.levels_per_snapshot) if self.levels_per_snapshot else None,
            },
            "u": {
                "count": len(self.u_series),
                "distinct": len(set(self.u_series)),
                "eq_1_events": self.u_eq_1,
                "gaps": gap_stats(self.u_deltas),
            },
            "seq": {
                "count": len(self.seq_series),
                "distinct": len(set(self.seq_series)),
                "gaps": gap_stats(self.seq_deltas),
            },
            "duplicates": {"u": self.duplicate_u, "seq": self.duplicate_seq},
            "freshness": {
                "latency_ms": {
                    "n": len(self.latencies_ms),
                    "min": min(self.latencies_ms) if self.latencies_ms else None,
                    "max": max(self.latencies_ms) if self.latencies_ms else None,
                    "p50": sorted(self.latencies_ms)[len(self.latencies_ms) // 2] if self.latencies_ms else None,
                },
                "ts_range": ([min(self.ts_series), max(self.ts_series)] if self.ts_series else None),
            },
            "structure_issues": {"crossed_or_unsorted_snapshots": self.crossed_books},
        }


def main() -> None:
    ap = argparse.ArgumentParser(description="P0 Bybit public orderbook observer (read-only)")
    ap.add_argument("--duration", type=float, default=0, help="duración en segundos (0=60)")
    ap.add_argument("--symbol", default=DEFAULT_SYMBOL)
    ap.add_argument("--depth", type=int, default=DEFAULT_DEPTH)
    ap.add_argument("--out", default=None, help="directorio de salida (default: evidence/<ts>)")
    args = ap.parse_args()

    duration = args.duration or 60
    base = Path(args.out) if args.out else Path(__file__).parent / "evidence"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    outdir = base / ts

    observer = P0OrderBookObserver(symbol=args.symbol, depth=args.depth, duration=duration, outdir=outdir)

    asyncio.run(observer.run())

    summary = observer.summarize()
    summary_path = outdir / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print("=== RESUMEN ===")
    print(json.dumps(summary, indent=2))
    print(f"Evidencia en: {outdir}")


if __name__ == "__main__":
    main()
