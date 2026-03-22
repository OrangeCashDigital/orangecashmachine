"""
snapshot.py
===========

Snapshots globales del Data Lake — OrangeCashMachine.

Un snapshot es una foto inmutable del estado completo del data lake
en un momento dado. Permite responder: "¿qué datos exactos tenía
el sistema cuando corrí este backtest?"

Estructura
----------
data_lake/_manifests/
    snapshot_20260318T051224.json   ← snapshot por run
    latest.json                     ← apunta al último snapshot

Formato de snapshot
-------------------
{
  "snapshot_id": "20260318T051224",
  "created_at": "2026-03-18T05:12:24Z",
  "git_hash": "45d015d",
  "datasets": [
    {
      "exchange": "kucoin",
      "symbol": "BTC/USDT",
      "timeframe": "1m",
      "version_id": "v000002",
      "min_ts": "...",
      "max_ts": "...",
      "rows": 46,
      "checksum": "..."
    }
  ]
}
"""

from __future__ import annotations

import json
from core.utils import get_git_hash
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger


# ==========================================================
# Constants
# ==========================================================

_MANIFESTS_DIR = "data_platform/data_lake/_manifests"
_SILVER_ROOT   = "data_platform/data_lake/silver/ohlcv"


# ==========================================================
# SnapshotManager
# ==========================================================

class SnapshotManager:
    """
    Crea y gestiona snapshots globales del data lake.

    Un snapshot captura el estado de todos los datasets silver
    en un momento dado, usando sus versiones actuales (latest.json).

    Uso
    ---
    manager = SnapshotManager()
    snapshot_id = manager.create_snapshot()
    print(f"Snapshot: {snapshot_id}")

    # Más tarde, para reproducir:
    snapshot = manager.load_snapshot(snapshot_id)
    """

    def __init__(
        self,
        base_path: Optional[str | Path] = None,
    ) -> None:
        if base_path:
            self._base = Path(base_path).resolve()
        else:
            self._base = Path(__file__).resolve().parents[3]

        self._manifests_dir = self._base / _MANIFESTS_DIR
        self._silver_root   = self._base / _SILVER_ROOT
        self._manifests_dir.mkdir(parents=True, exist_ok=True)

    # ======================================================
    # Public API
    # ======================================================

    def create_snapshot(self) -> str:
        """
        Crea un snapshot del estado actual del data lake silver.

        Recorre todos los _versions/latest.json del silver layer
        y construye un manifest global con el estado completo.

        Returns
        -------
        str
            snapshot_id generado (timestamp-based).
        """
        now = datetime.now(timezone.utc)
        snapshot_id = now.strftime("%Y%m%dT%H%M%S")

        datasets = self._collect_datasets()

        snapshot: Dict = {
            "snapshot_id": snapshot_id,
            "created_at":  now.isoformat(),
            "git_hash":    get_git_hash(),
            "total_datasets": len(datasets),
            "total_rows":  sum(d.get("rows", 0) for d in datasets),
            "datasets":    datasets,
        }

        # Dedup: si los checksums no cambiaron respecto al ultimo snapshot, no crear uno nuevo
        latest_path = self._manifests_dir / "latest.json"
        if latest_path.exists():
            try:
                prev = json.loads(latest_path.read_text(encoding="utf-8"))
                prev_sigs = {
                    (d["exchange"], d["symbol"], d["timeframe"]): d.get("checksums")
                    for d in prev.get("datasets", [])
                }
                new_sigs = {
                    (d["exchange"], d["symbol"], d["timeframe"]): d.get("checksums")
                    for d in datasets
                }
                if prev_sigs == new_sigs:
                    logger.debug(
                        "Snapshot skip (no changes) | datasets={} rows={}",
                        len(datasets), snapshot["total_rows"],
                    )
                    return prev["snapshot_id"]
            except Exception as exc:
                logger.warning("Snapshot dedup check failed | {}", exc)

        # Escribir snapshot específico
        path = self._manifests_dir / f"snapshot_{snapshot_id}.json"
        path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")

        # Actualizar latest
        latest = self._manifests_dir / "latest.json"
        latest.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")

        logger.info(
            "Snapshot created | id={} datasets={} rows={} git={}",
            snapshot_id,
            len(datasets),
            snapshot["total_rows"],
            snapshot["git_hash"],
        )

        return snapshot_id

    def load_snapshot(self, snapshot_id: str = "latest") -> Optional[Dict]:
        """
        Carga un snapshot por ID o el último disponible.

        Parameters
        ----------
        snapshot_id : str
            "latest" para el último, o un ID específico como "20260318T051224".

        Returns
        -------
        dict con el snapshot, o None si no existe.
        """
        if snapshot_id == "latest":
            path = self._manifests_dir / "latest.json"
        else:
            path = self._manifests_dir / f"snapshot_{snapshot_id}.json"

        if not path.exists():
            logger.warning("Snapshot not found | id={}", snapshot_id)
            return None

        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error("Snapshot read failed | id={} error={}", snapshot_id, exc)
            return None

    def list_snapshots(self) -> List[str]:
        """Lista todos los snapshot IDs disponibles."""
        return sorted(
            p.stem.replace("snapshot_", "")
            for p in self._manifests_dir.glob("snapshot_*.json")
        )

    # ======================================================
    # Internal
    # ======================================================

    def _collect_datasets(self) -> List[Dict]:
        """
        Recorre el silver layer y recopila el estado latest de cada dataset.
        """
        datasets = []

        if not self._silver_root.exists():
            return datasets

        # Buscar todos los latest.json en el silver layer
        for latest_path in sorted(self._silver_root.rglob("_versions/latest.json")):
            try:
                version = json.loads(latest_path.read_text(encoding="utf-8"))

                # Extraer info clave del dataset
                dataset_info: Dict = {
                    "exchange":   version.get("exchange", "unknown"),
                    "symbol":     version.get("symbol", "unknown"),
                    "timeframe":  version.get("timeframe", "unknown"),
                    "version_id": version.get("version_id", "unknown"),
                    "git_hash":   version.get("git_hash", "unknown"),
                    "written_at": version.get("written_at", ""),
                    "rows":       sum(
                        p.get("rows", 0)
                        for p in version.get("partitions", [])
                    ),
                    "min_ts":     self._min_ts(version),
                    "max_ts":     self._max_ts(version),
                    "checksums":  [
                        p.get("checksum", "")
                        for p in version.get("partitions", [])
                    ],
                }
                datasets.append(dataset_info)

            except Exception as exc:
                logger.warning(
                    "Failed reading dataset version | path={} error={}",
                    latest_path, exc,
                )

        return datasets

    @staticmethod
    def _min_ts(version: Dict) -> str:
        partitions = version.get("partitions", [])
        tss = [p.get("min_ts", "") for p in partitions if p.get("min_ts")]
        return min(tss) if tss else ""

    @staticmethod
    def _max_ts(version: Dict) -> str:
        partitions = version.get("partitions", [])
        tss = [p.get("max_ts", "") for p in partitions if p.get("max_ts")]
        return max(tss) if tss else ""


# ==========================================================
# Helpers
# ==========================================================
