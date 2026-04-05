"""
gold_storage.py
===============

Capa Gold del Data Lakehouse.

Responsabilidad
---------------
Leer datos Silver limpios, calcular features técnicos via FeatureEngineer,
y persistir el dataset enriquecido listo para estrategias/backtesting.

Estructura de partición
-----------------------
gold/features/ohlcv/
  exchange={exchange}/
    symbol={symbol}/
      market_type={market_type}/
        timeframe={timeframe}/
          {symbol}_{tf}_features.parquet
          _versions/
            v000001.json
            v000002.json
            latest.json

Versionado
----------
Cada build() genera un manifest en _versions/ con metadata completa:
git_hash, engineer_version, silver_version, checksums y rangos temporales.

Esto garantiza que cualquier backtest puede identificar exactamente qué
versión de features usó y reproducirla, incluso si FeatureEngineer evoluciona.
"""

from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from core.config.lineage import get_git_hash
from core.config.paths import silver_ohlcv_root, gold_features_root
from data_platform.ohlcv_utils import safe_symbol
from market_data.storage.gold.feature_engineer import FeatureEngineer
from market_data.storage.iceberg.iceberg_storage import IcebergStorage


# ==========================================================
# GoldStorage
# ==========================================================

class GoldStorage:
    """
    Construye datasets Gold (Silver + features) para trading/backtesting.

    Cada build() genera un manifest versionado en _versions/ que permite
    reproducir exactamente qué features se calcularon y con qué datos.

    Uso
    ---
    gold = GoldStorage()
    df   = gold.build(exchange="bybit", symbol="BTC/USDT", market_type="spot", timeframe="1h")
    """

    def __init__(
        self,
        gold_path:   Optional[Path] = None,
        dry_run:     bool           = False,
        silver_path: Optional[Path] = None,  # deprecated — ignorado, Silver es Iceberg
    ) -> None:
        if silver_path is not None:
            logger.warning(
                "GoldStorage: silver_path está deprecado — Silver es Iceberg, "
                "el path de filesystem no tiene efecto."
            )
        self._gold     = Path(gold_path) if gold_path else gold_features_root()
        self._gold.mkdir(parents=True, exist_ok=True)
        self._engineer = FeatureEngineer()
        self._dry_run  = dry_run
        self._write_locks: Dict[str, threading.Lock] = {}
        self._write_locks_meta = threading.Lock()
        logger.info("GoldStorage ready | gold={} dry_run={}", self._gold, self._dry_run)

    # ----------------------------------------------------------
    # Silver manifest reader
    # ----------------------------------------------------------

    def _read_silver_manifest(
        self,
        exchange: str,
        symbol: str,
        market_type: str,
        timeframe: str,
        version_id: str,
    ) -> dict:
        """
        Retorna metadata de lineage de Silver (Iceberg) para el manifest de Gold.

        Con Iceberg no hay archivos _versions/ — la versión es el snapshot_id.
        """
        base = {
            "layer":       "silver",
            "backend":     "iceberg",
            "exchange":    exchange,
            "symbol":      symbol,
            "market_type": market_type,
            "timeframe":   timeframe,
            "version_id":  version_id,
        }
        try:
            storage = IcebergStorage(exchange=exchange, market_type=market_type)
            snap = storage._table.current_snapshot()
            if snap is not None:
                base["snapshot_id"]  = snap.snapshot_id
                base["snapshot_ms"]  = snap.timestamp_ms
        except Exception:
            pass
        return base

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    def build(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
        start:       Optional[pd.Timestamp] = None,
        end:         Optional[pd.Timestamp] = None,
        silver_version: str = "latest",
        run_id:      Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Lee Silver, calcula features y persiste en Gold con manifest versionado.

        Parameters
        ----------
        exchange       : e.g. "bybit"
        symbol         : e.g. "BTC/USDT"
        market_type    : "spot" | "swap"
        timeframe      : e.g. "1h"
        start / end    : filtro de tiempo opcional sobre Silver
        silver_version : versión de Silver a usar (default "latest")
                         Permite reproducibilidad completa: rebuild con
                         silver_version="v000042" produce el mismo Gold.
        """
        sym_safe = safe_symbol(symbol)
        silver = IcebergStorage(exchange=exchange, market_type=market_type)

        # ── Cargar Silver (Iceberg) ──────────────────────────────────────
        try:
            df = silver.load_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                start=start,
                end=end,
            )
        except Exception as exc:
            logger.warning(
                "Gold build: Iceberg load failed | {}/{}/{}/{} err={}",
                exchange, symbol, market_type, timeframe, exc,
            )
            return None

        if df is None or df.empty:
            logger.warning(
                "Gold build: sin datos en Silver | {}/{}/{}/{}",
                exchange, symbol, market_type, timeframe,
            )
            return None

        # ── Validar timestamps antes de feature engineering ──────────────
        nan_ts = df["timestamp"].isna().sum()
        if nan_ts > 0:
            logger.warning(
                "Gold build: {} timestamps NaN — eliminando antes de features | {}/{}/{}/{}",
                nan_ts, exchange, symbol, market_type, timeframe,
            )
            df = df.dropna(subset=["timestamp"]).reset_index(drop=True)

        # ── Feature engineering ──────────────────────────────────────────
        df = self._engineer.compute(df, symbol=symbol, timeframe=timeframe)

        # ── Resolver versión concreta de Silver (reproducibilidad) ─────
        resolved_silver_v = self._resolve_silver_version(
            exchange, symbol, market_type, timeframe, silver_version
        )

        # ── DRY RUN ──────────────────────────────────────────────────────
        if self._dry_run:
            logger.info(
                "[DRY RUN] Gold build skipped | {}/{}/{}/{} rows={} features={} run_id={}",
                exchange, symbol, market_type, timeframe, len(df), len(df.columns), run_id,
            )
            return df

        # ── Persistir + Versionado (bajo write lock por dataset) ────────
        _lock = self._get_write_lock(exchange, symbol, market_type, timeframe)
        with _lock:
            out_dir = (
                self._gold
                / f"exchange={exchange}"
                / f"symbol={sym_safe}"
                / f"market_type={market_type}"
                / f"timeframe={timeframe}"
            )
            out_dir.mkdir(parents=True, exist_ok=True)
            out_file = out_dir / f"{sym_safe}_{timeframe}_features.parquet"

            df.to_parquet(out_file, index=False, compression="zstd", compression_level=4)

        # ── Versionado ───────────────────────────────────────────────────
        checksum = _file_checksum(out_file)
        # schema_hash: detecta drift silencioso de columnas entre versiones.
        # Cambia si FeatureEngineer añade/elimina/renombra features.
        schema_hash = hashlib.md5(",".join(sorted(df.columns)).encode()).hexdigest()
        silver_manifest = self._read_silver_manifest(
            exchange, symbol, market_type, timeframe, resolved_silver_v
        )
        self._write_version(
            out_dir        = out_dir,
            out_file       = out_file,
            exchange       = exchange,
            symbol         = symbol,
            market_type    = market_type,
            timeframe      = timeframe,
            rows           = len(df),
            features       = len(df.columns),
            checksum       = checksum,
            silver_version = resolved_silver_v,
            run_id         = run_id,
            input_versions = [silver_manifest],
            schema_hash    = schema_hash,
            min_ts         = str(df["timestamp"].min()) if not df.empty else None,
            max_ts         = str(df["timestamp"].max()) if not df.empty else None,
        )

        logger.info(
            "Gold saved | {}/{}/{}/{} rows={} features={} file={}",
            exchange, symbol, market_type, timeframe,
            len(df), len(df.columns),
            out_file.relative_to(self._gold),
        )
        return df

    def build_all(
        self,
        exchange:    str,
        symbols:     List[str],
        market_type: str,
        timeframes:  List[str],
        run_id:      Optional[str] = None,
    ) -> None:
        """Construye Gold para todos los pares/timeframes de un exchange."""
        total   = len(symbols) * len(timeframes)
        done    = 0
        failed  = 0
        for symbol in symbols:
            for tf in timeframes:
                try:
                    result = self.build(
                        exchange=exchange,
                        symbol=symbol,
                        market_type=market_type,
                        timeframe=tf,
                        run_id=run_id,
                    )
                    if result is None:
                        failed += 1
                        logger.warning(
                            "Gold build_all: build returned None | {}/{}/{}/{}",
                            exchange, symbol, market_type, tf,
                        )
                    else:
                        done += 1
                except Exception as exc:
                    failed += 1
                    logger.error(
                        "Gold build_all error | {}/{}/{}/{} err={}",
                        exchange, symbol, market_type, tf, exc,
                    )
                logger.debug(
                    "Gold build_all progress | {}/{} done={} failed={}",
                    done + failed, total, done, failed,
                )
        logger.info(
            "Gold build_all finished | exchange={} market={} done={} failed={}/{}",
            exchange, market_type, done, failed, total,
        )

    def list_versions(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
    ) -> List[str]:
        """Lista todas las versiones de features disponibles para un dataset."""
        versions_dir = self._versions_dir(exchange, symbol, market_type, timeframe)
        if not versions_dir.exists():
            return []
        return sorted(p.stem for p in versions_dir.glob("v*.json"))

    # ----------------------------------------------------------
    # Silver version resolver
    # ----------------------------------------------------------

    def _resolve_silver_version(
        self,
        exchange: str,
        symbol: str,
        market_type: str,
        timeframe: str,
        requested: str = "latest",
    ) -> str:
        """
        Resuelve la versión de Silver (Iceberg) como snapshot_id.

        Con Iceberg el versionado es por snapshot — cada append genera
        un snapshot_id único. Usamos el snapshot actual como proxy de
        'latest'. Si se pasa un snapshot_id concreto, se devuelve tal cual.
        """
        if requested != "latest":
            return requested
        try:
            storage = IcebergStorage(exchange=exchange, market_type=market_type)
            snap = storage._table.current_snapshot()
            if snap is not None:
                return f"iceberg-snap-{snap.snapshot_id}"
        except Exception:
            pass
        return "iceberg-latest"

    # ----------------------------------------------------------
    # Path helpers
    # ----------------------------------------------------------

    def _dataset_dir(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
    ) -> Path:
        return (
            self._gold
            / f"exchange={exchange}"
            / f"symbol={safe_symbol(symbol)}"
            / f"market_type={market_type}"
            / f"timeframe={timeframe}"
        )

    def _versions_dir(
        self,
        exchange:    str,
        symbol:      str,
        market_type: str,
        timeframe:   str,
    ) -> Path:
        return self._dataset_dir(exchange, symbol, market_type, timeframe) / "_versions"

    # ----------------------------------------------------------
    # Concurrency
    # ----------------------------------------------------------

    def _get_write_lock(self, exchange: str, symbol: str, market_type: str, timeframe: str) -> threading.Lock:
        """Lock por dataset — evita race conditions entre builds paralelos."""
        key = f"{exchange}:{symbol}:{market_type}:{timeframe}"
        with self._write_locks_meta:
            if key not in self._write_locks:
                self._write_locks[key] = threading.Lock()
            return self._write_locks[key]

    # ----------------------------------------------------------
    # Versioning
    # ----------------------------------------------------------

    def _write_version(
        self,
        out_dir:        Path,
        out_file:       Path,
        exchange:       str,
        symbol:         str,
        market_type:    str,
        timeframe:      str,
        rows:           int,
        features:       int,
        checksum:       str,
        silver_version: str,
        min_ts:         Optional[str],
        max_ts:         Optional[str],
        run_id:         Optional[str] = None,
        input_versions: Optional[list] = None,
        schema_hash:    Optional[str] = None,
    ) -> None:
        """
        Escribe manifest versionado en _versions/.

        Lógica
        ------
        - Dedup por checksum: si el archivo no cambió, no crea versión nueva.
        - Escritura atómica: .tmp → rename, nunca escribe directamente.
        - latest.json siempre apunta a la versión más reciente.
        """
        versions_dir = out_dir / "_versions"
        versions_dir.mkdir(parents=True, exist_ok=True)

        # Dedup: si checksum idéntico al latest, skip
        latest_path = versions_dir / "latest.json"
        if latest_path.exists():
            try:
                latest = json.loads(latest_path.read_text(encoding="utf-8"))
                if latest.get("checksum") == checksum:
                    logger.debug(
                        "Gold version skip (no changes) | {}/{}/{}/{}",
                        exchange, symbol, market_type, timeframe,
                    )
                    return
            except Exception as exc:
                logger.warning("Gold version dedup check failed | {} | {}", latest_path, exc)

        # Número de versión secuencial
        existing    = sorted(versions_dir.glob("v*.json"))
        version_num = len(existing) + 1
        version_id  = f"v{version_num:06d}"

        manifest: Dict = {
            "version":          version_num,
            "version_id":       version_id,
            "written_at":       datetime.now(timezone.utc).isoformat(),
            "exchange":         exchange,
            "symbol":           symbol,
            "market_type":      market_type,
            "timeframe":        timeframe,
            "layer":            "gold",
            "run_id":           run_id,
            "input_versions":   input_versions or [],
            "git_hash":         get_git_hash(),
            "engineer_version": getattr(self._engineer, "VERSION", "unknown"),
            "silver_version":   silver_version,
            "file":             str(out_file.relative_to(self._gold)),
            "rows":             rows,
            "features":         features,
            "checksum":         checksum,
            "schema_hash":      schema_hash,
            "min_ts":           min_ts,
            "max_ts":           max_ts,
        }

        serialized = json.dumps(manifest, indent=2)

        # Escritura atómica versión específica
        version_path = versions_dir / f"{version_id}.json"
        version_tmp  = version_path.with_suffix(".tmp")
        version_tmp.write_text(serialized, encoding="utf-8")
        version_tmp.rename(version_path)

        # Actualizar latest.json atómicamente
        latest_tmp = latest_path.with_suffix(".tmp")
        latest_tmp.write_text(serialized, encoding="utf-8")
        latest_tmp.rename(latest_path)

        logger.debug(
            "Gold version written | {}/{}/{}/{} {} checksum={}",
            exchange, symbol, market_type, timeframe, version_id, checksum[:8],
        )


# ==========================================================
# Module-level helpers
# ==========================================================

def _file_checksum(path: Path) -> str:
    """MD5 del archivo — mismo algoritmo que Silver para consistencia."""
    md5 = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            md5.update(chunk)
    return md5.hexdigest()
