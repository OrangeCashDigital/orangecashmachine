"""
silver_storage.py
=================

Capa Silver del Data Lakehouse.

Responsabilidad
---------------
Almacenar datos OHLCV limpios, deduplicados y validados.
Mantener un registro de versiones por dataset para reproducibilidad.

Diferencias con Bronze
----------------------
• Hace merge + dedup (last-write-wins)
• Normaliza schema y tipos
• Genera versiones trazables en _versions/
• Es determinista: misma entrada → misma salida

Estructura de partición
-----------------------
silver/ohlcv/
  exchange={exchange}/
    symbol={symbol}/
      timeframe={timeframe}/
        {year}/{month}[/{day}]/
          {symbol}_{tf}.parquet
          {symbol}_{tf}.meta.json
        _versions/
          v000001.json
          v000002.json
          latest.json

Versionado
----------
Cada escritura (save_ohlcv) genera un nuevo archivo en _versions/
que lista todas las particiones tocadas, con checksums y rangos.
latest.json apunta siempre a la versión más reciente.
"""

from __future__ import annotations
import os

import asyncio
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from core.config.lineage import get_git_hash
from core.config.paths import silver_ohlcv_root
from data_platform.ohlcv_utils import safe_symbol as _safe_symbol_fn
from data_platform.ohlcv_utils import safe_symbol, normalize_ohlcv_df
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Literal
from market_data.processing.utils.timeframe import Timeframe

import pandas as pd
from loguru import logger

# Worker pool para I/O de particiones — compartido por instancia
_PARTITION_EXECUTOR = ThreadPoolExecutor(
    max_workers=min(32, (os.cpu_count() or 4) * 2),
    thread_name_prefix="silver-io",
)


# ==========================================================
# Constants
# ==========================================================

REQUIRED_COLUMNS: tuple[str, ...] = (
    "timestamp", "open", "high", "low", "close", "volume"
)


WriteMode = Literal["append", "overwrite"]

# Write lock Redis: TTL generoso para evitar deadlocks si el proceso crashea.
# Si el lock no se libera en _LOCK_TTL_SECONDS, Redis lo expira automáticamente.
_LOCK_TTL_SECONDS:     int = 60
_LOCK_RETRY_INTERVAL:  float = 0.1   # segundos entre reintentos
_LOCK_MAX_WAIT:        float = 30.0  # máximo de espera antes de abortar

# Script Lua para release atómico del write lock.
# Definido una sola vez a nivel de módulo — Redis lo cachea por SHA1.
# GET + DEL en un roundtrip, sin race condition entre verificar y borrar.
_LOCK_RELEASE_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


# ==========================================================
# Manifest Cache
# ==========================================================

class _ManifestCache:
    """
    Cache en memoria para manifests latest.json con TTL configurable.

    Thread-safe via threading.Lock — safe en pipelines concurrentes con
    múltiples threads accediendo al mismo SilverStorage.

    Sin dependencia externa: no requiere Redis ni ningún servicio adicional.
    Diseñado para ser instanciado una vez por SilverStorage y vivir
    el tiempo de vida de la instancia.

    Política de invalidación
    ------------------------
    - TTL pasivo: entradas expiradas se descartan en el próximo get().
    - Invalidación activa: _write_version() llama a invalidate() tras
      cada escritura, garantizando que la siguiente lectura ve el estado nuevo.
    """

    def __init__(self, ttl_seconds: float = 60.0) -> None:
        self._ttl   = ttl_seconds
        self._store: Dict[str, tuple] = {}  # key → (manifest, expires_at)
        self._lock  = threading.Lock()

    def get(self, key: str) -> Optional[Dict]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            manifest, expires_at = entry
            if time.monotonic() > expires_at:
                del self._store[key]
                return None
            return manifest

    def set(self, key: str, manifest: Dict) -> None:
        with self._lock:
            self._store[key] = (manifest, time.monotonic() + self._ttl)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


# ==========================================================
# Exceptions
# ==========================================================

class SilverStorageError(Exception):
    """Base error."""

class InvalidDataFrameError(SilverStorageError):
    """Invalid DataFrame."""

class PartitionWriteError(SilverStorageError):
    """Partition write failure."""


# ==========================================================
# SilverStorage
# ==========================================================

class SilverStorage:
    """
    Storage OHLCV limpio con versionado para la capa Silver.

    Garantías
    ---------
    • Idempotente: reinsertar mismos datos no genera duplicados
    • Determinista: misma entrada → misma salida
    • Versionado: cada escritura genera una versión trazable
    • Reproducible: el loader puede pedir versión X o as_of=T

    Uso
    ---
    silver = SilverStorage(exchange="kucoin")
    silver.save_ohlcv(df, symbol="BTC/USDT", timeframe="1m", run_id=run_id)
    """

    def __init__(
        self,
        base_path:    Optional[str | Path] = None,
        exchange:     Optional[str]        = None,
        market_type:  str                  = "spot",
        redis_client  = None,
        dry_run:      bool                 = False,
    ) -> None:
        self._base_path:   Path = _resolve_base_path(base_path)
        self._exchange:    Optional[str] = exchange.lower() if exchange else None
        self._market_type: str = market_type.lower()
        self._redis   = redis_client  # opcional — sin Redis, no hay lock
        self._dry_run = dry_run
        self._manifest_cache = _ManifestCache(ttl_seconds=60.0)
        # Registrar script Lua una sola vez por instancia.
        # Redis lo cachea por SHA1 — register_script solo hace overhead en el primer call.
        self._lock_release = (
            self._redis.register_script(_LOCK_RELEASE_LUA)
            if self._redis is not None else None
        )
        self._base_path.mkdir(parents=True, exist_ok=True)
        logger.info(
            "SilverStorage ready | exchange={} market_type={} path={} redis={} dry_run={}",
            self._exchange or "shared", self._market_type, self._base_path,
            "enabled" if self._redis else "disabled",
            self._dry_run,
        )

    @contextmanager
    def _write_lock(self, symbol: str, timeframe: str):
        """
        Advisory write lock via Redis SET NX EX.

        Protege contra dos Prefect workers escribiendo el mismo dataset
        en paralelo. Si Redis no está disponible, procede sin lock (degraded).

        El lock se identifica por exchange/symbol/timeframe para granularidad
        máxima — locks distintos no se bloquean entre sí.
        """
        if self._redis is None:
            yield  # sin Redis → sin lock, proceder
            return

        from market_data.observability.metrics import (
            WRITE_LOCK_WAIT_DURATION, WRITE_LOCK_CONFLICTS, WRITE_LOCK_STARVATION,
        )

        lock_key = (
            f"ocm:write_lock:{self._exchange or 'shared'}"
            f":{symbol.replace('/', '_')}:{timeframe}"
        )
        lock_val = f"{os.getpid()}-{id(self)}"
        waited   = 0.0
        acquired = False

        while waited < _LOCK_MAX_WAIT:
            result = self._redis.set(
                lock_key, lock_val,
                nx=True, ex=_LOCK_TTL_SECONDS,
            )
            if result:
                acquired = True
                break
            if waited == 0.0:
                WRITE_LOCK_CONFLICTS.labels(
                    exchange=self._exchange or "shared",
                    symbol=symbol, timeframe=timeframe,
                ).inc()
                logger.debug(
                    "Write lock contention | exchange={} symbol={} timeframe={} — waiting",
                    self._exchange, symbol, timeframe,
                )

            time.sleep(_LOCK_RETRY_INTERVAL)
            waited += _LOCK_RETRY_INTERVAL

        WRITE_LOCK_WAIT_DURATION.labels(
            exchange=self._exchange or "shared",
            symbol=symbol, timeframe=timeframe,
        ).observe(waited)

        if not acquired:
            WRITE_LOCK_STARVATION.labels(
                exchange=self._exchange or "shared",
                symbol=symbol, timeframe=timeframe,
            ).inc()
            logger.error(
                "Write lock timeout | exchange={} symbol={} timeframe={} waited={}s — proceeding without lock",
                self._exchange, symbol, timeframe, waited,
            )

        try:
            yield
        finally:
            if acquired:
                try:
                    # Release atómico via Lua — script pre-registrado en __init__.
                    # Si el lock ya expiró o fue tomado por otro proceso, no-op (retorna 0).
                    self._lock_release(keys=[lock_key], args=[lock_val])
                except Exception as exc:
                    logger.warning("Write lock release failed | {} | {}", lock_key, exc)

    # ======================================================
    # Public API
    # ======================================================

    def save_ohlcv(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        mode: WriteMode = "append",
        run_id: Optional[str] = None,
    ) -> None:
        """
        Guarda OHLCV limpio y genera una nueva versión del dataset.

        Flujo
        -----
        1. Validar y normalizar DataFrame
        2. Particionar por año/mes (o día para 1m)
        3. Para cada partición: merge + dedup + escritura atómica
        4. Generar versión en _versions/

        Parameters
        ----------
        df : pd.DataFrame
            Datos OHLCV ya transformados.
        symbol : str
            Par de trading.
        timeframe : str
            Intervalo temporal.
        mode : "append" | "overwrite"
            append = merge con existente (recomendado).
            overwrite = reemplaza sin merge.
        run_id : str, optional
            ID del run de ingestión para correlación con bronze.
        """
        _t0 = time.monotonic()
        _validate_dataframe(df)
        df = _normalize_dataframe(df)

        if self._dry_run:
            logger.info(
                "[DRY RUN] Silver save_ohlcv skipped | {}/{} exchange={} rows={} run_id={}",
                symbol, timeframe, self._exchange or "shared", len(df), run_id,
            )
            return

        # Write lock: previene race conditions entre Prefect workers
        # que procesen el mismo symbol/timeframe en paralelo.
        with self._write_lock(symbol, timeframe):
            return self._save_ohlcv_locked(df, symbol, timeframe, mode, run_id, _t0)

    def _save_ohlcv_locked(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        mode: WriteMode,
        run_id: Optional[str],
        _t0: float,
    ) -> None:
        """Implementación de save_ohlcv bajo write lock."""
        use_daily = (timeframe == Timeframe.M1)

        if use_daily:
            groups = [
                (int(y), int(m), int(d), part)
                for (y, m, d), part in df.groupby(
                    [df["timestamp"].dt.year, df["timestamp"].dt.month, df["timestamp"].dt.day],
                    sort=True,
                )
            ]
            tasks = [
                (part, symbol, timeframe, y, m, mode, d, run_id)
                for y, m, d, part in groups
            ]
        else:
            groups = [
                (int(y), int(m), part)
                for (y, m), part in df.groupby(
                    [df["timestamp"].dt.year, df["timestamp"].dt.month],
                    sort=True,
                )
            ]
            tasks = [
                (part, symbol, timeframe, y, m, mode, None, run_id)
                for y, m, part in groups
            ]

        # Escritura paralela de particiones independientes
        futures = [
            _PARTITION_EXECUTOR.submit(self._write_partition, *args)
            for args in tasks
        ]
        partitions_written: List[Dict] = []
        errors = []
        for f in futures:
            try:
                partitions_written.append(f.result())
            except Exception as exc:
                errors.append(exc)

        if errors:
            raise PartitionWriteError(
                f"Silver write failed for {len(errors)}/{len(futures)} partitions: {errors[0]}"
            )

        # Log de resumen (no por partición individual — reduce ruido en 1m)
        _duration_ms = int((time.monotonic() - _t0) * 1000)
        total_rows = sum(p.get("rows", 0) for p in partitions_written)
        logger.debug(
            "Silver written | {}/{} exchange={} partitions={} rows={} duration={}ms",
            symbol, timeframe, self._exchange or "shared",
            len(partitions_written), total_rows, _duration_ms,
        )

        # Generar versión del dataset
        if partitions_written:
            self._write_version(symbol, timeframe, partitions_written, run_id)

    def get_last_timestamp(
        self,
        symbol: str,
        timeframe: str,
    ) -> Optional[pd.Timestamp]:
        """
        Obtiene el último timestamp disponible.

        Lee max_ts del manifest latest.json en O(1) — sin abrir ningún parquet.
        Fallback a lectura de parquet si el manifest no existe o no tiene max_ts.
        """
        versions_dir = self._dataset_root(symbol, timeframe) / "_versions"
        latest_path  = versions_dir / "latest.json"

        if latest_path.exists():
            try:
                manifest   = json.loads(latest_path.read_text(encoding="utf-8"))
                partitions = manifest.get("partitions", [])
                max_ts_strs = [p["max_ts"] for p in partitions if "max_ts" in p]
                if max_ts_strs:
                    return max(pd.Timestamp(ts, tz="UTC") for ts in max_ts_strs)
            except Exception as exc:
                logger.warning(
                    "latest.json max_ts read failed, fallback a parquet | "
                    "symbol={} timeframe={} error={}",
                    symbol, timeframe, exc,
                )

        # Fallback: leer parquet (dataset sin versión o manifest corrupto)
        files = self._find_partition_files(symbol, timeframe)
        if not files:
            return None
        timestamps: List[pd.Timestamp] = []
        for f in files:
            ts = _read_max_timestamp(f)
            if ts is not None:
                timestamps.append(ts)
        return max(timestamps) if timestamps else None

    def get_version(self, symbol: str, timeframe: str, version: str = "latest") -> Optional[Dict]:
        """
        Obtiene el manifest de una versión específica del dataset.

        Parameters
        ----------
        version : str
            "latest" para la última versión, o "v000001" para una específica.

        Returns
        -------
        dict con la info de la versión, o None si no existe.
        """
        versions_dir = self._versions_dir(symbol, timeframe)
        if version == "latest":
            path = versions_dir / "latest.json"
        else:
            path = versions_dir / f"{version}.json"

        if not path.exists():
            return None

        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Version read failed | {} | {}", path, exc)
            return None


    def load_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Lee datos OHLCV desde las particiones Parquet de Silver.

        Usa find_partition_files() con pruning temporal — no abre parquets
        fuera del rango solicitado.

        Parameters
        ----------
        start / end : filtro temporal opcional (inclusivo en ambos extremos).

        Returns
        -------
        DataFrame OHLCV ordenado por timestamp, o None si no hay datos.
        """
        files = self.find_partition_files(
            symbol=symbol,
            timeframe=timeframe,
            since=start,
            until=end,
        )
        if not files:
            logger.debug(
                "load_ohlcv: sin particiones | {}/{} exchange={}",
                symbol, timeframe, self._exchange or "shared",
            )
            return None

        parts = []
        for f in files:
            try:
                parts.append(pd.read_parquet(f))
            except Exception as exc:
                logger.warning(
                    "load_ohlcv: parquet read failed | {} | {}", f, exc
                )

        if not parts:
            return None

        df = pd.concat(parts, ignore_index=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = (
            df.sort_values("timestamp")
            .drop_duplicates(subset="timestamp", keep="last")
            .reset_index(drop=True)
        )

        if start is not None:
            df = df[df["timestamp"] >= start]
        if end is not None:
            df = df[df["timestamp"] <= end]

        return df if not df.empty else None

    # ======================================================
    # Path helpers
    # ======================================================

    @staticmethod
    def _safe_symbol(symbol: str) -> str:
        return safe_symbol(symbol)

    def _dataset_root(self, symbol: str, timeframe: str) -> Path:
        """
        Raíz del dataset para este symbol/timeframe.
        market_type separa spot de futuros físicamente en el lake:
          exchange=bybit/symbol=BTC_USDT/market_type=spot/timeframe=1h/
          exchange=bybit/symbol=BTC_USDT:USDT/market_type=swap/timeframe=1h/
        """
        safe_sym = self._safe_symbol(symbol)
        if self._exchange:
            return (
                self._base_path
                / f"exchange={self._exchange}"
                / f"symbol={safe_sym}"
                / f"market_type={self._market_type}"
                / f"timeframe={timeframe}"
            )
        return (
            self._base_path
            / f"symbol={safe_sym}"
            / f"market_type={self._market_type}"
            / f"timeframe={timeframe}"
        )

    def _versions_dir(self, symbol: str, timeframe: str) -> Path:
        path = self._dataset_root(symbol, timeframe) / "_versions"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _partition_dir(
        self,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
        day: Optional[int] = None,
    ) -> Path:
        path = self._dataset_root(symbol, timeframe) / str(year) / f"{month:02d}"
        if day is not None:
            path = path / f"{day:02d}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _partition_file(
        self,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
        day: Optional[int] = None,
    ) -> Path:
        safe = self._safe_symbol(symbol)
        return self._partition_dir(symbol, timeframe, year, month, day) / f"{safe}_{timeframe}.parquet"

    def find_partition_files(
        self,
        symbol:    str,
        timeframe: str,
        since:     Optional[pd.Timestamp] = None,
        until:     Optional[pd.Timestamp] = None,
    ) -> List[Path]:
        """
        Lista archivos de partición usando latest.json como índice primario.

        Evita rglob O(N archivos) — lee el manifest en O(1) y resuelve paths.
        Fallback a rglob si no existe versión (dataset nuevo o sin versión aún).

        Parameters
        ----------
        since : pd.Timestamp, optional
            Excluye particiones cuyo max_ts < since (sin datos relevantes).
        until : pd.Timestamp, optional
            Excluye particiones cuyo min_ts > until (sin datos relevantes).
        """
        # Cache-first: evita I/O repetido en pipelines concurrentes.
        # TTL=60s — balance entre frescura y rendimiento.
        # Invalidación activa en _write_version() garantiza consistencia post-escritura.
        _cache_key = f"{symbol}:{timeframe}"
        manifest   = self._manifest_cache.get(_cache_key)

        if manifest is None:
            versions_dir = self._dataset_root(symbol, timeframe) / "_versions"
            latest_path  = versions_dir / "latest.json"
            if latest_path.exists():
                try:
                    manifest = json.loads(latest_path.read_text(encoding="utf-8"))
                    self._manifest_cache.set(_cache_key, manifest)
                except Exception as exc:
                    logger.warning(
                        "latest.json read failed, fallback a rglob | "
                        "symbol={} timeframe={} error={}",
                        symbol, timeframe, exc,
                    )

        if manifest is not None:
            try:
                partitions = manifest.get("partitions", [])
                files = []
                for p in partitions:
                    full_path = self._base_path / p["path"]
                    if not full_path.exists():
                        import os as _os_env
                        if _os_env.getenv("OCM_ENV", "development") == "production":
                            raise PartitionWriteError(
                                f"[production] Partition en manifest no existe en disco: {full_path}"
                            )
                        logger.warning(
                            "Partition en manifest no existe en disco | path={}",
                            full_path,
                        )
                        continue
                    # Pruning temporal: excluir particiones fuera del rango solicitado.
                    if since or until:
                        part_min = p.get("min_ts")
                        part_max = p.get("max_ts")
                        if part_min and part_max:
                            try:
                                p_min = pd.Timestamp(part_min, tz="UTC")
                                p_max = pd.Timestamp(part_max, tz="UTC")
                                if since and p_max < since:
                                    continue  # partición anterior al rango
                                if until and p_min > until:
                                    continue  # partición posterior al rango
                            except Exception:
                                pass  # si falla el parse, incluir partición (safe)

                    # Validación O(1): file_size del manifest vs disco.
                    # Evita pd.read_parquet en hot path — coste O(N filas) → O(1).
                    # Para validación profunda de contenido usar verify_integrity() explícito.
                    expected_size = p.get("file_size")
                    if expected_size is not None:
                        actual_size = full_path.stat().st_size
                        if actual_size != expected_size:
                            logger.error(
                                "File size mismatch — partición posiblemente corrupta | "
                                "path={} expected_bytes={} actual_bytes={}",
                                full_path, expected_size, actual_size,
                            )
                            continue  # excluir del resultado
                    files.append(full_path)
                if files:
                    return sorted(files)
                # manifest vacío o todos los paths inválidos → fallback
            except Exception as exc:
                logger.warning(
                    "latest.json read failed, fallback a rglob | symbol={} timeframe={} error={}",
                    symbol, timeframe, exc,
                )
                self._manifest_cache.invalidate(_cache_key)

        # Fallback: rglob (dataset nuevo o manifest corrupto)
        return self._find_partition_files(symbol, timeframe)

    def _find_partition_files(self, symbol: str, timeframe: str) -> List[Path]:
        root = self._dataset_root(symbol, timeframe)
        if not root.exists():
            return []
        pattern = f"{self._safe_symbol(symbol)}_{timeframe}.parquet"
        return sorted(root.rglob(pattern))

    # ======================================================
    # Partition writer
    # ======================================================

    def _write_partition(
        self,
        df: pd.DataFrame,
        symbol: str,
        timeframe: str,
        year: int,
        month: int,
        mode: WriteMode,
        day: Optional[int] = None,
        run_id: Optional[str] = None,
    ) -> Dict:
        """
        Escritura atómica de una partición. Devuelve metadata de la partición.

        Secuencia:
        1. Merge con existente si mode=append
        2. Dedup (last-write-wins)
        3. Escribir a .tmp
        4. Atomic rename .tmp → .parquet
        5. Escribir .meta.json sidecar
        """
        file_path = self._partition_file(symbol, timeframe, year, month, day)
        temp_path = file_path.with_suffix(".tmp")
        meta_path = file_path.with_suffix(".meta.json")

        try:
            if file_path.exists() and mode == "append":
                df = _merge_full(df, file_path)

            df = _clean_partition(df)

            df.to_parquet(temp_path, compression="snappy", index=False)
            temp_path.replace(file_path)

            partition_meta = _write_partition_meta(meta_path, df, symbol, timeframe, run_id=run_id)

            label = f"{year}/{month:02d}/{day:02d}" if day is not None else f"{year}/{month:02d}"
            logger.debug(
                "Partition saved | {} {} {} rows={} [{} → {}]",
                symbol, timeframe, label, len(df),
                df["timestamp"].min().isoformat(),
                df["timestamp"].max().isoformat(),
            )

            # Añadir path relativo para el manifest de versión
            partition_meta["path"] = str(file_path.relative_to(self._base_path))
            return partition_meta

        except Exception as exc:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            raise PartitionWriteError(
                f"Failed writing partition {symbol}/{timeframe}/{year}/{month}"
            ) from exc

    # ======================================================
    # Versioning
    # ======================================================

    def _write_version(
        self,
        symbol: str,
        timeframe: str,
        partitions: List[Dict],
        run_id: Optional[str],
    ) -> None:
        """
        Genera un nuevo archivo de versión en _versions/.

        Formato
        -------
        {
          "version": 42,
          "version_id": "v000042",
          "run_id": "20260318T043000-abc12345",
          "written_at": "2026-03-18T04:30:00Z",
          "symbol": "BTC/USDT",
          "timeframe": "1m",
          "exchange": "kucoin",
          "partitions": [
            {
              "path": "...",
              "rows": 1440,
              "min_ts": "...",
              "max_ts": "...",
              "checksum": "..."
            }
          ]
        }
        """
        versions_dir = self._versions_dir(symbol, timeframe)

        # Dedup: si los checksums de todas las particiones no cambiaron, no crear version nueva
        latest_path = versions_dir / "latest.json"
        if latest_path.exists():
            try:
                latest = json.loads(latest_path.read_text(encoding="utf-8"))
                latest_checksums = {p["path"]: p.get("checksum") for p in latest.get("partitions", [])}
                new_checksums    = {p["path"]: p.get("checksum") for p in partitions}
                if latest_checksums == new_checksums:
                    logger.debug(
                        "Version skip (no changes) | {}/{} exchange={}",
                        symbol, timeframe, self._exchange or "shared",
                    )
                    return
            except Exception as exc:
                logger.warning("Version dedup check failed | {} | {}", latest_path, exc)

        # Calcular número de versión secuencial
        existing = sorted(versions_dir.glob("v*.json"))
        version_num = len(existing) + 1
        version_id = f"v{version_num:06d}"

        # Consolidar particiones previas con las nuevas.
        # latest.json es un manifest delta — cada versión solo contiene las
        # particiones escritas en esa operación. Para que el manifest refleje
        # el estado completo del dataset, fusionamos previas + nuevas usando
        # path como clave de deduplicación (las nuevas ganan en colisión).
        merged: dict = {}
        if latest_path.exists():
            try:
                prev = json.loads(latest_path.read_text(encoding="utf-8"))
                for p in prev.get("partitions", []):
                    if "path" in p:
                        merged[p["path"]] = p
            except Exception:
                pass  # manifest corrupto — empezar desde cero
        for p in partitions:
            if "path" in p:
                merged[p["path"]] = p
        all_partitions = sorted(merged.values(), key=lambda p: p.get("path", ""))

        version_data: Dict = {
            "version":     version_num,
            "version_id":  version_id,
            "run_id":      run_id or "unknown",
            "written_at":  datetime.now(timezone.utc).isoformat(),
            "symbol":      symbol,
            "timeframe":   timeframe,
            "exchange":    self._exchange or "shared",
            "market_type": self._market_type,
            "layer":       "silver",
            "git_hash":    get_git_hash(),
            "partitions":  all_partitions,
        }

        # Escritura atómica del manifest:
        # 1. Escribir versión específica vía tmp + rename
        # 2. Actualizar latest.json vía tmp + rename
        # Si crashea entre pasos, latest.json sigue apuntando a versión anterior (safe).
        serialized = json.dumps(version_data, indent=2)

        version_path = versions_dir / f"{version_id}.json"
        version_tmp  = version_path.with_suffix(".tmp")
        version_tmp.write_text(serialized, encoding="utf-8")
        version_tmp.replace(version_path)

        latest_path = versions_dir / "latest.json"
        latest_tmp  = latest_path.with_suffix(".tmp")
        latest_tmp.write_text(serialized, encoding="utf-8")
        latest_tmp.replace(latest_path)

        # Invalidar cache — el manifest acaba de cambiar en disco
        self._manifest_cache.invalidate(f"{symbol}:{timeframe}")

        logger.info(
            "Dataset version created | {}/{} {} exchange={} partitions={}",
            symbol, timeframe, version_id, self._exchange or "shared", len(partitions),
        )


# ==========================================================
# Helpers (puros)
# ==========================================================

def _resolve_base_path(base_path: Optional[str | Path]) -> Path:
    """
    Resuelve el path base de Silver.

    Orden de resolución:
    1. base_path explícito (argumento del constructor)
    2. core.config.paths.silver_ohlcv_root() — lee storage.data_lake.path del YAML
       o OCM_DATA_LAKE_PATH si está seteada
    """
    if base_path:
        return Path(base_path).resolve()
    return silver_ohlcv_root()


def _validate_dataframe(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        raise InvalidDataFrameError("DataFrame vacío")
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise InvalidDataFrameError(f"Missing columns: {sorted(missing)}")


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.dropna(subset=["timestamp"])
    return df


def _merge_full(new_df: pd.DataFrame, file_path: Path) -> pd.DataFrame:
    existing = pd.read_parquet(file_path)
    existing["timestamp"] = pd.to_datetime(existing["timestamp"], utc=True)
    combined = pd.concat([existing, new_df], ignore_index=True)
    return (
        combined
        .pipe(normalize_ohlcv_df)
    )


def _clean_partition(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.sort_values("timestamp")
        .drop_duplicates(subset="timestamp", keep="last")
        .reset_index(drop=True)
    )


def _read_max_timestamp(file: Path) -> Optional[pd.Timestamp]:
    """
    Deprecated: use SilverStorage.get_last_timestamp() que lee max_ts del
    manifest en O(1) en lugar de abrir el parquet.
    Mantenida por compatibilidad — se eliminará en la próxima limpieza.
    """
    logger.warning(
        "_read_max_timestamp() está deprecada — usa get_last_timestamp() | file={}", file
    )
    try:
        df = pd.read_parquet(file, columns=["timestamp"])
        return pd.to_datetime(df["timestamp"].max(), utc=True)
    except Exception as exc:
        logger.warning("Timestamp read failed | {} | {}", file, exc)
        return None


def _write_partition_meta(
    meta_path: Path,
    df: pd.DataFrame,
    symbol: str,
    timeframe: str,
    run_id: Optional[str] = None,
) -> Dict:
    """Escribe sidecar .meta.json y devuelve el dict para el manifest de versión."""
    try:
        ts_col = df["timestamp"]
        # Checksum sobre todo el DataFrame (timestamp + OHLCV values)
        # pd.util.hash_pandas_object es deterministico y cubre todos los valores
        checksum = hashlib.md5(
            pd.util.hash_pandas_object(df[["timestamp","open","high","low","close","volume"]], index=False).values.tobytes()
        ).hexdigest()

        file_path = meta_path.with_suffix(".parquet")
        # file_size calculado post-rename atómico — nunca None en producción.
        # Si el archivo no existe (test/dry-run) se guarda None explícitamente.
        file_size = file_path.stat().st_size if file_path.exists() else None

        meta: Dict = {
            "symbol":     symbol,
            "timeframe":  timeframe,
            "rows":       len(df),
            "min_ts":     ts_col.min().isoformat(),
            "max_ts":     ts_col.max().isoformat(),
            "checksum":   checksum,
            "file_size":  file_size,
            "written_at": datetime.now(timezone.utc).isoformat(),
            "layer":      "silver",
            "run_id":     run_id,
        }

        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        return meta

    except Exception as exc:
        logger.warning("Partition meta write failed (non-critical) | {} | {}", meta_path, exc)
        return {"rows": len(df), "error": str(exc)}
