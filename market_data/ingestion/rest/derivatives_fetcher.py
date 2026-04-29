# -*- coding: utf-8 -*-
"""
market_data/ingestion/rest/derivatives_fetcher.py
==================================================

Fetchers de derivados (funding_rate, open_interest) via REST CCXT.

Dominio
-------
  funding_rate  — tasa de financiación periódica (cada 8h en perpetuos)
  open_interest — contratos abiertos agregados (snapshot por llamada)

Liquidaciones excluidas — requieren endpoint distinto no disponible
en todos los exchanges via CCXT unified.

Cursor incremental
------------------
Mismo patrón de 3 niveles que TradesFetcher:
  1. Redis CursorStore  → clave (exchange, symbol, "<dataset>")
  2. DerivativesStorage.last_timestamp_ms() → fallback desde Iceberg
  3. None              → snapshot único sin filtro temporal

Para derivados la paginación no aplica — cada llamada retorna el
snapshot más reciente. El cursor evita re-escribir snapshots ya
almacenados.

SafeOps
-------
- SRP: un fetcher por dataset.
- Retry + backoff exponencial.
- Skip silencioso si el exchange no soporta el endpoint.
- dry_run=True loguea sin escribir ni actualizar cursor.

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

import pandas as pd
from loguru import logger

from market_data.adapters.exchange import (
    CCXTAdapter,
    ExchangeCircuitOpenError,
)
from market_data.storage.silver.derivatives_storage import DerivativesStorage
from market_data.ports.state import CursorStorePort as CursorStore      # DIP — tipo abstracto
from ocm_platform.infra.state.factories  import build_cursor_store as _build_cursor_store  # Fail-Soft fallback

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_RETRIES:         int   = 3
BACKOFF_BASE:        float = 1.5
MAX_BACKOFF_SECONDS: float = 20.0


# ---------------------------------------------------------------------------
# Parsers CCXT → DataFrame  (puros — sin efectos secundarios)
# ---------------------------------------------------------------------------

def _parse_funding_rate(raw: Dict[str, Any]) -> pd.DataFrame:
    """
    Convierte respuesta CCXT fetch_funding_rate a DataFrame normalizado.

    Produce columna semántica ``funding_rate`` lista para DerivativesStorage.
    Ref: https://docs.ccxt.com/#/?id=funding-rate-structure
    """
    ts_ms = raw.get("timestamp")
    rate  = raw.get("fundingRate")

    if ts_ms is None or rate is None:
        return pd.DataFrame()

    return pd.DataFrame([{
        "timestamp":    pd.Timestamp(int(ts_ms), unit="ms", tz="UTC"),
        "funding_rate": float(rate),
    }])


def _parse_open_interest(raw: Dict[str, Any]) -> pd.DataFrame:
    """
    Convierte respuesta CCXT fetch_open_interest a DataFrame normalizado.

    Produce columna semántica ``open_interest`` lista para DerivativesStorage.
    Ref: https://docs.ccxt.com/#/?id=open-interest-structure
    """
    ts_ms = raw.get("timestamp")
    oi    = raw.get("openInterest")

    if ts_ms is None or oi is None:
        return pd.DataFrame()

    return pd.DataFrame([{
        "timestamp":     pd.Timestamp(int(ts_ms), unit="ms", tz="UTC"),
        "open_interest": float(oi),
    }])


# ---------------------------------------------------------------------------
# Base interna — lógica de cursor y retry compartida (DRY)
# ---------------------------------------------------------------------------

class _BaseDerivativesFetcher:
    """
    Lógica de cursor incremental y retry compartida por todos los fetchers.

    No es parte de la API pública — subclases implementan
    ``_fetch_raw(symbol)`` y ``_parse(raw)``.
    """

    _dataset: str   # definido por subclase

    def __init__(
        self,
        exchange_client: CCXTAdapter,
        storage:         DerivativesStorage,
        market_type:     str  = "swap",
        dry_run:         bool = False,
    ) -> None:
        if exchange_client is None:
            raise ValueError(f"{type(self).__name__}: exchange_client es obligatorio")
        if storage is None:
            raise ValueError(f"{type(self).__name__}: storage es obligatorio")

        self._client      = exchange_client
        self._storage     = storage
        self._market_type = market_type
        self._dry_run     = dry_run
        self._exchange_id = getattr(exchange_client, "_exchange_id", "unknown")
        self._log         = logger.bind(
            exchange=self._exchange_id,
            dataset=self._dataset,
        )

        # Cursor store — degradación controlada si Redis no disponible
        try:
            self._cursor = _build_cursor_store()
        except Exception as exc:
            self._log.warning(
                "CursorStore no disponible — usando storage fallback | error={}", exc
            )
            self._cursor = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_symbol(self, symbol: str) -> int:
        """
        Fetcha snapshot del dataset para ``symbol``.

        Usa cursor para evitar re-escribir snapshots ya almacenados.
        Retorna filas escritas (0 si ya actualizado o endpoint no soportado).
        """
        raw = await self._fetch_raw_with_retry(symbol)
        if raw is None:
            return 0

        df = self._parse(raw)
        if df.empty:
            return 0

        # Dedup por cursor: si el timestamp ya está almacenado, skip
        snapshot_ts_ms = int(df["timestamp"].iloc[0].timestamp() * 1000)
        last_ts_ms     = await self._resolve_cursor(symbol)

        if last_ts_ms is not None and snapshot_ts_ms <= last_ts_ms:
            self._log.debug(
                "Snapshot ya almacenado — skip | symbol={} ts_ms={}",
                symbol, snapshot_ts_ms,
            )
            return 0

        # Añadir symbol antes de escribir (DerivativesStorage._normalize
        # añade exchange y market_type, pero symbol viene del fetcher)
        df["symbol"] = symbol

        rows = self._storage.upsert(df)
        await self._update_cursor(symbol, snapshot_ts_ms)
        return rows

    # ------------------------------------------------------------------
    # Abstract-like — subclases implementan estos dos métodos
    # ------------------------------------------------------------------

    async def _fetch_raw(self, symbol: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def _parse(self, raw: Dict[str, Any]) -> pd.DataFrame:
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _fetch_raw_with_retry(
        self, symbol: str
    ) -> Optional[Dict[str, Any]]:
        """Llama a ``_fetch_raw`` con retry + backoff. None si no soportado."""
        last_exc: Exception | None = None

        for attempt in range(MAX_RETRIES):
            try:
                return await self._fetch_raw(symbol)
            except ExchangeCircuitOpenError:
                raise
            except Exception as exc:
                err_str = str(exc).lower()
                if "notsupported" in err_str or "not supported" in err_str:
                    self._log.debug(
                        "{} not supported | symbol={}", self._dataset, symbol
                    )
                    return None
                last_exc = exc
                if attempt < MAX_RETRIES - 1:
                    backoff = min(BACKOFF_BASE ** attempt, MAX_BACKOFF_SECONDS)
                    self._log.warning(
                        "fetch_{} error (intento {}/{}) | "
                        "symbol={} error={} retry_in={:.1f}s",
                        self._dataset, attempt + 1, MAX_RETRIES,
                        symbol, exc, backoff,
                    )
                    await asyncio.sleep(backoff)

        self._log.warning(
            "fetch_{} agotó reintentos | symbol={} last_error={}",
            self._dataset, symbol, last_exc,
        )
        return None

    async def _resolve_cursor(self, symbol: str) -> Optional[int]:
        """Nivel 1: Redis. Nivel 2: storage. Nivel 3: None."""
        if self._cursor is not None:
            try:
                ts = await self._cursor.get(
                    self._exchange_id, symbol, self._dataset
                )
                if ts is not None:
                    return ts
            except Exception as exc:
                self._log.warning(
                    "CursorStore.get falló — fallback | symbol={} error={}",
                    symbol, exc,
                )
        return self._storage.last_timestamp_ms(symbol)

    async def _update_cursor(self, symbol: str, ts_ms: int) -> None:
        """Actualiza cursor en Redis. Silencia errores — no crítico."""
        if self._dry_run or self._cursor is None:
            return
        try:
            await self._cursor.update(
                self._exchange_id, symbol, self._dataset, ts_ms
            )
        except Exception as exc:
            self._log.warning(
                "CursorStore.update falló (no crítico) | "
                "symbol={} ts_ms={} error={}",
                symbol, ts_ms, exc,
            )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"exchange={self._exchange_id!r}, "
            f"market_type={self._market_type!r}, "
            f"dry_run={self._dry_run})"
        )


# ---------------------------------------------------------------------------
# Fetchers concretos — SRP: uno por dataset
# ---------------------------------------------------------------------------

class FundingRateFetcher(_BaseDerivativesFetcher):
    """Fetcher de funding_rate para mercados perpetuos."""

    _dataset = "funding_rate"

    async def _fetch_raw(self, symbol: str) -> Optional[Dict[str, Any]]:
        client = await self._client._get_client()
        return await client.fetch_funding_rate(symbol)

    def _parse(self, raw: Dict[str, Any]) -> pd.DataFrame:
        return _parse_funding_rate(raw)


class OpenInterestFetcher(_BaseDerivativesFetcher):
    """Fetcher de open_interest (contratos abiertos)."""

    _dataset = "open_interest"

    async def _fetch_raw(self, symbol: str) -> Optional[Dict[str, Any]]:
        client = await self._client._get_client()
        return await client.fetch_open_interest(symbol)

    def _parse(self, raw: Dict[str, Any]) -> pd.DataFrame:
        return _parse_open_interest(raw)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

__all__ = ["FundingRateFetcher", "OpenInterestFetcher"]
