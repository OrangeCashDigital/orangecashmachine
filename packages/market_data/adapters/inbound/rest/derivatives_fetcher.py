# -*- coding: utf-8 -*-
"""
market_data/adapters/inbound/rest/derivatives_fetcher.py
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

from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    pass

import pandas as pd
from loguru import logger

from market_data.adapters.inbound.rest._cursor_factory import (
    build_cursor_store as _build_cursor_store,
)
from market_data.adapters.outbound.exchange import (
    CCXTAdapter,
    ExchangeCircuitOpenError,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_RETRIES: int = 3
BACKOFF_BASE: float = 1.5
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
    rate = raw.get("fundingRate")

    if ts_ms is None or rate is None:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp(int(ts_ms), unit="ms", tz="UTC"),
                "funding_rate": float(rate),
            }
        ]
    )


def _parse_open_interest(raw: Dict[str, Any]) -> pd.DataFrame:
    """
    Convierte respuesta CCXT fetch_open_interest a DataFrame normalizado.

    Produce columna semántica ``open_interest`` lista para DerivativesStorage.
    Ref: https://docs.ccxt.com/#/?id=open-interest-structure
    """
    ts_ms = raw.get("timestamp")
    oi = raw.get("openInterest")

    if ts_ms is None or oi is None:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp(int(ts_ms), unit="ms", tz="UTC"),
                "open_interest": float(oi),
            }
        ]
    )


# ---------------------------------------------------------------------------
# Base interna — lógica de cursor y retry compartida (DRY)
# ---------------------------------------------------------------------------


class _BaseDerivativesFetcher:
    """
    Lógica de cursor incremental y retry compartida por todos los fetchers.

    No es parte de la API pública — subclases implementan
    ``_fetch_raw(symbol)`` y ``_parse(raw)``.
    """

    _dataset: str  # definido por subclase

    def __init__(
        self,
        exchange_client: CCXTAdapter,
        storage: object,
        market_type: str = "swap",
        dry_run: bool = False,
    ) -> None:
        if exchange_client is None:
            raise ValueError(f"{type(self).__name__}: exchange_client es obligatorio")
        if storage is None:
            raise ValueError(f"{type(self).__name__}: storage es obligatorio")

        self._client = exchange_client
        self._storage = storage
        self._market_type = market_type
        self._dry_run = dry_run
        self._exchange_id = getattr(exchange_client, "_exchange_id", "unknown")
        self._log = logger.bind(
            exchange=self._exchange_id,
            dataset=self._dataset,
        )

        # Cursor store — degradación controlada si Redis no disponible
        try:
            self._cursor = _build_cursor_store()
        except Exception as exc:
            self._log.warning("CursorStore no disponible — usando storage fallback | error={}", exc)
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
        last_ts_ms = await self._resolve_cursor(symbol)

        if last_ts_ms is not None and snapshot_ts_ms <= last_ts_ms:
            self._log.debug(
                "Snapshot ya almacenado — skip | symbol={} ts_ms={}",
                symbol,
                snapshot_ts_ms,
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

    async def _fetch_raw_with_retry(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Llama a _fetch_raw delegando retry a retry_async (DRY).

        Retorna None si el endpoint no está soportado (Fail-Soft).
        Relanza ExchangeCircuitOpenError sin reintentar (Fail-Fast).
        """
        from market_data.adapters.inbound.rest._retry import retry_async

        return await retry_async(
            lambda: self._fetch_raw(symbol),
            attempts=MAX_RETRIES,
            backoff_base=BACKOFF_BASE,
            backoff_cap=MAX_BACKOFF_SECONDS,
            context=f"{type(self).__name__}.{self._dataset} symbol={symbol}",
            circuit_open_exc=ExchangeCircuitOpenError,
            not_supported_passthrough=True,
        )

    async def _resolve_cursor(self, symbol: str) -> Optional[int]:
        """Nivel 1: Redis. Nivel 2: storage. Nivel 3: None."""
        if self._cursor is not None:
            try:
                ts = await self._cursor.get(self._exchange_id, symbol, self._dataset)
                if ts is not None:
                    return ts
            except Exception as exc:
                self._log.warning(
                    "CursorStore.get falló — fallback | symbol={} error={}",
                    symbol,
                    exc,
                )
        return self._storage.last_timestamp_ms(symbol)

    async def _update_cursor(self, symbol: str, ts_ms: int) -> None:
        """Actualiza cursor en Redis. Silencia errores — no crítico."""
        if self._dry_run or self._cursor is None:
            return
        try:
            await self._cursor.update(self._exchange_id, symbol, self._dataset, ts_ms)
        except Exception as exc:
            self._log.warning(
                "CursorStore.update falló (no crítico) | symbol={} ts_ms={} error={}",
                symbol,
                ts_ms,
                exc,
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
        # TECH-DEBT: accede a _get_client() interno del adapter (Ley de Demeter).
        # Pendiente: exponer fetch_funding_rate en la API pública de CCXTAdapter.
        client = await self._client._get_client()
        return await client.fetch_funding_rate(symbol)

    def _parse(self, raw: Dict[str, Any]) -> pd.DataFrame:
        return _parse_funding_rate(raw)


class OpenInterestFetcher(_BaseDerivativesFetcher):
    """Fetcher de open_interest (contratos abiertos)."""

    _dataset = "open_interest"

    async def _fetch_raw(self, symbol: str) -> Optional[Dict[str, Any]]:
        # TECH-DEBT: accede a _get_client() interno del adapter (Ley de Demeter).
        # Pendiente: exponer fetch_open_interest en la API pública de CCXTAdapter.
        client = await self._client._get_client()
        return await client.fetch_open_interest(symbol)

    def _parse(self, raw: Dict[str, Any]) -> pd.DataFrame:
        return _parse_open_interest(raw)


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

__all__ = ["FundingRateFetcher", "OpenInterestFetcher"]
