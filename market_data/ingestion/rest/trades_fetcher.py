# -*- coding: utf-8 -*-
"""
market_data/ingestion/rest/trades_fetcher.py
============================================

Fetcher incremental de trades (tick data) con cursor real.

Responsabilidad única (SRP)
---------------------------
Descargar trades desde un exchange via CCXT y persistirlos en
``silver.trades`` a través de TradesStorage.
No conoce pipelines, orquestadores ni schedules.

Cursor incremental
------------------
Estrategia de dos niveles (fail-safe):
  1. Redis CursorStore → clave ``(exchange, symbol, "trades")``.
     Reutiliza la infraestructura existente sin modificarla (OCP).
  2. Storage fallback  → ``TradesStorage.last_timestamp_ms(symbol)``
     si Redis no está disponible.
  3. None             → backfill completo (sin cursor previo).

SafeOps
-------
* dry_run=True  → no escribe, no actualiza cursor.
* Paginación con límite de páginas (evita loops infinitos).
* Retry + backoff exponencial en errores de red.

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

import asyncio
from typing import Optional

import pandas as pd
from loguru import logger

from market_data.adapters.exchange import CCXTAdapter
from market_data.storage.silver.trades_storage import TradesStorage
from market_data.ports.state import CursorStorePort as CursorStore      # DIP — tipo abstracto
from infra.state.factories  import build_cursor_store as _build_cursor_store  # Fail-Soft fallback

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CURSOR_TIMEFRAME = "trades"     # key virtual para CursorStore
_MAX_PAGES        = 500          # límite anti-loop (SafeOps)
_PAGE_LIMIT       = 1_000        # trades por página CCXT
_RETRY_ATTEMPTS   = 3
_BACKOFF_BASE     = 1.5
_MAX_BACKOFF_S    = 15.0


# ---------------------------------------------------------------------------
# Parser CCXT → DataFrame (puro, sin efectos secundarios)
# ---------------------------------------------------------------------------

def _parse_trades(raw: list) -> pd.DataFrame:
    """
    Convierte respuesta CCXT ``fetch_trades`` a DataFrame normalizado.

    Filtra registros sin trade_id o timestamp (SafeOps).
    """
    if not raw:
        return pd.DataFrame()

    records = []
    for t in raw:
        trade_id = str(t.get("id") or "").strip()
        ts_ms    = t.get("timestamp")
        if not trade_id or ts_ms is None:
            continue
        records.append({
            "trade_id": trade_id,
            "timestamp": int(ts_ms),
            "symbol":   str(t.get("symbol") or ""),
            "side":     str(t.get("side") or ""),
            "price":    float(t.get("price") or 0.0),
            "amount":   float(t.get("amount") or 0.0),
            "cost":     float(t.get("cost") or
                             (float(t.get("price") or 0.0) *
                              float(t.get("amount") or 0.0))),
        })

    return pd.DataFrame(records) if records else pd.DataFrame()


# ---------------------------------------------------------------------------
# TradesFetcher
# ---------------------------------------------------------------------------

class TradesFetcher:
    """
    Descarga trades para un símbolo desde ``since_ms`` y los persiste.

    Parameters
    ----------
    exchange_client : CCXTAdapter ya configurado.
    storage         : TradesStorage (inyección de dependencia — testeable).
    market_type     : ``"spot"`` | ``"linear"`` | ``"inverse"``.
    dry_run         : si True, no escribe ni actualiza cursor.
    """

    def __init__(
        self,
        exchange_client: CCXTAdapter,
        storage:         TradesStorage,
        market_type:     str  = "spot",
        dry_run:         bool = False,
    ) -> None:
        if exchange_client is None:
            raise ValueError("TradesFetcher: exchange_client es obligatorio")
        if storage is None:
            raise ValueError("TradesFetcher: storage es obligatorio")

        self._client      = exchange_client
        self._storage     = storage
        self._market_type = market_type.lower()
        self._dry_run     = dry_run
        self._exchange_id = getattr(exchange_client, "_exchange_id", "unknown")
        self._log         = logger.bind(
            fetcher="trades",
            exchange=self._exchange_id,
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

    async def fetch_symbol(
        self,
        symbol:   str,
        since_ms: Optional[int] = None,
    ) -> int:
        """
        Descarga y persiste todos los trades nuevos para ``symbol``.

        Cursor resolution (orden de precedencia):
          1. ``since_ms`` explícito (backfill manual).
          2. Redis CursorStore.
          3. TradesStorage.last_timestamp_ms().
          4. None → fetch desde el principio disponible.

        Returns
        -------
        int : total de filas escritas en esta ejecución.
        """
        cursor_ms = await self._resolve_cursor(symbol, since_ms)
        self._log.debug(
            "fetch_symbol start | symbol={} cursor_ms={}", symbol, cursor_ms
        )

        total_rows  = 0
        last_ts_ms  = cursor_ms
        pages_read  = 0

        while pages_read < _MAX_PAGES:
            df = await self._fetch_page_with_retry(symbol, last_ts_ms)

            if df.empty:
                self._log.debug(
                    "fetch_symbol done (vacío) | symbol={} pages={} rows={}",
                    symbol, pages_read, total_rows,
                )
                break

            # Avanzar cursor antes de escribir (fail-safe: si write falla
            # el cursor no avanza → reintentará en el próximo run)
            page_max_ts = int(df["timestamp"].max())

            rows = self._storage.append(df)
            total_rows += rows
            pages_read += 1

            await self._update_cursor(symbol, page_max_ts)
            last_ts_ms = page_max_ts + 1   # +1ms evita re-fetch del último trade

            # Condición de fin: página incompleta → no hay más datos
            if len(df) < _PAGE_LIMIT:
                self._log.debug(
                    "fetch_symbol done (página incompleta) | "
                    "symbol={} pages={} rows={}",
                    symbol, pages_read, total_rows,
                )
                break

        if pages_read >= _MAX_PAGES:
            self._log.warning(
                "fetch_symbol: límite de páginas alcanzado | "
                "symbol={} max_pages={} — verificar volumen",
                symbol, _MAX_PAGES,
            )

        return total_rows

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _resolve_cursor(
        self,
        symbol:   str,
        since_ms: Optional[int],
    ) -> Optional[int]:
        """
        Determina desde qué timestamp arrancar.

        Orden de precedencia documentado en fetch_symbol.
        """
        if since_ms is not None:
            return since_ms

        # Nivel 1 — Redis
        if self._cursor is not None:
            try:
                ts = await self._cursor.get(
                    self._exchange_id, symbol, _CURSOR_TIMEFRAME
                )
                if ts is not None:
                    self._log.debug(
                        "Cursor desde Redis | symbol={} ts_ms={}", symbol, ts
                    )
                    return ts
            except Exception as exc:
                self._log.warning(
                    "CursorStore.get falló — fallback a storage | "
                    "symbol={} error={}",
                    symbol, exc,
                )

        # Nivel 2 — Storage fallback
        ts = self._storage.last_timestamp_ms(symbol)
        if ts is not None:
            self._log.debug(
                "Cursor desde storage | symbol={} ts_ms={}", symbol, ts
            )
            return ts

        # Nivel 3 — backfill completo
        self._log.debug("Sin cursor previo — backfill desde inicio | symbol={}", symbol)
        return None

    async def _fetch_page_with_retry(
        self,
        symbol:   str,
        since_ms: Optional[int],
    ) -> pd.DataFrame:
        """Fetcha una página con retry + backoff exponencial."""
        last_exc: Exception | None = None

        for attempt in range(1, _RETRY_ATTEMPTS + 1):
            try:
                raw = await self._client.fetch_trades(
                    symbol,
                    since=since_ms,
                    limit=_PAGE_LIMIT,
                )
                return _parse_trades(raw or [])
            except Exception as exc:
                last_exc = exc
                backoff = min(
                    _BACKOFF_BASE ** attempt,
                    _MAX_BACKOFF_S,
                )
                self._log.warning(
                    "fetch_trades error (intento {}/{}) | "
                    "symbol={} error={} retry_in={:.1f}s",
                    attempt, _RETRY_ATTEMPTS, symbol, exc, backoff,
                )
                if attempt < _RETRY_ATTEMPTS:
                    await asyncio.sleep(backoff)

        raise RuntimeError(
            f"TradesFetcher: {_RETRY_ATTEMPTS} intentos fallidos | "
            f"symbol={symbol} last_error={last_exc}"
        ) from last_exc

    async def _update_cursor(self, symbol: str, ts_ms: int) -> None:
        """Actualiza cursor en Redis (silencia errores — no crítico)."""
        if self._dry_run or self._cursor is None:
            return
        try:
            await self._cursor.update(
                self._exchange_id, symbol, _CURSOR_TIMEFRAME, ts_ms
            )
        except Exception as exc:
            self._log.warning(
                "CursorStore.update falló (no crítico) | "
                "symbol={} ts_ms={} error={}",
                symbol, ts_ms, exc,
            )

    def __repr__(self) -> str:
        return (
            f"TradesFetcher("
            f"exchange={self._exchange_id!r}, "
            f"market_type={self._market_type!r}, "
            f"dry_run={self._dry_run})"
        )


__all__ = ["TradesFetcher"]
