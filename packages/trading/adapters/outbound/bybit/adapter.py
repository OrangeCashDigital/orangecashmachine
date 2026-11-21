# -*- coding: utf-8 -*-
"""
trading/adapters/outbound/bybit/adapter.py
============================================

Adaptador real de Bybit para REST (testnet únicamente) siguiendo ADR-0016/0030/0048.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from decimal import Decimal
from typing import Any, Dict, Optional, cast

import requests
from loguru import logger

from shared.exceptions import OCMInfrastructureError
from trading.ports.outbound.transport import (
    ExchangeOrderStatus,
    OrderState,
    OrderTransport,
)

# Constantes de seguridad
BYBIT_TESTNET_BASE = "https://api-testnet.bybit.com"
ALLOWED_BASE_URLS = {BYBIT_TESTNET_BASE}


class BybitRestTransport(OrderTransport):
    """Transporte REST para Bybit v5 (Testnet)."""

    def __init__(
        self, api_key: str, api_secret: str, base_url: str = BYBIT_TESTNET_BASE, timeout: float = 10.0
    ) -> None:
        if base_url not in ALLOWED_BASE_URLS:
            raise RuntimeError(f"CRITICAL: Base URL no permitida: {base_url}. Bloqueado por seguridad.")

        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.timeout = timeout

        self._log = logger.bind(component="BybitRestTransport", base_url=base_url)

    def _dec(self, value: Any) -> Optional[float]:
        if value is None or str(value).strip() == "":
            return None
        return float(Decimal(str(value)))

    def _sign(self, payload: str, timestamp: str) -> str:
        return hmac.new(
            self.api_secret.encode("utf-8"),
            # codeql[py/weak-sensitive-data-hashing] -- falso positivo: HMAC, no KDF
            (timestamp + self.api_key + "5000" + payload).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _request(self, method: str, path: str, params: dict[str, Any]) -> Dict[str, Any]:
        timestamp = str(int(time.time() * 1000))
        query = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        signature = self._sign(query, timestamp)

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": "5000",
            "Content-Type": "application/json",
        }

        response = requests.request(
            method,
            f"{self.base_url}{path}",
            params=params if method == "GET" else None,
            json=params if method == "POST" else None,
            headers=headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        res = cast(dict[str, Any], response.json())
        if res.get("retCode") != 0:
            raise OCMInfrastructureError(f"Bybit API Error: {res.get('retMsg')}")
        return res

    def _to_order_status(self, raw_status: str) -> ExchangeOrderStatus:
        mapping = {
            "New": ExchangeOrderStatus.SUBMITTED,
            "PartiallyFilled": ExchangeOrderStatus.PARTIALLY_FILLED,
            "Filled": ExchangeOrderStatus.FILLED,
            "Cancelled": ExchangeOrderStatus.CANCELLED,
            "Rejected": ExchangeOrderStatus.REJECTED,
        }
        return mapping.get(raw_status, ExchangeOrderStatus.ERROR)

    def submit(self, symbol: str, side: str, qty: float, *, client_order_id: str) -> OrderState:
        try:
            res = self._request(
                "POST",
                "/v5/order/create",
                {
                    "category": "spot",
                    "symbol": symbol.replace("/", ""),
                    "side": side.capitalize(),
                    "orderType": "Market",
                    "qty": str(qty),
                    "orderLinkId": client_order_id,
                },
            )
            return OrderState(
                order_id=res["result"]["orderId"],
                status=ExchangeOrderStatus.SUBMITTED,
                filled_qty=0.0,
                fill_price=0.0,
                fees=0.0,
            )
        except Exception as e:
            self._log.error("Submit failed: {}", e)
            return OrderState(status=ExchangeOrderStatus.ERROR, error=str(e))

    def fetch_state(self, exchange_order_id: str) -> OrderState:
        try:
            res = self._request("GET", "/v5/order/realtime", {"category": "spot", "orderId": exchange_order_id})
            data = res["result"]["list"][0]
            return OrderState(
                order_id=data["orderId"],
                status=self._to_order_status(data["orderStatus"]),
                filled_qty=self._dec(data.get("cumExecQty")),
                fill_price=self._dec(data.get("avgPrice")),
                fees=self._dec(data.get("cumExecFee")),
            )
        except Exception as e:
            self._log.error("Fetch state failed: {}", e)
            return OrderState(status=ExchangeOrderStatus.ERROR, error=str(e))

    def cancel(self, symbol: str, exchange_order_id: str) -> OrderState:
        try:
            self._request(
                "POST",
                "/v5/order/cancel",
                {"category": "spot", "symbol": symbol.replace("/", ""), "orderId": exchange_order_id},
            )
            return OrderState(status=ExchangeOrderStatus.CANCELLED)
        except Exception as e:
            self._log.error("Cancel failed: {}", e)
            return OrderState(status=ExchangeOrderStatus.ERROR, error=str(e))

    def fetch_open_orders(self) -> list[OrderState]:
        return []

    def close(self) -> None:
        pass
