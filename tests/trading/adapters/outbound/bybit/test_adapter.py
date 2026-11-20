# -*- coding: utf-8 -*-
import pytest
import requests_mock
from trading.adapters.outbound.bybit.adapter import BybitRestTransport
from trading.ports.outbound.transport import ExchangeOrderStatus

def test_bybit_adapter_rejects_mainnet():
    with pytest.raises(RuntimeError, match="CRITICAL: Base URL no permitida"):
        BybitRestTransport(api_key="k", api_secret="s", base_url="https://api.bybit.com")

def test_hmac_signature_generation():
    adapter = BybitRestTransport(api_key="key", api_secret="secret")
    with requests_mock.Mocker() as m:
        m.get("https://api-testnet.bybit.com/v5/account/wallet-balance?accountType=UNIFIED&coin=USDT", json={"retCode": 0, "result": {"list": []}})
        try:
            adapter.get_balance("USDT")
        except:
            pass
        assert "X-BAPI-SIGN" in m.request_history[0].headers
        assert len(m.request_history[0].headers["X-BAPI-SIGN"]) == 64

def test_submit_success():
    adapter = BybitRestTransport(api_key="key", api_secret="secret")
    with requests_mock.Mocker() as m:
        m.post("https://api-testnet.bybit.com/v5/order/create", json={
            "retCode": 0,
            "result": {
                "orderId": "123",
                "orderStatus": "Filled",
                "cumExecQty": "0.00000001",
                "avgPrice": "50000.5",
                "cumExecFee": "0.1"
            }
        })
        order = adapter.submit("BTC/USDT", "buy", 0.00000001, client_order_id="c1")
        assert order.status == ExchangeOrderStatus.FILLED
        assert order.filled_qty == 0.00000001
        assert order.fill_price == 50000.5

