import pytest

from shared.contracts.stream_identity import StreamIdentity


def test_stream_identity_rejects_invalid_symbol_on_direct_construction():
    # parse() rechaza symbols con más de un ':'; construcción directa debe ser igual de estricta
    with pytest.raises(ValueError):
        StreamIdentity(exchange="bybit", symbol="BTC:USDT:PERP")


def test_stream_identity_round_trip_valid():
    identity = StreamIdentity(exchange="bybit", symbol="BTC/USDT")
    assert identity.stream_id == "ws:bybit:BTC/USDT"
    assert StreamIdentity.parse(identity.stream_id) == identity
