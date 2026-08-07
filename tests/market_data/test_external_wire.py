"""Tests del wire payload canónico externo y su routing key.

PROVENANCE (Contract PICO, gancho F3 → ADR-0017)
------------------------------------------------
- ExternalMetricPayload → DOCUMENTATION: campo canónico (source_id, metric,
  symbol, timestamp_ms, value, quality_flags) derivado del modelo unificado
  de fuentes externas (ADR-0014) y de los nombres de métrica detectados en la
  documentación/OpenAPI de CoinGlass/CMC (ver normalizers/coinglass.py,
  coinmarketcap.py). Los nombres concretos de campo del provider (fundingRate,
  openInterest, updateTime, ...) se resuelven en el normalizer, no aquí.
"""

from __future__ import annotations

from shared.kafka.schemas.external import (
    EXTERNAL_SCHEMA_VERSION,
    ExternalMetricPayload,
    ExternalSchemaVersionError,
)
from shared.kafka.serializer import deserialize, make_external_key, serialize


def _payload() -> ExternalMetricPayload:
    return ExternalMetricPayload(
        source_id="coinglass",
        metric="funding_rate",
        symbol="BTC/USDT",
        timestamp_ms=1700000000000,
        value="0.0001",
        quality_flags=("flag",),
    )


class TestRoundtrip:
    def test_serialize_deserialize_roundtrip(self):
        original = _payload()
        restored = deserialize(serialize(original), ExternalMetricPayload)
        assert restored == original

    def test_global_metric_symbol_is_none(self):
        p2 = ExternalMetricPayload(source_id="coinmarketcap", metric="btc_dominance", timestamp_ms=1, value="55.2")
        restored = deserialize(serialize(p2), ExternalMetricPayload)
        assert restored.symbol is None

    def test_quality_flags_list_roundtrips_to_tuple(self):
        restored = deserialize(serialize(_payload()), ExternalMetricPayload)
        assert restored.quality_flags == ("flag",)

    def test_wrong_schema_version_fails_fast(self):
        raw = serialize(_payload())
        data = raw.decode("utf-8").replace('"event_version": 1', '"event_version": 99')
        import json

        try:
            ExternalMetricPayload.from_dict(json.loads(data))
        except ExternalSchemaVersionError:
            return
        raise AssertionError("schema version mismatch debió lanzar")


class TestRoutingKey:
    def test_symbol_key(self):
        assert make_external_key("coinglass", "funding_rate", "BTC/USDT") == b"coinglass:funding_rate:BTC/USDT"

    def test_global_key(self):
        assert make_external_key("coinmarketcap", "btc_dominance", None) == b"coinmarketcap:btc_dominance:global"

    def test_schema_version_ssot(self):
        assert EXTERNAL_SCHEMA_VERSION == ExternalMetricPayload.SCHEMA_VERSION == 1
