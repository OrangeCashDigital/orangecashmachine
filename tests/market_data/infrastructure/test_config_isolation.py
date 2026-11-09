import pytest
from ocm.config.schema import AppConfig

def test_config_topic_defaults():
    # Simulate config loading with minimum required fields
    cfg_data = {
        "exchanges": {"bybit": {"enabled": True}},
        "pipeline": {
            "historical": {"timeframes": ["1m"]},
            "resample": {"targets": ["5m"]},
            "realtime": {},
            "timeouts": {}
        },
        "integrations": {"kafka": {"enabled": True}},
        "datasets": {"ohlcv": True}
    }
    config = AppConfig.model_validate(cfg_data)
    assert config.integrations.kafka.topic_gaps == "market.gaps"
    assert config.integrations.kafka.broker_identity == "production"

def test_config_test_override():
    cfg_data = {
        "exchanges": {"bybit": {"enabled": True}},
        "pipeline": {
            "historical": {"timeframes": ["1m"]},
            "resample": {"targets": ["5m"]},
            "realtime": {},
            "timeouts": {}
        },
        "integrations": {"kafka": {
            "enabled": False, 
            "topic_gaps": "market.gaps.test", 
            "broker_identity": "e2e-test-broker"
        }},
        "datasets": {"ohlcv": True}
    }
    config = AppConfig.model_validate(cfg_data)
    assert config.integrations.kafka.topic_gaps == "market.gaps.test"
    assert config.integrations.kafka.broker_identity == "e2e-test-broker"
