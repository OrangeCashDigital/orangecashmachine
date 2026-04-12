from core.config.loader.env_overrides import apply_env_overrides

def test_env_override_applies_correctly(monkeypatch):
    # Arrange
    monkeypatch.setenv(
        "OCM_PIPELINE__HISTORICAL__START_DATE",
        "2040-01-01T00:00:00Z",
    )
    config = {
        "pipeline": {
            "historical": {
                "start_date": "2020-01-01T00:00:00Z"
            }
        }
    }
    # Act
    updated = apply_env_overrides(config)
    # Assert
    assert updated["pipeline"]["historical"]["start_date"] == "2040-01-01T00:00:00Z"

def test_no_override_when_env_missing(monkeypatch):
    # Arrange
    monkeypatch.delenv("OCM_PIPELINE__HISTORICAL__START_DATE", raising=False)
    config = {
        "pipeline": {
            "historical": {
                "start_date": "2020-01-01T00:00:00Z"
            }
        }
    }
    # Act
    updated = apply_env_overrides(config)
    # Assert
    assert updated["pipeline"]["historical"]["start_date"] == "2020-01-01T00:00:00Z"

def test_nested_creation(monkeypatch):
    # Arrange
    monkeypatch.setenv("OCM_OBSERVABILITY__METRICS__PORT", "9999")
    config = {}
    # Act
    updated = apply_env_overrides(config)
    # Assert
    assert updated["observability"]["metrics"]["port"] == 9999
