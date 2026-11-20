import pytest

from shared.exceptions import MissingField
from shared.kafka.schemas._base import BasePayload


def test_base_payload_from_dict_rejects_none_event_id():
    with pytest.raises(MissingField):
        BasePayload.from_dict({"event_id": None, "occurred_at": "2026-01-01T00:00:00Z"})


def test_base_payload_from_dict_rejects_missing_event_id():
    with pytest.raises(MissingField):
        BasePayload.from_dict({"occurred_at": "2026-01-01T00:00:00Z"})


def test_base_payload_from_dict_valid():
    payload = BasePayload.from_dict({"event_id": "evt-1", "occurred_at": "2026-01-01T00:00:00Z"})
    assert payload.event_id == "evt-1"
    assert payload.occurred_at == "2026-01-01T00:00:00Z"
