"""Classifier vs REAL aiokafka CreateTopicsResponse objects (locked version).

Import is deliberately hard (no importorskip): if aiokafka moves or renames
the protocol classes, collection fails loudly instead of silently skipping.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest
from aiokafka.protocol.admin import (
    CreateTopicsResponse_v0,
    CreateTopicsResponse_v1,
    CreateTopicsResponse_v2,
)

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "provision_kafka_topics.py"
_spec = importlib.util.spec_from_file_location("provision_kafka_topics_real", _SCRIPT)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)

VERSIONS = [CreateTopicsResponse_v0, CreateTopicsResponse_v1, CreateTopicsResponse_v2]
OTHER_ERROR = 37  # INVALID_PARTITIONS


def _real(cls: Any, entries: list[tuple[str, int, str | None]]) -> Any:
    """Build a real response and round-trip it through the wire format."""
    if cls is CreateTopicsResponse_v0:
        resp = cls(topic_errors=[(t, c) for t, c, _ in entries])
    elif cls is CreateTopicsResponse_v1:
        resp = cls(topic_errors=list(entries))
    else:
        resp = cls(throttle_time_ms=0, topic_errors=list(entries))
    return cls.decode(resp.encode())


@pytest.mark.parametrize("cls", VERSIONS)
class TestRealResponseShape:
    def test_to_object_exposes_keys_the_classifier_reads(self, cls: Any) -> None:
        obj = _real(cls, [("t", 0, None)]).to_object()
        assert isinstance(obj["topic_errors"], list)
        entry = obj["topic_errors"][0]
        assert entry["topic"] == "t"
        assert entry["error_code"] == 0

    def test_created(self, cls: Any) -> None:
        assert mod.classify_topic_creation("t", _real(cls, [("t", 0, None)]))[0] == "created"

    def test_already_exists(self, cls: Any) -> None:
        r = _real(cls, [("t", 36, "exists")])
        assert mod.classify_topic_creation("t", r)[0] == "already_exists"

    def test_other_broker_error_is_failed(self, cls: Any) -> None:
        r = _real(cls, [("t", OTHER_ERROR, "bad")])
        cat, detail = mod.classify_topic_creation("t", r)
        assert cat == "failed"
        assert str(OTHER_ERROR) in detail

    def test_topic_missing_is_indeterminate(self, cls: Any) -> None:
        r = _real(cls, [("other", 0, None)])
        assert mod.classify_topic_creation("t", r)[0] == "indeterminate"

    def test_empty_topic_errors_is_indeterminate(self, cls: Any) -> None:
        assert mod.classify_topic_creation("t", _real(cls, []))[0] == "indeterminate"


class _StubAdmin:
    code = 0

    def __init__(self, bootstrap_servers: str) -> None: ...

    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def create_topics(self, new_topics: list[Any]) -> Any:
        name = new_topics[0].name
        return _real(CreateTopicsResponse_v1, [(name, type(self).code, None)])


@pytest.mark.parametrize(("code", "expected_exit"), [(0, 0), (36, 0), (OTHER_ERROR, 1)])
def test_exit_code_end_to_end_with_real_responses(
    monkeypatch: pytest.MonkeyPatch, code: int, expected_exit: int
) -> None:
    _StubAdmin.code = code
    monkeypatch.setattr(mod, "AIOKafkaAdminClient", _StubAdmin)
    monkeypatch.setattr(mod, "ALL_TOPICS", ["a", "b"])
    monkeypatch.setattr(sys, "argv", ["provision_kafka_topics"])
    assert mod.main() == expected_exit
