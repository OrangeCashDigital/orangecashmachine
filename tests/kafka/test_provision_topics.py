"""Unit tests for scripts/provision_kafka_topics.py (no broker required)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "provision_kafka_topics.py"
_spec = importlib.util.spec_from_file_location("provision_kafka_topics", _SCRIPT)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


class FakeResponse:
    def __init__(self, obj: Any = None, raises: Exception | None = None) -> None:
        self._obj = obj
        self._raises = raises

    def to_object(self) -> Any:
        if self._raises:
            raise self._raises
        return self._obj


def _resp(*entries: Any) -> FakeResponse:
    return FakeResponse({"topic_errors": list(entries)})


class TestClassify:
    def test_created_dict(self) -> None:
        r = _resp({"topic": "t", "error_code": 0, "error_message": None})
        assert mod.classify_topic_creation("t", r)[0] == "created"

    def test_already_exists_dict(self) -> None:
        r = _resp({"topic": "t", "error_code": 36, "error_message": "exists"})
        assert mod.classify_topic_creation("t", r)[0] == "already_exists"

    def test_tuple_entries(self) -> None:
        assert mod.classify_topic_creation("t", _resp(("t", 0, None)))[0] == "created"
        assert mod.classify_topic_creation("t", _resp(("t", 36, None)))[0] == "already_exists"

    def test_failed_includes_code_and_message(self) -> None:
        r = _resp({"topic": "t", "error_code": 41, "error_message": "boom"})
        cat, detail = mod.classify_topic_creation("t", r)
        assert cat == "failed"
        assert "41" in detail and "boom" in detail

    def test_only_matching_topic_is_used(self) -> None:
        r = _resp(
            {"topic": "other", "error_code": 0},
            {"topic": "t", "error_code": 36},
        )
        assert mod.classify_topic_creation("t", r)[0] == "already_exists"

    def test_topic_absent_is_indeterminate(self) -> None:
        r = _resp({"topic": "other", "error_code": 0})
        assert mod.classify_topic_creation("t", r)[0] == "indeterminate"

    def test_missing_topic_errors_is_indeterminate(self) -> None:
        assert mod.classify_topic_creation("t", FakeResponse({}))[0] == "indeterminate"

    def test_non_numeric_code_is_indeterminate(self) -> None:
        r = _resp({"topic": "t", "error_code": "0"})
        assert mod.classify_topic_creation("t", r)[0] == "indeterminate"

    def test_undecodable_response_is_indeterminate(self) -> None:
        r = FakeResponse(raises=ValueError("bad"))
        cat, detail = mod.classify_topic_creation("t", r)
        assert cat == "indeterminate"
        assert "bad" in detail


class FakeAdmin:
    instances: list["FakeAdmin"] = []

    def __init__(self, bootstrap_servers: str) -> None:
        self.closed = False
        self.responses: dict[str, Any] = {}
        FakeAdmin.instances.append(self)

    async def start(self) -> None: ...

    async def close(self) -> None:
        self.closed = True

    async def create_topics(self, new_topics: list[Any]) -> Any:
        name = new_topics[0].name
        resp = self.responses.get(name, _resp({"topic": name, "error_code": 0}))
        if isinstance(resp, Exception):
            raise resp
        return resp


@pytest.fixture
def fake_admin(monkeypatch: pytest.MonkeyPatch) -> type[FakeAdmin]:
    FakeAdmin.instances = []
    monkeypatch.setattr(mod, "AIOKafkaAdminClient", FakeAdmin)
    monkeypatch.setattr(mod, "ALL_TOPICS", ["a", "b", "c"])
    return FakeAdmin


class TestProvision:
    @pytest.mark.asyncio
    async def test_counts_mixed_results(self, fake_admin: type[FakeAdmin], monkeypatch: pytest.MonkeyPatch) -> None:
        orig = fake_admin.create_topics

        async def patched(self: FakeAdmin, new_topics: list[Any]) -> Any:
            name = new_topics[0].name
            code = {"a": 0, "b": 36, "c": 41}[name]
            return _resp({"topic": name, "error_code": code})

        monkeypatch.setattr(fake_admin, "create_topics", patched)
        counts = await mod.provision("x:1", 3, 1)
        monkeypatch.setattr(fake_admin, "create_topics", orig)
        assert counts == {"created": 1, "already_exists": 1, "failed": 1, "indeterminate": 0}
        assert fake_admin.instances[0].closed

    @pytest.mark.asyncio
    async def test_connection_error_propagates_and_closes(
        self, fake_admin: type[FakeAdmin], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def boom(self: FakeAdmin, new_topics: list[Any]) -> Any:
            raise ConnectionError("down")

        monkeypatch.setattr(fake_admin, "create_topics", boom)
        with pytest.raises(ConnectionError):
            await mod.provision("x:1", 3, 1)
        assert fake_admin.instances[0].closed


class TestMainExitCodes:
    def _run(self, monkeypatch: pytest.MonkeyPatch, **kw: Any) -> int:
        monkeypatch.setattr(sys, "argv", ["provision_kafka_topics"])
        monkeypatch.setattr(mod, "provision", AsyncMock(**kw))
        return mod.main()

    def test_exit_0_when_all_ok(self, monkeypatch: pytest.MonkeyPatch) -> None:
        counts = {"created": 2, "already_exists": 1, "failed": 0, "indeterminate": 0}
        assert self._run(monkeypatch, return_value=counts) == 0

    def test_exit_1_on_failed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        counts = {"created": 2, "already_exists": 0, "failed": 1, "indeterminate": 0}
        assert self._run(monkeypatch, return_value=counts) == 1

    def test_exit_1_on_indeterminate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        counts = {"created": 2, "already_exists": 0, "failed": 0, "indeterminate": 1}
        assert self._run(monkeypatch, return_value=counts) == 1

    def test_exit_1_on_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        assert self._run(monkeypatch, side_effect=ConnectionError("down")) == 1
