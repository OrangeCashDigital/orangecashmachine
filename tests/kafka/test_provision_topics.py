"""tests/kafka/test_provision_topics — Contador de provisión de topics Kafka.

Regresión del bug del contador: scripts/provision_kafka_topics.py contaba
"Creado" para todos los topics porque aiokafka 0.14.0 NO lanza
TopicAlreadyExistsError desde AIOKafkaAdminClient.create_topics(); devuelve
la CreateTopicsResponse con el resultado por topic en `topic_errors`.
La clasificación correcta se apoya en el error_code del broker:
    0 → created · 36 → already_exists · resto → failed · ausente → indeterminate

Sin broker real: admin falso inyectado por monkeypatch (unit).
"""

from __future__ import annotations

from typing import Any

import pytest

from scripts import provision_kafka_topics as pkg
from scripts.provision_kafka_topics import classify_topic_creation

# =============================================================================
# Stubs — respuesta del broker y admin client, sin red
# =============================================================================


class _FakeResponse:
    """Stub de CreateTopicsResponse: solo expone to_object()."""

    def __init__(self, obj: dict[str, Any]) -> None:
        self._obj = obj

    def to_object(self) -> dict[str, Any]:
        return self._obj


def _topic_errors(*entries: tuple[str, int, str]) -> _FakeResponse:
    """Construye una respuesta con entradas {topic, error_code, error_message}."""
    return _FakeResponse({"topic_errors": [{"topic": t, "error_code": c, "error_message": m} for t, c, m in entries]})


class _FakeAdmin:
    """Stub de AIOKafkaAdminClient: create_topics responde según results."""

    def __init__(
        self,
        results: dict[str, tuple[int, str]] | None = None,
        *,
        raise_on: str | None = None,
        **_: Any,
    ) -> None:
        self._results = results or {}
        self._raise_on = raise_on
        self.closed = False

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def create_topics(self, new_topics: list[Any], *args: Any, **kwargs: Any) -> _FakeResponse:
        name = new_topics[0].name
        if self._raise_on == name:
            raise ConnectionError("broker caído")
        code, msg = self._results.get(name, (36, "Topic already exists."))
        return _topic_errors((name, code, msg))


# =============================================================================
# classify_topic_creation — una entrada por categoría
# =============================================================================


def test_classify_created_when_broker_returns_zero() -> None:
    cat, detail = classify_topic_creation("ohlcv.raw", _topic_errors(("ohlcv.raw", 0, "")))
    assert cat == "created"
    assert "creación" in detail


def test_classify_already_exists_on_code_36() -> None:
    cat, _ = classify_topic_creation("ohlcv.raw", _topic_errors(("ohlcv.raw", 36, "Topic already exists.")))
    assert cat == "already_exists"


def test_classify_failed_on_other_broker_error_code() -> None:
    cat, detail = classify_topic_creation("x", _topic_errors(("x", 29, "Topic authorization failed.")))
    assert cat == "failed"
    assert "29" in detail and "authorization" in detail


def test_classify_indeterminate_when_topic_missing_from_response() -> None:
    cat, _ = classify_topic_creation("a", _topic_errors(("b", 0, "")))
    assert cat == "indeterminate"


def test_classify_indeterminate_when_no_topic_errors_key() -> None:
    cat, _ = classify_topic_creation("a", _FakeResponse({"throttle_time_ms": 0}))
    assert cat == "indeterminate"


def test_classify_indeterminate_when_to_object_raises() -> None:
    class _Broken:
        def to_object(self) -> dict[str, Any]:
            raise ValueError("respuesta corrupta")

    cat, _ = classify_topic_creation("a", _Broken())
    assert cat == "indeterminate"


def test_classify_tolerates_tuple_entries() -> None:
    """Entradas posicionales (decode sin to_object) también se clasifican."""
    cat, _ = classify_topic_creation("a", _FakeResponse({"topic_errors": [("a", 36, "ya")]}))
    assert cat == "already_exists"


# =============================================================================
# provision() con admin falso — contadores agregados
# =============================================================================


@pytest.mark.asyncio
async def test_provision_all_existing_reports_zero_created(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regresión del bug: 25 topics existentes → created=0, ya_existían=25."""
    admin = _FakeAdmin()  # default: todo responde 36 (ya existe)
    monkeypatch.setattr(pkg, "AIOKafkaAdminClient", lambda **_: admin)

    counts = await pkg.provision("localhost:9093", 3, 1)

    assert counts["created"] == 0
    assert counts["already_exists"] == len(pkg.ALL_TOPICS)
    assert counts["failed"] == 0
    assert counts["indeterminate"] == 0
    assert admin.closed is True


@pytest.mark.asyncio
async def test_provision_classifies_mixed_results(monkeypatch: pytest.MonkeyPatch) -> None:
    topics = pkg.ALL_TOPICS
    results = {
        topics[0]: (0, ""),  # created
        topics[1]: (29, "Topic authorization failed."),  # failed
        topics[2]: (36, "Topic already exists."),  # already_exists
        # el resto → default 36 (already_exists)
    }
    admin = _FakeAdmin(results)
    monkeypatch.setattr(pkg, "AIOKafkaAdminClient", lambda **_: admin)

    counts = await pkg.provision("localhost:9093", 3, 1)

    assert counts == {
        "created": 1,
        "already_exists": len(topics) - 2,
        "failed": 1,
        "indeterminate": 0,
    }


@pytest.mark.asyncio
async def test_provision_propagates_connectivity_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail-fast: un error de conexión NO se convierte en éxito ni en ya-existente."""
    admin = _FakeAdmin(raise_on=pkg.ALL_TOPICS[0])
    monkeypatch.setattr(pkg, "AIOKafkaAdminClient", lambda **_: admin)

    with pytest.raises(ConnectionError):
        await pkg.provision("localhost:9093", 3, 1)

    assert admin.closed is True  # finally cierra el admin


# =============================================================================
# main() — códigos de salida
# =============================================================================


def test_main_exit_0_when_all_already_exist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin = _FakeAdmin()  # todo 36
    monkeypatch.setattr(pkg, "AIOKafkaAdminClient", lambda **_: admin)
    monkeypatch.setattr("sys.argv", ["provision_kafka_topics"])

    assert pkg.main() == 0


def test_main_exit_1_when_any_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    topics = pkg.ALL_TOPICS
    admin = _FakeAdmin({topics[0]: (29, "Topic authorization failed.")})
    monkeypatch.setattr(pkg, "AIOKafkaAdminClient", lambda **_: admin)
    monkeypatch.setattr("sys.argv", ["provision_kafka_topics"])

    assert pkg.main() == 1


def test_main_exit_1_on_connectivity_error(monkeypatch: pytest.MonkeyPatch) -> None:
    admin = _FakeAdmin(raise_on=pkg.ALL_TOPICS[0])
    monkeypatch.setattr(pkg, "AIOKafkaAdminClient", lambda **_: admin)
    monkeypatch.setattr("sys.argv", ["provision_kafka_topics"])

    assert pkg.main() == 1
