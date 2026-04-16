from __future__ import annotations

"""tests/logging/test_sinks.py — Unit tests para LokiSink y PrometheusLogSink."""

import json
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from core.observability.sinks import LokiSink, PrometheusLogSink, _LOG_COUNTER


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_loguru_message(
    text: str = "test event",
    level_name: str = "INFO",
    extra: dict | None = None,
) -> MagicMock:
    """Construye un mensaje de Loguru simulado para tests."""
    msg = MagicMock()
    level = MagicMock()
    level.name = level_name

    msg.record = {
        "message": text,
        "level":   level,
        "time":    datetime.now(timezone.utc),
        "extra":   extra or {"run_id": "abc123", "env": "test", "service": "ocm"},
        "name":    "market_data.fetcher",
    }
    return msg


# ── LokiSink ─────────────────────────────────────────────────────────────────

class TestLokiSink:

    def test_push_sends_correct_content_type(self):
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push")
        mock_resp = MagicMock()
        mock_resp.status_code = 204

        with patch.object(sink._client, "post", return_value=mock_resp) as mock_post:
            sink(_make_loguru_message())
            call_kwargs = mock_post.call_args
            assert call_kwargs.kwargs["headers"]["Content-Type"] == "application/json"

    def test_push_payload_structure(self):
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push")
        captured_body = {}
        mock_resp = MagicMock()
        mock_resp.status_code = 204

        def capture_post(url, content, headers):
            captured_body.update(json.loads(content))
            return mock_resp

        with patch.object(sink._client, "post", side_effect=capture_post):
            sink(_make_loguru_message("hello"))

        assert "streams" in captured_body
        stream = captured_body["streams"][0]
        assert "stream" in stream
        assert "values" in stream
        assert len(stream["values"]) == 1
        ts_ns, log_line = stream["values"][0]
        assert log_line == "hello"
        assert ts_ns.isdigit()

    def test_push_includes_labels(self):
        sink = LokiSink(
            url="http://loki:3100/loki/api/v1/push",
            labels={"cluster": "prod-01"},
        )
        captured_body = {}
        mock_resp = MagicMock()
        mock_resp.status_code = 204

        def capture(url, content, headers):
            captured_body.update(json.loads(content))
            return mock_resp

        with patch.object(sink._client, "post", side_effect=capture):
            sink(_make_loguru_message(extra={"run_id": "xyz", "env": "production", "service": "ocm"}))

        labels = captured_body["streams"][0]["stream"]
        assert labels["cluster"] == "prod-01"
        assert labels["env"] == "production"
        assert labels["run_id"] == "xyz"

    def test_push_retries_on_http_error(self):
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push", max_retries=3)
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.text = "Service Unavailable"

        with patch.object(sink._client, "post", return_value=mock_resp) as mock_post:
            with patch("time.sleep"):  # no esperar en tests
                sink._push({"streams": []})
                assert mock_post.call_count == 3

    def test_push_succeeds_on_second_attempt(self):
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push", max_retries=3)
        fail_resp = MagicMock()
        fail_resp.status_code = 503
        fail_resp.text = ""
        ok_resp = MagicMock()
        ok_resp.status_code = 204

        responses = iter([fail_resp, ok_resp])

        with patch.object(sink._client, "post", side_effect=responses) as mock_post:
            with patch("time.sleep"):
                sink._push({"streams": []})
                assert mock_post.call_count == 2

    def test_push_fail_soft_on_exception(self):
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push", max_retries=2)

        with patch.object(sink._client, "post", side_effect=ConnectionError("refused")):
            with patch("time.sleep"):
                # No debe propagar la excepción
                sink(_make_loguru_message())

    def test_close_closes_client(self):
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push")
        with patch.object(sink._client, "close") as mock_close:
            sink.close()
            mock_close.assert_called_once()


# ── PrometheusLogSink ─────────────────────────────────────────────────────────

class TestPrometheusLogSink:

    def test_increments_counter_on_call(self):
        sink = PrometheusLogSink(service="test_service")
        msg  = _make_loguru_message(level_name="ERROR")

        before = _LOG_COUNTER.labels(level="error", service="test_service")._value.get()
        sink(msg)
        after  = _LOG_COUNTER.labels(level="error", service="test_service")._value.get()

        assert after == before + 1

    def test_does_not_raise_on_broken_message(self):
        sink = PrometheusLogSink()
        broken = MagicMock()
        broken.record = {}  # sin "level"
        # No debe propagar excepción
        sink(broken)

    @pytest.mark.parametrize("level", ["debug", "info", "warning", "error", "critical"])
    def test_increments_for_all_levels(self, level):
        sink = PrometheusLogSink(service=f"test_{level}")
        msg  = _make_loguru_message(level_name=level.upper())
        sink(msg)
        val  = _LOG_COUNTER.labels(level=level, service=f"test_{level}")._value.get()
        assert val >= 1
