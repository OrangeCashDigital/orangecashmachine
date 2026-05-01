from __future__ import annotations

"""
tests/logging/test_sinks.py
============================

Unit tests para LokiSink y PrometheusLogSink.

Arquitectura de LokiSink (v0.2.0) — API real:
  __call__(message)
      └─ _attempt(payload) → bool          # un intento HTTP, sin parámetro attempt
            └─ si falla y max_retries > 1
                  └─ _schedule_retry(payload, attempt=1)
                        └─ si attempt < max_retries → _attempt(payload)
                              └─ si falla → _schedule_retry(payload, attempt+1)
                        └─ si attempt >= max_retries → descarta (fail-soft)

Principio de test: cada método se prueba contra su firma real.
_attempt no recibe attempt; _schedule_retry sí.
"""

import json
import pytest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

from ocm_platform.observability.sinks import LokiSink, PrometheusLogSink, _LOG_COUNTER


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_loguru_message(
    text: str = "test event",
    level_name: str = "INFO",
    extra: dict | None = None,
) -> MagicMock:
    """Construye un mensaje de Loguru simulado para tests."""
    msg   = MagicMock()
    level = MagicMock()
    level.name = level_name
    msg.record = {
        "message": text,
        "level":   level,
        "time":    datetime.now(timezone.utc),
        "extra":   extra or {"run_id": "abc123", "env": "test", "service": "ocm"},
        "name":    "market_data.ingestion.rest.ohlcv_fetcher",
    }
    return msg


_PAYLOAD = {"streams": [{"stream": {"level": "info"}, "values": [["1234567890000000000", "test"]]}]}


# ── LokiSink — payload y etiquetas ───────────────────────────────────────────

class TestLokiSink:

    def test_push_sends_correct_content_type(self):
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push")
        mock_resp = MagicMock()
        mock_resp.status_code = 204

        with patch.object(sink._client, "post", return_value=mock_resp) as mock_post:
            sink(_make_loguru_message())
            headers = mock_post.call_args.kwargs["headers"]
            assert headers["Content-Type"] == "application/json"

    def test_push_payload_structure(self):
        """El payload enviado a Loki debe tener streams[].stream y streams[].values."""
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push")
        captured: dict = {}
        mock_resp = MagicMock()
        mock_resp.status_code = 204

        def capture_post(url, content, headers):
            captured.update(json.loads(content))
            return mock_resp

        with patch.object(sink._client, "post", side_effect=capture_post):
            sink(_make_loguru_message("hello"))

        assert "streams" in captured
        stream = captured["streams"][0]
        assert "stream" in stream and "values" in stream
        ts_ns, log_line = stream["values"][0]
        assert log_line == "hello"
        assert ts_ns.isdigit()

    def test_push_includes_custom_labels(self):
        """Labels extra del constructor deben aparecer en stream.stream."""
        sink = LokiSink(
            url="http://loki:3100/loki/api/v1/push",
            labels={"cluster": "prod-01"},
        )
        captured: dict = {}
        mock_resp = MagicMock()
        mock_resp.status_code = 204

        def capture(url, content, headers):
            captured.update(json.loads(content))
            return mock_resp

        with patch.object(sink._client, "post", side_effect=capture):
            sink(_make_loguru_message(extra={"run_id": "xyz", "env": "production", "service": "ocm"}))

        labels = captured["streams"][0]["stream"]
        assert labels["cluster"] == "prod-01"
        assert labels["env"]     == "production"
        assert labels["run_id"]  == "xyz"

    # ── _attempt — firma real: _attempt(self, payload) ────────────────────────

    def test_attempt_returns_true_on_2xx(self):
        """_attempt retorna True cuando el servidor responde 2xx."""
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push")
        mock_resp = MagicMock()
        mock_resp.status_code = 204

        with patch.object(sink._client, "post", return_value=mock_resp):
            assert sink._attempt(_PAYLOAD) is True

    def test_attempt_returns_false_on_5xx(self):
        """_attempt retorna False cuando el servidor responde 5xx."""
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push")
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.text = "Service Unavailable"

        with patch.object(sink._client, "post", return_value=mock_resp):
            assert sink._attempt(_PAYLOAD) is False

    def test_attempt_returns_false_on_network_exception(self):
        """_attempt es fail-soft: excepción de red → False, no propaga."""
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push")

        with patch.object(sink._client, "post", side_effect=ConnectionError("refused")):
            assert sink._attempt(_PAYLOAD) is False

    # ── _schedule_retry — contrato de escalado ────────────────────────────────

    def test_schedule_retry_calls_attempt_when_within_limit(self):
        """
        _schedule_retry(payload, attempt=1) con max_retries=3 debe
        ejecutar _attempt exactamente una vez más.
        Usamos threading.Event para sincronizar el Timer daemon.
        """
        import threading
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push", max_retries=3)
        mock_resp = MagicMock()
        mock_resp.status_code = 204  # éxito en el reintento

        done = threading.Event()

        original_attempt = sink._attempt

        def tracked_attempt(payload):
            result = original_attempt(payload)
            done.set()
            return result

        with patch.object(sink._client, "post", return_value=mock_resp):
            with patch.object(sink, "_attempt", side_effect=tracked_attempt):
                sink._schedule_retry(_PAYLOAD, attempt=1)
                done.wait(timeout=3)  # el Timer es daemon; esperamos máx 3s
                sink._attempt.assert_called_once_with(_PAYLOAD)

    def test_schedule_retry_stops_at_max_retries(self):
        """
        _schedule_retry(payload, attempt=max_retries) no debe llamar _attempt.
        El payload se descarta silenciosamente (SafeOps, fail-soft).
        """
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push", max_retries=3)

        with patch.object(sink, "_attempt") as mock_attempt:
            sink._schedule_retry(_PAYLOAD, attempt=3)  # == max_retries → stop
            # El Timer tiene delay — dar tiempo suficiente para que se ejecute si hubiera bug
            import time; time.sleep(0.05)
            mock_attempt.assert_not_called()

    def test_schedule_retry_stops_when_shutdown(self):
        """_schedule_retry no ejecuta _attempt si shutdown está activo."""
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push", max_retries=3)
        sink._shutdown.set()  # simular close() llamado

        with patch.object(sink, "_attempt") as mock_attempt:
            sink._schedule_retry(_PAYLOAD, attempt=1)
            import time; time.sleep(0.05)
            mock_attempt.assert_not_called()

    # ── __call__ — integración síncrona del primer intento ───────────────────

    def test_call_invokes_attempt_on_success(self):
        """
        __call__ exitoso: _attempt llamado una vez, _schedule_retry nunca.
        """
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push", max_retries=3)
        mock_resp = MagicMock()
        mock_resp.status_code = 204

        with patch.object(sink._client, "post", return_value=mock_resp):
            with patch.object(sink, "_schedule_retry") as mock_retry:
                sink(_make_loguru_message())
                mock_retry.assert_not_called()

    def test_call_schedules_retry_on_first_attempt_failure(self):
        """
        __call__ fallido con max_retries > 1: _schedule_retry(payload, attempt=1).
        """
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push", max_retries=3)
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.text = ""

        with patch.object(sink._client, "post", return_value=mock_resp):
            with patch.object(sink, "_schedule_retry") as mock_retry:
                sink(_make_loguru_message())
                mock_retry.assert_called_once()
                # attempt se pasa como kwarg: _schedule_retry(payload, attempt=1)
                assert mock_retry.call_args.kwargs.get("attempt") == 1

    def test_call_no_retry_when_max_retries_is_1(self):
        """
        max_retries=1 significa ningún reintento — _schedule_retry nunca llamado.
        """
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push", max_retries=1)
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.text = ""

        with patch.object(sink._client, "post", return_value=mock_resp):
            with patch.object(sink, "_schedule_retry") as mock_retry:
                sink(_make_loguru_message())
                mock_retry.assert_not_called()

    # ── Fail-soft ─────────────────────────────────────────────────────────────

    def test_call_does_not_propagate_exception(self):
        """
        __call__ es fail-soft: nunca propaga excepción al caller (Loguru).
        Un fallo en el sink no debe interrumpir el pipeline de logging.
        """
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push", max_retries=1)

        with patch.object(sink._client, "post", side_effect=RuntimeError("unexpected")):
            sink(_make_loguru_message())  # no debe propagar

    # ── close ─────────────────────────────────────────────────────────────────

    def test_close_closes_http_client(self):
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push")
        with patch.object(sink._client, "close") as mock_close:
            sink.close()
            mock_close.assert_called_once()

    def test_close_sets_shutdown_event(self):
        """shutdown.set() interrumpe Timers pendientes en _schedule_retry."""
        sink = LokiSink(url="http://loki:3100/loki/api/v1/push")
        assert not sink._shutdown.is_set()
        with patch.object(sink._client, "close"):
            sink.close()
        assert sink._shutdown.is_set()


# ── PrometheusLogSink ─────────────────────────────────────────────────────────

class TestPrometheusLogSink:

    def test_increments_counter_on_call(self):
        sink   = PrometheusLogSink(service="test_service")
        msg    = _make_loguru_message(level_name="ERROR")
        before = _LOG_COUNTER.labels(level="error", service="test_service")._value.get()
        sink(msg)
        after  = _LOG_COUNTER.labels(level="error", service="test_service")._value.get()
        assert after == before + 1

    def test_does_not_raise_on_broken_message(self):
        """Fail-soft: record malformado no propaga excepción."""
        sink   = PrometheusLogSink()
        broken = MagicMock()
        broken.record = {}  # sin "level"
        sink(broken)  # no debe lanzar

    @pytest.mark.parametrize("level", ["debug", "info", "warning", "error", "critical"])
    def test_increments_for_all_standard_levels(self, level):
        sink = PrometheusLogSink(service=f"test_{level}")
        msg  = _make_loguru_message(level_name=level.upper())
        sink(msg)
        val  = _LOG_COUNTER.labels(level=level, service=f"test_{level}")._value.get()
        assert val >= 1
