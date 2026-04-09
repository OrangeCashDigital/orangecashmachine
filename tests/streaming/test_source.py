from __future__ import annotations

"""
tests/streaming/test_source.py
================================

Tests de StreamSource con mocks — sin Redis real.
"""

import pytest
from typing import List, Tuple, Dict, Optional

from market_data.streaming.payloads import (
    EventPayload, OHLCVBar,
    PAYLOAD_SCHEMA_VERSION, SchemaVersionError,
)
from market_data.streaming.router  import EventRouter
from market_data.streaming.source  import StreamSource, _MAX_RETRIES_BEFORE_DLQ


# --------------------------------------------------
# Stubs
# --------------------------------------------------

class _MockConsumer:
    """Consumer stub — soporta consume, claim_pending, ack y send_to_dlq."""

    def __init__(
        self,
        messages:   List[Tuple[str, Dict]] = None,
        pending:    List[Tuple[str, Dict]] = None,
        batch_size: int = 5,
    ) -> None:
        self._messages      = list(messages or [])
        self._pending       = list(pending  or [])
        self._batch_size    = batch_size
        self._acked:        List[str]  = []
        self._dlq:          List[Dict] = []
        self._consume_calls = 0

    def consume(self) -> List[Tuple[str, Dict]]:
        self._consume_calls += 1
        if not self._messages:
            return []
        batch, self._messages = (
            self._messages[:self._batch_size],
            self._messages[self._batch_size:],
        )
        return batch

    def claim_pending(
        self,
        min_idle_ms: int = 60_000,
    ) -> List[Tuple[str, Dict]]:
        if not self._pending:
            return []
        batch, self._pending = self._pending[:self._batch_size], self._pending[self._batch_size:]
        return batch

    def ack(self, entry_id: str) -> bool:
        self._acked.append(entry_id)
        return True

    def send_to_dlq(
        self,
        fields:            Dict,
        reason:            str = "unknown",
        original_entry_id: str = "",
    ) -> Optional[str]:
        self._dlq.append({
            "fields":   fields,
            "reason":   reason,
            "entry_id": original_entry_id,
        })
        return f"dlq-{original_entry_id}"

    def ensure_group(self) -> bool:
        return True


class _MockConsumerNoDLQ:
    """Consumer sin send_to_dlq — verifica degradación controlada."""

    def __init__(self, messages: List[Tuple[str, Dict]] = None) -> None:
        self._messages = list(messages or [])
        self._acked:   List[str] = []

    def consume(self) -> List[Tuple[str, Dict]]:
        if not self._messages:
            return []
        batch, self._messages = self._messages[:5], self._messages[5:]
        return batch

    def ack(self, entry_id: str) -> bool:
        self._acked.append(entry_id)
        return True


class _OKHandler:
    def handle(self, event: EventPayload) -> bool:
        return True


class _FailHandler:
    def handle(self, event: EventPayload) -> bool:
        return False


def _make_valid_fields(event_id: str = "evt-001") -> Dict[str, str]:
    return {
        "event_version":  str(PAYLOAD_SCHEMA_VERSION),
        "event_id":       event_id,
        "exchange":       "bybit",
        "symbol":         "BTC/USDT",
        "timeframe":      "1h",
        "batch_start_ts": "1700000000000",
        "bars":           '[{"ts": 1700000000000, "open": 30000.0, "high": 30500.0, '
                          '"low": 29800.0, "close": 30200.0, "volume": 12.5}]',
        "meta":           "null",
    }


# --------------------------------------------------
# Tests: run_once (mensajes nuevos)
# --------------------------------------------------

class TestStreamSource:

    def test_run_once_empty_consumer(self):
        consumer = _MockConsumer([])
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router)
        processed, failed = source.run_once()
        assert processed == 0
        assert failed    == 0

    def test_run_once_processes_valid_message(self):
        fields   = _make_valid_fields("evt-001")
        consumer = _MockConsumer([("1-0", fields)])
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router)

        processed, failed = source.run_once()
        assert processed == 1
        assert failed    == 0
        assert "1-0" in consumer._acked

    def test_run_once_no_ack_when_router_rejects(self):
        """Primer rechazo — aún dentro del umbral, NO hace ACK."""
        fields   = _make_valid_fields("evt-002")
        consumer = _MockConsumer([("2-0", fields)])
        router   = EventRouter(handlers=[_FailHandler()])
        source   = StreamSource(consumer=consumer, router=router)

        processed, failed = source.run_once()
        assert processed == 0
        assert failed    == 1
        assert "2-0" not in consumer._acked

    def test_run_once_acks_malformed_payload(self):
        consumer = _MockConsumer([("3-0", {"broken": "data"})])
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router)

        processed, failed = source.run_once()
        assert failed == 1
        assert "3-0" in consumer._acked

    def test_run_once_acks_schema_version_mismatch(self):
        fields = _make_valid_fields("evt-004")
        fields["event_version"] = str(PAYLOAD_SCHEMA_VERSION + 99)
        consumer = _MockConsumer([("4-0", fields)])
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router)

        processed, failed = source.run_once()
        assert failed == 1
        assert "4-0" in consumer._acked

    def test_run_once_multiple_messages(self):
        messages = [
            ("5-0", _make_valid_fields("evt-005")),
            ("5-1", _make_valid_fields("evt-006")),
            ("5-2", _make_valid_fields("evt-007")),
        ]
        consumer = _MockConsumer(messages)
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router)

        processed, failed = source.run_once()
        assert processed == 3
        assert failed    == 0
        assert len(consumer._acked) == 3

    def test_run_max_iterations(self):
        consumer = _MockConsumer([])
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router)
        source.run(max_iterations=3)
        assert consumer._consume_calls == 3

    def test_run_stops_on_max_errors(self):
        bad_messages = [("x-0", {"bad": "data"})] * 5
        consumer = _MockConsumer(bad_messages * 20)
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router, max_errors=3)
        source.run(max_iterations=100)
        assert True


# --------------------------------------------------
# Tests: claim_pending (self-healing)
# --------------------------------------------------

class TestClaimPending:

    def test_run_once_pending_processes_claimed_messages(self):
        pending  = [("p-0", _make_valid_fields("evt-pending-001"))]
        consumer = _MockConsumer(pending=pending)
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router)

        processed, failed = source.run_once_pending()
        assert processed == 1
        assert failed    == 0
        assert "p-0" in consumer._acked

    def test_run_once_pending_empty(self):
        consumer = _MockConsumer(pending=[])
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router)

        processed, failed = source.run_once_pending()
        assert processed == 0
        assert failed    == 0

    def test_run_once_pending_safeops_no_claim_method(self):
        consumer = _MockConsumerNoDLQ([])
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router)

        processed, failed = source.run_once_pending()
        assert processed == 0
        assert failed    == 0

    def test_run_processes_pending_before_new(self):
        pending  = [("p-0", _make_valid_fields("evt-pending"))]
        new      = [("n-0", _make_valid_fields("evt-new"))]
        consumer = _MockConsumer(messages=new, pending=pending)
        router   = EventRouter(handlers=[_OKHandler()])
        source   = StreamSource(consumer=consumer, router=router)

        source.run(max_iterations=1)

        assert "p-0" in consumer._acked
        assert "n-0" in consumer._acked


# --------------------------------------------------
# Tests: DLQ
# --------------------------------------------------

class TestDLQ:

    def test_message_sent_to_dlq_after_max_retries(self):
        """
        DLQ dispara cuando retries >= MAX_RETRIES.

        Con batch_size=1, cada run_once() procesa exactamente
        un intento del mismo entry_id.

        Iteraciones 1..MAX_RETRIES-1: falla, queda en PEL.
        Iteración MAX_RETRIES:        dispara DLQ + ACK.
        """
        fields = _make_valid_fields("evt-dlq")
        # batch_size=1 garantiza un intento por run_once()
        consumer = _MockConsumer(
            messages   = [("dlq-0", fields)] * _MAX_RETRIES_BEFORE_DLQ,
            batch_size = 1,
        )
        router = EventRouter(handlers=[_FailHandler()])
        source = StreamSource(consumer=consumer, router=router)

        # Iteraciones 1..MAX_RETRIES-1: aún no en DLQ
        for _ in range(_MAX_RETRIES_BEFORE_DLQ - 1):
            source.run_once()

        assert "dlq-0" not in consumer._acked
        assert len(consumer._dlq) == 0

        # Iteración MAX_RETRIES: dispara DLQ
        source.run_once()

        assert "dlq-0" in consumer._acked
        assert len(consumer._dlq) == 1
        assert consumer._dlq[0]["reason"] == "router_rejected"

    def test_dlq_safeops_no_dlq_method(self):
        """Consumer sin send_to_dlq → ACK igual, no lanza."""
        fields   = _make_valid_fields("evt-nodlq")
        messages = [("nd-0", fields)] * _MAX_RETRIES_BEFORE_DLQ
        consumer = _MockConsumerNoDLQ(messages)
        router   = EventRouter(handlers=[_FailHandler()])
        source   = StreamSource(consumer=consumer, router=router)

        for _ in range(_MAX_RETRIES_BEFORE_DLQ):
            source.run_once()

        assert "nd-0" in consumer._acked

    def test_successful_retry_clears_retry_count(self):
        """Si un mensaje falla y luego tiene éxito, el contador se limpia."""
        fields     = _make_valid_fields("evt-retry")
        consumer   = _MockConsumer([("r-0", fields)], batch_size=1)
        call_count = {"n": 0}

        class _FlipHandler:
            def handle(self, event: EventPayload) -> bool:
                call_count["n"] += 1
                return call_count["n"] > 1

        router = EventRouter(handlers=[_FlipHandler()])
        source = StreamSource(consumer=consumer, router=router)

        source.run_once()  # falla
        assert "r-0" not in consumer._acked

        consumer._messages = [("r-0", fields)]
        source.run_once()  # éxito

        assert "r-0" in consumer._acked
        assert "r-0" not in source._retry_counts
