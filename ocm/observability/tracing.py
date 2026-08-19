"""
ocm/observability/tracing.py
=============================

Tracing distribuido para OrangeCashMachine — OpenTelemetry (B-17/H-18).

v0.2.0 — integración real con OpenTelemetry SDK + exporter OTLP HTTP.

Arquitectura
------------
- ``TracingRuntime``: lifecycle manager (idempotente, fail-soft) del subsistema
  de tracing, equivalente a ``MetricsRuntime`` en métricas.
- ``init_tracing()``: arranque único desde el entrypoint (composition root).
- ``trace_event()``: context manager que abre un span y propaga un request-id
  al contexto de logs (loguru ``contextualize``) y al span (atributo
  ``request.id``). Es el mecanismo único de request-id en OCM.
- ``request_id()``: lee el request-id activo del contexto.

Comportamiento
--------------
- Fail-soft: si el exporter falla o la config es inválida, se loguea WARNING
  y el sistema continúa sin tracing — nunca bloquea el pipeline.
- Idempotente: ``start()``/``init_tracing()`` son seguros de llamar varias veces.
- Disabled por defecto (``tracing.enabled: false``): los spans se crean como
  no-recording (OTel no-op) y no se exportan — coste despreciable.
- ``trace_event()`` SIEMPRE propaga el request-id a logs, incluso con tracing
  deshabilitado: la correlación request-id ↔ logs es independiente del exporter.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Optional

from loguru import logger as _log
from opentelemetry import trace as _trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# Nombre del tracer OTel para OCM — constante, no un literal suelto.
_TRACER_NAME: str = "ocm"

# Request-id activo del contexto — ContextVar propio, independiente del provider
# OTel, para que la correlación con logs funcione incluso con tracing
# deshabilitado. `trace_event()` lo setea; `request_id()` lo lee.
_request_id_ctx: ContextVar[str] = ContextVar("ocm_request_id", default="")

_ACTIVE_TRACER_PROVIDER: TracerProvider | None = None
_STARTED: bool = False


class TracingRuntime:
    """Lifecycle manager del subsistema de tracing (OpenTelemetry).

    Sigue el patrón de :class:`MetricsRuntime`:
    - Idempotente: ``start()`` no falla si ya está iniciado.
    - Fail-soft: errores loguean WARNING, nunca propagan.
    - Stateful: expone ``.started`` para introspección.
    - Thread-safe: lock en ``start()``.
    """

    def __init__(
        self,
        enabled: bool = False,
        endpoint: Optional[str] = None,
        service_name: str = "orangecashmachine",
        sample_ratio: float = 1.0,
    ) -> None:
        self.enabled = enabled
        self.endpoint = endpoint
        self.service_name = service_name
        self.sample_ratio = sample_ratio
        self._started = False
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, tracing_cfg) -> "TracingRuntime":
        """Construye TracingRuntime desde TracingConfig (schema.py)."""
        return cls(
            enabled=bool(getattr(tracing_cfg, "enabled", False)),
            endpoint=getattr(tracing_cfg, "endpoint", None),
            service_name=getattr(tracing_cfg, "service_name", "orangecashmachine"),
            sample_ratio=float(getattr(tracing_cfg, "sample_ratio", 1.0)),
        )

    @property
    def started(self) -> bool:
        return self._started

    def start(self, *, validate_only: bool = False) -> bool:
        """Configura el TracerProvider global con exporter OTLP (si hay endpoint).

        Parámetros
        ----------
        validate_only : si True, no configura exporter — sistema en validación.

        Retorna True si tracing quedó activo, False si no.
        """
        global _ACTIVE_TRACER_PROVIDER, _STARTED

        if not self.enabled:
            _log.debug("tracing_disabled")
            return False

        if validate_only:
            _log.debug("tracing_skipped | reason=validate_only")
            return False

        with self._lock:
            if _STARTED:
                _log.debug("tracing_already_started | endpoint={}", self.endpoint)
                return True

            try:
                provider = TracerProvider(
                    resource=Resource.create({SERVICE_NAME: self.service_name}),
                    sampler=ParentBased(root=TraceIdRatioBased(self.sample_ratio)),
                )
                if self.endpoint:
                    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=self.endpoint)))
                # Propagadores W3C: tracecontext + baggage (estándar OTel).
                set_global_textmap(CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()]))
                _trace.set_tracer_provider(provider)
                _ACTIVE_TRACER_PROVIDER = provider
                _STARTED = True
                self._started = True
                _log.bind(endpoint=self.endpoint, service=self.service_name).info("tracing_started")
                return True
            except Exception as exc:
                _log.bind(endpoint=self.endpoint, error=str(exc)).warning("tracing_start_failed")
                return False

    def shutdown(self) -> None:
        """Limpieza explícita del subsistema.

        OTel no expone un stop global reversible — registramos el estado.
        """
        global _STARTED
        if _STARTED:
            _log.bind(endpoint=self.endpoint).debug("tracing_shutdown")
            _STARTED = False


# Singleton de proceso — único TracingRuntime por proceso
_runtime: Optional[TracingRuntime] = None


def get_tracing_runtime() -> Optional[TracingRuntime]:
    """Acceso al runtime activo. None si aún no inicializado."""
    return _runtime


def init_tracing(cfg, *, validate_only: bool = False) -> TracingRuntime:
    """Inicializa y arranca el TracingRuntime desde AppConfig.

    Llama una sola vez desde el entrypoint. Seguro llamar múltiples veces
    — ``start()`` es idempotente.
    """
    global _runtime
    obs = getattr(cfg, "observability", None)
    if obs is None or obs.tracing is None:
        _log.debug("tracing_config_missing")
        _runtime = TracingRuntime(enabled=False)
        return _runtime

    _runtime = TracingRuntime.from_config(obs.tracing)
    _runtime.start(validate_only=validate_only)
    return _runtime


def request_id() -> str:
    """Devuelve el request-id activo del contexto (o ``"-"``).

    Se lee del ContextVar propio de OCM — funciona incluso sin tracing
    habilitado, porque ``trace_event()`` lo setea siempre.
    """
    return _request_id_ctx.get() or "-"


# Tipos de valores aceptados por OTel para span attributes (Mapping de
# opentelemetry.sdk.trace) — evita `object` que mypy rechaza.
_SpanAttributeValue = str | bool | int | float


@contextmanager
def trace_event(
    span_name: str,
    *,
    request_id: Optional[str] = None,
    **attributes: _SpanAttributeValue,
) -> Iterator[_trace.Span]:
    """Context manager que abre un span OTel y propaga un request-id.

    Uso (p.ej. en un consumer de market_data)::

        with trace_event("QualityPipelineConsumer.handle",
                         request_id=event.event_id,
                         exchange=event.batch.exchange):
            self._process(event)

    Comportamiento
    --------------
    - Abre un span ``span_name`` (no-recording si tracing deshabilitado).
    - Fija el atributo ``request.id`` en el span (si request_id provisto).
    - Propaga ``request_id`` al contexto de loguru vía ``logger.contextualize``
      — TODOS los logs emitidos dentro del bloque llevan ``request_id``.
    - Los atributos extra (exchange, symbol, ...) se fijan en el span.

    Es el mecanismo ÚNICO de request-id en OCM (B-17): no existe otro.
    """
    tracer = _trace.get_tracer(_TRACER_NAME)
    attrs: dict[str, _SpanAttributeValue] = dict(attributes)
    if request_id is not None:
        attrs["request.id"] = request_id

    with tracer.start_as_current_span(span_name, attributes=attrs) as span:
        if request_id is not None:
            from loguru import logger

            token = _request_id_ctx.set(request_id)
            try:
                with logger.contextualize(request_id=request_id):
                    yield span
            finally:
                _request_id_ctx.reset(token)
        else:
            yield span


@contextmanager
def trace_consumer_event(
    span_name: str,
    event,
    **extra_attributes: _SpanAttributeValue,
) -> Iterator[_trace.Span]:
    """Span OTel + request-id desde el event_id de un domain event (B-17).

    SSOT de la propagación de request-id en consumers: extrae
    ``event.event_id`` (UUID4 canónico de cada domain event) como request-id
    y lo propaga al span (``request.id``) y al contexto de logs.

    Uso en ``BaseConsumer``/subclases::

        with trace_consumer_event(f"{self.__class__.__name__}.handle", event):
            self._process(event)

    Parámetros
    ----------
    span_name        : nombre del span (p.ej. "QualityPipelineConsumer.handle").
    event            : DomainEvent con ``.event_id`` (todos en market_data).
    extra_attributes : atributos adicionales para el span (exchange, symbol, ...).

    Devuelve el span activo (no-recording si tracing deshabilitado).
    """
    event_id = getattr(event, "event_id", None)
    attrs: dict[str, _SpanAttributeValue] = dict(extra_attributes)
    if isinstance(event_id, str) and event_id:
        attrs.setdefault("request.id", event_id)

    with trace_event(
        span_name,
        request_id=event_id if isinstance(event_id, str) else None,
        **attrs,
    ) as span:
        yield span


__all__ = [
    "TracingRuntime",
    "get_tracing_runtime",
    "init_tracing",
    "request_id",
    "trace_consumer_event",
    "trace_event",
]
