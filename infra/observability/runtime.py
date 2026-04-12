"""
infra/observability/runtime.py
================================
MetricsRuntime — lifecycle manager del subsistema de métricas.

Responsabilidades:
  • Estado explícito (_started, _mode)
  • Idempotencia: start() seguro si se llama múltiples veces
  • Execution-awareness: respeta validate_only y execution_mode
  • Separación pull (HTTP server) vs push (Pushgateway)

Uso:
    runtime = MetricsRuntime.from_config(cfg.observability.metrics)
    runtime.start()           # pull — levanta HTTP server si enabled
    runtime.push(exchange)    # push — envía al Pushgateway
    runtime.shutdown()        # limpieza explícita
"""
from __future__ import annotations

import threading
import time as _time
from enum import Enum
from typing import Optional

from loguru import logger as _log
from prometheus_client import CollectorRegistry, REGISTRY


class MetricsMode(str, Enum):
    PULL  = "pull"   # Prometheus scrape via HTTP
    PUSH  = "push"   # Pushgateway batch
    HYBRID = "hybrid" # ambos


class MetricsRuntime:
    """
    Lifecycle manager del subsistema de métricas.

    Diseño
    ------
    • Idempotente: start() no falla si ya está iniciado
    • Fail-soft: errores loguean WARNING, nunca propagan
    • Stateful: expone .started para introspección
    • Thread-safe: lock en start()
    """

    def __init__(
        self,
        enabled:  bool             = False,
        port:     int              = 8000,
        mode:     MetricsMode      = MetricsMode.PULL,
        gateway:  str              = "localhost:9091",
        registry: CollectorRegistry = REGISTRY,
    ) -> None:
        self.enabled  = enabled
        self.port     = port
        self.mode     = mode
        self.gateway  = gateway
        self.registry = registry

        self._started  = False
        self._lock     = threading.Lock()

    @classmethod
    def from_config(cls, metrics_cfg) -> "MetricsRuntime":
        """Construye MetricsRuntime desde MetricsConfig (schema.py)."""
        mode_str = getattr(metrics_cfg, "mode", "pull")
        try:
            mode = MetricsMode(mode_str)
        except ValueError:
            mode = MetricsMode.PULL

        return cls(
            enabled  = metrics_cfg.enabled,
            port     = metrics_cfg.port,
            mode     = mode,
            gateway  = getattr(metrics_cfg, "gateway", "localhost:9091"),
            registry = REGISTRY,
        )

    @property
    def started(self) -> bool:
        return self._started

    def start(self, *, validate_only: bool = False) -> bool:
        """
        Levanta el servidor HTTP de métricas (modo pull).

        Parámetros
        ----------
        validate_only : si True, no levanta servidor — sistema en modo validación.

        Retorna True si el servidor quedó activo, False si no.
        """
        if not self.enabled:
            _log.debug("metrics_disabled")
            return False

        if validate_only:
            _log.debug("metrics_skipped | reason=validate_only")
            return False

        if self.mode == MetricsMode.PUSH:
            _log.debug("metrics_skipped | reason=push_only_mode")
            return False

        with self._lock:
            if self._started:
                _log.debug("metrics_already_started | port={}", self.port)
                return True

            try:
                from prometheus_client import start_http_server
                start_http_server(self.port, registry=self.registry)
                self._started = True
                _log.bind(port=self.port, mode=self.mode).info("metrics_server_started")
                return True
            except OSError as exc:
                _log.bind(port=self.port, error=str(exc)).warning("metrics_server_failed")
                return False

    def push(self, exchange: str = "local") -> bool:
        """
        Empuja métricas al Pushgateway (modo push/hybrid).

        SafeOps: nunca propaga excepción.
        Retorna True si push exitoso.
        """
        if not self.enabled:
            return False

        if self.mode == MetricsMode.PULL:
            return False

        from prometheus_client import push_to_gateway

        job = f"ocm_pipeline_{exchange}"
        try:
            from infra.observability.server import PIPELINE_LAST_RUN, PIPELINE_HEARTBEAT
            PIPELINE_LAST_RUN.labels(exchange=exchange).set(_time.time())
            PIPELINE_HEARTBEAT.labels(exchange=exchange).inc()
            push_to_gateway(self.gateway, job=job, registry=self.registry, timeout=5)
            _log.bind(job=job, gateway=self.gateway).debug("metrics_pushed")
            return True
        except Exception as exc:
            _log.bind(job=job, gateway=self.gateway, error=str(exc)).warning("metrics_push_failed")
            return False

    def shutdown(self) -> None:
        """
        Limpieza explícita del subsistema.
        prometheus_client no expone stop() — registramos el estado.
        """
        if self._started:
            _log.bind(port=self.port).debug("metrics_runtime_shutdown")
            self._started = False


# Singleton de proceso — único MetricsRuntime por proceso
_runtime: Optional[MetricsRuntime] = None


def get_metrics_runtime() -> Optional[MetricsRuntime]:
    """Acceso al runtime activo. None si aún no inicializado."""
    return _runtime


def init_metrics_runtime(cfg, *, validate_only: bool = False) -> MetricsRuntime:
    """
    Inicializa y arranca el MetricsRuntime desde AppConfig.

    Llama una sola vez desde main.py. Seguro llamar múltiples veces
    — start() es idempotente.
    """
    global _runtime
    obs = getattr(cfg, "observability", None)
    if obs is None or obs.metrics is None:
        _log.debug("metrics_config_missing")
        _runtime = MetricsRuntime(enabled=False)
        return _runtime

    _runtime = MetricsRuntime.from_config(obs.metrics)
    _runtime.start(validate_only=validate_only)
    return _runtime
