# -*- coding: utf-8 -*-
"""
infrastructure/bootstrap/container.py
======================================

OCMContainer — Composition Root del sistema market_data.

Responsabilidad única
---------------------
Construir y cablear TODAS las dependencias concretas del sistema.
Es el único lugar donde se instancian clases de infraestructura.

Regla de dependencias (DIP)
---------------------------
application/  → consume puertos (interfaces)
infrastructure/ → provee implementaciones
container.py  → cablea ambos

Resultado
---------
  ┌─────────────────────────────────────────────────────────┐
  │  OCMContainer                                           │
  │  ├─ orchestrator   : PipelineOrchestrator               │
  │  ├─ resample_uc    : ResampleUseCase                    │
  │  ├─ storage_factory: StorageFactoryPort impl            │
  │  └─ (internal)                                          │
  │      ├─ redis_client                                     │
  │      ├─ cursor_store                                     │
  │      └─ snapshot_manager                                 │
  └─────────────────────────────────────────────────────────┘

Uso (Dagster resource)
----------------------
  container = OCMContainer.from_app_config(app_cfg)
  orchestrator = container.orchestrator
  summary = await orchestrator.run(request)

Principios: SRP · DIP · SSOT · KISS · Fail-Fast en construcción.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from ocm.config.hydra_loader import AppConfig


# =============================================================================
# Helpers de construcción — privados al módulo
# =============================================================================

def _build_redis_client(cfg: "AppConfig") -> Optional[object]:
    """
    Construye cliente Redis desde configuración.
    Retorna None si Redis no está configurado (Fail-Soft para entornos sin Redis).
    """
    try:
        from infrastructure.timeouts import Timeouts
        import redis

        redis_cfg = getattr(cfg, "redis", None)
        if redis_cfg is None:
            logger.info("Container: Redis no configurado — CursorStore usará memoria")
            return None

        client = redis.Redis(
            host             = redis_cfg.host,
            port             = redis_cfg.port,
            db               = getattr(redis_cfg, "db", 0),
            password         = getattr(redis_cfg, "password", None),
            socket_connect_timeout = Timeouts.REDIS_CONNECT_S,
            socket_timeout         = Timeouts.REDIS_OPERATION_S,
            decode_responses       = True,
        )
        # Ping de verificación — Fail-Fast si Redis está caído en inicio
        client.ping()
        logger.info("Container: Redis conectado ({}:{})", redis_cfg.host, redis_cfg.port)
        return client
    except ImportError:
        logger.warning("Container: redis-py no instalado — CursorStore en memoria")
        return None
    except Exception:
        logger.opt(exception=True).warning(
            "Container: Redis no disponible — CursorStore en memoria (Fail-Soft)"
        )
        return None


def _build_cursor_store(
    redis_client: Optional[object],
    exchange:     str,
    market_type:  str,
) -> object:
    """Construye CursorStore con backend Redis o memoria."""
    from infrastructure.storage.iceberg.cursor_store import CursorStore
    return CursorStore(
        exchange     = exchange,
        market_type  = market_type,
        redis_client = redis_client,
    )


def _build_snapshot_manager(exchange: str, market_type: str) -> object:
    from infrastructure.storage.iceberg.snapshot_manager import SnapshotManager
    return SnapshotManager(exchange=exchange, market_type=market_type)


# =============================================================================
# OCMContainer — Composition Root
# =============================================================================

@dataclass
class OCMContainer:
    """
    Composition Root del sistema market_data.

    Construir via from_app_config() en producción.
    Construir directamente en tests de integración pasando mocks.

    Atributos públicos (puertos — no implementaciones concretas)
    -----------------------------------------------------------
    orchestrator   : PipelineOrchestrator — caso de uso de ingesta.
    resample_uc    : ResampleUseCase      — caso de uso de resample.

    Atributos internos
    ------------------
    _redis_client  : cliente Redis (o None).
    _app_cfg       : configuración completa de la aplicación.
    """

    orchestrator:  object = field(init=False)
    resample_uc:   object = field(init=False)

    _app_cfg:      Optional["AppConfig"] = field(default=None, repr=False)
    _redis_client: Optional[object]      = field(default=None, repr=False, init=False)

    def __post_init__(self) -> None:
        """Construcción en orden de dependencias (dependency-satisfaction)."""
        logger.info("OCMContainer: inicializando Composition Root")

        # 1. Infraestructura de bajo nivel (sin dependencias)
        self._redis_client = (
            _build_redis_client(self._app_cfg)
            if self._app_cfg is not None
            else None
        )

        # 2. Use cases — no dependen de infra directamente (DIP)
        from market_data.application import PipelineOrchestrator, ResampleUseCase
        from market_data.infrastructure.observability.metrics_adapter import (
            PrometheusResampleMetrics,
            PrometheusRepairMetrics,
        )
        from market_data.factories.pipeline_factory import ConcretePipelineFactory
        self.orchestrator = PipelineOrchestrator(factory=ConcretePipelineFactory())
        from market_data.adapters.outbound.storage.iceberg_factory import IcebergStorageFactory
        self.resample_uc  = ResampleUseCase(
            storage_factory = IcebergStorageFactory(),
            metrics         = PrometheusResampleMetrics(),
        )
        # Exponer factories de métricas para pipelines construidos externamente.
        self.resample_metrics = PrometheusResampleMetrics
        self.repair_metrics   = PrometheusRepairMetrics

        logger.info("OCMContainer: listo")

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def from_app_config(cls, app_cfg: "AppConfig") -> "OCMContainer":
        """
        Factory para producción: construye el container desde AppConfig.

        Fail-Fast: lanza ConfigurationError si la configuración es inválida.
        """
        from market_data.exceptions import ConfigurationError
        if app_cfg is None:
            raise ConfigurationError("OCMContainer.from_app_config: app_cfg es None")
        logger.info("OCMContainer: construyendo desde AppConfig")
        return cls(_app_cfg=app_cfg)

    @classmethod
    def for_testing(cls) -> "OCMContainer":
        """
        Factory para tests: sin Redis, sin config externa.
        Todos los backends en memoria.
        """
        logger.info("OCMContainer: modo testing (sin Redis, sin config externa)")
        return cls(_app_cfg=None)

    # ── Helpers de acceso a sub-servicios ─────────────────────────────────────

    def cursor_store_for(self, exchange: str, market_type: str) -> object:
        """Retorna un CursorStore para el exchange/market_type dado."""
        return _build_cursor_store(self._redis_client, exchange, market_type)

    def snapshot_manager_for(self, exchange: str, market_type: str) -> object:
        """Retorna un SnapshotManager para el exchange/market_type dado."""
        return _build_snapshot_manager(exchange, market_type)
