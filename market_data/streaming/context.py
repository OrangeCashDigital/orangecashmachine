from __future__ import annotations

"""
market_data/streaming/context.py
==================================

StreamingContext — contexto ligero para el path event-driven.

Problema que resuelve
---------------------
RuntimeContext lleva AppConfig completo (exchanges, credenciales,
storage, pipeline). Serializar eso en cada evento Redis Stream
produce mensajes de 10-50 KB con datos sensibles en tránsito.

Para el path event-driven solo se necesita:
  - env           → qué entorno está activo
  - run_id        → trazabilidad cruzada OCM ↔ Prefect
  - pushgateway   → dónde empujar métricas
  - deployment    → qué deployment de Prefect disparar

StreamingContext es serializable, inmutable y no contiene
credenciales ni config pesada.

Contrato
--------
  ctx  = StreamingContext.from_runtime(runtime_context)
  d    = ctx.to_dict()
  ctx2 = StreamingContext.from_dict(d)
  assert ctx == ctx2

Principios: SRP · inmutable · serializable · sin credenciales
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.config.runtime_context import RuntimeContext


@dataclass(frozen=True)
class StreamingContext:
    """
    Contexto mínimo para el path event-driven.

    Attributes
    ----------
    env             : Entorno activo (development | staging | production).
    run_id          : Identificador único del run OCM (trazabilidad).
    pushgateway     : host:port del Prometheus Pushgateway.
    deployment      : Nombre del deployment Prefect a disparar.
    created_at      : Momento de construcción (UTC ISO 8601).
    context_version : Versión del schema de este contexto.
    """

    env:             str
    run_id:          str
    pushgateway:     str
    deployment:      str
    created_at:      str
    context_version: int = 1  # SSoT: CONTEXT_SCHEMA_VERSION de payloads.py

    # --------------------------------------------------
    # Constructors
    # --------------------------------------------------

    @classmethod
    def from_runtime(
        cls,
        ctx: "RuntimeContext",
        deployment: str = "market_data_ingestion/default",
    ) -> "StreamingContext":
        """
        Construye un StreamingContext a partir de un RuntimeContext completo.

        El RuntimeContext NO se almacena — solo se extraen los campos
        necesarios. Rompe la dependencia en tiempo de ejecución entre
        el path streaming y AppConfig.
        """
        return cls(
            env         = ctx.environment,
            run_id      = ctx.run_id,
            pushgateway = ctx.pushgateway,
            deployment  = deployment,
            created_at  = datetime.now(timezone.utc).isoformat(),
        )

    @classmethod
    def from_env(
        cls,
        deployment: str = "market_data_ingestion/default",
    ) -> "StreamingContext":
        """
        Construye un StreamingContext desde variables de entorno.

        Útil cuando no hay RuntimeContext disponible (workers standalone,
        tests de integración, scripts).
        """
        import os
        from core.config.loader.env_resolver import resolve_env

        env         = resolve_env()
        _pgw_raw    = os.getenv("PUSHGATEWAY_HOST_PORT", "localhost:9091")
        # Normalizar a host:port — la env var puede contener solo el puerto
        pushgateway = _pgw_raw if ":" in _pgw_raw else f"localhost:{_pgw_raw}"
        run_id      = uuid.uuid4().hex[:12]

        return cls(
            env         = env,
            run_id      = run_id,
            pushgateway = pushgateway,
            deployment  = deployment,
            created_at  = datetime.now(timezone.utc).isoformat(),
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StreamingContext":
        """Deserializa desde dict (ej: payload de Redis Stream).

        Valida context_version — falla explícitamente si el schema
        es incompatible en lugar de producir datos corruptos.
        """
        from market_data.streaming.payloads import (
            CONTEXT_SCHEMA_VERSION,
            SchemaVersionError,
        )
        version = int(data.get("context_version", 1))
        if version != CONTEXT_SCHEMA_VERSION:
            raise SchemaVersionError(
                f"StreamingContext schema v{version} incompatible "
                f"con v{CONTEXT_SCHEMA_VERSION} esperada."
            )
        return cls(
            env             = str(data["env"]),
            run_id          = str(data["run_id"]),
            pushgateway     = str(data["pushgateway"]),
            deployment      = str(data["deployment"]),
            created_at      = str(data["created_at"]),
            context_version = version,
        )

    # --------------------------------------------------
    # Serialization
    # --------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Serializa a dict plano — seguro para Redis Streams / JSON."""
        return {
            "context_version": self.context_version,
            "env":             self.env,
            "run_id":          self.run_id,
            "pushgateway":     self.pushgateway,
            "deployment":      self.deployment,
            "created_at":      self.created_at,
        }
