# -*- coding: utf-8 -*-
"""
infrastructure/dagster/resources.py
====================================

Recursos Dagster para OrangeCashMachine.

OCMResource — Composition Root de Dagster
------------------------------------------
Responsabilidad única: construir y exponer RuntimeContext y OHLCVStorage
como recursos inyectables. Los assets dependen de esta abstracción — nunca
construyen AppConfig, IcebergStorageFactory ni IcebergStorage directamente.

  build_runtime_context() → RuntimeContext (config + run_id)
  get_storage(exchange, market_type) → OHLCVStorage (via StorageFactoryPort)

Caché por instancia
-------------------
OCMResource se instancia UNA VEZ por run de Dagster.
runtime_context y _storage_factory se cachean con PrivateAttr (Pydantic v2):
  - run_id consistente a través de todos los accesos al asset en ese run.
  - PrivateAttr es el contrato Pydantic v2 correcto para estado mutable
    en subclases de BaseModel (ConfigurableResource hereda de él).

SafeOps
-------
Si la config falla → lanza en build_runtime_context() (Fail-Fast en startup).
Dagster muestra el error en el asset catalog antes de materializar.

Principios: DIP · SRP · Fail-Fast · SafeOps · SSOT · Composition Root
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from dagster import ConfigurableResource
from pydantic import PrivateAttr

from ocm.config.hydra_loader import load_appconfig_standalone
from ocm.runtime.run_config  import RunConfig
from ocm.runtime.context     import RuntimeContext
from market_data.ports.outbound.storage         import OHLCVStorage
from market_data.ports.outbound.storage_factory import StorageFactoryPort


class OCMResource(ConfigurableResource):
    """
    Recurso Dagster que expone RuntimeContext y OHLCVStorage a los assets.

    Parámetros configurables vía dagster.yaml o launchpad:
      env: Entorno OCM (development | production | test).
           Si vacío, RunConfig.from_env() resuelve OCM_ENV en tiempo de ejecución.

    Caché
    -----
    _runtime_context y _storage_factory se inicializan lazy y se cachean
    por instancia.  En Dagster, OCMResource se instancia una vez por run →
    run_id idéntico en todos los accesos dentro del mismo run.

    Uso en asset
    ------------
        @asset
        def my_asset(context, ocm: OCMResource):
            runtime_ctx = ocm.runtime_context
            storage     = ocm.get_storage("bybit", "spot")
    """

    env: str = ""  # vacío = delegar a OCM_ENV en ejecución (SSOT)

    # PrivateAttr: estado mutable en un BaseModel Pydantic v2.
    # No forman parte del esquema serializable del recurso.
    _runtime_context: Optional[RuntimeContext] = PrivateAttr(default=None)
    _storage_factory: Optional[StorageFactoryPort] = PrivateAttr(default=None)

    # ── RuntimeContext ────────────────────────────────────────────────────────

    def build_runtime_context(self) -> RuntimeContext:
        """
        Construye RuntimeContext desde variables de entorno.

        Lee OCM_ENV en tiempo de EJECUCIÓN (no import-time) → SSOT correcto.
        Fail-Fast: lanza si AppConfig es inválido.
        """
        explicit_env = self.env or None
        run_cfg      = RunConfig.from_env(explicit_env=explicit_env)
        app_cfg      = load_appconfig_standalone(env=run_cfg.env)

        return RuntimeContext(
            app_config = app_cfg,
            run_config = run_cfg,
            started_at = datetime.now(timezone.utc),
        )

    @property
    def runtime_context(self) -> RuntimeContext:
        """
        RuntimeContext cacheado por instancia.

        Garantiza run_id consistente a través de todos los accesos
        dentro del mismo run de Dagster.
        """
        if self._runtime_context is None:
            self._runtime_context = self.build_runtime_context()
        return self._runtime_context

    # ── OHLCVStorage — Composition Root (DIP) ────────────────────────────────

    def get_storage(
        self,
        exchange:    str,
        market_type: str  = "spot",
        dry_run:     bool = False,
    ) -> OHLCVStorage:
        """
        Retorna OHLCVStorage para (exchange, market_type) via StorageFactoryPort.

        DIP: los assets piden storage aquí — nunca instancian IcebergStorageFactory.
        Fail-Fast: lanza RuntimeError si el backend no puede inicializarse.
        """
        factory: StorageFactoryPort = self._get_storage_factory()
        return factory.get_storage(
            exchange    = exchange,
            market_type = market_type,
            dry_run     = dry_run,
        )

    def _get_storage_factory(self) -> StorageFactoryPort:
        """
        Retorna la factory de storage, cacheada por instancia.

        Import lazy: no carga Iceberg en import-time.
        PrivateAttr garantiza que la asignación es válida en Pydantic v2.
        """
        if self._storage_factory is None:
            from market_data.adapters.outbound.storage.iceberg_factory import (
                IcebergStorageFactory,
            )
            self._storage_factory = IcebergStorageFactory()
        return self._storage_factory
