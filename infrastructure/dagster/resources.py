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

SafeOps
-------
Si la config falla → lanza en build_runtime_context() (Fail-Fast en startup).
Dagster muestra el error en el asset catalog antes de materializar.

Principios: DIP · SRP · Fail-Fast · SafeOps · SSOT · Composition Root
"""
from __future__ import annotations

from datetime import datetime, timezone

from dagster import ConfigurableResource

from ocm_platform.config.hydra_loader import load_appconfig_standalone
from ocm_platform.runtime.run_config  import RunConfig
from ocm_platform.runtime.context     import RuntimeContext
from market_data.ports.outbound.storage         import OHLCVStorage
from market_data.ports.outbound.storage_factory import StorageFactoryPort


class OCMResource(ConfigurableResource):
    """
    Recurso Dagster que expone RuntimeContext y OHLCVStorage a los assets.

    Parámetros configurables vía dagster.yaml o launchpad:
      env: Entorno OCM (development | production | test).
           Si vacío, RunConfig.from_env() resuelve OCM_ENV.

    Uso en asset
    ------------
        @asset
        def my_asset(context, ocm: OCMResource):
            runtime_ctx = ocm.runtime_context
            storage     = ocm.get_storage("bybit", "spot")
    """

    env: str = ""  # vacío = delegar a OCM_ENV / settings.yaml

    # ── RuntimeContext ────────────────────────────────────────────────────────

    def build_runtime_context(self) -> RuntimeContext:
        """
        Construye RuntimeContext desde variables de entorno.

        Fail-Fast: lanza si AppConfig es inválido.
        Dagster captura la excepción y la muestra en el asset catalog.
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
        RuntimeContext construido bajo demanda.

        No cacheado — cada asset call recibe run_id único (SSOT: RunConfig).
        """
        return self.build_runtime_context()

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
        Cache: gestionado por IcebergStorageFactory (SSOT — sin cache duplicado).

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
        Retorna la factory de storage. Import lazy — no carga Iceberg en import-time.

        Singleton a nivel de instancia de OCMResource.
        En Dagster, OCMResource se instancia una vez por run → safe.
        """
        if not hasattr(self, "_storage_factory"):
            from market_data.adapters.outbound.storage.iceberg_factory import IcebergStorageFactory
            # object.__setattr__ porque ConfigurableResource puede ser frozen-like
            object.__setattr__(self, "_storage_factory", IcebergStorageFactory())
        return self._storage_factory
