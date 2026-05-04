# -*- coding: utf-8 -*-
"""
dagster_assets/resources.py
============================

Recursos Dagster para OrangeCashMachine.

Responsabilidad única (SRP)
---------------------------
Construir y exponer el RuntimeContext como recurso inyectable.
Los assets dependen de esta abstracción — nunca construyen AppConfig
directamente (DIP).

Diseño
------
OCMResource es el Composition Root de Dagster:
  - Lee OCM_ENV igual que RunConfig.from_env()
  - Construye AppConfig vía load_appconfig_standalone()
  - Expone RuntimeContext inmutable a todos los assets

SafeOps
-------
- Si la config falla, el recurso lanza en setup_for_execution()
  (Fail-Fast en startup, no mid-run).
- Dagster muestra el error en el asset catalog antes de materializar.

Principios: DIP · SRP · Fail-Fast · SafeOps · SSOT
"""
from __future__ import annotations

from datetime import datetime, timezone

from dagster import ConfigurableResource

from ocm_platform.config.hydra_loader import load_appconfig_standalone
from ocm_platform.runtime.run_config import RunConfig
from ocm_platform.runtime.context import RuntimeContext


class OCMResource(ConfigurableResource):
    """
    Recurso Dagster que expone RuntimeContext a los assets.

    Parámetros configurables vía dagster.yaml o launchpad:
      env: Entorno OCM (development | production | test).
           Si vacío, RunConfig.from_env() resuelve OCM_ENV.

    Uso en asset:
        @asset
        def my_asset(context: AssetExecutionContext, ocm: OCMResource):
            runtime_ctx = ocm.runtime_context
            config = runtime_ctx.app_config
    """

    env: str = ""  # vacío = delegar a OCM_ENV / settings.yaml

    def build_runtime_context(self) -> RuntimeContext:
        """
        Construye RuntimeContext desde variables de entorno.

        Fail-Fast: lanza ConfigurationError si AppConfig es inválido.
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

        No se cachea a nivel de instancia — cada asset call construye
        un contexto fresco con run_id único (SSOT: run_id en RunConfig).
        """
        return self.build_runtime_context()
