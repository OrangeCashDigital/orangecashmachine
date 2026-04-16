# ==============================================================================
# OrangeCashMachine — L4: Pydantic Validation Layer
# ==============================================================================
#
# RESPONSABILIDAD ÚNICA: tipos y estructura.
#
# LO QUE NO HACE ESTA CAPA:
#   ✗ Reglas de negocio cross-field (→ L5 rules.py)
#   ✗ Defaults que contradigan Hydra (→ Hydra es SSOT de defaults)
#   ✗ Mutación post-validación
#
# PRINCIPIO:
#   Los defaults Pydantic aquí son "fallback estructural" para campos
#   opcionales que Hydra no gestiona — NUNCA precedencia sobre L1/L2.
# ==============================================================================

from __future__ import annotations

from typing import Any

from loguru import logger


def validate_and_coerce(raw_dict: dict[str, Any]) -> Any:
    """
    Convierte plain dict → AppConfig typed y validado.

    Raises:
        ValidationError: si tipos o estructura no son correctos.
        ConfigPipelineError: wrapeado por el pipeline con stage=VALIDATED.
    """
    from core.config.schema import AppConfig  # SSOT — schema.py, no models

    app_config = AppConfig.model_validate(raw_dict)
    logger.debug("config_pipeline_l4 | pydantic=ok env={}", app_config.environment.name)
    return app_config
