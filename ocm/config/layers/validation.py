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
#   ✗ Wrappear ConfigPipelineError — eso lo hace pipeline._l4_validate
#
# PRINCIPIO:
#   Los defaults Pydantic aquí son "fallback estructural" para campos
#   opcionales que Hydra no gestiona — NUNCA precedencia sobre L1/L2.
# ==============================================================================

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from loguru import logger

if TYPE_CHECKING:
    from ocm.config.schema import AppConfig

__all__ = ["validate_config", "validate_and_coerce"]


def validate_config(raw_dict: dict[str, Any]) -> "AppConfig":
    """Convierte plain dict → AppConfig typed y validado.

    Responsabilidad ÚNICA: validar tipos y estructura contra el schema Pydantic.
    Sin reglas de negocio — esas son responsabilidad de L5 (rules.py).

    Args:
        raw_dict: Dict post-coerción (output de L3).

    Returns:
        AppConfig validado, inmutable post-construcción.

    Raises:
        pydantic.ValidationError: Si tipos o estructura no son correctos.
            Nota: pipeline._l4_validate wrappea esto en ConfigPipelineError.
    """
    from ocm.config.schema import AppConfig  # SSOT — schema.py, no models

    app_config = AppConfig.model_validate(raw_dict)
    logger.debug("config_pipeline_l4 | pydantic=ok env={}", app_config.environment.name)
    return app_config


# ---------------------------------------------------------------------------
# Alias de compatibilidad — el nombre validate_and_coerce era semánticamente
# incorrecto (L4 no coerciona; coerción es responsabilidad de L3). Conservado
# para no romper callers existentes durante la transición.
# Deprecated: usar validate_config directamente.
# ---------------------------------------------------------------------------
validate_and_coerce = validate_config
