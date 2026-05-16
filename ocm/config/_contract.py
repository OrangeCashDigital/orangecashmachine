# ==============================================================================
# ocm/config/_contract.py — Contrato de interfaz pública del bounded context
# ==============================================================================
#
# PROPÓSITO:
#   Importar explícitamente TODOS los símbolos declarados en __all__ de cada
#   módulo público. Si algún símbolo es eliminado o renombrado sin actualizar
#   este archivo, el import falla en CI antes de llegar a producción.
#
#   Este módulo convierte "rompí la interfaz pública" de un fallo silencioso
#   en un error de import-time detectable en el primer pytest.
#
# USO EN CI:
#   tests/ocm/config/test_exception_wrapping.py::test_public_contract_importable
#   importlib.import_module("ocm.config._contract")
#
# MANTENIMIENTO:
#   Si __all__ de cualquier módulo cambia → actualizar este archivo en el
#   mismo commit. Es el registro canónico de la superficie pública.
#
# PRINCIPIO DIP:
#   Los consumidores dependen de este contrato, no de los módulos de
#   implementación directamente — el contrato absorbe el cambio.
# ==============================================================================
from __future__ import annotations

# ── Jerarquía de excepciones (SSOT: loader/exceptions.py) ────────────────────
from ocm.config.loader.exceptions import (  # noqa: F401
    ConfigurationError,
    ConfigFileNotFoundError,
    ConfigParseError,
    ConfigValidationError,
)

# ── Pipeline L1→L5 (SSOT: pipeline.py) ───────────────────────────────────────
from ocm.config.pipeline import (  # noqa: F401
    ConfigPipeline,
    ConfigPipelineError,
    ConfigStage,
    ConfigTransition,
)

# ── L3: Coerción canónica (SSOT: layers/coercion.py) ─────────────────────────
from ocm.config.layers.coercion import (  # noqa: F401
    BOOL_TRUE,
    BOOL_FALSE,
    coerce_scalar_values,
    coerce_string,
)

# ── L2: Override de entorno (SSOT: layers/env_override.py) ───────────────────
from ocm.config.layers.env_override import (  # noqa: F401
    apply_env_overrides,
)

# ── L4: Validación Pydantic (SSOT: layers/validation.py) ─────────────────────
from ocm.config.layers.validation import (  # noqa: F401
    validate_config,
)

# ── L5: Reglas de negocio (SSOT: layers/rules.py) ────────────────────────────
from ocm.config.layers.rules import (  # noqa: F401
    ConfigRuleViolation,
    apply_business_rules,
)

# ── Loaders públicos (SSOT: hydra_loader.py) ─────────────────────────────────
from ocm.config.hydra_loader import (  # noqa: F401
    hydra_cfg_to_appconfig,
    load_appconfig_from_hydra,
    load_appconfig_standalone,
)

# ── Facade del bounded context (SSOT: __init__.py) ───────────────────────────
from ocm.config import (  # noqa: F401
    ConfigurationError      as _ce,    # re-export verificado
    ConfigValidationError   as _cve,   # re-export verificado
    ConfigPipelineError     as _cpe,   # re-export verificado
)
