# ==============================================================================
# OrangeCashMachine — Config Layers Package
# ==============================================================================
#
# Cada módulo en este package = UNA capa del ConfigPipeline.
#
# ORDEN CANÓNICO (no modificar):
#   coercion.py      → L3: motor canónico OmegaConf → dict nativo (SSOT bool/None)
#   env_override.py  → L2: mutación por OCM_* env vars
#   validation.py    → L4: validación Pydantic (tipos únicamente)
#   rules.py         → L5: reglas de negocio cross-field
#
# L1 (Hydra compose) se orquesta en pipeline.py::_l1_hydra_compose().
# L3 tiene módulo propio (coercion.py) — SRP, no vive en pipeline.py.
# ==============================================================================
