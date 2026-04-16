# ==============================================================================
# OrangeCashMachine — Config Layers Package
# ==============================================================================
#
# Cada módulo en este package = UNA capa del ConfigPipeline.
#
# ORDEN CANÓNICO (no modificar):
#   env_override.py  → L2: mutación por OCM_* env vars
#   validation.py    → L4: validación Pydantic (tipos únicamente)
#   rules.py         → L5: reglas de negocio cross-field
#
# L1 (Hydra) y L3 (OmegaConf coercion) están en pipeline.py directamente
# porque no tienen lógica propia — son pasos de infraestructura.
# ==============================================================================
