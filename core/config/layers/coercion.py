# ==============================================================================
# OrangeCashMachine — L3: Canonical Coercion Engine
# ==============================================================================
#
# RESPONSABILIDAD ÚNICA:
#   Convertir el plain dict post-OmegaConf a tipos Python nativos ANTES
#   de que Pydantic lo vea. Este es el ÚNICO lugar en todo el sistema
#   donde se parsean strings a bool/int/float/None.
#
# PRINCIPIOS:
#   SSOT   — una sola función de coerción; nadie más parsea strings
#   SRP    — solo coerción, sin validación ni reglas de negocio
#   OCP    — extender añadiendo paths a _NULLABLE_PATHS; sin tocar la lógica
#   DRY    — elimina los 5× coerce_env_strings duplicados en schema.py
#   Fail-Fast — string bool no reconocido → ValueError inmediato con contexto
#   KISS   — recursivo simple; sin alias ni mappings especiales
#
# GARANTÍA DE CONTRATO:
#   Cuando Pydantic recibe el dict, TODOS los campos son tipos nativos.
#   Ningún validator de schema.py necesita parsear strings.
#
# NULLABLE PATHS (path-based — robusto ante campos nuevos):
#   Cualquier campo cuyo path completo esté en _NULLABLE_PATHS se convierte
#   "" → None. Añadir paths aquí, nunca usar _NULLABLE_KEYS (key-name-based).
#
# FLUJO EN pipeline.py L3:
#   OmegaConf.to_container()
#     → strip_hydra_internals()    (hydra_loader.py — sin cambios)
#     → coerce_scalar_values()     ← ESTE MÓDULO (nuevo, reemplaza normalize_empty_strings)
#     → Pydantic L4               (recibe tipos limpios, sin coerce_env_strings)
# ==============================================================================

from __future__ import annotations

from typing import Any

from loguru import logger

# ---------------------------------------------------------------------------
# Constantes de coerción — SSOT para todo el sistema.
# Nadie más define estas constantes; todos importan desde aquí.
# Sincronizadas con L2 env_override.py (_BOOL_TRUE/_BOOL_FALSE).
# ---------------------------------------------------------------------------

BOOL_TRUE:  frozenset[str] = frozenset({"1", "true",  "yes", "on"})
BOOL_FALSE: frozenset[str] = frozenset({"0", "false", "no",  "off"})

# ---------------------------------------------------------------------------
# Nullable paths — path-based (robusto ante campos nuevos).
#
# Formato: tupla de strings representando el path completo desde la raíz.
# Ejemplo: ("integrations", "postgres", "password")
#
# REGLA: añadir aquí CUALQUIER campo Optional[X] que Hydra/oc.env pueda
# resolver como string vacío "". Key-name-based (_NULLABLE_KEYS) queda
# deprecado y eliminado — ver hydra_loader.py.
#
# OCP: añadir paths sin modificar la lógica de coerce_scalar_values().
# ---------------------------------------------------------------------------

_NULLABLE_PATHS: frozenset[tuple[str, ...]] = frozenset({
    # integrations.postgres
    ("integrations", "postgres", "user"),
    ("integrations", "postgres", "password"),
    ("integrations", "postgres", "database"),
    # integrations.redis
    ("integrations", "redis", "password"),
    # integrations.kafka
    ("integrations", "kafka", "password"),
    # exchanges — credenciales opcionales
    ("api_password",),   # dentro de cada ExchangeConfig (path relativo al nodo)
})


# ---------------------------------------------------------------------------
# Motor canónico
# ---------------------------------------------------------------------------

def coerce_scalar_values(
    d: dict[str, Any],
    _path: tuple[str, ...] = (),
) -> None:
    """Coerciona in-place un dict anidado a tipos Python nativos.

    Recorre recursivamente el dict y convierte:
      - str "true"/"false"/... → bool   (fail-fast si no reconocido en campo bool)
      - str "123"              → int    (solo si parseable sin pérdida)
      - str "1.5"              → float  (solo si parseable sin pérdida)
      - str ""  en _NULLABLE_PATHS → None
      - Cualquier otro str    → str    (sin modificar — Pydantic valida)

    Args:
        d:     Dict a coercionar (mutado in-place).
        _path: Path acumulado desde la raíz (uso interno de recursión).

    Raises:
        ValueError: Si un campo bool recibe un string no reconocido.
                    Fail-fast — mejor error en startup que bug silencioso.

    Nota sobre bool:
        No intentamos inferir si un campo "debería" ser bool — solo convertimos
        los valores que son inequívocamente booleanos (BOOL_TRUE ∪ BOOL_FALSE).
        Si el string no está en ninguno de los dos sets y el campo espera bool,
        Pydantic lo rechazará en L4 con ValidationError claro.
    """
    if not isinstance(d, dict):
        return

    for key, value in d.items():
        current_path = _path + (key,)

        if isinstance(value, dict):
            coerce_scalar_values(value, current_path)
            continue

        if isinstance(value, list):
            _coerce_list(value, current_path)
            continue

        if not isinstance(value, str):
            continue  # ya es tipo nativo — nada que hacer

        # ── string vacío en campo nullable ────────────────────────────────
        if value == "" and (current_path in _NULLABLE_PATHS or (key,) in _NULLABLE_PATHS):
            d[key] = None
            logger.debug("coerce_scalar | path={} '' → None", ".".join(current_path))
            continue

        # ── intentar coerción de tipo ─────────────────────────────────────
        coerced = _coerce_string(value, current_path)
        if coerced is not value:   # identidad: cambió
            d[key] = coerced
            logger.debug(
                "coerce_scalar | path={} {!r} → {!r} ({})",
                ".".join(current_path),
                value,
                coerced,
                type(coerced).__name__,
            )


def _coerce_list(lst: list[Any], path: tuple[str, ...]) -> None:
    """Coerciona in-place elementos de una lista (no recursivo en sub-dicts)."""
    for i, item in enumerate(lst):
        if isinstance(item, dict):
            coerce_scalar_values(item, path + (str(i),))
        elif isinstance(item, str):
            lst[i] = _coerce_string(item, path + (str(i),))


def _coerce_string(value: str, path: tuple[str, ...]) -> Any:
    """Convierte un string a su tipo nativo más apropiado.

    Orden de intentos: bool → int → float → str (fallback seguro).

    Bool:
        Solo convierte si el string está EXACTAMENTE en BOOL_TRUE o BOOL_FALSE.
        Un string "maybe" no es bool — se deja como str para que Pydantic falle
        con ValidationError si el campo espera bool. Fail-fast explícito.

    Int/Float:
        Intento optimista — si falla, se deja como str.
        No usamos coerción de float si el valor es int exacto (evita 6379 → 6379.0).
    """
    stripped = value.strip()
    lower    = stripped.lower()

    # ── bool ─────────────────────────────────────────────────────────────
    if lower in BOOL_TRUE:
        return True
    if lower in BOOL_FALSE:
        return False

    # ── int ───────────────────────────────────────────────────────────────
    try:
        int_val = int(stripped)
        # Rechazar floats que int() acepta (e.g. "6379.0" → int 6379 silencioso)
        if str(int_val) == stripped:
            return int_val
    except (ValueError, OverflowError):
        pass

    # ── float ─────────────────────────────────────────────────────────────
    try:
        return float(stripped)
    except (ValueError, OverflowError):
        pass

    # ── str fallback ──────────────────────────────────────────────────────
    return value
