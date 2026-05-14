# ==============================================================================
# OrangeCashMachine — L3: Canonical Coercion Engine
# ==============================================================================
#
# RESPONSABILIDAD ÚNICA:
#   Convertir el plain dict post-OmegaConf a tipos Python nativos seguros ANTES
#   de que Pydantic lo vea. Este es el ÚNICO lugar del sistema donde se normalizan
#   strings a bool/None.
#
# PRINCIPIOS:
#   SSOT     — una sola función de coerción; nadie más parsea strings
#   SRP      — solo coerción segura, sin validación ni reglas de negocio
#   OCP      — extender añadiendo paths a _NULLABLE_PATHS; sin tocar la lógica
#   DRY      — elimina los 5× coerce_env_strings duplicados en schema.py
#   Fail-Fast — string bool no reconocido no es error en L3; Pydantic falla en L4
#   KISS     — recursivo simple; sin alias ni mappings especiales
#
# GARANTÍA DE CONTRATO (post-fix):
#   L3 solo convierte lo que es semánticamente inequívoco:
#     - "true"/"false"/... → bool   (significado universal, sin ambigüedad)
#     - ""  en _NULLABLE_PATHS → None
#
#   L3 NO convierte int ni float. Pydantic (L4) es la única fuente de verdad
#   para los tipos numéricos: el schema declara `port: int`, `timeout: float`,
#   `password: str`. Pydantic coerciona desde str con pleno conocimiento del
#   campo de destino. Esto evita que "101" (password) se convierta a 101 (int)
#   antes de llegar a un campo declarado como str.
#
#   Antes del fix, "101" → int(101) en L3 → Pydantic recibía un int donde
#   esperaba str → ValidationError. Fix: eliminar coerción numérica de L3.
#
# NULLABLE PATHS (path-based — robusto ante campos nuevos):
#   Cualquier campo Optional[X] que Hydra/oc.env pueda resolver como string
#   vacío "" debe listarse aquí.
#
#   OCP: añadir paths sin modificar la lógica de coerce_scalar_values().
#
# FLUJO EN pipeline.py L3:
#   OmegaConf.to_container()
#     → strip_hydra_internals()    (hydra_loader.py — sin cambios)
#     → coerce_scalar_values()     ← ESTE MÓDULO
#     → Pydantic L4               (recibe strings limpios; coerce numérico en schema)
# ==============================================================================

from __future__ import annotations

from typing import Any

# loguru importado de forma lazy para evitar circular:
#   coercion → loguru → stdlib logging → core/logging/ → loguru
# core/logging/ sombrea el stdlib logging — el import lazy rompe el ciclo.
def _get_logger():  # noqa: ANN201
    from loguru import logger as _logger  # lazy — solo al primer log
    return _logger

# ---------------------------------------------------------------------------
# Constantes de coerción — SSOT para todo el sistema.
# Nadie más define estas constantes; todos importan desde aquí.
# Sincronizadas con L2 env_override.py (_BOOL_TRUE/_BOOL_FALSE).
# ---------------------------------------------------------------------------

# "1" y "0" eliminados intencionalmente — son ambiguos sin contexto de schema:
#   - redis.db="1"      → debe ser int 1 (Pydantic coerce en L4)
#   - enabled="1"       → podría ser bool True (pero "true"/"yes" son más explícitos)
#   - version="1"       → debe quedar str "1"
# L3 solo coerciona lo que es inequívoco semánticamente sin conocer el tipo destino.
# "true"/"false"/"yes"/"no"/"on"/"off" son inequívocamente booleanos en cualquier
# contexto. "1"/"0" no lo son — delegar coerción al schema (L4, Pydantic, SSOT).
BOOL_TRUE:  frozenset[str] = frozenset({"true",  "yes", "on"})
BOOL_FALSE: frozenset[str] = frozenset({"false", "no",  "off"})

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
    """Coerciona in-place un dict anidado a tipos Python nativos seguros.

    Recorre recursivamente el dict y convierte ÚNICAMENTE lo que es
    semánticamente inequívoco sin conocer el tipo de destino del campo:

      - str "true"/"false"/... → bool   (BOOL_TRUE ∪ BOOL_FALSE)
      - str ""  en _NULLABLE_PATHS → None
      - Cualquier otro str    → str    (sin modificar — Pydantic coerce en L4)
      - No-str ya nativo      → sin cambios

    IMPORTANTE — por qué NO convertimos int/float aquí:
        L3 no conoce el schema de destino. Un campo declarado `password: str`
        y un campo declarado `port: int` pueden recibir el mismo string "6379".
        Convertir a int en L3 rompe el campo str; dejar como str permite que
        Pydantic coercione port a int y valide password como str en L4, donde
        el schema es la única fuente de verdad para los tipos.

    Args:
        d:     Dict a coercionar (mutado in-place).
        _path: Path acumulado desde la raíz (uso interno de recursión).
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
            continue  # ya es tipo nativo (int/float/bool de YAML) — nada que hacer

        # ── string vacío en campo nullable ────────────────────────────────
        if value == "" and (current_path in _NULLABLE_PATHS or (key,) in _NULLABLE_PATHS):
            d[key] = None
            _get_logger().debug("coerce_scalar | path={} '' → None", ".".join(current_path))
            continue

        # ── bool inequívoco ───────────────────────────────────────────────
        coerced = _coerce_string(value, current_path)
        if coerced is not value:   # identidad: cambió
            d[key] = coerced
            _get_logger().debug(
                "coerce_scalar | path={} {!r} → {!r} ({})",
                ".".join(current_path),
                value,
                coerced,
                type(coerced).__name__,
            )


def _coerce_list(lst: list[Any], path: tuple[str, ...]) -> None:
    """Coerciona in-place elementos de una lista (recursivo en sub-dicts)."""
    for i, item in enumerate(lst):
        if isinstance(item, dict):
            coerce_scalar_values(item, path + (str(i),))
        elif isinstance(item, str):
            lst[i] = _coerce_string(item, path + (str(i),))


def _coerce_string(value: str, path: tuple[str, ...]) -> Any:  # noqa: ARG001
    """Convierte un string al tipo nativo inequívoco, o lo devuelve sin cambios.

    Solo convierte booleans semánticos (BOOL_TRUE ∪ BOOL_FALSE).
    Cualquier otro string se devuelve intacto para que Pydantic lo procese
    en L4 con conocimiento del tipo de destino declarado en el schema.

    Int/Float: deliberadamente excluidos.
        Pydantic con `str` strict=False acepta "101" como str.
        Pydantic con `int` acepta "6379" como int.
        Pydantic con `float` acepta "0.5" como float.
        L3 no tiene esa información — no debe adivinar.
    """
    stripped = value.strip()
    lower    = stripped.lower()

    # ── bool inequívoco ───────────────────────────────────────────────────
    if lower in BOOL_TRUE:
        return True
    if lower in BOOL_FALSE:
        return False

    # ── str fallback — Pydantic decide en L4 ─────────────────────────────
    return value
