# ==============================================================================
# OrangeCashMachine — L2: Environment Override Layer
# ==============================================================================
#
# RESPONSABILIDAD ÚNICA:
#   Aplicar OCM_* vars DESPUÉS de Hydra compose, ANTES de Pydantic.
#   Esta es la ÚNICA capa autorizada para mutar config con env vars en runtime.
#
# PRECEDENCIA DENTRO DE L2:
#   ${oc.env:VAR} en YAML → resuelta por Hydra en L1 (ya está en DictConfig)
#   OCM_VAR__KEY           → aplicada aquí, GANA sobre L1 para la misma clave
#
# CONVENCIÓN DE NOMBRES:
#   OCM_<SECCIÓN>__<CLAVE>  →  cfg.<sección>.<clave>
#   Separador: doble guión bajo (__)
#
# EJEMPLOS:
#   OCM_INTEGRATIONS__REDIS__ENABLED=false → integrations.redis.enabled = False
#   OCM_SAFETY__DRY_RUN=false             → safety.dry_run = False
#   OCM_PIPELINE__HISTORICAL__BACKFILL_MODE=true → pipeline.historical.backfill_mode = True
#
# REGLAS:
#   - Coerción bool únicamente — int/float delegados a Pydantic L4 (SSOT del schema)
#   - Claves malformadas → warning + skip (fail-soft en L2)
#   - Retorna nuevo DictConfig — NO muta el input (inmutabilidad)
#   - Retorna (DictConfig, int) — el int es el conteo de overrides aplicados
#
# COERCIÓN DE TIPOS EN L2:
#   L2 convierte ÚNICAMENTE booleans semánticos inequívocos ("true"/"false"/...).
#   Int/float se dejan como str para que Pydantic L4 los coercione con pleno
#   conocimiento del tipo de destino declarado en el schema.
#   Razón: "6379" podría ser port: int o api_key: str — L2 no puede saberlo.
#   Ref: core/config/layers/coercion.py — motor canónico, misma lógica.
# ==============================================================================

from __future__ import annotations

import os

from loguru import logger
from omegaconf import DictConfig, OmegaConf

# SSOT para constantes booleanas — importadas desde el motor canónico de coerción.
# L2 y L3 usan el mismo conjunto de strings reconocidos; sin duplicación ni riesgo
# de divergencia silenciosa entre capas.
# Ref: core/config/layers/coercion.py — BOOL_TRUE / BOOL_FALSE.
from ocm_platform.config.layers.coercion import BOOL_FALSE as _BOOL_FALSE
from ocm_platform.config.layers.coercion import BOOL_TRUE as _BOOL_TRUE

_OCM_PREFIX = "OCM_"
_SEPARATOR  = "__"


def apply_env_overrides(
    cfg: DictConfig,
    *,
    env: "os.Mapping[str, str] | None" = None,
) -> tuple[DictConfig, int]:
    """Aplica OCM_* env vars como overrides estructurados sobre el DictConfig.

    Itera sobre variables de entorno con prefijo OCM_, parsea su path
    estructurado (OCM_SECTION__KEY → section.key), coerciona el valor
    y merge sobre el DictConfig existente.

    Args:
        cfg: DictConfig a mutar con los overrides.
        env: Mapping de variables de entorno. Si None, usa os.environ.
             Pasar un dict explícito en tests para aislar del entorno del proceso.

    Returns:
        (DictConfig, int): nuevo DictConfig mergeado + cantidad de overrides aplicados.
                           Si no hay overrides, retorna el input sin modificar.
    """
    _env = env if env is not None else os.environ
    overrides: dict = {}
    skipped:   list[str] = []

    for key, raw_value in _env.items():
        if not key.startswith(_OCM_PREFIX):
            continue

        remainder = key[len(_OCM_PREFIX):]
        parts     = remainder.lower().split(_SEPARATOR)

        # Requiere al menos SECCIÓN + CLAVE (2 partes).
        # OCM_FOO (sin __) no tiene path estructurado válido — skip con warning.
        if len(parts) < 2:
            skipped.append(key)
            logger.warning(
                "config_l2_env_override | malformed_key={} "
                "hint=expected_OCM_SECTION__KEY_or_OCM_SECTION__SUBSECTION__KEY",
                key,
            )
            continue

        value = _coerce_value(raw_value)

        # Construye dict anidado: ["integrations", "redis", "enabled"] → {...}
        node = overrides
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value

        logger.debug(
            "config_l2_env_override | applied key={} path={} value={!r}",
            key, ".".join(parts), value,
        )

    if skipped:
        logger.warning(
            "config_l2_env_override | malformed_keys_count={} keys={}",
            len(skipped), skipped,
        )

    if not overrides:
        return cfg, 0

    override_cfg   = OmegaConf.create(overrides)
    merged         = OmegaConf.merge(cfg, override_cfg)
    mutation_count = _count_leaves(overrides)

    logger.info("config_l2_env_override | ocm_overrides_applied={}", mutation_count)

    return merged, mutation_count  # type: ignore[return-value]


def _coerce_value(value: str) -> bool | str:
    """Coerciona un string de env var al tipo nativo mínimo seguro.

    Convierte ÚNICAMENTE booleans semánticos inequívocos.
    Cualquier otro string se retorna intacto para que Pydantic L4 lo
    coercione con pleno conocimiento del tipo de destino en el schema.

    Lógica idéntica a coercion._coerce_string() — SSOT en coercion.py.
    L2 no llama a _coerce_string() directamente para evitar acoplar L2
    a una función privada de L3; las constantes públicas BOOL_TRUE/BOOL_FALSE
    son el contrato compartido entre ambas capas.

    Por qué NO convertir int/float aquí:
        "6379" podría ser port: int o api_key: str según el campo de destino.
        L2 no conoce el schema — Pydantic L4 es la única fuente de verdad
        para los tipos numéricos.

    Args:
        value: String crudo de la variable de entorno.

    Returns:
        True/False si es boolean semántico reconocido, str original en caso contrario.
    """
    lower = value.lower()
    if lower in _BOOL_TRUE:
        return True
    if lower in _BOOL_FALSE:
        return False
    return value  # str — Pydantic coerce int/float en L4


def _count_leaves(d: dict) -> int:
    """Cuenta hojas en un dict anidado (número de overrides reales aplicados)."""
    count = 0
    for v in d.values():
        if isinstance(v, dict):
            count += _count_leaves(v)
        else:
            count += 1
    return count
