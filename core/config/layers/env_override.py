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
#   - No coerciones complejas — solo bool/int/float/str
#   - Claves malformadas → warning + skip (fail-soft en L2)
#   - Retorna nuevo DictConfig — NO muta el input (inmutabilidad)
#   - Retorna (DictConfig, int) — el int es el conteo de overrides aplicados
# ==============================================================================

from __future__ import annotations

import os

from loguru import logger
from omegaconf import DictConfig, OmegaConf

_OCM_PREFIX = "OCM_"
_SEPARATOR  = "__"

# Constantes locales — SSOT dentro de L2 para evitar dependencia circular con L4 (schema).
# Deben mantenerse sincronizadas con schema._ENV_BOOL_TRUE / _ENV_BOOL_FALSE.
_BOOL_TRUE:  frozenset[str] = frozenset({"1", "true",  "yes", "on"})
_BOOL_FALSE: frozenset[str] = frozenset({"0", "false", "no",  "off"})


def apply_env_overrides(cfg: DictConfig) -> tuple[DictConfig, int]:
    """
    Aplica OCM_* env vars como overrides estructurados sobre el DictConfig.

    Returns:
        (DictConfig, int): nuevo DictConfig mergeado + cantidad de overrides aplicados.
                           Si no hay overrides, retorna el input sin modificar.
    """
    overrides: dict = {}
    skipped: list[str] = []

    for key, raw_value in os.environ.items():
        if not key.startswith(_OCM_PREFIX):
            continue

        remainder = key[len(_OCM_PREFIX):]
        parts = remainder.lower().split(_SEPARATOR)

        # Requiere al menos SECCIÓN + CLAVE (2 partes)
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

        logger.debug("config_l2_env_override | applied key={} path={} value={!r}", key, ".".join(parts), value)

    if skipped:
        logger.warning("config_l2_env_override | malformed_keys_count={} keys={}", len(skipped), skipped)

    if not overrides:
        return cfg, 0

    override_cfg = OmegaConf.create(overrides)
    merged = OmegaConf.merge(cfg, override_cfg)

    mutation_count = _count_leaves(overrides)
    logger.info("config_l2_env_override | ocm_overrides_applied={}", mutation_count)

    return merged, mutation_count  # type: ignore[return-value]


def _coerce_value(value: str) -> bool | int | float | str:
    """
    Coerción de tipos mínima para valores de variables de entorno.

    Orden de intentos: bool → int → float → str (fallback seguro).
    No lanza excepciones: siempre retorna un tipo válido.

    Nota: usa _ENV_BOOL_TRUE/_ENV_BOOL_FALSE de schema.py como SSOT
    para el contrato de strings booleanos reconocidos.
    """
    lower = value.lower()
    if lower in _BOOL_TRUE:
        return True
    if lower in _BOOL_FALSE:
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def _count_leaves(d: dict) -> int:
    """Cuenta hojas en un dict anidado (número de overrides reales)."""
    count = 0
    for v in d.values():
        if isinstance(v, dict):
            count += _count_leaves(v)
        else:
            count += 1
    return count
