from __future__ import annotations

"""
core/config/hydra_loader.py
===========================

Bridge entre Hydra/OmegaConf y Pydantic (AppConfig).

Responsabilidad única:
    Recibir un DictConfig de Hydra, convertirlo a dict Python limpio
    y pasarlo a AppConfig para validación Pydantic completa.

Por qué existe:
    - AppConfig tiene ``extra="forbid"`` — no tolera campos internos de Hydra.
    - OmegaConf devuelve DictConfig/ListConfig — Pydantic necesita tipos nativos.
    - Las interpolaciones ``${oc.env:VAR,default}`` deben resolverse antes de Pydantic.

Flujo::

    DictConfig
        → to_container()           # resuelve interpolaciones, convierte a dict nativo
        → _strip_hydra_internals   # elimina _target_, _recursive_, hydra.*
        → _normalize_empty_strings # "" → None en campos nullable
        → apply_env_overrides      # OCM_* tienen prioridad sobre YAML
        → AppConfig(**raw)         # validación Pydantic completa

ADVERTENCIA — load_appconfig_standalone:
    OmegaConf.load() ignora la directiva ``# @package`` de Hydra.
    Por eso cada módulo se carga con _load_module() que envuelve
    el contenido en el namespace correcto antes del merge.

Patrones válidos de módulo YAML
---------------------------------
Patrón A — @package _global_ (con wrapper manual):
    # @package _global_
    pipeline:
      realtime:
        campo: valor
    → _load_module retorna raw_cfg directamente (sin entrada en _MODULE_PACKAGES).
    → El wrapper manual ubica el contenido en el namespace correcto.

Patrón B — @package explícito (contenido plano):
    # @package pipeline.historical
    campo: valor
    → _load_module envuelve el contenido usando _MODULE_PACKAGES[rel_path].
    → REQUIERE entrada en _MODULE_PACKAGES. Sin ella → campos al root → ValidationError.

Regla de consistencia (verificada al importar):
    _MODULE_PACKAGES.keys() ⊆ set(_MODULE_GLOBS)
    Todo módulo con @package no-global DEBE estar en ambas estructuras.

Principios: KISS · SafeOps · Fail-Fast · Sin efectos secundarios
"""

import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from loguru import logger
from omegaconf import DictConfig, OmegaConf

from core.config.schema import AppConfig
from core.config.loader.snapshot import write_config_snapshot

_HYDRA_INTERNAL: frozenset[str] = frozenset(
    {"_target_", "_recursive_", "_convert_", "hydra"}
)
_NULLABLE_KEYS: frozenset[str] = frozenset({"password", "user", "database"})

# ---------------------------------------------------------------------------
# Registro de módulos con @package no-global (Patrón B).
#
# REGLA: si un módulo YAML usa ``# @package pipeline.X`` con contenido plano,
# DEBE tener entrada aquí. Sin entrada → campos caen al root → ValidationError.
#
# NO registrar módulos con ``# @package _global_`` (Patrón A) — esos se
# mergean directamente al root via su wrapper manual y no necesitan envoltura.
#
# Clave: path relativo al config_dir.
# Valor: namespace canónico (dot-separated).
# ---------------------------------------------------------------------------
_MODULE_PACKAGES: dict[str, str] = {
    "pipeline/historical.yaml": "pipeline.historical",
    "pipeline/resample.yaml":   "pipeline.resample",
}

# ---------------------------------------------------------------------------
# Orden canónico de carga — mismo orden que config.yaml defaults list.
# Modificar solo si se añade/elimina un archivo de configuración.
# ---------------------------------------------------------------------------
_MODULE_GLOBS: list[str] = [
    "pipeline/historical.yaml",
    "pipeline/realtime.yaml",
    "pipeline/resample.yaml",
    "exchanges/bybit.yaml",
    "exchanges/kucoin.yaml",
    "exchanges/kucoinfutures.yaml",
    "observability/logging.yaml",
    "observability/metrics.yaml",
    "storage/datalake.yaml",
    "datasets.yaml",
    "risk/risk.yaml",
    "features.yaml",
]

# ---------------------------------------------------------------------------
# Fail-Fast: verificar consistencia entre _MODULE_PACKAGES y _MODULE_GLOBS
# al tiempo de importación. Detecta divergencias antes del primer run.
# ---------------------------------------------------------------------------
_packages_not_in_globs = _MODULE_PACKAGES.keys() - set(_MODULE_GLOBS)
if _packages_not_in_globs:
    raise AssertionError(
        f"hydra_loader: módulos en _MODULE_PACKAGES ausentes de _MODULE_GLOBS: "
        f"{sorted(_packages_not_in_globs)}. "
        f"Añádelos a _MODULE_GLOBS o elimínalos de _MODULE_PACKAGES."
    )


def strip_hydra_internals(raw: dict[str, Any]) -> None:
    """Elimina in-place los campos internos de Hydra del dict raíz."""
    for key in _HYDRA_INTERNAL:
        raw.pop(key, None)


def normalize_empty_strings(d: dict[str, Any]) -> None:
    """Convierte ``""`` → ``None`` en campos nullable de forma recursiva.

    Defensiva ante nodos YAML ``null`` (OmegaConf los convierte a None):
    si el valor es None no hay nada que normalizar — se omite en silencio.
    """
    if d is None:  # nodo YAML null en el nivel razíz — nada que hacer
        return
    for k, v in d.items():
        if isinstance(v, dict):
            normalize_empty_strings(v)
        elif v is None:
            pass  # YAML null — ya es None, no se modifica
        elif isinstance(v, str) and v == "" and k in _NULLABLE_KEYS:
            d[k] = None


def _load_module(config_dir: Path, rel_path: str) -> Optional[DictConfig]:
    """Carga un módulo YAML respetando su namespace canónico.

    OmegaConf.load() ignora ``# @package``. Este helper replica el
    comportamiento de Hydra compose:

    - Patrón A (@package _global_): retorna raw_cfg sin modificar.
      El wrapper manual del YAML ya ubica el contenido correctamente.
    - Patrón B (@package explícito): envuelve el contenido plano en
      el namespace registrado en _MODULE_PACKAGES.

    Args:
        config_dir: Directorio raíz de configuración.
        rel_path:   Path relativo al módulo (e.g. ``"pipeline/historical.yaml"``).

    Returns:
        DictConfig con el namespace correcto, o None si el archivo no existe.
    """
    fpath = config_dir / rel_path
    if not fpath.exists():
        logger.debug("_load_module | module_missing={}", fpath)
        return None

    raw_cfg = OmegaConf.load(fpath)
    namespace = _MODULE_PACKAGES.get(rel_path)

    if namespace is None:
        # Patrón A: @package _global_ — mergear directamente al root.
        return raw_cfg  # type: ignore[return-value]

    # Patrón B: @package pipeline.historical → {"pipeline": {"historical": ...}}
    parts = namespace.split(".")
    wrapped: Any = OmegaConf.to_container(raw_cfg, resolve=False)
    for part in reversed(parts):
        wrapped = {part: wrapped}
    return OmegaConf.create(wrapped)


def hydra_cfg_to_appconfig(cfg: DictConfig) -> AppConfig:
    """Convierte un DictConfig de Hydra en AppConfig via ConfigPipeline formal.

    Delega al ConfigPipeline (L1→L5) que es el Único flujo autorizado.
    No contiene lógica propia de transformación — SSOT en pipeline.py.

    Args:
        cfg: DictConfig compuesto por Hydra.

    Returns:
        AppConfig validado e inmutable.

    Raises:
        ConfigPipelineError: Si cualquier capa del pipeline falla (identifica la capa).
        pydantic.ValidationError: Propagada desde L4 si la config no es válida.
    """
    from core.config.pipeline import ConfigPipeline
    return ConfigPipeline(cfg).run()


def load_appconfig_from_hydra(
    cfg: DictConfig,
    *,
    env: str = "unknown",
    run_id: Optional[str] = None,
    write_snapshot: bool = True,
) -> AppConfig:
    """Pipeline completo: DictConfig → AppConfig + snapshot de auditoría.

    Args:
        cfg:            DictConfig compuesto por Hydra.
        env:            Nombre del entorno activo.
        run_id:         ID del run (SSOT: generado por RunConfig o caller).
                        Si None, se omite del snapshot pero no se fabrica aquí.
        write_snapshot: Si True (default), persiste el snapshot de auditoría.

    Returns:
        AppConfig validado e inmutable.
    """
    config = hydra_cfg_to_appconfig(cfg)

    if write_snapshot and run_id is not None:
        try:
            raw_json = json.dumps(
                OmegaConf.to_container(cfg, resolve=True),
                default=str,
            )
            config_hash = hashlib.sha256(raw_json.encode()).hexdigest()
            write_config_snapshot(
                config,
                run_id=run_id,
                config_hash=config_hash,
                env=env,
            )
        except Exception as exc:
            logger.warning("snapshot_failed | error={}", exc)

    return config


def load_appconfig_standalone(
    env: Optional[str] = None,
    config_dir: Optional[Path] = None,
    *,
    write_snapshot: Optional[bool] = None,
) -> AppConfig:
    """Carga AppConfig sin contexto Hydra activo.

    Replica el merge de Hydra respetando los namespaces ``@package``
    de cada módulo. OmegaConf.load() ignora ``# @package`` — por eso
    cada módulo se carga con _load_module() que aplica el patrón correcto.

    Orden de merge (mismo que config.yaml defaults):
        base.yaml → módulos → env/{env}.yaml → settings.yaml

    Args:
        env:            Entorno activo. Si None, resuelto desde ``OCM_ENV``.
        config_dir:     Directorio de configs YAML. Si None, usa ``config/``.
        write_snapshot: Si None, solo escribe snapshot en producción.

    Returns:
        AppConfig validado e inmutable.

    Raises:
        FileNotFoundError: Si ``config_dir`` o ``base.yaml`` no existen.
        pydantic.ValidationError: Si la configuración resultante no es válida.
    """
    from core.config.loader.env_resolver import resolve_env, load_dotenv_for_env

    _env = resolve_env(env)
    load_dotenv_for_env(_env)

    _dir = Path(config_dir).resolve() if config_dir else Path("config").resolve()

    if not _dir.exists():
        raise FileNotFoundError(f"config_dir not found: {_dir}")

    base_path = _dir / "base.yaml"
    if not base_path.exists():
        raise FileNotFoundError(f"base.yaml not found in: {_dir}")

    cfg = OmegaConf.load(base_path)

    for rel in _MODULE_GLOBS:
        module_cfg = _load_module(_dir, rel)
        if module_cfg is not None:
            cfg = OmegaConf.merge(cfg, module_cfg)

    env_file = _dir / "env" / f"{_env}.yaml"
    if env_file.exists():
        cfg = OmegaConf.merge(cfg, OmegaConf.load(env_file))
    else:
        logger.debug("load_appconfig_standalone | env_file_missing={}", env_file)

    settings_file = _dir / "settings.yaml"
    if settings_file.exists():
        cfg = OmegaConf.merge(cfg, OmegaConf.load(settings_file))

    _snapshot = write_snapshot if write_snapshot is not None else (_env == "production")
    logger.debug(
        "load_appconfig_standalone | env={} config_dir={} snapshot={}",
        _env, _dir, _snapshot,
    )
    return load_appconfig_from_hydra(cfg, env=_env, write_snapshot=_snapshot)
