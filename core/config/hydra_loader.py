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

Principios: KISS · SafeOps · Sin efectos secundarios
"""

import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from loguru import logger
from omegaconf import DictConfig, OmegaConf

from core.config.schema import AppConfig
from core.config.loader.snapshot import write_config_snapshot
from core.config.loader.env_overrides import apply_env_overrides

_HYDRA_INTERNAL: frozenset[str] = frozenset(
    {"_target_", "_recursive_", "_convert_", "hydra"}
)
_NULLABLE_KEYS: frozenset[str] = frozenset({"password", "user", "database"})


def _strip_hydra_internals(raw: dict[str, Any]) -> None:
    """Elimina in-place los campos internos de Hydra del dict raíz.

    Args:
        raw: Dict mutable resultado de ``OmegaConf.to_container()``.
    """
    for key in _HYDRA_INTERNAL:
        raw.pop(key, None)


def _normalize_empty_strings(d: dict[str, Any]) -> None:
    """Convierte ``""`` → ``None`` en campos nullable de forma recursiva.

    Args:
        d: Dict mutable a normalizar in-place.
    """
    for k, v in d.items():
        if isinstance(v, dict):
            _normalize_empty_strings(v)
        elif isinstance(v, str) and v == "" and k in _NULLABLE_KEYS:
            d[k] = None


def hydra_cfg_to_appconfig(cfg: DictConfig) -> AppConfig:
    """Convierte un DictConfig de Hydra en un AppConfig validado por Pydantic.

    Args:
        cfg: DictConfig compuesto por Hydra.

    Returns:
        AppConfig validado e inmutable.

    Raises:
        pydantic.ValidationError: Si la configuración no pasa validación.
        omegaconf.OmegaConfException: Si hay interpolaciones no resolubles.
    """
    raw: dict[str, Any] = OmegaConf.to_container(cfg, resolve=True)  # type: ignore[assignment]

    _strip_hydra_internals(raw)
    _normalize_empty_strings(raw)
    raw = apply_env_overrides(raw)

    logger.debug("hydra_cfg_to_appconfig | top_keys={}", list(raw.keys()))
    return AppConfig(**raw)


def load_appconfig_from_hydra(
    cfg: DictConfig,
    *,
    env: str = "unknown",
    write_snapshot: bool = True,
) -> AppConfig:
    """Pipeline completo: DictConfig → AppConfig + snapshot de auditoría.

    Args:
        cfg: DictConfig compuesto por Hydra.
        env: Nombre del entorno activo.
        write_snapshot: Si True (default), persiste el snapshot de auditoría.

    Returns:
        AppConfig validado e inmutable.
    """
    config = hydra_cfg_to_appconfig(cfg)

    if write_snapshot:
        try:
            raw_json = json.dumps(
                OmegaConf.to_container(cfg, resolve=True),
                default=str,
            )
            config_hash = hashlib.sha256(raw_json.encode()).hexdigest()
            run_id = config_hash[:12]
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

    Replica el merge de Hydra: ``base.yaml → env/{env}.yaml → settings.yaml``
    usando OmegaConf directamente, sin estado global de Hydra.

    Args:
        env: Entorno activo. Si None, se resuelve desde ``OCM_ENV``.
        config_dir: Directorio de configs YAML. Si None, usa ``config/``.
        write_snapshot: Si None, solo escribe snapshot en producción.

    Returns:
        AppConfig validado e inmutable.

    Raises:
        FileNotFoundError: Si ``config_dir`` o ``base.yaml`` no existen.
        pydantic.ValidationError: Si la configuración resultante no es válida.
    """
    from omegaconf import OmegaConf as _OC
    from core.config.loader.env_resolver import resolve_env, load_dotenv_for_env

    _env = resolve_env(env)
    load_dotenv_for_env(_env)

    _dir = Path(config_dir).resolve() if config_dir else Path("config").resolve()

    if not _dir.exists():
        raise FileNotFoundError(f"config_dir not found: {_dir}")

    base_path = _dir / "base.yaml"
    if not base_path.exists():
        raise FileNotFoundError(f"base.yaml not found in: {_dir}")

    cfg = _OC.load(base_path)

    env_file = _dir / "env" / f"{_env}.yaml"
    if env_file.exists():
        cfg = _OC.merge(cfg, _OC.load(env_file))
    else:
        logger.debug("load_appconfig_standalone | env_file_missing={}", env_file)

    settings_file = _dir / "settings.yaml"
    if settings_file.exists():
        cfg = _OC.merge(cfg, _OC.load(settings_file))

    _snapshot = write_snapshot if write_snapshot is not None else (_env == "production")
    logger.debug(
        "load_appconfig_standalone | env={} config_dir={} snapshot={}",
        _env, _dir, _snapshot,
    )
    return load_appconfig_from_hydra(cfg, env=_env, write_snapshot=_snapshot)
