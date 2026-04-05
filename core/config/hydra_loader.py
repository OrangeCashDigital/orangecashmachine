from __future__ import annotations

"""
core/config/hydra_loader.py
============================
Bridge entre Hydra/OmegaConf y Pydantic (AppConfig).

Responsabilidad única:
    Recibir un DictConfig de Hydra, convertirlo a dict Python limpio,
    y pasarlo a AppConfig para validación Pydantic completa.

Por qué existe:
    - AppConfig tiene extra="forbid" — no tolera campos internos de Hydra
    - OmegaConf devuelve DictConfig/ListConfig — Pydantic necesita dict/list nativos
    - Las interpolaciones ${oc.env:VAR,default} deben resolverse antes de Pydantic

Principios: KISS · SafeOps · Sin efectos secundarios
"""

import hashlib
import json
from typing import Optional

from omegaconf import DictConfig, OmegaConf
from loguru import logger

from core.config.schema import AppConfig
from core.config.loader.snapshot import write_config_snapshot


# Campos internos que Hydra puede inyectar — romperían extra="forbid"
_HYDRA_INTERNAL = {"_target_", "_recursive_", "_convert_", "hydra"}

# Campos Optional[str] que OmegaConf resuelve como "" — convertir a None
_NULLABLE_KEYS = {"password", "user", "database"}


def _normalize_empty_strings(d: dict) -> None:
    """Recorre recursivamente y convierte "" → None en campos nullable."""
    for k, v in d.items():
        if isinstance(v, dict):
            _normalize_empty_strings(v)
        elif isinstance(v, str) and v == "" and k in _NULLABLE_KEYS:
            d[k] = None


def hydra_cfg_to_appconfig(cfg: DictConfig) -> AppConfig:
    """
    Convierte DictConfig de Hydra → AppConfig validado por Pydantic.

    Pasos:
        1. to_container() — resuelve ${oc.env:...}, convierte a dict nativo
        2. Eliminar campos internos de Hydra
        3. Normalizar strings vacíos → None
        4. AppConfig(**raw) — validación Pydantic completa
    """
    raw: dict = OmegaConf.to_container(
        cfg,
        resolve=True,
        
    )

    for key in _HYDRA_INTERNAL:
        raw.pop(key, None)

    _normalize_empty_strings(raw)

    logger.debug("hydra_cfg_to_appconfig | top_keys={}", list(raw.keys()))

    return AppConfig(**raw)


def load_appconfig_from_hydra(
    cfg: DictConfig,
    *,
    env: str = "unknown",
    write_snapshot: bool = True,
) -> AppConfig:
    """
    Pipeline completo: DictConfig → AppConfig + snapshot de auditoría.

    Uso desde main.py:
        config = load_appconfig_from_hydra(cfg, env="development")
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
            write_config_snapshot(config, run_id=run_id, config_hash=config_hash, env=env)
        except Exception as exc:
            logger.warning("snapshot_failed | error={}", exc)

    return config
