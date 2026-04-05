# -*- coding: utf-8 -*-
"""
infra/state/factories.py
=========================

Factories centralizadas para todos los stores de infra/state/.

Responsabilidad
---------------
Construir instancias de RedisCursorStore, GapRegistry y
LatenessCalibrationStore desde variables de entorno, con
degradación controlada cuando Redis no está disponible.

Por qué este módulo existe
--------------------------
Antes, cada módulo de dominio (gap_registry.py, lateness_calibration.py)
tenía su propia factory con imports condicionales dentro de la función
para evitar circulares. Esto:

  1. Hacía los módulos difíciles de testear — no se podía patchear
     el import sin hackear sys.modules.
  2. Ocultaba el grafo de dependencias real del lector.
  3. Violaba SRP — un registro de gaps no debería saber cómo
     conectarse a Redis.

Solución
--------
Las factories viven aquí. Los módulos de dominio solo reciben
un RedisCursorStore ya construido en su constructor.

Uso
---
    from infra.state.factories import (
        build_cursor_store,
        build_gap_registry,
        build_lateness_calibration_store,
    )

    store    = build_cursor_store()
    registry = build_gap_registry()
    cal      = build_lateness_calibration_store()

SafeOps
-------
build_gap_registry y build_lateness_calibration_store retornan None
si Redis no está disponible. El caller decide si degradar o fallar.
"""
from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from loguru import logger

from infra.state.cursor_store import RedisCursorStore, build_cursor_store_from_env

if TYPE_CHECKING:
    from infra.state.gap_registry import GapRegistry
    from infra.state.lateness_calibration import LatenessCalibrationStore


def build_cursor_store(env: Optional[str] = None) -> RedisCursorStore:
    """
    Construye un RedisCursorStore desde variables de entorno.

    Thin wrapper sobre build_cursor_store_from_env que permite a los
    callers importar solo desde infra.state.factories sin conocer
    cursor_store.py directamente.

    Parameters
    ----------
    env : str, optional
        Entorno explícito (e.g. "production"). Si None, resuelve
        via resolve_env() (OCM_ENV → settings.yaml → "development").
    """
    return build_cursor_store_from_env(env=env)


def build_gap_registry(
    env: Optional[str] = None,
) -> Optional["GapRegistry"]:
    """
    Construye un GapRegistry desde variables de entorno.

    Returns None si Redis no está disponible — el caller decide
    si operar en modo degradado o fallar explícitamente.

    Parameters
    ----------
    env : str, optional
        Entorno explícito. Si None, usa resolve_env().
    """
    from infra.state.gap_registry import GapRegistry

    try:
        store = build_cursor_store_from_env(env=env)
        if not store.is_healthy():
            logger.warning(
                "build_gap_registry: Redis no disponible — registry deshabilitado"
            )
            return None
        return GapRegistry(store)
    except Exception as exc:
        logger.warning("build_gap_registry: no se pudo inicializar | error={}", exc)
        return None


def build_lateness_calibration_store(
    env: Optional[str] = None,
) -> Optional["LatenessCalibrationStore"]:
    """
    Construye un LatenessCalibrationStore desde variables de entorno.

    Returns None si Redis no está disponible.

    Parameters
    ----------
    env : str, optional
        Entorno explícito. Si None, usa resolve_env().
    """
    from infra.state.lateness_calibration import LatenessCalibrationStore

    try:
        store = build_cursor_store_from_env(env=env)
        if not store.is_healthy():
            logger.warning(
                "build_lateness_calibration_store: "
                "Redis no disponible — calibración deshabilitada"
            )
            return None
        return LatenessCalibrationStore(store)
    except Exception as exc:
        logger.warning(
            "build_lateness_calibration_store: no se pudo inicializar | error={}", exc
        )
        return None
