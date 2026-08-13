# -*- coding: utf-8 -*-
"""
tests/ocm/config/test_structured_parity.py
============================================

B-09 (H-05): paridad entre los módulos declarados en
``config/config.yaml`` (defaults de Hydra) y ``_MODULE_GLOBS``
(loader standalone — ``load_appconfig_standalone``).

Invariante
----------
Los dos paths de carga deben componer exactamente los mismos módulos:

- Path Hydra  : ``uv run ocm`` → ``@hydra.main(config_name="config")``
  (apps/app/cli/main.py:216) compone ``config/config.yaml`` defaults.
- Path standalone : ``live_hydra.py``/``paper_hydra.py``/``streaming_hydra.py``
  → ``load_appconfig_standalone()`` itera ``_MODULE_GLOBS``.

Si divergen, existe configuración YAML silenciosamente inactiva en un path
(ver B-09: ``market_data/external_ingestion.yaml`` y ``portfolio/portfolio.yaml``).

El test NO hardcodea la lista: la deriva de ``config/config.yaml`` y la
compara contra ``_MODULE_GLOBS`` real. Normaliza elementos sintácticos de
Hydra que no son módulos (base, schema:*, env:*, _self_).
"""

from __future__ import annotations

import yaml

from ocm.config.hydra_loader import _MODULE_GLOBS
from shared.utils.repo import repo_root

# Elementos de config.yaml defaults que NO son módulos `.yaml` mergeables por
# el standalone loader — directivas de Hydra o el archivo raíz (cargado
# explícitamente por load_appconfig_standalone vía base_path).
_HYDRA_ONLY_PREFIXES = ("schema:", "env:", "_self_")
_BASE_OVERRIDES = {"base", "base.yaml"}


def _config_yaml_modules() -> set[str]:
    """Deriva la lista real de módulos de config/config.yaml defaults.

    Returns:
        Conjunto de módulos (con sufijo `.yaml`) que config.yaml declara.
        Excluye directivas Hydra (`schema:`, `env:`, `_self_`) y `base`
        (cargado explícitamente por el standalone, fuera de _MODULE_GLOBS).
    """
    root = yaml.safe_load((repo_root() / "config" / "config.yaml").read_text())
    defaults = root["defaults"]
    modules: set[str] = set()
    for entry in defaults:
        if isinstance(entry, dict):
            continue
        name = str(entry)
        if name in _BASE_OVERRIDES:
            continue
        if name.startswith(_HYDRA_ONLY_PREFIXES):
            continue
        modules.add(name if name.endswith(".yaml") else f"{name}.yaml")
    return modules


def _module_glob_set() -> set[str]:
    """Módulos reales que el standalone loader itera (con sufijo `.yaml`)."""
    return {g if g.endswith(".yaml") else f"{g}.yaml" for g in _MODULE_GLOBS}


def test_module_globs_match_config_yaml_defaults() -> None:
    """Paridad: cada módulo de config.yaml defaults está en _MODULE_GLOBS y viceversa."""
    declared = _config_yaml_modules()
    globbed = _module_glob_set()

    assert declared == globbed, (
        "Diverge paridad config.yaml defaults ↔ _MODULE_GLOBS (B-09). "
        f"Solo en config.yaml (faltan en _MODULE_GLOBS): {sorted(declared - globbed)}. "
        f"Solo en _MODULE_GLOBS (faltan en config.yaml): {sorted(globbed - declared)}."
    )


def test_declared_modules_exist_on_disk() -> None:
    """Cada módulo declarado debe existir físicamente — sin referencias rotas."""
    cfg_dir = repo_root() / "config"
    missing = [m for m in _config_yaml_modules() if not (cfg_dir / m).exists()]
    assert not missing, f"Módulos declarados inexistentes: {missing}"


def test_external_ingestion_declared_in_both_paths() -> None:
    """B-09: external_ingestion debe estar en config.yaml y en _MODULE_GLOBS."""
    assert "market_data/external_ingestion.yaml" in _config_yaml_modules()
    assert "market_data/external_ingestion.yaml" in _module_glob_set()


def test_portfolio_declared_in_both_paths() -> None:
    """B-09: portfolio debe estar en config.yaml y en _MODULE_GLOBS."""
    assert "portfolio/portfolio.yaml" in _config_yaml_modules()
    assert "portfolio/portfolio.yaml" in _module_glob_set()
