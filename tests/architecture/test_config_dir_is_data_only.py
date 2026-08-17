"""
tests/architecture/test_config_dir_is_data_only.py
─────────────────────────────────────────────────────
Contrato ejecutable: config/ es exclusivamente configuración declarativa
(YAML, TOML, JSON, .env) — nunca código Python.

Razón: separar datos de configuración (config/) de código que los
interpreta/valida/tipa (ocm/config/ — Hydra Structured Configs,
modelos Pydantic, loaders). Mezclar ambos rompe SSOT y hace ambiguo
qué directorio es responsable de qué. Ver ocm/config/structured/ para
dónde vive el código correspondiente (ej. market_data_feeds.py).

Este test es el equivalente, para la frontera config/ vs ocm/config/,
de lo que import-linter hace para fronteras entre paquetes Python —
pero import-linter no puede expresarlo porque config/ no es un
paquete importable (no tiene __init__.py). De ahí que este contrato
viva como test explícito en vez de como contrato en
architecture_linter/importlinter.toml.
"""

from __future__ import annotations

from pathlib import Path

FORBIDDEN_EXTENSIONS = {".py", ".pyc", ".pyo"}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_config_directory_contains_no_python_files():
    config_dir = _repo_root() / "config"
    assert config_dir.is_dir(), f"config/ no encontrado en {config_dir}"

    offenders = [
        p.relative_to(config_dir) for p in config_dir.rglob("*") if p.is_file() and p.suffix in FORBIDDEN_EXTENSIONS
    ]

    assert not offenders, (
        "config/ debe contener solo configuración declarativa "
        "(YAML/TOML/JSON/.env) — nunca código Python. "
        f"Archivos Python encontrados: {offenders}. "
        "Mover la lógica a ocm/config/structured/ o ocm/config/loader/ "
        "según corresponda."
    )
