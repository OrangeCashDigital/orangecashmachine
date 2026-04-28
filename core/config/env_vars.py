"""Shim: alias de ocm_platform.config.env_vars via sys.modules.

sys.modules aliasing garantiza que monkeypatch.setattr sobre este
módulo afecte al módulo canónico (mismo objeto en memoria).
TEMPORAL: eliminar cuando todos los imports apunten a ocm_platform.*
"""
import sys as _sys
import importlib as _importlib

# Importar el módulo real
_real = _importlib.import_module("ocm_platform.config.env_vars")

# Registrar este nombre como alias del módulo real en sys.modules.
# Tras esta línea, `import core.config.env_vars` devuelve el mismo objeto que
# `import ocm_platform.config.env_vars` — monkeypatch.setattr funciona en ambos.
_sys.modules[__name__] = _real
