"""Shim: alias de ocm_platform.config.hydra_loader via sys.modules.

sys.modules aliasing garantiza que patch("core.config.hydra_loader.X")
intercepte la misma referencia que usa el módulo canónico en runtime.
TEMPORAL: eliminar cuando todos los imports apunten a ocm_platform.*
"""
import sys as _sys
import importlib as _importlib

_real = _importlib.import_module("ocm_platform.config.hydra_loader")
_sys.modules[__name__] = _real
