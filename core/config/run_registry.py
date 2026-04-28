"""Shim: alias de ocm_platform.config.run_registry via sys.modules."""
import sys as _sys
import importlib as _importlib

_real = _importlib.import_module("ocm_platform.config.run_registry")
_sys.modules[__name__] = _real
