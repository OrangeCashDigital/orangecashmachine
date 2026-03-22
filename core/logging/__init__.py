"""
core/logging
============
Sistema de logging centralizado para OrangeCashMachine.

Uso
---
    from core.logging import setup_logging
    setup_logging(debug=True)
"""

from core.logging.setup import setup_logging, InterceptHandler

__all__ = ["setup_logging", "InterceptHandler"]
