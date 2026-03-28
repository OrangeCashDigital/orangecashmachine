"""
core/logging
============
Sistema de logging centralizado para OrangeCashMachine.

Uso
---
    from core.logging import bootstrap_logging, configure_logging

    bootstrap_logging(debug=True, run_id=run_id, env="development")
    configure_logging(cfg=app_config.observability.logging, env="production")
"""

from core.logging.setup import (
    bootstrap_logging,
    configure_logging,
    setup_logging,       # deprecated — eliminar en v2
    InterceptHandler,
)

__all__ = ["bootstrap_logging", "configure_logging", "setup_logging", "InterceptHandler"]
