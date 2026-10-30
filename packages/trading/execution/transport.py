# -*- coding: utf-8 -*-
"""Shim trading.ports.execution.transport contrato."""

from trading.adapters.execution.paper_transport import PaperTransport  # noqa: F401  # TODO: eliminar shim
from trading.ports.execution.transport import (  # noqa: F401  # TODO: eliminar shim
    OrderResult,
    OrderState,
    OrderStatus,
    OrderTransport,
)

__all__ = ["OrderStatus", "OrderState", "OrderResult", "OrderTransport", "PaperTransport"]
