# -*- coding: utf-8 -*-
"""Shim temporal: contrato en trading.ports.execution.transport, PaperTransport en trading.adapters.execution.paper_transport."""

from trading.adapters.execution.paper_transport import PaperTransport  # noqa: F401  # TODO: eliminar shim
from trading.ports.execution.transport import (  # noqa: F401  # TODO: eliminar shim
    OrderResult,
    OrderState,
    OrderStatus,
    OrderTransport,
)

__all__ = ["OrderStatus", "OrderState", "OrderResult", "OrderTransport", "PaperTransport"]
