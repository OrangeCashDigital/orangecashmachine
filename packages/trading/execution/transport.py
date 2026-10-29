# -*- coding: utf-8 -*-
"""Shim temporal de compatibilidad.

Contrato: trading.ports.execution.transport. PaperTransport: trading.adapters.execution.paper_transport.
"""

from trading.adapters.execution.paper_transport import PaperTransport  # noqa: F401  # TODO: eliminar shim
from trading.ports.execution.transport import (  # noqa: F401  # TODO: eliminar shim
    ExchangeOrderStatus,
    OrderResult,
    OrderState,
    OrderTransport,
)

__all__ = ["ExchangeOrderStatus", "OrderState", "OrderResult", "OrderTransport", "PaperTransport"]
