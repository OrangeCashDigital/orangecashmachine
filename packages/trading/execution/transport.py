# -*- coding: utf-8 -*-
"""Shim temporal de compatibilidad.

Contrato: trading.ports.outbound.transport. PaperTransport: trading.adapters.outbound.paper_transport.
"""

from trading.adapters.outbound.paper_transport import PaperTransport  # noqa: F401  # TODO: eliminar shim
from trading.ports.outbound.transport import (  # noqa: F401  # TODO: eliminar shim
    ExchangeOrderStatus,
    OrderResult,
    OrderState,
    OrderTransport,
)

__all__ = ["ExchangeOrderStatus", "OrderState", "OrderResult", "OrderTransport", "PaperTransport"]
