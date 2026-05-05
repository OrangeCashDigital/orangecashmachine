# -*- coding: utf-8 -*-
"""
market_data/ports/
===================

Todos los puertos (contratos DIP) del bounded context market_data.

Inbound (driving)           : EventConsumerPort, MarketDataSourcePort
Outbound (driven)           : EventPublisherPort, AnomalyRegistryPort,
                               LineagePort, StoragePort
"""
from market_data.ports.inbound.event_consumer   import EventConsumerPort, Handler  # noqa
from market_data.ports.outbound.event_publisher import EventPublisherPort            # noqa
from market_data.ports.outbound.anomaly_registry import AnomalyRegistryPort          # noqa
from market_data.ports.outbound.lineage          import LineagePort                  # noqa
from market_data.ports.market_data_source        import MarketDataSourcePort         # noqa

__all__ = [
    "Handler",
    "EventConsumerPort",
    "EventPublisherPort",
    "AnomalyRegistryPort",
    "LineagePort",
    "MarketDataSourcePort",
]
