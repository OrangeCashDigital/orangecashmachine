# ports/__init__.py
# Bounded context: market_data — Puertos Hexagonales
#
# INBOUND  (driving/primary)  : ports/inbound/
#   — Interfaces que el exterior usa para ACTIVAR la aplicación.
#   — Implementadas por: application/use_cases/, application/consumers/
#
# OUTBOUND (driven/secondary) : ports/outbound/
#   — Interfaces que la aplicación usa para LLAMAR servicios externos.
#   — Implementadas por: adapters/outbound/, infrastructure/
#
# Importar siempre desde el submódulo:
#   from market_data.ports.outbound.storage import OHLCVStorage
#   from market_data.ports.inbound.event_consumer import EventConsumerPort
