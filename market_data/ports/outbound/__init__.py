# ports/outbound/__init__.py
# Driven (secondary) ports — interfaces que el dominio usa para llamar
# a servicios externos. Las implementaciones concretas viven en
# adapters/outbound/ e infrastructure/.
#
# Importar siempre desde el submódulo específico:
#   from market_data.ports.outbound.storage import OHLCVStorage
# Este __init__.py NO re-exporta para evitar imports circulares.
