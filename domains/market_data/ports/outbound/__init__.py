# ports/outbound/__init__.py
# Driven (secondary) ports — interfaces que el dominio usa para llamar
# a servicios externos. Las implementaciones concretas viven en
# adapters/outbound/ e infrastructure/.
#
# Importar siempre desde el submódulo específico:
#   from market_data.ports.outbound.storage import OHLCVStorage
# Este __init__.py NO re-exporta para evitar imports circulares.

# Kafka ports — añadidos en Bloque 3 (migración Kappa)
from market_data.ports.outbound.kafka_producer import (  # noqa: F401
    KafkaProducerPort,
    TOPIC_OHLCV_RAW,
    TOPIC_OHLCV_VALIDATED,
    TOPIC_OHLCV_FEATURES,
    TOPIC_DLQ,
)
from market_data.ports.outbound.kafka_consumer import (  # noqa: F401
    KafkaConsumerPort,
    KafkaMessage,
)
