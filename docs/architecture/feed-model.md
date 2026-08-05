# Modelo Unificado de Ingestión de Datos en OCM

## Propósito

Este documento es el SSOT conceptual del modelo de ingestión de datos de
OrangeCashMachine (OCM). Define, sin ambigüedad, qué es una fuente de
datos, qué es un mecanismo de ingestión, qué es un feed, qué es un evento,
y cómo conviven Kafka, el EventBus local e Iceberg dentro de la arquitectura
Kappa del sistema. Es el lugar al que cualquier desarrollador debería acudir
para entender cómo entra información a OCM, antes de tocar
`AppConfig.feeds`, `FeedsConfig`, `FeedOrchestrator`, o de diseñar un nuevo
mecanismo de adquisición de datos.

Este documento no registra decisiones arquitectónicas por sí mismo — esa
función la cumple `docs/architecture/decisions/ADR-0013-modelo-unificado-ingestion-datos.md`,
que referencia este documento como su fuente normativa. Si hay conflicto
entre ambos, el ADR es el registro de qué se decidió y cuándo; este
documento es la explicación completa de por qué y cómo.

## Índice

1. Fuente de datos, mecanismo de ingestión y feed
2. Feed: definición precisa
3. Ejemplos reales por fuente y mecanismo
4. El modelo de eventos
5. EventBus local vs. Kafka
6. Kafka como SSOT operacional; Iceberg como proyección materializada
7. Features reales vs. indicadores compuestos externos
8. Alcance de `FeedOrchestrator`, `FeedsConfig`, `AppConfig.feeds`
9. Diagrama de arquitectura completo
10. Preguntas frecuentes / casos límite

## 1. Fuente de datos, mecanismo de ingestión y feed

Tres conceptos que se confunden con frecuencia y que OCM distingue de
forma estricta:

- **Fuente de datos**: de dónde viene la información. Un exchange (Bybit,
  Binance, KuCoin), un proveedor on-chain (Glassnode, CryptoQuant), un
  proveedor de índices (CoinMarketCap, CoinGlass), una fuente
  macroeconómica (FRED, DXY), un sistema de noticias o sentimiento.
- **Mecanismo de ingestión**: cómo se adquiere esa información. Feed en
  tiempo real (streaming), consulta REST periódica (polling), replay
  histórico, proceso batch, o cálculo derivado internamente sobre datos
  ya ingestados.
- **Feed**: uno de los mecanismos de ingestión posibles — específicamente,
  el que mantiene un flujo continuo de información. No es sinónimo de
  fuente ni de exchange.

Estos tres conceptos son ortogonales entre sí. Una misma fuente puede
exponer varios mecanismos según el dato (un exchange que da trades por
WebSocket pero funding rate por REST). Un mismo mecanismo puede aplicarse
a fuentes muy distintas (polling sirve tanto para Glassnode como para
FRED). Ninguno de los tres define a los otros dos.

## 2. Feed: definición precisa

Un feed no representa un activo ni un evento individual, sino un flujo
continuo de información (stream) proveniente de una fuente de datos. En
OCM, un feed representa un flujo continuo de información mantenido
mediante un mecanismo de streaming — habitualmente WebSocket, aunque la
arquitectura no depende de un protocolo específico. Un FIX Market Data
Feed, un stream SSE, un multicast feed, o incluso un topic de Kafka
consumido en tiempo real, caerían igualmente bajo este concepto: lo que
define un feed no es el protocolo, sino que sea un flujo continuo.

A través de un feed circulan miles o millones de eventos, entendiendo un
evento como una unidad individual de información: un trade ejecutado, una
actualización del order book, una variación del funding rate, una
liquidación, un cambio en el open interest.

La relación correcta es:

Feed → Flujo continuo → Eventos 
Nunca Feed = Evento, y tampoco Feed = Fuente de datos.

## 3. Ejemplos reales por fuente y mecanismo

| Fuente | Dato | Mecanismo típico | ¿Es un feed? |
|---|---|---|---|
| Bybit / Binance / KuCoin | Trades, order book | WebSocket | Sí |
| Bybit / Binance / KuCoin | Funding rate | REST periódico | No (polling) |
| Glassnode / CryptoQuant | Métricas on-chain | REST periódico / batch | No |
| CoinMarketCap / CoinGlass | Market cap, dominance, OI agregado | REST periódico | No |
| FRED / DXY | Indicadores macro | REST periódico / batch | No |
| Cualquier fuente | Datos históricos | Replay | No |
| Interno (Gold) | Features derivadas (return_1, log_return, volatility_20, high_low_spread, vwap) | Cálculo derivado | No |

Esta tabla es ilustrativa, no exhaustiva. El criterio para clasificar una
integración nueva es siempre el mismo: ¿mantiene una conexión persistente
de streaming, o consulta/procesa datos de forma puntual/periódica? Lo
primero es un feed; lo segundo no, sin importar cuán frecuente sea el
polling.

El mecanismo "cálculo derivado" existe conceptualmente, pero hoy solo
produce las cinco features Gold (§7). Los indicadores compuestos (BTC
Dominance, Altcoin Season Index, CVD, Order Flow Imbalance, Open Interest
o Funding agregados, etc.) todavía no forman parte del modelo interno:
cuando aparezcan provendrán de adapters externos consumidos por polling y
se tratarán como ingestión externa, no como features internas.

## 4. El modelo de eventos

Independientemente del mecanismo utilizado para adquirir la información,
todos los datos se normalizan y representan como eventos dentro del mismo
modelo de eventos de la plataforma, permitiendo que el resto de
componentes procese la información de forma uniforme sin conocer su
origen. No existe un único pipeline físico — lo que existe es un único
modelo de eventos al que todos los mecanismos de ingestión convergen.

Esto implica que los exchanges dejan de ser el centro del sistema y pasan
a ser simplemente productores de eventos, al mismo nivel conceptual que
cualquier otra fuente de información, sin importar si llegaron vía feed,
polling o batch.

## 5. EventBus local vs. Kafka

No deben confundirse. `EventBusPort`/`InMemoryEventBus` es un mecanismo de
notificación estrictamente intra-proceso, pub/sub síncrono, sin
persistencia ni cruce de fronteras de proceso (ver ADR-0002, serie
heredada, Decisión 2). Su rol es desacoplar sub-responsabilidades dentro
de un mismo proceso — hoy, exclusivamente, el observador aditivo de
calidad (`QualityPipelineConsumer`) que registra lineage sobre chunks ya
publicados a Kafka.

Kafka es el único mecanismo para transportar datos entre procesos
distintos. Ningún mecanismo de ingestión —feed, polling, batch, replay,
cálculo derivado— debe usar el EventBus local como sustituto de Kafka para
cruzar esa frontera.

Regla práctica: si el consumidor de un evento corre en otro proceso (otro
worker, otro servicio, otro contenedor), es Kafka. Si el consumidor es un
observador dentro del mismo proceso que ya produjo el evento, puede ser el
EventBus local.

## 6. Kafka como SSOT operacional; Iceberg como proyección materializada

El log de eventos publicado en Kafka constituye la Single Source of Truth
operacional de la arquitectura Kappa. Las capas Bronze, Silver y Gold
implementadas sobre Iceberg no constituyen una nueva Single Source of
Truth, sino que representan proyecciones materializadas y especializadas
derivadas de ese mismo flujo de eventos, optimizadas para casos de uso
analíticos, históricos y de entrenamiento de modelos.

En términos prácticos: si Iceberg y Kafka llegaran a divergir, Kafka gana
— Iceberg se reconstruye replayando el log de eventos, nunca al revés.
Cualquier componente que necesite el estado canónico y más reciente de un
evento consulta Kafka (o un consumer directo de Kafka); cualquier
componente que necesite análisis histórico, entrenamiento o backtesting
consulta Iceberg.


## 7. Features reales vs. indicadores compuestos externos

Las únicas features actualmente implementadas son calculadas durante
el pipeline de transformación hacia la capa Gold, almacenadas en
Iceberg y consumidas exclusivamente a través de `FeatureReaderPort`:

- `return_1`
- `log_return`
- `volatility_20`
- `high_low_spread`
- `vwap`

Cualquier otro indicador mencionado en roadmaps o discusiones —CVD,
Order Flow Imbalance, BTC Dominance, Altcoin Season Index, Open
Interest agregado, Funding agregado, etc.— **no existe hoy como
feature interna**. Cuando estos indicadores se incorporen, lo harán
como **datos derivados, analytics o alpha research** construidos sobre
eventos ya ingestados — no como feeds ni como mecanismos de ingestión.
Sus insumos crudos pueden llegar por adapters externos (CoinMarketCap,
CoinGlass, Glassnode, CryptoQuant, FRED) consumidos mediante polling
REST (ver §3) dentro del dominio `market_data` (ver ADR-0013), pero el
indicador en sí es un cómputo posterior sobre datos integrados, no un
feed.

Un adapter externo únicamente adquiere datos desde una fuente
externa. Una feature representa un dato derivado e integrado dentro
del modelo interno de OCM. Que un adapter pueda obtener una métrica
(por ejemplo, BTC Dominance o Funding agregado) no implica que esa
métrica forme parte del modelo de features de la plataforma. La
integración, normalización, persistencia y exposición mediante
`FeatureReaderPort` constituyen una responsabilidad distinta.

## 8. Alcance de `FeedOrchestrator`, `FeedsConfig`, `AppConfig.feeds`

`AppConfig.feeds`, `FeedsConfig` y `FeedOrchestrator` tienen un alcance
específico y deliberadamente acotado: describen y orquestan
exclusivamente los feeds de mercado en tiempo real que requieren mantener
conexiones vivas tipo streaming. No asumen la responsabilidad de todas las
fuentes de información de la plataforma. Su función consiste en abrir,
mantener, supervisar y coordinar esos flujos continuos declarados en la
configuración y publicar los eventos generados hacia Kafka.

El resto de mecanismos de ingestión (REST polling, pipelines históricos,
procesos programados o cálculos derivados) siguen una orquestación
independiente —con su propio ciclo de vida, su propio scheduling y sus
propios adapters— pero convergen exactamente en el mismo modelo de
eventos (§4).

**No forzar en `FeedsConfig`:** listar exchanges/fuentes que en realidad
se consumen por polling (Glassnode, CoinMarketCap, FRED) no es solo un
error de nombres — implica un volumen y una naturaleza de ingestión
distintos (ver §3) para los que `FeedOrchestrator` no fue diseñado.

**Dónde vive el dominio de ingestión no-streaming:** dentro del bounded
context `market_data`, como capacidad interna separada de los feeds
streaming. Polling, batch y replay no son un bounded context distinto —
son mecanismos de adquisición del mismo dominio: adquieren información
de mercado, la normalizan y la publican hacia Kafka como eventos, igual
que los feeds. La creación de un BC independiente queda reservada a una
futura separación de dominio real (ver `ADR-0013`).

Estructura interna de `market_data` por capacidades:

    market_data
    ├── realtime_feeds        (WebSocket / FIX / SSE — FeedOrchestrator,
    │                           FeedsConfig, AppConfig.feeds)
    └── external_ingestion    (futuro — polling REST, batch, replay,
                                scheduling de proveedores externos)

Ambas capacidades convergen en el mismo flujo: fuente externa → mecanismo
de ingestión → normalización → eventos de dominio → Kafka (SSOT
operacional) → consumidores → Iceberg / features / estrategias.

`FeedOrchestrator` **no** gestiona REST polling, batch, replay, ni
proveedores macro u on-chain; esos mecanismos siguen perteneciendo al
dominio `market_data`, pero bajo la capacidad `external_ingestion`.

## 9. Diagrama de arquitectura completo

Fuente de datos
│
▼
Mecanismo de ingestión
(Feed WS, REST Polling,
Batch, Replay, Cálculo derivado)
│
▼
Normalización
│
▼
Eventos del dominio
│
▼
Kafka (SSOT operacional)
│
┌──────┴───────────┐
▼                   ▼
Streaming         Materialización
consumers         Bronze/Silver/Gold
│                   │        (Iceberg — proyección,
▼                   ▼         nunca nueva SSOT)
Features       Features históricas
│                   │
└─────────┬─────────┘
          │
          ▼
      Estrategias
          │
          ▼
       Órdenes

          ▲
          │
Indicadores compuestos
(futuro — ver §7) 
## 10. Preguntas frecuentes / casos límite

**¿Un topic de Kafka consumido en tiempo real es un feed?**
Conceptualmente sí — es un flujo continuo. En la práctica, dentro de OCM,
`FeedOrchestrator` orquesta feeds *externos* (hacia exchanges); un
consumer de Kafka interno no pasa por `FeedOrchestrator`, tiene su propio
mecanismo de consumo.

**¿Si hago polling cada 1 segundo, deja de ser polling y se vuelve feed?**
No. La frecuencia no define el mecanismo. Polling es una serie de
consultas puntuales, sin importar cuán frecuentes; un feed mantiene una
única conexión persistente. La diferencia es estructural, no de
velocidad.

**¿Dónde entra Cryptofeed (Trade Aggregator, roadmap de ADR-0002
heredado)?**
Es el ejemplo canónico de feed real dentro de OCM — WebSocket persistente
hacia un exchange. Es el tipo de mecanismo que sí encaja de lleno en
`FeedOrchestrator`/`FeedsConfig`.

**¿Una feature calculada sobre datos de un feed en tiempo real (CVD,
Order Flow Imbalance) es en sí misma un feed?**
No. Es una feature derivada de eventos que llegaron por un feed. El feed
es el mecanismo de transporte; la feature es un cómputo posterior sobre
los eventos ya ingestados (ver §7).
