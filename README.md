# OrangeCashMachine (OCM)

OrangeCashMachine (OCM) es una **plataforma modular de trading algorítmico cuantitativo en criptomonedas**, diseñada para construir **infraestructuras profesionales de trading y análisis de mercado**.

El sistema sigue principios modernos de ingeniería de datos y software:

- Data engineering
- Arquitecturas event-driven
- Pipelines reproducibles
- Infraestructura desacoplada

El objetivo del proyecto es construir una plataforma cuantitativa **escalable**, donde cada componente tenga una **responsabilidad clara** dentro del sistema.

---

## Objetivo de la Plataforma

OrangeCashMachine busca construir una infraestructura capaz de soportar:

- Investigación cuantitativa
- Generación de señales
- Backtesting reproducible
- Trading automatizado
- Ingestión de datos de mercado a gran escala

La arquitectura separa claramente cuatro dominios principales:

1. **Market Data**
2. **Research**
3. **Execution**
4. **Monitoring**

Esta separación permite escalar cada componente independientemente.

---

## Principios de Arquitectura

La arquitectura sigue estándares de **infraestructuras de trading profesionales**:

- Event-Driven Systems
- Data Platform Architecture
- Microservice Decoupling
- Reproducible Data Pipelines

Estas prácticas son comunes en fondos cuantitativos, trading firms, fintechs de alta frecuencia y plataformas de análisis financiero.

---

## Arquitectura General

OCM utiliza una arquitectura **event-driven y desacoplada**:

Exchanges
     │
     ▼
Data Collectors
     │
     ▼
Event Bus (Kafka)
     │
 ┌───┼───┐
 │   │   │
 ▼   ▼   ▼
Data Lake   Feature Store   Stream DB
(Parquet)   (Parquet)       (ClickHouse)
     │
     ▼
   Research

Este diseño evita cuellos de botella en la ingesta de datos y permite escalar cada componente de forma independiente.

---

## Capas del Sistema

El sistema se organiza en **cuatro capas principales**:

1. **Market Data Layer**
2. **Data Platform Layer**
3. **Strategy / Research Layer**
4. **Execution Layer**

Flujo general:

Exchange
   ↓
Market Data Layer
   ↓
Data Platform
   ↓
Strategy Layer
   ↓
Execution Layer

Cada capa puede evolucionar como un **servicio independiente**.

---

## Market Data Layer

Responsable de recolectar datos de mercado desde exchanges.

**Funciones principales:**

- Ingestión histórica
- Ingestión en streaming
- Normalización de datos
- Publicación de eventos de mercado

### Componentes:

Market Data
│
├── Historical Pipeline
└── Stream Collectors

### Historical Data Pipeline

Descarga datasets reproducibles desde APIs REST.

**Flujo:**

Exchange REST API
        ↓
Async Fetcher
        ↓
Data Transformer
        ↓
Schema Validation
        ↓
Parquet Writer
        ↓
Data Lake

**Responsabilidades:**

- Descarga de datos históricos
- Normalización de datasets
- Persistencia eficiente
- Actualización incremental

---

### Real-Time Stream Pipeline

La ingestión en tiempo real utiliza **WebSockets** y un **Event Bus** para desacoplar el sistema.

**Flujo:**

Exchange WebSocket
        ↓
Async Stream Collector
        ↓
Kafka Event Bus
        ↓
Stream Consumers

**Consumidores posibles:**

- Storage Workers
- Feature Generators
- Analytics Services

Este diseño permite manejar **altos volúmenes de eventos** sin bloquear el sistema.

---

## Data Platform Layer

Gestiona almacenamiento y distribución de datos.

**Componentes principales:**

Data Platform
│
├── Data Lake
├── Feature Store
└── Stream Database

### Data Lake

Datos históricos almacenados en **Parquet**.

**Ventajas:**

- Formato columnar eficiente
- Compresión optimizada
- Compatible con herramientas analíticas
- Escalable

**Ejemplo de estructura:**

data_lake/
 └── ohlcv/
     └── exchange=bybit/
         └── symbol=BTCUSDT/
             └── timeframe=1m/
                 └── year=2024/

### Feature Store

Almacena variables derivadas utilizadas por modelos y estrategias.

**Ejemplos de features:**

- Indicadores técnicos
- Factores cuantitativos
- Métricas de microestructura
- Señales de trading

Permite **reutilizar features** entre:

- Research
- Backtesting
- Trading en vivo

### Stream Database

Almacena datos en tiempo real en una base analítica.

**Tecnología:** ClickHouse

**Ventajas:**

- Alto rendimiento en series temporales
- Ingestión de gran volumen
- Consultas analíticas rápidas
- Compresión eficiente

---

## Research Layer

Consume datasets desde el **Data Lake** y **Feature Store**.

**Responsabilidades:**

- Exploración de datos
- Generación de señales
- Entrenamiento de modelos
- Experimentación cuantitativa

---

## Execution Layer

Responsable de ejecutar órdenes en exchanges.

**Funciones:**

- Gestión de órdenes
- Control de riesgo
- Ejecución algorítmica
- Conexión con exchanges

---

## Observabilidad

Incluye mecanismos de **monitorización y operación segura**:

- Logging estructurado
- Metrics (Prometheus)
- Dashboards de monitoring
- Error tracking

---

## Principios de Ingeniería

- **SOLID:** Cada componente tiene una única responsabilidad.
- **KISS:** Simplicidad y claridad priorizadas.
- **DRY:** Lógica común reutilizable entre módulos.
- **SafeOps:** Operación segura mediante validación, manejo de errores, retries y logging.

---

## Roadmap

Próximas etapas del proyecto:

1. Integración completa con Prefect
2. Implementación del Event Bus
3. Integración con ClickHouse
4. Feature Engineering pipelines
5. Backtesting framework
6. Motor de ejecución de trading
7. Sistema de monitoreo y risk management

---

## Objetivo Final

OCM busca evolucionar hacia una **plataforma cuantitativa completa**, capaz de operar en entornos de trading automatizado y análisis de mercado a gran escala.

La **arquitectura modular** permite escalar gradualmente cada componente sin comprometer la mantenibilidad.
