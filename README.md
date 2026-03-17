OrangeCashMachine

OrangeCashMachine (OCM) es una plataforma modular para trading algorítmico cuantitativo en criptomonedas diseñada para construir infraestructuras profesionales de trading y análisis de mercado.

El sistema sigue principios modernos de:
	•	data engineering
	•	arquitecturas event-driven
	•	pipelines reproducibles
	•	infraestructura desacoplada

El objetivo del proyecto es construir una plataforma cuantitativa escalable donde cada componente tenga una responsabilidad clara dentro del sistema.

⸻

Objetivo de la Plataforma

OrangeCashMachine busca construir una infraestructura capaz de soportar:
	•	investigación cuantitativa
	•	generación de señales
	•	backtesting reproducible
	•	trading automatizado
	•	ingestión de datos de mercado a gran escala

La arquitectura separa claramente cuatro dominios principales:

Market Data
Research
Execution
Monitoring

Esta separación permite escalar cada componente independientemente.

Principios de Arquitectura

La arquitectura sigue principios utilizados en infraestructuras de trading profesionales:
	•	Event-Driven Systems
	•	Data Platform Architecture
	•	Microservice Decoupling
	•	Reproducible Data Pipelines

Estos principios son comunes en infraestructuras utilizadas por:
	•	fondos cuantitativos
	•	trading firms
	•	fintechs de alta frecuencia
	•	plataformas de análisis financiero

Arquitectura General

OrangeCashMachine utiliza una arquitectura event-driven desacoplada.

                Exchanges
                     │
                     ▼
              Data Collectors
                     │
                     ▼
               Event Bus
               (Kafka)
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
   Data Lake     Feature Store  Stream DB
   (Parquet)       (Parquet)    (ClickHouse)
        │
        ▼
      Research

Este diseño evita cuellos de botella en la ingesta de datos y permite escalar cada componente del sistema.

Capas del Sistema

El sistema se organiza en cuatro capas principales.

Market Data Layer
Data Platform Layer
Strategy Layer
Execution Layer

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

Cada capa puede evolucionar como un servicio independiente.

⸻

Market Data Layer

Responsable de recolectar datos de mercado desde exchanges.

Funciones principales:
	•	ingestión histórica
	•	ingestión de streaming
	•	normalización de datos
	•	publicación de eventos de mercado

Componentes:

Market Data
│
├── Historical Pipeline
└── Stream Collectors

Historical Data Pipeline

El pipeline histórico descarga datasets reproducibles desde APIs REST.

flujo:

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

flujo:

Responsabilidades:
	•	descarga de datos históricos
	•	normalización de datasets
	•	persistencia eficiente
	•	actualización incremental

⸻

Real-Time Stream Pipeline

La ingestión en tiempo real utiliza WebSockets y un Event Bus para desacoplar el sistema.

Exchange WebSocket
        ↓
Async Stream Collector
        ↓
Kafka Event Bus
        ↓
Stream Consumers

lo consumidores pueden incluir:

Storage Workers
Feature Generators
Analytics Services

Este diseño permite manejar altos volúmenes de eventos de mercado sin bloquear el sistema.

Data Platform Layer

La Data Platform gestiona almacenamiento y distribución de datos.

Componentes principales:

Data Platform
│
├── Data Lake
├── Feature Store
└── Stream Database

Data Lake

Los datos históricos se almacenan en Parquet.

Ventajas:
	•	formato columnar eficiente
	•	compresión optimizada
	•	compatibilidad con herramientas analíticas
	•	almacenamiento escalable

Ejemplo de estructura:

data_lake/
 └── ohlcv/
     └── exchange=bybit/
         └── symbol=BTCUSDT/
             └── timeframe=1m/
                 └── year=2024/

Feature Store

El Feature Store almacena variables derivadas utilizadas por modelos y estrategias.

Ejemplos de features:
	•	indicadores técnicos
	•	factores cuantitativos
	•	métricas de microestructura
	•	señales de trading

Esto permite reutilizar features entre:
	•	research
	•	backtesting
	•	trading en vivo

⸻

Stream Database

Los datos en tiempo real se almacenan en una base de datos analítica.

Tecnología utilizada:

ClickHouse

Ventajas:
	•	alto rendimiento en series temporales
	•	ingestión de gran volumen
	•	consultas analíticas rápidas
	•	compresión eficiente

⸻

Research Layer

La capa de research consume datasets desde el Data Lake y el Feature Store.

Responsabilidades:
	•	exploración de datos
	•	generación de señales
	•	entrenamiento de modelos
	•	experimentación cuantitativa

⸻

Execution Layer

Responsable de ejecutar órdenes en exchanges.

Funciones:
	•	gestión de órdenes
	•	control de riesgo
	•	ejecución algorítmica
	•	conexión con exchanges

⸻

Observabilidad

El sistema incluye mecanismos de observabilidad para operación segura.

componentes:

Logging estructurado
Metrics (Prometheus)
Monitoring dashboards
Error tracking

Principios de Ingeniería

El proyecto sigue principios de ingeniería ampliamente utilizados en sistemas de producción.

SOLID

Cada componente tiene una única responsabilidad.

KISS

Las soluciones priorizan simplicidad y claridad.

DRY

La lógica común se reutiliza entre módulos.

SafeOps

El sistema prioriza la operación segura mediante:
	•	validación de configuración
	•	manejo robusto de errores
	•	retries controlados
	•	logging estructurado
	•	observabilidad

⸻

Roadmap

Próximas etapas del proyecto:
	1.	Integración completa con Prefect
	2.	Implementación del Event Bus
	3.	Integración con ClickHouse
	4.	Feature Engineering pipelines
	5.	Backtesting framework
	6.	Motor de ejecución de trading
	7.	Sistema de monitoreo y risk management

⸻

Objetivo Final

OrangeCashMachine busca evolucionar hacia una plataforma cuantitativa completa capaz de operar en entornos de trading automatizado y análisis de mercado a gran escala.

La arquitectura modular permite escalar gradualmente cada componente sin comprometer la mantenibilidad del sistema.

