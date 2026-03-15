# OrangeCashMachine

**OrangeCashMachine (OCM)** es una plataforma modular para **trading algorítmico cuantitativo en criptomonedas** diseñada para construir infraestructuras de trading profesionales utilizando principios modernos de **data engineering, microservicios y pipelines reproducibles**.

El objetivo del proyecto es crear un sistema escalable donde cada componente tenga una responsabilidad clara dentro de la arquitectura:

- Ingestión de datos de mercado
- Procesamiento cuantitativo
- Ejecución de órdenes
- Monitoreo y control de riesgo

La arquitectura está inspirada en infraestructuras utilizadas por **fondos cuantitativos, trading firms y plataformas fintech**, donde la separación de responsabilidades mejora significativamente:

- escalabilidad
- tolerancia a fallos
- reproducibilidad de datos
- mantenibilidad del sistema

---

# Referencias de Arquitectura

La arquitectura del sistema sigue patrones utilizados en sistemas financieros modernos:

- Nasyrova, A. (2025). *Microservices for Financial Trading Systems.*
- Purella, S. (2025). *Scalable Trading Infrastructures.*
- Khraisha, T. (2024). *Financial Data Engineering.*
- Harenslak, B., & De Ruiter, J. (2021). *Data Pipelines with Apache Airflow.*

Estos trabajos describen cómo los sistemas financieros separan **batch ingestion, streaming ingestion y procesamiento analítico** para garantizar robustez y escalabilidad.

---

# Arquitectura General

OrangeCashMachine implementa una arquitectura por capas utilizada en sistemas cuantitativos profesionales.

```
                Exchanges
                     │
        ┌────────────────┴────────────────┐
        │                                 │
        ▼                                 ▼
Historical ingestion               Stream collectors
   (REST API)                       (WebSockets)
        │                                 │
        ▼                                 ▼
      Prefect                           Kafka
   (orchestration)                        │
        │                                 ▼
        ▼                           Stream processor
      Data Lake                           │
    (Parquet/S3)                          ▼
        │                           Real-time DB
        ▼                           (ClickHouse)
     Research
```

Esta arquitectura separa dos tipos fundamentales de procesamiento de datos.

---

# Batch Processing

Procesamiento de datos históricos.

Responsabilidades:

- ingestión histórica
- pipelines reproducibles
- datasets analíticos
- actualización incremental

Tecnologías utilizadas:

- **Prefect 3** (orquestación)
- **Python Async Pipelines**
- **Parquet Data Lake**

---

# Stream Processing

Procesamiento de datos en tiempo real.

Responsabilidades:

- ingestión de streams
- baja latencia
- distribución de datos a sistemas downstream

Tecnologías utilizadas:

- **WebSocket collectors**
- **Kafka message broker**
- **Stream processors**
- **ClickHouse analytical database**

---

# Filosofía del Sistema

Los sistemas profesionales de trading separan tres componentes fundamentales.

```
Datos (Oído)
Estrategia (Cerebro)
Ejecución (Mano)
```

Representado como:

```
Data Ingestion → Strategy → Execution
```

Este diseño permite:

- escalar estrategias sin duplicar datos
- reducir llamadas directas al exchange
- mantener datasets históricos consistentes
- ejecutar backtests reproducibles
- desacoplar infraestructura de trading

---

# Arquitectura por Capas

OrangeCashMachine organiza el sistema en cuatro capas principales:

```
Market Data Layer
Strategy Layer
Execution Layer
Monitoring Layer
```

Flujo de datos:

```
Exchange
   ↓
Market Data Layer
   ↓
Strategy Layer
   ↓
Execution Layer
   ↓
Monitoring Layer
```

Cada capa puede evolucionar como un **microservicio independiente**.

---

# Estado Actual del Proyecto

El desarrollo actual se encuentra enfocado en la **Market Data Layer**, que constituye la base del sistema completo.

Esta capa construye la infraestructura de datos necesaria para:

- research cuantitativo
- backtesting
- trading en vivo
- entrenamiento de modelos

---

# Market Data Layer

La **Market Data Layer** se encarga de recolectar, normalizar y almacenar datos de mercado provenientes de exchanges.

Responsabilidades principales:

- descarga de datos históricos
- ingestión de streams en tiempo real
- validación de datasets
- almacenamiento eficiente
- distribución de datos al sistema de research

---

# Market Data System

El primer microservicio implementado dentro del proyecto es el **Market Data System**.

Este sistema construye y mantiene el **Data Lake de mercado** utilizado por el resto de la plataforma.

Arquitectura interna:

```
Market Data System
│
├── Historical Data Pipeline
└── Real-Time Stream Pipeline
```

---

# Historical Data Pipeline

El pipeline histórico es responsable de descargar y mantener datasets de mercado reproducibles.

Flujo de procesamiento:

```
Exchange REST API
        ↓
Async Fetcher
        ↓
Transformer
        ↓
Schema Validation
        ↓
Parquet Storage
        ↓
Data Lake
```

La orquestación del pipeline se realiza mediante **Prefect 3**, lo que permite:

- scheduling automático
- retries
- observabilidad
- ejecución distribuida
- historial de ejecuciones

---

# Real-Time Stream Pipeline

El pipeline de streaming captura datos en tiempo real utilizando WebSockets.

Flujo:

```
Exchange WebSocket
        ↓
Async Stream Collector
        ↓
Kafka Message Broker
        ↓
Stream Processing
        ↓
ClickHouse
```

Esto permite:

- ingestión de alta frecuencia
- procesamiento en tiempo real
- almacenamiento analítico optimizado

---

# Data Lake

Los datos históricos se almacenan en un **Data Lake basado en Parquet**.

Ventajas:

- formato columnar altamente eficiente
- compresión optimizada
- escalabilidad horizontal
- compatibilidad con herramientas analíticas

Estructura típica:

```
data_lake/
 └── ohlcv/
     └── BTC_USDT/
         └── 2024/
             └── 01/
                 BTC_USDT_1m.parquet
```

---

# Base de Datos Analítica

Para consultas rápidas sobre datos de mercado se utiliza **ClickHouse**.

ClickHouse es ampliamente utilizado en sistemas de trading debido a:

- alto rendimiento en series temporales
- compresión eficiente
- consultas analíticas rápidas
- ingestión de grandes volúmenes de datos

---

# Estructura del Proyecto

```
orangecashmachine
│
├── market_data
│   ├── connectors
│   │   └── exchange_client_async.py
│   │
│   ├── historical
│   │   ├── fetcher.py
│   │   ├── storage.py
│   │   └── transformer.py
│   │
│   ├── pipelines
│   │   └── historical_pipeline.py
│   │
│   ├── schemas
│   │   └── ohlcv_schema.py
│   │
│   └── streams
│       ├── stream_manager.py
│       ├── trades_stream.py
│       └── orderbook_stream.py
│
├── data_platform
│   └── loaders
│
├── research
│   └── data
│
├── trading
│
└── config
```

---

# Componentes Principales

## Exchange Client

Responsabilidad:

- establecer conexión con exchanges
- gestionar rate limits
- manejar credenciales
- abstraer APIs de exchange

Este componente es reutilizable por todo el sistema.

---

## Historical Fetcher

Responsabilidad:

- descargar datos OHLCV históricos
- manejar paginación
- respetar rate limits

---

## Transformer

Responsabilidad:

- limpieza de datos
- normalización de columnas
- conversión de timestamps
- eliminación de duplicados

---

## Storage

Responsabilidad:

- persistir datasets en Parquet
- organizar data lake
- evitar duplicados
- escritura atómica

---

## Pipelines

Responsabilidad:

- orquestar ejecución de tareas
- manejar concurrencia
- coordinar fetcher, transformer y storage

---

# Principios de Ingeniería Aplicados

El proyecto sigue principios fundamentales de ingeniería de software.

### SOLID

Cada módulo tiene una responsabilidad clara y desacoplada.

### KISS

Se priorizan soluciones simples y mantenibles.

### DRY

La lógica común se reutiliza mediante componentes compartidos.

### SafeOps

El sistema incluye mecanismos de seguridad operativa:

- validación de datos
- logging estructurado
- retries controlados
- manejo de errores robusto

---

# Roadmap

Próximas etapas del proyecto:

1. Integración completa con Prefect
2. Implementación de streaming con Kafka
3. Integración con ClickHouse
4. Feature Engineering pipelines
5. Backtesting framework
6. Motor de ejecución de trading
7. Sistema de monitoreo y risk management

---

# Objetivo del Proyecto

OrangeCashMachine busca evolucionar hacia una **infraestructura cuantitativa completa**, capaz de soportar:

- research cuantitativo
- entrenamiento de modelos
- trading automatizado
- procesamiento de datos de mercado a gran escala

La arquitectura modular permite escalar el sistema gradualmente sin comprometer su mantenibilidad.