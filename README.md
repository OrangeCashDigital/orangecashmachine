OrangeCashMachine

OrangeCashMachine (OCM) es una plataforma modular para trading algorítmico cuantitativo en criptomonedas diseñada para ejecutar múltiples estrategias de trading mediante una arquitectura basada en microservicios.

El objetivo del proyecto es construir un sistema profesional de trading automatizado donde cada componente tenga una responsabilidad clara:
	•	Ingestión de datos
	•	Procesamiento cuantitativo
	•	Ejecución de órdenes
	•	Monitoreo y control de riesgo

La arquitectura está inspirada en sistemas profesionales utilizados por fondos cuantitativos, donde la separación de responsabilidades mejora significativamente la escalabilidad, tolerancia a fallos y mantenibilidad del sistema.

Referencias de arquitectura:
	•	Nasyrova, 2025 — Microservices for financial trading systems
	•	Purella, 2025 — Scalable trading infrastructures

Arquitectura General

La arquitectura de OrangeCashMachine está organizada en capas funcionales:

Market Data Layer
Strategy Layer
Execution Layer
Monitoring Layer

Cada capa funciona como un microservicio independiente.

Exchange
   ↓
Market Data Layer
   ↓
Strategy Layer
   ↓
Execution Layer
   ↓
Monitoring Layer

Filosofía del Sistema

Los sistemas profesionales de trading separan tres componentes fundamentales:

Datos (Oído)
Estrategia (Cerebro)
Ejecución (Mano)

Esto evita que un bot sea responsable de demasiadas tareas.
Data Ingestion → Strategy → Execution

Este diseño permite:
	•	escalar estrategias sin duplicar datos
	•	reducir llamadas directas al exchange
	•	mantener datasets históricos consistentes
	•	ejecutar backtests realistas

⸻

Estado Actual del Proyecto

Actualmente el proyecto se encuentra en la primera etapa de desarrollo:

Market Data Layer

Esta capa es el corazón del sistema.

Responsabilidades:
	•	descargar datos del exchange
	•	mantener datasets históricos
	•	actualizar datos en tiempo real
	•	distribuir datos a las estrategias

⸻

Market Data System

El primer microservicio implementado es el Market Data System.

Este sistema se encarga de extraer, normalizar y almacenar datos de mercado.

Arquitectura interna:

Market Data System
│
├── Historical Data Pipeline
└── Real-Time Stream Pipeline

1️⃣ exchange_client.py  ✅ PRIMERO

Responsabilidad:
	•	crear conexión con exchange
	•	manejar rate limits
	•	manejar configuración
	•	manejar credenciales
	•	reutilizable por todo el sistema

⸻

2️⃣ fetcher.py

Responsabilidad:
	•	descargar OHLCV histórico
	•	usar paginación
	•	usar exchange_client

⸻

3️⃣ storage.py

Responsabilidad:
	•	guardar en Parquet
	•	crear carpetas
	•	evitar duplicados

⸻

4️⃣ transformer.py

Responsabilidad:
	•	limpiar datos
	•	convertir timestamps
	•	normalizar columnas

⸻

5️⃣ main.py

Responsabilidad:
	•	orquestar pipeline

