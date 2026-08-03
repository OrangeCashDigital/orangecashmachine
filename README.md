# OrangeCashMachine 🟠

Data lakehouse para datos de mercado de criptoactivos. Ingestiona datos de múltiples
exchanges, los procesa en una arquitectura **medallion** (Bronze → Silver → Gold) sobre
**Apache Iceberg** y expone capas de datos limpias y reproducibles con *time-travel*.

Arquitectura Clean/Hexagonal por **bounded contexts**, contratos de frontera verificados
estáticamente por `import-linter` en cada CI, configuración por **Hydra** y observabilidad
con **Prometheus / Grafana / Loki**.

[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![Hydra](https://img.shields.io/badge/hydra-1.3-lightblue.svg)](https://hydra.cc/)
[![ccxt](https://img.shields.io/badge/ccxt-4.3-orange.svg)](https://github.com/ccxt/ccxt)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](pyproject.toml)

---

## ¿Qué es OrangeCashMachine?

OrangeCashMachine es un **pipeline profesional de datos de mercado cripto** que convierte
raw feeds de exchanges en un warehouse analítico reproducible:

```mermaid
flowchart LR
    ex[Bybit · KuCoin · KuCoinFutures] --> bronze[Bronze — raw Parquet]
    bronze --> silver[Silver — limpio + manifiestos de versión]
    silver --> gold[Gold — features en Iceberg]
```

| Capa   | Contenido                                                    |
|--------|---------------------------------------------------------------|
| Bronze | Datos crudos por exchange, con retención y reingestión        |
| Silver | Datos limpios, normalizados, con manifiestos de versión        |
| Gold   | Features procesadas listas para análisis, con *time-travel*   |

Cada escritura registra **lineage** (`git_hash`, `written_at`) para reproducibilidad.

## ¿Qué problema resuelve?

Construir datasets históricos de cripto confiables es costoso: cada exchange tiene
comportamientos distintos, faltan datos y los snapshots cambian sin aviso. OrangeCashMachine
centraliza esa complejidad en un solo lugar — las capacidades se describen abajo.

## Características principales

| Área           | Detalle                                                             |
