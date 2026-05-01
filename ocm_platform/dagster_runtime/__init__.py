"""
ocm_platform.dagster_runtime
============================

Namespace canónico de integración Dagster para OrangeCashMachine.

Contenido
---------
Este paquete expone adaptadores, helpers y configuración que
pertenecen a la capa de orquestación Dagster pero no son assets.

Los assets viven en dagster_assets/ (raíz del proyecto),
separados intencionalmente para respetar la convención de Dagster
(dagster dev -f dagster_defs.py resuelve desde la raíz).

Principios: SSOT · SRP · DIP
"""
