# -*- coding: utf-8 -*-
"""
market_data/storage/iceberg/_catalog_singleton.py
===================================================

Singleton del catálogo Iceberg — única responsabilidad: crear y devolver
la instancia SqlCatalog compartida por proceso.

Privado al paquete — importar via catalog.py, nunca directamente.

Paths
-----
SSOT: ocm_platform.config.paths.data_lake_root()

    iceberg_catalog/   → data_lake_root().parent / "iceberg_catalog"
    iceberg_warehouse/ → data_lake_root().parent / "iceberg_warehouse"

Precedencia de resolución (data_lake_root):
    1. OCM_DATA_LAKE_PATH (env var)          — máxima prioridad
    2. storage.data_lake.path (YAML)         — configurable por entorno
    3. repo_root()/data_platform/data_lake   — fallback estructural

Thread-safety
-------------
_LOCK garantiza que el singleton se construya exactamente una vez
incluso bajo carga concurrente (Prefect workers, asyncio tasks).

Ref: https://py.iceberg.apache.org/configuration/#sql-catalog
"""
from __future__ import annotations

import threading
from typing import Optional

from pyiceberg.catalog.sql import SqlCatalog

# SSOT de paths — nunca calcular paths aquí directamente.
from ocm_platform.config.paths import data_lake_root

_CATALOG: Optional[SqlCatalog] = None
_LOCK:    threading.Lock        = threading.Lock()


def get_catalog() -> SqlCatalog:
    """Devuelve el catálogo Iceberg compartido, creándolo exactamente una vez.

    Thread-safe: usa double-checked locking para evitar construcción
    duplicada bajo carga concurrente sin pagar el lock en el camino caliente.

    Paths resueltos desde data_lake_root() — SSOT de configuración de storage.
    Independiente del CWD y del entorno gracias a la cadena de resolución
    de paths.py (env var → YAML → fallback estructural).

    SqlCatalog SQLite: adecuado para single-node.
    Para multi-nodo → REST catalog o Hive Metastore (cambio en este módulo únicamente).
    """
    global _CATALOG

    # Camino caliente — sin lock si ya está inicializado.
    if _CATALOG is not None:
        return _CATALOG

    with _LOCK:
        # Double-checked: otro thread pudo haber construido mientras esperábamos.
        if _CATALOG is not None:
            return _CATALOG

        # SSOT: data_lake_root() resuelve en orden:
        #   OCM_DATA_LAKE_PATH → YAML → repo_root()/data_platform/data_lake
        lake     = data_lake_root()
        base     = lake.parent          # data_platform/ (o el parent del lake path)
        cat_path  = base / "iceberg_catalog"
        ware_path = base / "iceberg_warehouse"

        cat_path.mkdir(parents=True, exist_ok=True)
        ware_path.mkdir(parents=True, exist_ok=True)

        _CATALOG = SqlCatalog(
            "ocm",
            **{
                "uri":       f"sqlite:///{cat_path}/catalog.db",
                "warehouse": str(ware_path.absolute()),
            },
        )

    return _CATALOG


def reset_catalog() -> None:
    """Descarta el singleton — solo para tests.

    ADVERTENCIA: no llamar en producción. Invalida todas las referencias
    existentes al catálogo sin cerrar conexiones activas.
    """
    global _CATALOG
    with _LOCK:
        _CATALOG = None
