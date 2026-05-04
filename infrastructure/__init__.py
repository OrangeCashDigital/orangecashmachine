# -*- coding: utf-8 -*-
"""
infrastructure/
===============

Capa de infraestructura — adapters de frameworks externos.

Submódulos
----------
  dagster/   — orquestación declarativa (assets, resources, definitions)

Regla de dependencias
---------------------
infrastructure/ importa desde application/ y ports/
infrastructure/ NO es importado desde domain/ ni application/

Principios: DIP · Clean Architecture · SRP
"""
