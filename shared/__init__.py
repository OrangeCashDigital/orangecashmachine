# -*- coding: utf-8 -*-
"""
shared/
=======

Kernel compartido entre todos los bounded contexts.

Estructura
----------
  types/      — value objects, entities, domain events (stdlib-only)
  contracts/  — protocols/abstracciones inter-BC (DIP · OCP)
  exceptions/ — excepciones base compartidas
  utils/      — utilidades puras sin lógica de negocio

Regla de dependencia (BC-09 en pyproject.toml):
  shared/ → SOLO stdlib y third-party
  PROHIBIDO importar desde: market_data, trading, portfolio,
                             ocm_platform, infrastructure, apps, data_platform

Esto garantiza que shared/ sea el nivel más bajo del grafo de dependencias,
nunca creando ciclos.
"""
