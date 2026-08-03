# -*- coding: utf-8 -*-
"""
shared/
=======

Kernel compartido entre todos los bounded contexts.

Estructura
----------
  types/      — value objects, entities, domain events (stdlib-only)
  contracts/  — protocols/abstracciones inter-BC (DIP · OCP)
  enums.py    — vocabulario de dominio cross-BC (literales, SSOT de enums)
  exceptions/ — excepciones base compartidas
  kafka/      — wire schemas + serializer + topics (SSOT del bus)
  utils/      — utilidades puras sin lógica de negocio

Regla de dependencia (BC-01 en architecture/importlinter.toml):
  shared/ → SOLO stdlib y third-party
  PROHIBIDO importar desde: market_data, trading, portfolio,
                             ocm, infrastructure, apps, data_platform

Esto garantiza que shared/ sea el nivel más bajo del grafo de dependencias,
nunca creando ciclos.
"""
