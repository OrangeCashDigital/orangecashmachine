# -*- coding: utf-8 -*-
"""
domain/
=======

Core de dominio puro — sin dependencias de infraestructura ni frameworks.

Regla de oro: ningún módulo dentro de domain/ puede importar desde
trading/, market_data/, ocm_platform/, data_platform/ ni librerías
de terceros salvo stdlib y pydantic (para value objects).

Submódulos
----------
  value_objects/  — tipos inmutables sin identidad
  entities/       — tipos con identidad y ciclo de vida
  events/         — domain events publicados entre bounded contexts
"""
