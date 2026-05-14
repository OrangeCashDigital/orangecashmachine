# -*- coding: utf-8 -*-
"""
shared/utils/
=============

Utilidades puras sin lógica de negocio. Solo stdlib y third-party.

Regla: ZERO imports de módulos internos del proyecto.
Si una utilidad necesita importar de trading/, market_data/, etc.,
pertenece a ese bounded context, no aquí.
"""

__all__: list[str] = []
