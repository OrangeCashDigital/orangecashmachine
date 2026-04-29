# -*- coding: utf-8 -*-
"""
app/use_cases/
==============

Application layer — casos de uso del sistema OCM.

Cada use case orquesta bounded contexts para cumplir
un objetivo de negocio. No contiene lógica de dominio.

  run_market_data  → ingesta y procesamiento de datos
  run_paper        → ciclo de paper trading
  run_live         → ciclo de live trading (futuro)
  rebalance        → rebalanceo de portfolio

Principios: SOLID · SRP · DIP · KISS
"""
