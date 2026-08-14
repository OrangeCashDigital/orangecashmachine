# Arquitectura de OrangeCashMachine (OCM) — mapa canónico

Este documento responde, con evidencia de código, cinco preguntas:

1. ¿Qué puedo ejecutar hoy?
2. ¿Qué está en construcción?
3. ¿Qué existe como componente interno (embebido)?
4. ¿Qué existe como proceso independiente?
5. ¿Qué está planeado para convertirse en microservicio?

Ver ADR-0024 para la evidencia detallada y las decisiones formales
detrás de este mapa.

## Arquitectura actual (lo que realmente corre)

```
                    OCM (runtime real hoy)
                          │
              ┌───────────┴───────────┐
              │                       │
        paper_hydra.py          live_hydra.py
     (apps/app/cli/paper_hydra) (apps/app/cli/live_hydra)
              │                       │
              └───────────┬───────────┘
                           ▼
              PortfolioCompositionRoot.assemble(config)
                  (packages/portfolio/bootstrap/composition_root.py)
                           │
                           ▼
                    PortfolioService
                     │            │
                     ▼            ▼
              RedisPositionStore  InMemoryPositionStore
                (config.integrations.redis.enabled decide)

  TradingCompositionRoot.assemble_live()/assemble_paper()
  (packages/trading/bootstrap/composition_root.py)
  recibe portfolio ya ensamblado, ensambla Strategy + RiskManager +
  Executor + OMS + TradingEngine — usado por
  apps/app/use_cases/execute_live.py / execute_paper.py.

  market-data: proceso independiente REAL (FastAPI)
  packages/market_data/main.py — python -m market_data.main
  /health · /ready · /ohlcv/{exchange}/{symbol}/{timeframe}
  Estado operativo: actualmente DETENIDO (ni Docker ni local).
```

**Nota sobre el estado embebido de trading/portfolio:** el motor de
trading y el portfolio corren *dentro del mismo proceso* que
`paper_hydra.py`/`live_hydra.py`. Esto es correcto y deliberado para el
estado actual — no es una limitación temporal a "arreglar", es la forma
en que el sistema opera hoy en NIVEL 1.

## Arquitectura en evolución (dirección, no implementación)

```
   market-data (microservicio)      trading (microservicio)      portfolio (microservicio)
        :8001 — NIVEL 1                  :8002 — NIVEL 2               :8003 — NIVEL 2
        FastAPI operativo                composition root real,        composition root real,
        python -m market_data.main       usado en producción           usado en producción
                                          embebida; sin main.py         embebida; sin main.py
              │                                 │                             │
              └──────HTTP (MARKET_DATA_URL)─────┘                             │
                      (env var ya definida                                    │
                       en docker-compose.yml,                                 │
                       aún sin consumidor real)                               │
                                                  ── comunicación futura ──────┘
                                                     (contrato/eventos/API,
                                                      no implementada todavía)
```

`microservices` es una **dirección arquitectónica**, documentada como tal
en `docker-compose.yml` (profile `microservices`) y ahora formalizada en
ADR-0024. No implica que los tres servicios deban estar operativos hoy.

## Matriz de madurez

| Componente | Nivel | Entry point ejecutable | Tests dominio | Config Hydra propia | Import-linter | Docker |
|---|---|---|---|---|---|---|
| **market-data** | 1 — Implementado | `python -m market_data.main` ✅ | Sin `TestClient` (brecha conocida) | Sí | BC-50 | Compose ✅ / Dockerfile CMD ahora alineado |
| **trading** | 2 — En construcción, real | No existe; runtime puro (`TradingEngine`) sin `__main__` | 9 archivos (`tests/trading/`) | No existe `config/trading/` | BC-12, BC-36, BC-50 | `command: python -m trading.main` (módulo inexistente) |
| **portfolio** | 2 — En construcción, real | No existe | 4 archivos (`tests/portfolio/`) | Sí, `config/portfolio/portfolio.yaml` | BC-13, BC-43, BC-44 | `command: python -m portfolio.main` (módulo inexistente) |
| **paper** (embebido) | 1 — Implementado | `apps/app/cli/paper_hydra.py` ✅ | Cubierto vía trading/portfolio | vía `AppConfig` | — | No es servicio Docker propio |
| **live** (embebido) | 1 — Implementado (con hallazgo B-23 abierto, independiente) | `apps/app/cli/live_hydra.py` ✅ | Cubierto vía trading/portfolio | vía `AppConfig` | — | No es servicio Docker propio |

## Criterio para pasar de NIVEL 2 a NIVEL 1

Un bounded context embebido (hoy: trading, portfolio) se considera listo
para convertirse en proceso independiente cuando tiene, siguiendo el
patrón ya validado por market-data:

- Entrypoint HTTP propio (`main.py` con FastAPI, lifespan, `ExecutionGuard`)
- `/health` y `/ready` reales, no stubs
- Tests de integración HTTP (`TestClient`) — nota: market-data mismo
  todavía no cumple esto (ver ADR-0024), es brecha compartida
- Config Hydra propia bajo `config/<bc>/`
- Contrato de comunicación explícito con los demás servicios (HTTP/eventos)
- Validación en CI del arranque del servicio, no solo de sintaxis de compose

## Referencias

- ADR-0024 — dirección arquitectónica microservicios (evidencia completa)
- ADR-0003 — interfaz angosta `TradingCompositionRoot`
- ADR-0006 — portfolio posee el estado de posiciones
- ADR-0012 — `TradingEngine` como runtime puro
- `architecture/importlinter.toml` — contratos BC-12, BC-13, BC-36, BC-43, BC-44, BC-50
