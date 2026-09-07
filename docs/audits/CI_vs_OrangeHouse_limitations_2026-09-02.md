# Limitaciones CI vs OrangeHouse — B-49 Production Gates

## Resumen

Este documento describe las limitaciones conocidas de la validación de B-49 G4-G9 en GitHub Actions CI estándar y CI de integración, y por qué ciertos gates solo pueden validarse completamente en OrangeHouse (producción real).

---

## Matriz de Ejecución por Gate

| Gate | CI estándar (`gate-ci`) | CI integración (`gate-integration`) | OrangeHouse |
|------|------------------------|-------------------------------------|-------------|
| **G1** | ✅ PASS/FAIL | ✅ PASS/FAIL | ✅ PASS/FAIL |
| **G2** | ✅ PASS/FAIL | ✅ PASS/FAIL | ✅ PASS/FAIL |
| **G3** | ✅ PASS/FAIL | ✅ PASS/FAIL | ✅ PASS/FAIL |
| **G4** | ✅ Validación estática | ✅ Verify systemd* | ✅ Instalación real |
| **G5** | ⏭️ SKIPPED | ✅ Docker ps running | ✅ systemctl active |
| **G6** | ⏭️ SKIPPED | ✅ Service containers | ✅ Infra real |
| **G7** | ⏭️ SKIPPED | 🚫 BLOCKED | ✅ health_check.sh |
| **G8** | ✅ Código (IS_STUB + guards) | ✅ Código + infra opcional | ✅ Completo |
| **G9** | ⏭️ SKIPPED | ⏭️ SKIPPED | ✅ Bronze + run_id |
| **G10** | ✅ PASS/FAIL | ✅ PASS/FAIL | ✅ PASS/FAIL |
| **G11** | ✅ PASS/FAIL | ✅ PASS/FAIL | ✅ PASS/FAIL |

* systemd-analyze verify solo si systemd está disponible en el runner

---

## Detalle por Gate

### G4 — DEPLOYMENT CONFIGURATION

**CI estándar**: Validación estática completa (templates, docker-compose, host.env.example, renderizado install_systemd.sh). NO ejecuta `systemd-analyze verify` (systemd no disponible en runners GitHub Actions).

**CI integración**: Igual que CI estándar + `systemd-analyze verify` si systemd está disponible en el runner (típicamente no).

**OrangeHouse**: Instalación real via `install_systemd.sh` + `systemctl daemon-reload` + verificación de units instaladas.

**Evidencia CI**: Logs de validación estática.

---

### G5 — RUNTIME PROCESSES (Liveness Only)

**CI estándar**: **SKIPPED** — No hay runtime real (systemd ni containers de aplicación).

**CI integración**: Verifica `docker ps` para containers `ocm_market_data` y `ocm_streaming` running. **NOTA**: Los containers de aplicación NO se levantan en CI integración por defecto (perfil `microservices` en docker-compose). El gate reportará FAIL a menos que se levante el perfil `microservices`.

**OrangeHouse**: `systemctl is-active ocm-market-data.service` + `systemctl is-active ocm-streaming.service`.

**Evidencia CI**: Estado de containers Docker.

---

### G6 — EXTERNAL DEPENDENCIES

**CI estándar**: **SKIPPED** — No hay Kafka/Redis reales.

**CI integración**: Service containers Kafka (con Zookeeper) + Redis con auth. Verifica `kafka-broker-api-versions` y `redis-cli -a $PASS ping`. Valida consistencia de variables `KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_BOOTSTRAP_SERVERS_INTERNAL`.

**OrangeHouse**: `docker exec ocm_kafka kafka-broker-api-versions --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS` + `docker exec ocm_redis redis-cli -a $REDIS_PASSWORD ping`.

**Evidencia CI**: Logs de conectividad broker API + Redis PONG.

---

### G7 — FUNCTIONAL HEALTH

**CI estándar**: **SKIPPED** — Requiere systemd (L1) + pipeline real produciendo datos (L3/L4).

**CI integración**: **BLOCKED** — `health_check.sh` requiere:
- L1: systemd units activas (no disponible en GitHub Actions)
- L3: Tópicos Kafka con mensajes frescos (pipeline no corre en CI)
- L4: Archivos parquet Bronze < 15 min (pipeline no produce en CI)

**OrangeHouse**: **Único entorno válido** — Ejecuta `./deploy/scripts/health_check.sh` completo que verifica L1-L4 + OBS. Exit 0 = todos HEALTHY.

**Evidencia CI**: N/A — Solo OrangeHouse.

---

### G8 — TRADING SAFETY

**CI estándar**: Verifica código:
- `grep IS_STUB packages/trading/` → detecta `IS_STUB = True` o `False`
- Si `IS_STUB = True` → PASS (trading bloqueado)
- Si `IS_STUB = False` → Verifica risk guards en código (capital_usd>0, stop_loss, max_retries>=1, guard inyectado, reconciliation fail-closed)

**CI integración**: Igual que CI estándar + opcionalmente verifica tópicos de órdenes en Kafka.

**OrangeHouse**: Verificación completa (código + runtime).

**Estado actual**: `IS_STUB = False` (trading habilitado intencionalmente, F3/B-12 completado). G8 PASS si risk guards verificados en código.

---

### G9 — DATA FRESHNESS

**CI estándar**: **SKIPPED** — No hay pipeline activo.

**CI integración**: **SKIPPED** — Pipeline market-data no corre en CI (requiere CCXT, exchanges, etc.).

**OrangeHouse**: **Único entorno válido** — `find bronze -mmin -15` + verificación de `run_id` válido en metadatos parquet (polars). Confirma que el pipeline real (ingestion_loop + bronze_writer_loop) está produciendo datos recientes.

**Evidencia CI**: N/A — Solo OrangeHouse.

---

## Mock HTTP Removido

El workflow `ocm-ci-integration.yml` **ya no incluye** el mock HTTP simple en puerto 8001 que se usaba anteriormente para G5.

**Razón**: El mock (`http.server.BaseHTTPRequestHandler` respondiendo `/health`) **no representa** `market_data.main` real:
- No tiene FastAPI, `/ready`, `/ohlcv`
- No tiene background tasks (ingestion_loop, bronze_writer_loop, feed_orchestrator)
- No conecta a Kafka/Redis
- Siempre retorna healthy

**Impacto**: G5 en CI integración ahora verifica solo liveness de containers (si se levanta perfil `microservices`), no health HTTP. G7 no se ejecuta en CI integración por diseño.

---

## KAFKA_BOOTSTRAP Variables

Dos variables separadas por contexto:

| Variable | Valor | Contexto |
|----------|-------|----------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9093` | Procesos en HOST (systemd units) — puerto EXTERNAL |
| `KAFKA_BOOTSTRAP_SERVERS_INTERNAL` | `kafka:9092` | Procesos en Docker (health_check.sh via docker exec) — puerto INTERNAL |

Ambas definidas en `deploy/host.env` y `deploy/host.env.example`. El código de aplicación usa `KAFKA_BOOTSTRAP_SERVERS` (cargado via EnvironmentFile en systemd units). `health_check.sh` usa `KAFKA_BOOTSTRAP_SERVERS_INTERNAL`.

---

## Criterio para B-49 = HECHO

B-49 solo se considera **HECHO** cuando:

1. **CI estándar**: G1,G2,G3,G4(static),G8(code),G10,G11 → PASS; G5,G6,G7,G9 → SKIPPED
2. **CI integración**: G4(verify),G5(Docker),G6(services),G8(full) → PASS; G7 BLOCKED, G9 SKIPPED
3. **OrangeHouse**: **TODOS G4-G9 → PASS** con evidencia real:
   - G4: systemd units instaladas y verify OK
   - G5: Ambos servicios systemd active
   - G6: Kafka broker API + Redis PONG + config consistente
   - G7: `health_check.sh` exit 0 (3 dominios HEALTHY)
   - G8: IS_STUB=False + risk guards verificados
   - G9: Bronze parquet <15min con run_id válido

**NO se aceptan**: Mocks, fixtures, atajos, o rebaja de criterios para forzar PASS.

---

## Referencias

- `scripts/check_production_gates.py` — Implementación de gates con detección de entorno
- `.github/workflows/ocm-ci.yml` — CI estándar (gate-ci)
- `.github/workflows/ocm-ci-integration.yml` — CI integración (gate-integration)
- `deploy/scripts/health_check.sh` — Contrato de salud oficial (L1-L4)
- `deploy/scripts/install_systemd.sh` — Instalación/verificación systemd
- `deploy/host.env.example` — Variables de host (incluye KAFKA_BOOTSTRAP dual)