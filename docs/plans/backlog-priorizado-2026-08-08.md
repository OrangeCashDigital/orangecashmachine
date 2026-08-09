# Backlog priorizado por riesgo — auditoría streaming canary (2026-08-08)

**Fuente:** docs/audits/2026-08-08-streaming-canary-audit.md (SSOT de hallazgos)
**Tracking formal:** docs/plans/tracking.yaml (IDs H-NN / B-NN)
**Regla:** este documento es una vista de priorización, no una fuente de verdad
alternativa. Ante cualquier discrepancia, el audit.md y tracking.yaml mandan.

---

## 🔴 Capital / data-integrity risk

*(ninguno detectado en esta auditoría)*

Criterio de entrada a esta categoría: el sistema opera o podría operar con datos
incorrectos/stale/corruptos **sin saberlo**, con riesgo directo de decisión de
trading errónea. Los hallazgos de hoy son de *ausencia de detección* ante fallos
ya visibles (logs, métricas), no de *corrupción silenciosa en curso*. Si F-009
(gap detection) se posterga indefinidamente, reevaluar si escala a esta categoría.

---

## 🟠 Market-data reliability

### F-009 / H-24 / B-25 — Sin retry/DLQ/gap detection en pipeline WS→Kafka
- **Severidad:** P2 | **Estado:** PENDIENTE
- Un delta de order book perdido no se detecta ni se recupera automáticamente.
- Bloqueador de diseño: requiere sequence numbers en `OrderBookDeltaPayload`
  (no existen hoy) + decisión de arquitectura de DLQ (candidato a ADR).

### F-010 / H-25 / B-26 — Sin alertas operacionales WS/Kafka
- **Severidad:** P2 | **Estado:** PENDIENTE
- `ocm_kafka_events_failed_total` existe pero no dispara ninguna alerta.
- Menor esfuerzo que F-009: extensión directa de `deploy/monitoring/alerts.yml`
  siguiendo el formato de las 3 reglas ya existentes. Candidato a "quick win".

---

## 🟡 Architecture / maintainability

### F-011 / H-26 / B-27 — `infrastructure/` raíz ambiguo vs. capa Clean Architecture
- **Severidad:** P3 | **Estado:** PENDIENTE
- Sin dependencia cruzada real verificada — riesgo bajo, confusión de naming.
- Incluye corrección de docstring falso en `timeouts.py` (import que no existe).

---

## 🔵 Documentation / hygiene

### F-012 / H-27 / B-28 — Timeouts con claims de p99/SLA sin fuente verificable
- **Severidad:** P3 | **Estado:** PENDIENTE
- No es defecto funcional; riesgo indirecto si alguien recalibra confiando en
  números no verificados como si fueran medición real.

---

## Recomendación de secuencia

1. **F-010** primero — menor esfuerzo, cierra el gap de "nadie se entera a las 3AM"
   sin requerir decisiones de diseño nuevas.
2. **F-009** — mayor esfuerzo, requiere ADR; evaluar si el volumen actual del
   canary (ver F2.6c: 138.5 msg/s, 0 errores en 30 min) justifica priorizarlo
   ahora o esperar a que `live_hydra.py` efectivamente use estos producers.
3. **F-011 y F-012** — sin urgencia operativa; agrupables en un único PR de
   limpieza cuando haya ventana disponible.

*(F-008/H-23/B-24 no aparece aquí — ya cerrado, ver commit cade1d3)*

---

## Addendum 2026-08-08 (sesion continuacion) — Kafka delivery semantics

### F-013 / B-29 — produce() descarta bool de send_async(), falso event_published
- **Severidad:** P0 | **Estado:** PENDIENTE
- KafkaProducerAdapter.produce() (infrastructure/kafka/producer.py) hace
  `await self.send_async(...)` sin evaluar el bool de retorno; send_async()
  retorna False ante cualquier excepcion de Kafka pero produce() no lo
  propaga. Afecta 4 de 6 WS producers confirmados (orderbook, oi,
  liquidations, funding) que llaman produce() en vez de send_async().
- Candidato a reevaluar si escala a categoria Capital/data-integrity risk:
  a diferencia de F-009/F-010, aqui el sistema no solo omite deteccion —
  metrifica exito falso en ocm_kafka_events_published_total.

### F-014 / B-30 — close() no hace flush() antes de stop() del cliente Kafka
- **Severidad:** P0 | **Estado:** PENDIENTE
- close() llama directo a self._producer.stop() sin flush() previo;
  mensajes en buffer local (linger_ms=5, batching activo) pueden perderse
  en cada shutdown ordenado (SIGTERM, ADR-0022).
- Docstring de stop() afirma "flush implicito" — no verificado en codigo,
  es falso.

### F-015 / B-31 — Sin enable_idempotence en AIOKafkaProducer
- **Severidad:** P1 | **Estado:** PENDIENTE
- acks=all configurado pero enable_idempotence ausente (default False en
  aiokafka); retry sin idempotencia productor-side puede duplicar
  mensajes en el topic.

### F-016 / B-32 — Alertmanager receiver 'default' sin destino configurado
- **Severidad:** P0 | **Estado:** PENDIENTE
- deploy/monitoring/alertmanager.yml — receiver sin slack/email/webhook
  configs. Complementa F-010/B-26: aunque se agreguen las reglas que B-26
  propone, ninguna alerta notificaria a un humano sin esto.

### F-017 / B-33 — _PUSH_EXCHANGE hardcodeado invalida aislamiento por job
- **Severidad:** P0 | **Estado:** PENDIENTE
- apps/app/cli/streaming_hydra.py:73 — _PUSH_EXCHANGE = "orderbook" fijo;
  el comentario de diseño ("un job por exchange evita last-write-wins")
  es falso en la practica porque el label nunca varia entre exchanges.
- Bloqueante conceptual para cualquier alerta de heartbeat por exchange
  (ver F-018/B-34).

### F-018 / B-34 — Alerta PipelineDown apunta a metrica inexistente
- **Severidad:** P0 | **Estado:** PENDIENTE
- deploy/monitoring/alerts.yml:6 — absent(ocm_pipeline_runs_total); esa
  metrica no existe en ningun .py del proyecto (busqueda exhaustiva sin
  resultados). Metrica real: ocm_pipeline_heartbeat_total. La unica regla
  que en teoria cubre el deadman-switch del canary esta rota.

### F-019 / B-35 — Captura generica en send_async() sin distinguir causas
- **Severidad:** P1 | **Estado:** PENDIENTE
- except Exception unico en send_async() colapsa ~44 BrokerResponseError
  + KafkaTimeoutError/KafkaConnectionError/KafkaUnavailableError en el
  mismo reason=write_error. No crea ADR nuevo — extiende el mismo espacio
  de problema que F-009/B-25 (DLQ/gap detection ya pendiente).

### F-020 / B-36 — OnchainKafkaProducer hereda de clase no encontrada
- **Severidad:** P0 (candidato, pendiente confirmar) | **Estado:** UNVERIFIED
- onchain_producer.py hereda de BaseKafkaProducer; grep exhaustivo en todo
  el repo no encuentra esa clase definida. Posible import roto.

### F-021 / B-37 — Metodo legacy produce(payload, key) sin callers
- **Severidad:** P3 | **Estado:** PENDIENTE
- Codigo muerto confirmado en orderbook_producer.py; mismo patron
  presente en oi/liquidations/funding_producer.py, no verificado si
  tienen callers activos.

---

## Recomendacion de secuencia (addendum)

1. **F-018 (B-34)** — sin esto ninguna alerta de heartbeat sirve.
2. **F-017 (B-33)** — resolver junto con F-018, cualquier alerta nueva
   hereda el problema de aislamiento por exchange si no se corrige antes.
3. **F-016 (B-32)** — en paralelo, condicion necesaria para que cualquier
   alerta llegue a un humano.
4. **F-013 / F-014 (B-29 / B-30)** — mayor impacto real en integridad de
   datos; requieren cambio de contrato en KafkaProducerPort, agrupables
   en un mismo PR.
5. **F-015 (B-31)** — evaluar junto con F-013/F-014, mismo archivo.
6. **F-019 (B-35)** — se resuelve naturalmente al abordar B-25, no
   requiere esfuerzo aislado.
7. **F-020 / F-021 (B-36 / B-37)** — sin urgencia, agrupables en PR de
   limpieza. F-020 requiere verificacion previa (no confirmado aun si es
   import roto real).

*(Auditoria de codigo real, no inferencia — ver docs/audits/2026-08-08-streaming-canary-audit.md seccion "Kafka failure semantics / delivery guarantees" para evidencia completa archivo:linea.)*
