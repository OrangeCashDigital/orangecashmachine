# F2.6c — Capacity Planning empírico (canary 30 min bajo arranque manual)

- **Estado:**
  - F2.6c HECHO — medición empírica 30 min completada y verificada (2026-08-08)
  - Cierre formalizado 2026-08-10 (entregable documental faltante creado aquí)
- **Dependencia:** F2.6b (streaming_hydra.py MVP) HECHO; insumo directo de
  f2_6d (decisión de escalabilidad con evidencia).
- **Parent:** F2.6 Capacity Planning (docs/PLAN-Maestro-Ingenieria.md §4 F2.6);
  f2_6a (teórico, HECHO) en `docs/planning/f2_6a-capacity-teorico.md`.

## Objetivo

Contrastar la estimación teórica de F2.6a con medición real: msg/s, bytes/s,
CPU/RAM, latencia p50/p99 de procesamiento, errores y heartbeat del proceso
`streaming` (canary F2.6b, Bybit, 3 símbolos PERP) corriendo 30 minutos con
los 4 producers WS (orderbook/funding/oi/liquidations) escribiendo a Kafka
local. Conclusión explícita DOD de F2.6: ¿sigue siendo suficiente la
arquitectura de **proceso único (`systemd`) + Kafka local**?

## Evidencia (artefactos crudos)

| Artefacto | Contenido | Fuente de las cifras |
|---|---|---|
| `artifacts/f26c/canary_30m.log` | Log de arranque/cierre del canary: prod: bybit `['BTC-USDT-PERP','ETH-USDT-PERP','SOL-USDT-PERP']`, depth 50; kafka_producer_started x4; streaming_started 01:09:19; shutdown limpio 01:40:50 | Estado del proceso y símbolos |
| `artifacts/f26c/canary_cpu.csv` (317 filas) | Muestras cada 5 s: `ts_epoch,cpu_pct,rss_kb`; 316 muestras: cpu 0.00% constante, RSS 41340 KB constante | CPU/RAM real vs. tiempo |
| `artifacts/f26c/pushgateway_{10,15,20,25,30}min.txt` | Snapshots Prometheus del job `ocm_pipeline_orderbook` a 5 ventanas durante el canary | Contadores/latencia/heartbeat |
| `artifacts/f26c/canary30.pid` | PID del proceso (solo control) | — |

## Tabla de mediciones (fuente por celda)

| Métrica | Valor medido | Fuente en artefactos |
|---|---|---|
| Eventos publicados (orderbook.raw) | **249,380** | `pushgateway_30min.txt:99` `ocm_kafka_events_published_total` |
| Eventos procesados con commit exitoso | **249,380** | `pushgateway_30min.txt:93` `ocm_kafka_events_processed_total` |
| Errores (failed) | **0** | `ocm_kafka_events_failed_total` ausente en los 6 snapshots; 0 errores en `canary_30m.log` |
| msg/s promedio (ventana 30 min) | **138.5 msg/s** | 249,380 / 1,800 s = 138.5 (derivación del contador) |
| msg/s rate Prometheus `rate[30m]` | **126.4–132.0** | rate() sobre los snapshots (la ventana Prometheus no incluye el completo de 1,800 s exactos por post-procesamiento) → rango entre ventanas 10–30 min |
| Latencia procesamiento p50 | **7.55 ms** | `histogram_quantile(0.50, ocm_kafka_processing_latency_ms_bucket[30min])` sobre buckets de `pushgateway_30min.txt:102-113` |
| Latencia procesamiento p99 | **33.8 ms** | `histogram_quantile(0.99, ...)` (le=25: 246,591 / le=50: 247,403 → cuantil interpolado en [25,50) ms) |
| Latencia promedio | **7.39 ms** | `_sum` 1.842s / `_count` 249,380 (`pushgateway_30min.txt:114-115`) |
| CPU | **0.00 % promedio/máximo** | `canary_cpu.csv` (316 muestras/5 s, todo 0.00) — I/O-bound asíncrono |
| RAM | **40.4 MB RSS estable** | `canary_cpu.csv` `rss_kb=41340` sin variación |
| Bytes/s estimados a Kafka | **~38.2 KB/s** | 0.3×2363 (snapshots) + 138.2×278 (deltas); ~282.5 bytes/evento (estimación por tamaño de schema, no medición directa) |
| Throughput Kafka directo | **no medido** | sin exporter de broker durante el canary; solo `estimated_kafka_bytes_per_second` |
| Lag Kafka | **no medido** | topic no consumido durante el canary (`ocm_kafka_events_processed` == published: el contador "processed" es del push de métricas, no de un consumer de orderbook.raw) |
| Heartbeat | **139/139 pushes OK** | `pushgateway_http_push_duration_seconds_count{method=put} = 139` (`pushgateway_30min.txt:177`); job=ocm_pipeline_orderbook a 15 s |
| Símbolos activos (book depth 50) | **3** (BTC/ETH/SOL USDT PERP) | `canary_30m.log` `orderbook_stream_starting` |

## Análisis vs. presupuesto teórico (F2.6a)

| Dimensión | Teórico F2.6a | Medido F2.6c | Veredicto |
|---|---|---|---|
| msg/s (pico estimado) | ~160 msg/s pico | 138.5 promedio; ventana Prometheus 126.4–132.0 | Dentro del rango estimado |
| Bytes/s ingreso Kafka | ~54 KB/s con overhead | ~38.2 KB/s estimado | Por debajo de la cota |
| Latencia p50 E2E | 10–30 ms | 7.39 prom. / 7.55 p50 (procesamiento) | Mejor que la cota |
| Latencia p99 E2E | 60–150 ms | 33.8 ms (procesamiento) | Mejor que la cota |
| CPU / RAM | sin presupuesto formal documentado en doc único (hardware por registrar) | 0.00 % CPU / 40.4 MB | Margen enorme |

## Conclusión (DOD F2.6)

**"Arquitectura de proceso único (`systemd`) + Kafka local suficiente para el
canary de F2.6b": SÍ, con evidencia empírica.**

El canary de 30 min (Bybit, 3 símbolos PERP, 4 producers WS) consumió
**0.00 % de CPU y 40.4 MB de RAM** con **0 errores** y latencia de
procesamiento p50/p99 de **7.55/33.8 ms** — muy por debajo de las cotas del
umbral de invalidación fijado en F2.6a (>50–100 símbolos activos, >50 % CPU,
p99 >500 ms, o lag creciente). **No se identifica ningún déficit** que exija
segundo proceso, particionado Kafka, orquestador ni tooling de escala para el
volumen del canary.

**Alcance de la suficiencia (límite):** la medición cubre el canary
(3 símbolos, 1 exchange, profundidad 50). La suficiencia para la **producción
final** (más exchanges/símbolos, 6–12 meses) se afirma como válida por margen
de CPU/RAM observado, pero debe **re-verificarse si el número de libros
activos escala más allá del umbral de F2.6a** (>50–100 símbolos) o si
aparece lag/CPU en condiciones de tensión de mercado. Decisiones de escala
posteriores deben re-validarse con la misma metodología (ver f2_6d).

## Decisión f2_6d (insumo)

Este documento es la evidencia que f2_6d consume: la conclusión de f2_6d es
**"proceso único suficiente; sin ADR de escalabilidad con la evidencia
actual"**. Véase la entrada `f2_6d_decision_escalabilidad` en tracking.yaml
para la resolución completa (cierre 2026-08-10).

## Seguimiento / pendientes derivados

- `kafka_throughput` y `kafka_lag` no medidos (sin exporter de broker / sin
  consumer de orderbook.raw). Opcional para una re-medición: exporter de
  Kafka y consumer de prueba para cerrar estas dos celdas.
- Registro del hardware de orangehouse (CPU cores, RAM, NIC) en un doc único,
  como f2_6a ya señalaba como pendiente — no capturado en el canary.
- El modo `streaming` sigue sin unit systemd formal (canary arrancado
  manualmente; `criterios_aceptacion.systemd_reinicia_correctamente` de f2_6b
  quedó NO_VERIFICADO). Pendiente de despliegue F3/ADR-0022.