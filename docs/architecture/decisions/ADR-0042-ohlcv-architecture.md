# ADR-0042 — OHLCV Live / Historical / Recovery Architecture

**Estado:** Obsoleto (degradado a documentación + tracking, Fase 0 — 2026-09-24)
**Fecha:** 2026-09-21
**Bounded context(s) afectado(s):** market_data

> **Disposición Fase 0 (2026-09-24, decisión B2):** este documento se degrada de
> ADR propuesta a **documentación de referencia** (análisis OHLCV live/historical/
> recovery). No contiene ninguna decisión arquitectónica pendiente que bloquee
> implementación: las vías de acción quedan registradas en
> `docs/plans/tracking.yaml` (bloque `fase0_gobernanza.b2` y hallazgos MD-RECOVERY-*,
> Fase 3 del Plan Maestro). El contenido técnico se conserva como contexto
> normativo-referencial para la Fase 3, no como ADR normativo.

## 1. Contexto

La auditoría maestre ha confirmado factualemente:

- `DETECTION ≠ RECOVERY` — QualityPipeline detecta gaps pero no dispara recuperación (MD-RECOVERY-002)
- `RepairStrategy` está implementada pero **desconectada del runtime** — ningún caller en main.py selecciona `mode="repair"` (MD-RECOVERY-001, MD-RECOVERY-002)
- `IncrementalStrategy` SÍ se ejecuta en main.py con `mode="incremental"` hardcodeado (línea 174)
- `BackfillStrategy` SÍ ejecuta vía CLI con `backfill_mode=true` (entrypoint.py)
- `RepairStrategy` nunca se selecciona en producción; `mode="repair"` es teóricamente posible pero operativamente inactivo
- `PipelineContext.storage = None` por diseño en `OHLCVPipeline`; assert fail-fast si alguien lo intenta usar
- `F-RT-01` errores pandas TypeError desde imagen Docker stale 2026-08-20
- Dataset Bronze/Silver/Gold congelado (sin scheduler/cron activo)

El problema arquitectónico: OCM tiene las piezas individuales correctas (IncrementalStrategy, BackfillStrategy, Quality gap detection, RepairStrategy code) pero el diseño de integración es insuficiente. Ningún componente orquesta la recuperación.

## 2. Problema

Definir la arquitectura OHLCV para OCM que aclare:

- Qué modo se ejecuta en runtime por defecto
- Quién tiene ownership de Storage
- Quién orquesta la recuperación de gaps
- Cómo interactúan LIVE, HISTORICAL, BACKFILL, REPAIR, RECONCILIATION
- El boundary entre Quality (detección) y Recovery (acción)

## 3. Drivers de decisión

Las siguientes propiedades importan:

- **Propiedad de runtime**: qué modo se ejecuta realmente en producción (incremental por defecto)
- **Propiedad de ownership**: quién es dueño de Storage (y bajo qué condiciones)
- **Propiedad de orquestación**: quién decide y ejecuta recovery ante gaps detectados
- **Propiedad de隔离**: LIVE vs HISTORICAL vs RECOVERY deben permanecer separados
- **Propiedad de fail-soft**: comportamiento cuando fallan componentes (Docker stale, service crash-loop)

## 4. Consideradas alternativas

**A. Mantener estado actual (sín decisión arquitectónica)**
- *Ventaja*: No cambiar nada que funcione parcialmente
- *Desventaja*: DETECTION ≠ RECOVERY permanece sin resolver; RepairStrategy inactiva; gaps detectados pero sin acción; Bronze/Silver/Gold frozen; riesgo de datos inconsistentes

**B. Documentar arquitectura OHLCV con ownership explícita**
- *Ventaja*: Decisión clara sobre modo por defecto, ownership de storage, orquestación de recovery; evidencia para futuras implementaciones
- *Desventaja*: Requiere definir decisiones que hoy no están resueltas; necesita definición de modos y ownership

**C. Reparar arquitectura OHLCV ahora**
- *Ventaja*: Corregir Detection ≠ Recovery; conectar RepairStrategy; poner Bronze/Silver/Gold en operación
- *Desventaja*: Requiere múltiples cambios simultáneos; Docker image stale debe rebuild; servicio systemd debe repararse; Bronze writer debe crearse; no es decisión aislada

## 5. Decisión

**Definir arquitectura OHLCV documentada con boundaries explícitas**, pero **no implementar fixes todavía**. La arquitectura debe:

1. **Definir modo por defecto**: `IncrementalStrategy` es el modo ejecutándose en `main.py:174`; `mode="repair"` nunca se selecciona; esta es la realidad operativa
2. **Definir ownership de Storage**: `PipelineContext.storage = None` por diseño en `OHLCVPipeline`; el caller debe inyectarlo; `RepairStrategy` requiere storage inyectado (assert fail-fast si es None); no hay ownership definida actualmente
3. **Definir orquestación de recovery**: `QualityPipeline` detecta gaps y emite métricas/logs (`is_new`, `GAP_DETECTED`); `RepairStrategy` puede heal gaps pero requiere `ctx.storage is not None` y caller que invoque `mode="repair"`; **actualmente nadie invoca repair en runtime**
4. **Definir boundaries LIVE/HISTORICAL/RECOVERY/RECONCILIATION**:
   - **LIVE**: `IncrementalStrategy` vía main.py ingestion loop; `mode="incremental"` hardcodeado; Quality `gap detection` confirma `is_new`; datos fluyen Kafka → Bronze
   - **HISTORICAL**: `BackfillStrategy` vía CLI `entrypoint.py` con `backfill_mode=true`; paginación REST histórica; datos fluyen Kafka → Bronze
   - **RECOVERY**: `Quality` detecta gaps; `RepairStrategy` puede heal si tiene storage inyectado; **actualmente nobody invokes repair** en runtime
   - **RECONCILIATION**: `Quality` `is_new` + deduplicación; `BackfillStrategy` handling de datos históricos; separación de duties

## 5. Consecuencias

### Consecuencias positivas

- Claridad sobre modo por defecto (`incremental`) vs otros modos (`backfill`, `repair` — inactivos)
- Claridad sobre ownership de Storage (`PipelineContext.storage = None`; caller debe inyectar)
- Claridad sobre orquestación: Quality deteca, RipPair repara — pero nadie invoca repair actualmente
- Claridad sobre boundaries LIVE/HISTORICAL/RECOVERY/RECONCILIATION
- Evidencia documentada para futuras decisiones; no suposiciones

### Consecuencias negativas

- No arregla el Docker stale (2026-08-20); el F-RT-01 persiste
- No conecta RepairStrategy al runtime; sigue desconectada
- No reactiva Bronze/Silver/Gold; siguen frozen
- No cambia el modo de ejecución en main.py; `mode="incremental"` sigue hardcodeado

### Consecuencias operacionales

- Los equipos saben que modo es por defecto y cuáles están inactivos
- Los equipos saben que RepairStrategy existe pero requiere storage inyección y caller explícito
- Los equipos saben que Quality detecta gaps pero no dispara recovery automáticamente
- Los gapes detectados requieren intervención manual o script hasta que se implemente la recovery architecture

## 6. Alternativas Rechazadas

- **Mantener silencio**: rechazado porque la auditoría maestre confirma DETECTION ≠ RECOVERY como finding confirmado; requiere documentación
- **Implementar fixes ahora**: rechazado porque esta tarea es exclusivamente read-only/audit; implementation es fase posterior; múltiples dependencies (Docker, systemd, Bronze writer) hacen prematuro
- **Cambiar modo por defecto a repair**: rechazado porque modo="repair" nunca se selecciona en main.py; requiere cambiar main.py y otros componentes; no es decisión aislada

## 6. Alcance

- **Dentro**: definición de arquitectura OHLCV con boundaries LIVE/HISTORICAL/RECOVERY/RECONCILIATION; modo por defecto; ownership de storage; orquestación de recovery; evidencia documentada
- **Fuera**: implementar Docker rebuild; conectar RepairStrategy al runtime; reactivar Bronze/Silver/Gold; cambiar main.py modo; modificar systemd; modificar Kafka offsets

## 7. Dependencias

- Requiere clasificación confirmada de DETECTION ≠ RECOVERY (MD-RECOVERY-002) ✅
- Requiere confirmación de RepairStrategy desconectada (MD-RECOVERY-001, MD-RECOVERY-002) ✅
- Requiere estado actual de modes en main.py (incremental hardcodeado) ✅
- Requiere estado actual Bronze/Silver/Gold (frozen) ✅

## 7. Frontera de implementación

Esta decisión define la arquitectura; la implementación (rebuild Docker, conectar RepairStrategy, reactivar Bronze) es una fase posterior. La frontera indica:

- **Definición**: arquitectura OHLCV documentada con boundaries y modes
- **Pendiente**: rebuild Docker image, conectar RepairStrategy, reactivar Bronze/Silver/Gold
- **Future**: definición de scripts de recovery, systemd units actualizados, health checks

## Referencias

- AUDIT_OCM_ohlcv-repair-backfill-execution-paths_2026-09-21.md (MD-RECOVERY-001)
- AUDIT_OCM_ohlcv-repair-backfill-causality_2026-09-21.md (MD-RECOVERY-002)
- AUDIT_OCM_market-data-e2e-incompleteness_2026-09-21.md (runtime reality)
- AUDIT_OCM_master-market-data-architecture-reconciliation_2026-09-21.md (architectural overview)
- ADR-0039 (runtime canonical: systemd vs Docker)

## Disposición Fase 0 (2026-09-24) — decisiones candidatas movidas a tracking

Tras la decisión B2, los cuatro puntos de decisión que este documento enumeraba quedan
**sin carácter normativo** y se re-registran como vías pendientes en
`docs/plans/tracking.yaml` (bloque `fase0_gobernanza.b2.arch_pending`), para su
resolución en Fase 3 / conforme avance el Plan Maestro:

1. **Ownership de Storage** (`PipelineContext.storage = None`; quién inyecta y bajo qué condiciones) → pendiente
2. **Orquestación de recovery** (quién invoca `mode="repair"` tras detección de Quality; DETECTION ≠ RECOVERY) → pendiente
3. **Disparo de RepairStrategy** (caller explícito en runtime; hoy ningún caller) → pendiente
4. **Máquina de estado de recovery** (LIVE/HISTORICAL/BACKFILL/REPAIR/RECONCILIATION → orquestación) → pendiente

Ninguna de estas vías bloquea la aceptación de ADR-0041 ni la Fase 0; todas
pertenecen a Fase 3 (recuperación/reconciliation) del Plan Maestro.
