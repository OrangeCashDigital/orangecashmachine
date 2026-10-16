# AUDITORÍA DE ESTRUCTURA DE DOCUMENTACIÓN — OrangeCashMachine

- **ID**: DOC-001
- **Fecha**: 2026-09-22
- **Modo**: READ-ONLY absoluto. Cero modificaciones.
- **Scope**: Auditoría de `docs/` completa. Estructura, ADRs, PLAN-Maestro, duplicidad, higiene.
- **Baseline**: commit `857cfe60`, branch `feat/m22b-governance`.

---

## REPRODUCIBILIDAD

```
- commit: 857cfe60d0c5e3b45b4fd235a978d69a8bf56090
- branch: feat/m22b-governance
- protocolo: AUDIT_PROTOCOL v2.1
- comandos: ls, find, cat, grep, stat, wc, read
- resultado: DRIFT DETECTED
```球

---

## 1. EXECUTIVE SUMMARY

**Veredicto**: `DRIFT DETECTED`

El directorio `docs/` tiene **problemas estructurales significativos**:

1. **Dos directorios de auditoría** (`docs/audit/` y `docs/audits/`) con contenido superpuesto.
2. **Documentos de referencia sueltos** en `docs/` raíz (PDFs, ZIPs, `.bak`).
3. **`docs/architecture/` mezcla** documentos numerados antiguos (`0000-0002.md`), ADRs, documentos `SUPERSEDED`, y un subdirectorio `logs/`.
4. **`docs/proposals/`** tiene un ADR (`ADR-0028-bookbuilder-design.md`) que debería estar en `docs/architecture/decisions/`.
5. **`docs/PLAN-Maestro-Ingenieria.md.bak`** — archivo backup en la raíz de docs.
6. **ADR numbering gaps**: ADR-0001, ADR-0002, ADR-0008, ADR-0018, ADR-0019 no existen.
7. **`docs/planning/`** tiene archivos de capacidad que no están en la estructura canonical.
8. **ADR-0041, ADR-0042, ADR-0043** todos en estado "Propuesto" — no aceptados.

---

## 2. ESTRUCTURA ACTUAL DE `docs/`

### 2.1 Árbol completo

```
docs/
├── architecture/
│   ├── 0000-principios-arquitectonicos.md          ← Antiguo, no ADR
│   ├── 0001-bounded-contexts-composition-root.md     ← Antiguo, no ADR
│   ├── 0002-event-driven-kappa-architecture.md       ← Antiguo, no ADR
│   ├── SUPERSEDED-0003-composition-root-jerarquico.md ← Debería eliminarse
│   ├── SUPERSEDED-0004-rebalance-service-capacidad-adelantada.md ← Debería eliminarse
│   ├── SUPERSEDED-0005-hydra-reemplaza-cli-legado.md ← Debería eliminarse
│   ├── decisions/                                  ← 40 ADRs + template
│   │   ├── ADR-0003 through ADR-0043               ← Gaps: 0001, 0002, 0008, 0018, 0019
│   │   └── ADR-template.md
│   ├── recovered/
│   │   └── trading-bootstrap-forensic-analysis.md  ← Solo 1 archivo
│   ├── logs/                                       ← Inusual en architecture/
│   ├── GOVERNANCE.md
│   ├── README.md
│   ├── BLINDAJE-APPS-2026-08-06.md
│   └── feed-model.md
├── audit/                                          ← PROBLEMA: Duplicado
│   ├── deployment-runtime-architecture-adversarial-audit.md (146KB)
│   ├── deployment-runtime-architecture-audit.md (56KB)
│   └── deployment-runtime-implementation-plan.md (57KB)
├── audits/                                         ← 114 archivos AUDIT_OCM_*
│   └── ... (114 archivos)
├── knowledge/
│   ├── manifest.yaml
│   ├── README.md
│   ├── mappings/
│   └── notes/
├── plans/
│   ├── tracking.yaml                               ← SSOT
│   ├── backlog-priorizado-2026-08-08.md
│   └── engineering-guardrails.md
├── planning/                                       ← Fuera de estructura canonical
│   ├── f2_6a-capacity-teorico.md
│   └── fase3.5c-capacity-empirico.md
├── proposals/                                      ← Debería estar en decisions/
│   └── ADR-0028-bookbuilder-design.md
├── discovery_profiles/
│   └── bybit.md
├── PLAN-Maestro-Ingenieria.md
├── PLAN-Maestro-Ingenieria.md.bak                  ← PROBLEMA: Backup en docs/
├── DOMAIN.md
├── Clean Architecture A Craftsman Guide...pdf       ← Documento de referencia
├── freqtrade-develop.zip                            ← Documento de referencia
├── hummingbot-master.zip                            ← Documento de referencia
└── nautilus_trader-develop.zip                      ← Documento de referencia
```

### 2.2 Conteo

| Directorio | Archivos | Tamaño total |
|---|---|---|
| `docs/architecture/decisions/` | 40 ADRs + template | — |
| `docs/audits/` | 114 archivos | ~2.8MB |
| `docs/audit/` | 3 archivos | ~260KB |
| `docs/architecture/` (raíz) | 9 archivos incl. SUPERSEDED | — |
| `docs/` (raíz) | 4 PDFs + 3 ZIPs + .bak | ~50MB |
| `docs/plans/` | 3 archivos | — |
| `docs/planning/` | 2 archivos | — |
| `docs/proposals/` | 1 archivo | — |
| `docs/knowledge/` | manifest + mappings + notes | — |

---

## 3. PROBLEMAS DETECTADOS

### 3.1 Duplicidad

| Problema | Detalle | Severidad |
|---|---|---|
| **`docs/audit/` vs `docs/audits/`** | Dos directorios con contenido de auditoría. `docs/audit/` tiene 3 archivos grandes (260KB) que son versiones anteriores de lo que hay en `docs/audits/`. | `P0` |
| **`docs/proposals/ADR-0028-bookbuilder-design.md`** | Un ADR en `proposals/` en vez de `architecture/decisions/`. | `P1` |
| **`docs/PLAN-Maestro-Ingenieria.md.bak`** | Backup en la raíz de docs. | `P1` |
| **`docs/architecture/SUPERSEDED-0003-0005.md`** | Documentos obsoletos en `architecture/`. | `P1` |

### 3.2 ADRs

| Problema | Detalle | Severidad |
|---|---|---|
| **ADR-0001, ADR-0002 no existen** | La numeración empieza en ADR-0003. | `P2` |
| **ADR-0008 no existe** | Gap entre ADR-0007 y ADR-0009. | `P2` |
| **ADR-0018, ADR-0019 no existen** | Gaps en la numeración. | `P2` |
| **ADR-0041, ADR-0042, ADR-0043 todos "Propuesto"** | No aceptados. ADR-0033 también "Propuesto" pero `check_production_gates.py` SÍ existe. | `P2` |
| **ADR-0033 "Propuesto"** | `scripts/check_production_gates.py` existe pero ADR no aceptado. | `P1` |
| **ADR-0028 en `proposals/`** | Debería estar en `decisions/`. | `P1` |

### 3.3 Estructura de ARCHITECTURE

| Problema | Detalle | Severidad |
|---|---|---|
| **`docs/architecture/0000-0002.md`** | Documentos numerados antiguos que no son ADRs. Deberían reubicarse o eliminarse. | `P1` |
| **`docs/architecture/SUPERSEDED-0003-0005.md`** | Documentos marcados como SUPERSEDED en el directorio principal. | `P1` |
| **`docs/architecture/logs/`** | Subdirectorio de logs dentro de `architecture/`. No tiene sentido. | `P2` |
| **`docs/architecture/recovered/`** | Solo 1 archivo. Podría consolidarse en `audits/`. | `P2` |
| **`docs/architecture/README.md`** | Dice "Estado operativo: actualmente DETENIDO" para market-data, pero el sistema SÍ está corriendo. | `P1` |

### 3.4 PLAN-Maestro

| Problema | Detalle | Severidad |
|---|---|---|
| **`PLAN-Maestro-Ingenieria.md.bak`** | Backup en docs raíz. | `P1` |
| **`docs/planning/`** | Archivos de capacidad (`f2_6a-capacity-teorico.md`, `fase3.5c-capacity-empirico.md`) que no están en la estructura canonical del PLAN-Maestro. | `P2` |
| **`docs/plans/backlog-priorizado`** | Backlog que debería estar en `tracking.yaml`. | `P2` |
| **`docs/plans/engineering-guardrails.md`** | Guías que podrían estar en `GOVERNANCE.md`. | `P2` |

### 3.5 Archivos sueltos en `docs/` raíz

| Archivo | Tipo | Acción |
|---|---|---|
| `Clean Architecture A Craftsman Guide...pdf` | PDF de referencia | Mover a `docs/knowledge/` |
| `freqtrade-develop.zip` | ZIP de referencia | Mover a `docs/knowledge/` |
| `hummingbot-master.zip` | ZIP de referencia | Mover a `docs/knowledge/` |
| `nautilus_trader-develop.zip` | ZIP de referencia | Mover a `docs/knowledge/` |
| `PLAN-Maestro-Ingenieria.md.bak` | Backup | Eliminar o mover a `.git` ignore |
| `DOMAIN.md` | Documento | ¿Depende de docs/architecture/GOVERNANCE.md? |

### 3.6 `docs/README.md` o equivalente

No existe `docs/README.md` que sirva como punto de entrada al directorio de documentación.

---

## 4. MATRIZ DE ESTADO DE ADRs

| ADR | Estado | ¿Aceptado? | ¿Implementado? | ¿En location correcta? |
|---|---|---|---|---|
| ADR-0003 | ¿Aceptado? | ❓ | ❓ | ✅ `decisions/` |
| ADR-0004 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0005 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0006 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0007 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0008 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0009 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0010 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0011 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0012 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0013 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0014 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0015 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0016 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0017 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0020 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0021 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0022 | Aceptado | ✅ | ✅ | ✅ |
| ADR-0023 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0024 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0025 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0026 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0027 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0028 | Propuesto | ❌ | ❌ | ❌ `proposals/` |
| ADR-0029 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0030 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0031 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0032 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0033 | Propuesto | ❌ | ✅ (`check_production_gates.py` existe) | ✅ |
| ADR-0034 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0035 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0036 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0037 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0038 | ¿Aceptado? | ❓ | ❓ | ✅ |
| ADR-0039 | Aceptado | ✅ | ⚠️ Parcial | ✅ |
| ADR-0040 | Aceptado | ✅ | ⚠️ Parcial | ✅ |
| ADR-0041 | Propuesto | ❌ | ❌ | ✅ |
| ADR-0042 | Propuesto | ❌ | ❌ | ✅ |
| ADR-0043 | Propuesto | ❌ | ❌ | ✅ |

---

## 5. ESTRUCTURA CANÓNICA PROPUESTA

Según `docs/architecture/GOVERNANCE.md` y `PLAN-Maestro-Ingenieria.md`, la estructura debería ser:

```
docs/
├── architecture/
│   ├── decisions/                    ← TODOS los ADRs aquí
│   │   ├── ADR-0001.md              ← FALTANTE (crear o eliminar gap)
│   │   ├── ADR-0002.md              ← FALTANTE (crear o eliminar gap)
│   │   ├── ADR-0003.md through ADR-0043.md
│   │   └── ADR-template.md
│   ├── GOVERNANCE.md
│   ├── README.md                    ← Mapa canónico actualizado
│   └── recovered/                   ← Solo documentos recuperados forenses
├── plans/
│   ├── tracking.yaml                ← SSOT
│   └── engineering-guardrails.md    ← Guías de ingeniería
├── knowledge/
│   ├── manifest.yaml
│   ├── README.md
│   ├── mappings/
│   ├── notes/
│   └── references/                  ← PDFs, ZIPs, libros
│       ├── Clean Architecture...pdf
│       ├── freqtrade-develop.zip
│       └── ...
├── audits/                          ← TODOS los AUDIT_OCM_* aquí (114 archivos)
├── PLANNING.md                      ← O eliminar docs/planning/
└── PLAN-Maestro-Ingenieria.md       ← Único plan maestro
```

### 5.1 Cambios necesarios

| Acción | Origen | Destino | Prioridad |
|---|---|---|---|
| MOVER | `docs/proposals/ADR-0028-bookbuilder-design.md` | `docs/architecture/decisions/ADR-0028-bookbuilder-design.md` | `P1` |
| MOVER | `docs/architecture/0000-*.md` | `docs/architecture/recovered/` o `docs/audits/` | `P1` |
| ELIMINAR | `docs/architecture/SUPERSEDED-0003-0005.md` | — | `P1` |
| MOVER | `docs/PLAN-Maestro-Ingenieria.md.bak` | `docs/` ignore o eliminar | `P1` |
| MOVER | `docs/*.pdf` | `docs/knowledge/references/` | `P2` |
| MOVER | `docs/*.zip` | `docs/knowledge/references/` | `P2` |
| ELIMINAR | `docs/architecture/logs/` | — | `P2` |
| CONSOLIDAR | `docs/audit/*.md` | `docs/audits/AUDIT_OCM_deployment-runtime-*.md` | `P0` |
| ELIMINAR | `docs/planning/` | Consolidar en `docs/plans/` o `PLAN-Maestro` | `P2` |
| CREAR | `docs/README.md` | Punto de entrada a documentación | `P1` |
| ACTUALIZAR | `docs/architecture/README.md` | Corregir "DETENIDO" por "ACTIVO" | `P1` |
| ACEPTAR | ADR-0033 | Cambiar estado a "Aceptado" | `P1` |

---

## 6. CONTRADICCIONES DOCUMENTADAS

| # | Contradicción | Fuente A | Fuente B | Estado |
|---|---|---|---|---|
| C-1 | `docs/architecture/README.md` dice market-data "DETENIDO" | `architecture/README.md` | `ps aux` muestra process corriendo | **CONTRADICCIÓN** |
| C-2 | `docs/audit/` duplica `docs/audits/` | `docs/audit/` | `docs/audits/` | **DRIFT** |
| C-3 | `docs/proposals/ADR-0028` en location incorrecta | `proposals/` | `decisions/` | **DRIFT** |
| C-4 | ADR-0033 "Propuesto" pero `check_production_gates.py` existe | ADR-0033 | `scripts/` | **CONTRADICCIÓN** |
| C-5 | `PLAN-Maestro-Ingenieria.md.bak` en docs raíz | `PLAN-Maestro` | `.gitignore` | **DRIFT** |
| C-6 | `docs/planning/` no está en estructura canonical | `planning/` | `PLAN-Maestro` §4 | **DRIFT** |
| C-7 | ADR numbering gaps (0001, 0002, 0008, 0018, 0019) | `decisions/` | `GOVERNANCE.md` §3 | **DRIFT** |
| C-8 | `docs/architecture/logs/` en directorio de arquitectura | `architecture/logs/` | Estructura canónica | **DRIFT** |

---

## 7. PLAN DE REMEDIACIÓN

### Paso 1: Consolidar auditorías (P0)

```bash
# Mover docs/audit/ a docs/audits/
mv /home/orangemusic/trading/orangecashmachine/docs/audit/*.md \
   /home/orangemusic/trading/orangecashmachine/docs/audits/
rmdir /home/orangemusic/trading/orangecashmachine/docs/audit
```

### Paso 2: Mover ADR de proposals (P1)

```bash
mv /home/orangemusic/trading/orangecashmachine/docs/proposals/ADR-0028-bookbuilder-design.md \
   /home/orangemusic/trading/orangecashmachine/docs/architecture/decisions/
```

### Paso 3: Mover archivos de referencia (P2)

```bash
mkdir -p /home/orangemusic/trading/orangecashmachine/docs/knowledge/references/
mv /home/orangemusic/trading/orangecashmachine/docs/*.pdf \
   /home/orangemusic/trading/orangecashmachine/docs/knowledge/references/
mv /home/orangemusic/trading/orangecashmachine/docs/*.zip \
   /home/orgemusic/trading/orangecashmachine/docs/knowledge/references/
```

### Paso 4: Eliminar archivos obsoletos (P1)

```bash
rm -f /home/orangemusic/trading/orangecashmachine/docs/PLAN-Maestro-Ingenieria.md.bak
rm -f /home/orangemusic/trading/orangecashmachine/docs/architecture/SUPERSEDED-*.md
```

### Paso 5: Crear `docs/README.md` (P1)

```markdown
# Documentación de OrangeCashMachine (OCM)

## Estructura
- `architecture/decisions/` — ADRs (númeración secuencial)
- `plans/tracking.yaml` — SSOT de hallazgos y deuda
- `audits/` — Informes de auditoría (AUDIT_OCM_*.md)
- `knowledge/` — Conocimiento del proyecto (manifest, mappings, notas)
- `architecture/GOVERNANCE.md` — Gobernanza arquitectónica
- `PLAN-Maestro-Ingenieria.md` — Plan maestro de ingeniería

## Referencias
- `knowledge/references/` — Libros, papers, repositorios externos
```

### Paso 6: Actualizar `architecture/README.md` (P1)

Cambiar "Estado operativo: actualmente DETENIDO" por "Estado operativo: ACTIVO" para market-data.

### Paso 7: Eliminar `docs/planning/` (P2)

```bash
# Consolidar contenido en docs/plans/ o en PLAN-Maestro
rm -rf /home/orangemusic/trading/orangecashmachine/docs/planning/
```

### Paso 8: Eliminar `docs/architecture/logs/` (P2)

```bash
rm -rf /home/orangemusic/trading/orangecashmachine/docs/architecture/logs/
```

### Paso 9: Aceptar ADR-0033 (P1)

Actualizar `docs/architecture/decisions/ADR-0033-production-gate-binary.md`:
- Cambiar `Estado: Propuesto` a `Estado: Aceptado`
- Actualizar sección de implementación con evidencia de `scripts/check_production_gates.py`

### Paso 10: Actualizar `.gitignore` (P2)

Agregar:
```
docs/PLAN-Maestro-Ingenieria.md.bak
docs/architecture/SUPERSEDED-*.md
```

---

## 8. REGISTRO DE AUDITORÍA

| Item | Valor |
|---|---|
| Protocolo | AUDIT_PROTOCOL v2.1 |
| Método | READ-ONLY |
| Comandos ejecutados | `ls`, `find`, `cat`, `grep`, `stat`, `wc`, `read` |
| Archivos modificados | **NINGUNO** |
| Servicios reiniciados | **NINGUNO** |

**Detenerse. Esperar autorización humana.**
