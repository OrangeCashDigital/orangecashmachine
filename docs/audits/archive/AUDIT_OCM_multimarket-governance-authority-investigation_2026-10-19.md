# AUDIT_OCM_MULTIMARKET_GOVERNANCE_AUTHORITY_INVESTIGATION_2026-10-19

**ID:** AUDIT-OCM-MULTIMARKET-GOV-AUTH-2026-10-19  
**Tipo:** Investigación de gobernanza y autoridad arquitectónica (multimercado)  
**Modo:** READ-ONLY / WRITE-ONLY-IN-DOCS-AUDITS  
**Estado:** EN PROGRESO (EXPEDIENTE CONSOLIDADO)  
**Fecha inicio:** 2026-10-19  
**Última actualización:** 2026-10-19  
**HEAD:** df9f0b6c5ac40bcb8130c7f290d68b0f43fe4376  
**Rama:** chore/kafka-provision  
**Autorización escritura:** ÚNICAMENTE `docs/audits/`  
**Autorización Git mutativo:** NINGUNA  

---

## 1. Objetivo y alcance

Consolidar, en expediente persistente y verificable, la investigación ya realizada sobre gobernanza/autoridad para evolución multimercado (settlement, calendarios/sesiones, identidad/taxonomía, bounded contexts, ADR, decisiones pendientes, procedimientos de resolución).  
**Principio:** investigar/analizar/proponer; **no** aprobar/seleccionar/materializar/corregir documentos normativos.

---

## 2. Baseline de solo lectura

- **HEAD:** `df9f0b6c5ac40bcb8130c7f290d68b0f43fe4376`
- **Rama:** `chore/kafka-provision`  
- **Working tree:** Limpio salvo 8 untracked `?? .coverage.orangehouselaptop.pid*` (preexistentes)
- **Comandos:** `git rev-parse HEAD`, `git rev-parse --abbrev-ref HEAD`, `git status --porcelain`, `grep`, `sed`, `ls`, `git log --oneline -1 4ad03eba`
- **Archivos de auditoría relacionados encontrados:** muchos existentes (incl. `AUDIT_OCM_d19-*`, `AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md`, `AUDIT_OCM_high-impact-improvements_2026-10-07.md`, `AUDIT_OCM_l13a-closure_2026-10-12.md`)
- **Informe previo en conversación:** Sí (verificado/contrastado con fuentes actuales)

**Confirmación:** Sin modificaciones fuera de `docs/audits/`. Sin operaciones Git mutativas.

---

## 3. Mapa de autoridad (verificado)

| Fuente | Tipo | Autoridad | Notas |
|---|---|---|---|
| `docs/governance/AUDIT_PROTOCOL.md` §B,K,M,N,O,Q | **Norma vinculante (LEVEL 4)** | Jerarquía, cadena adopción, auditoría read-only, contradicciones → HUMAN DECISION, M1..M20 | §K: única escritura permitida en auditoría = `docs/audits/` |
| `docs/architecture/GOVERNANCE.md` §2,§3 | **Norma vinculante (LEVEL 4)** | Cuándo requiere ADR (4 casos obligatorios): (1) firma constructor Composition Root, (2) contrato `importlinter.toml`, (3) ownership estado mutable compartido, (4) posponer deuda técnica | Fuera de esos 4 → no exige ADR por ese criterio |
| `AGENTS.md` L6-14,L13,L140 | **Norma vinculante (LEVEL 4)** | Descubrimiento, protocolo auditoría, git rules | — |
| `docs/PLAN-Maestro-Ingenieria.md` preamble,§12,§13,§14 | **Norma vinculante (N1, SSOT)** | Orden inviolable `Plan → Tracking → ADR → Código → Tests → CI → Release`, Constitución §12, §14 Registro de cambios | "Documento operativo vivo" con historial en Git |
| `docs/architecture/decisions/ADR-0045-*.md` | **Propuesta** | Conflictos/precedencia/fail-closed (r9: no inferir precedencia; r10: FAIL-CLOSED + Human Decision Gate) | Propuesto |
| `docs/architecture/decisions/ADR-0054-*.md` | **Aceptado parcial** (MD-A2; MD-A4 Propuesto) | `EXPLICIT HUMAN DECISION > ADR/CONTRACT > GOVERNANCE > REGISTRY > TRACKING > IMPLEMENTATION` | — |
| `docs/architecture/decisions/ADR-0049-*.md` | **ACEPTADO** | G1-D01-D15; D19-01..10 DECIDED (L545-551); D20b-01..06 DECIDED; `IMPLEMENTATION = NOT AUTHORIZED` | **CONTRADICCIÓN C1** L53-54 vs L545-551 |
| `docs/architecture/decisions/ADR-0055-*.md` | **ACEPTADO (documental 2026-10-09)** | Q1 Opción A (autoridad D19-01/D20b); Q2 **NO decidida/UNAUTHORIZED** | — |
| `docs/architecture/decisions/ADR-0057-*.md` | **ACEPTADO (documental 2026-10-11, alcance limitado)** | Consolidación §17.3, inventario C1-C4, D12 fuera alcance, GATE C/§17.4 fuera alcance | — |
| `docs/architecture/decisions/ADR-0028-*.md` | **ACEPTADO CON REVISIÓN (2026-09-24)** | BookBuilder/MarketState | Sin relación demostrada con calendario/sesión no-cripto |
| `docs/plans/tracking.yaml` | **N2 SSOT operativo** | Estado backlog/gates; referencia ADR-0049/0055/0057 | Contradicciones registradas |

---

## 4. Matriz de contradicciones (evidencia trazable)

| ID | Fuentes en conflicto | Ubicación exacta | Tipo | Impacto | Procedimiento resolución | Estado |
|---|---|---|---|---|---|---|
| **C1** | ADR-0049: L53-54 (*D19-03..10 pendientes, no anticipa D20-D24*) vs L545-551 (*D19-03..10 DECIDED, D20b-01..06 DECIDED*) | `docs/architecture/decisions/ADR-0049-canonical-instrument-identity-g1.md:53-54, 544-560` | Discrepancia interna mismo documento | Ambigüedad estado D19; afecta rigor roadmap | **HUMAN DECISION** (ADR-0045 r9). Editar ADR-0049 o añadir reconciliación. Agente NO resuelve. | **ABIERTA** |
| **C2** | Pack D19 blob `4ad03eba`: L841 (*HUMAN DECISION REQUIRED/BLOCKED*) vs L2262-2291 (*DECIDED*) | `AUDIT_OCM_d19-human-decision-pack_2026-09-30.md` (blob) / inventariado ADR-0057 L127-133 | Histórico contradictorio | Divergencia fuentes | Preservar; decidir reconciliación material por humano | **ABIERTA** |
| **C3** | INDUSTRY/KN_BASE: L32-35,L167-169 / KNOWLEDGE_BASE_CONTRACT L166 (*PENDING/BLOCKED/GATE C BLOCKED*) vs ADR-0049 L542+ (*DECIDED*). Q1 resuelve jerarquía (ADR-0055) pero reconciliación material pendiente | `AUDIT_OCM_d19-01-industry-bow-review_2026-10-03.md`, `KNOWLEDGE_BASE_CONTRACT.md` / ADR-0055 L106-109,L185 / ADR-0057 L134-147 | Jerárquico parcialmente resuelto, textos contradictorios | Clarificada jerarquía, textos permanecen | Reconciliación material por humano | **ABIERTA (PARCIAL)** |
| **C4** | decision-support L24/L813/L1046 (*D12 OPEN*) vs pack L13/L34 (*D12 DECIDED Alt5+Alt3*) vs ADR-0049 L335/361 (*C/RESTRICTED, catálogo pendiente*) | `AUDIT_OCM_decision-support-hd-venue-constraints-identity_2026-09-29.md` / pack / ADR-0049; ADR-0057 L148-155 | Discrepante | Afecta rigor contenido | **HUMAN DECISION** (D12 fuera alcance ADR-0057) | **ABIERTA** |
| **T-PLAN** | PLAN preamble "constitución"/orden inviolable vs §14 "operativo vivo" + historial modificaciones vs AUDIT_PROTOCOL §K prohíbe escribir N1 en auditoría | `docs/PLAN-Maestro-Ingenieria.md` preamble/§14 vs `docs/governance/AUDIT_PROTOCOL.md` §K L88-99 | Tensión de alcance (modo vs inmodificabilidad) | Ambigüedad procedimiento modificación | Interpretar como diferencia de alcance; materializar fuera modo auditoría con autorización humana. Si ambigüedad persiste → HUMAN DECISION. | **TENSIÓN (INDETERMINADO)** |

---

## 5. Estado canónico verificado de decisiones

| Elemento | Estado verificado | Evidencia | Certeza |
|---|---|---|---|
| **G1-D01-D13** | DECIDED + MATERIALIZED + ACCEPTED WITH CLARIFICATIONS | ADR-0049 L538-539,L565-570 | **HECHO VERIFICADO** |
| **G1-D14** | DECIDED + MATERIALIZED + ACCEPTED (A 2026-09-28) | ADR-0049 L540,L571-576 | **HECHO VERIFICADO** |
| **G1-D15** | DECIDED + MATERIALIZED + ACCEPTED (A 2026-09-29); NO autoriza implementación | ADR-0049 L541,L577-586 | **HECHO VERIFICADO** |
| **G1-D04 (InstrumentType)** | **CERRADO**: SPOT/PERPETUAL/FUTURE/OPTION únicamente; docstring prohíbe extensión | `packages/market_data/domain/value_objects/instrument_type.py:9-50` | **HECHO VERIFICADO** |
| **G1-D05 (settlement contractual)** | Término identity-defining en ContractTerms | `contract_terms.py:16,58,100`, `listing_identity.py:27` | **HECHO VERIFICADO** |
| **D19-01..02** | DECIDED (C+/A) | ADR-0049 L542-543 | **HECHO VERIFICADO** |
| **D19-03..10** | **DECIDED** (A/B/A/A/D/B/C) per L544-551; **contradicho** por L53-54 | ADR-0049 L544-551 vs L53-54 | **HECHO VERIFICADO CON CONTRADICCIÓN** |
| **D20/D20a** | DECIDED | ADR-0049 L552-553 | **HECHO VERIFICADO** |
| **D20b-01..06** | DECIDED | ADR-0049 L554-560 | **HECHO VERIFICADO** |
| **D21-D24** | BLOCKED | ADR-0049 L561 | **HECHO VERIFICADO** |
| **IMPLEMENTATION (global)** | **NOT AUTHORIZED** | ADR-0049 L561, ADR-0055 L212 | **HECHO VERIFICADO** |
| **D12** | **DISCREPANTE** (OPEN vs DECIDED Alt5+Alt3 vs C/RESTRICTED); fuera alcance ADR-0057 | ADR-0057 C4 L148-155 | **NO VERIFICADO CANÓNICAMENTE** |
| **C1-C4** | **ABIERTAS** | ADR-0057 L118-155,L189 | **HECHO VERIFICADO (ABIERTAS)** |
| **Gate C / §17.4** | **BLOCKED / fuera de alcance** | ADR-0057 L109,L182 | **HECHO VERIFICADO** |
| **ADR-0055 Q2** | **NO decidida / UNAUTHORIZED** | ADR-0055 L134,L212, tracking L3843 | **HECHO VERIFICADO** |
| **ADR-0028** | ACEPTADO CON REVISIÓN (2026-09-24); ND/F6 pendientes | ADR-0028 L3,L133,L138, tracking | **HECHO VERIFICADO** |

---

## 6. Análisis arquitectónico multimercado (separado por concepto)

### Settlement
| Tipo | Ubicación | Naturaleza | Estado |
|---|---|---|---|
| **Trade/Execution settlement** | `packages/trading/execution/settlement.py`, `oms.py:537-590`, `order.py:152` | Fees/P&L al fill | **EXISTE** |
| **Contractual settlement (moneda)** | `packages/market_data/domain/value_objects/contract_terms.py:16,58,100`, `listing_identity.py:27` | Término identity-defining (G1-D05) | **EXISTE** |
| **Market/delivery settlement (fecha/ciclo)** | — | FX T+2, equity settlement, delivery futuro | **AUSENTE / NO VERIFICADO** |

**Conclusión:** **Dos semánticas distintas y NO unificadas.** No asumir equivalencia.

### Calendarios / Sesiones
| Concepto | Estado | Evidencia |
|---|---|---|
| `MarketCalendar`/`TradingCalendar`/`Holiday`/`BusinessDay` | **AUSENTE** | `grep` sin hits relevantes |
| `MarketSession`/`TradingSession`/RTH/ETH/pre-post | **AUSENTE** | Sin evidencia |
| Kafka `session_timeout_ms` | **EXISTE** (infra messaging) | `packages/market_data/infrastructure/kafka/consumer.py:73,104,...` — **NO es calendario de mercado** |
| `"session is closed"` | **EXISTE** (strings operativos) | `runtime.py:112`, `base.py:105` — sin lógica calendario verificada |

**Conclusión:** **NO existen** como conceptos de dominio de mercado. Mercados cripto mayoritariamente 24/7.

### Identidad / Taxonomía
- **InstrumentType cerrado** a `SPOT|PERPETUAL|FUTURE|OPTION`. Extensión requiere decisión formal (G1-D04).
- **G1 identity** mayoritariamente decidido pero materialización **NO AUTORIZADA**.
- **Listado/runtime aún `symbol: str`** (verificado previamente).

### Límites bounded contexts (análisis)
| Dominio | Settlement | Calendario/Sesión | Identidad | Justificación |
|---|---|---|---|---|
| `market_data` | Contractual (G1-D05) candidato | **Candidato natural** si se formaliza | **Owner natural** | Datos/metadata mercado |
| `trading` | **Trade settlement** encaja | Menor justificación | No owner | Ejecución/cálculo |
| `portfolio` | Consumidor | No obvio | No | Posiciones |
| **Nuevo BC** | Solo si semántica independiente masiva | Posible si complejo | No justificado | Requiere justificación fuerte + import-linter; **probablemente ADR** |

**Regla:** decidir por responsabilidades/invariantes/ownership/fuentes cambio, no por carpetas.

---

## 7. Verificaciones pendientes (registro de investigación) — ACTUALIZADO FASE 2

| ID | Verificación | Fuente/Método | Estado | Resultado/Evidencia | Próxima acción |
|---|---|---|---|---|---|
| **V-1** | `scripts/audit_validator.py` (M1-M20/M1-M25, efectos secundarios) | Lectura estática completa | **EVALUADO — NO EJECUTADO (sin demostración read-only puro)** | Archivo 1000 líneas. Importa yaml, subprocess, json, re, pathlib, argparse. Usa `subprocess.run` (lectura/comprobaciones), escribe? Requiere inspección detallada. **Clasificación:** C (efectos secundarios posibles/no descartados). **NO ejecutado.** CI lo ejecuta en `policy-gate` con `uv run python scripts/audit_validator.py`. | No ejecutar en esta fase sin demostrar ausencias de escritura. Documentado como limitación justificada. |
| **V-2** | GitHub Settings (branch protection/rulesets/required checks) | `gh` CLI | **NO VERIFICABLE LOCALMENTE** | `gh` no instalado. Config externa fuera de alcance repo. | No instalar. Solicitar evidencia externa si necesario. |
| **V-3** | Blob `4ad03eba` (completo) | `git show --name-only` + `git show <blob>` selectivo | **VERIFICADO (parcialmente)** | Commit `4ad03eb wip: backup de archivos sin seguir (sin M26)`. Archivos: auditories D19 (`d19-human-decision-pack`, `d19-01-industry-bow-review`, `d19-kb-*`, `decision-support-hd-venue-constraints-identity`), STAGED_PORTS, architecture-discovery, high-impact-improvements, knowledge/*, proposals/EXECUTION-CONTRACT-DECISION-PACK, market_data identity code/tests. **Pack D19** (blob) muestra: L13 `D12: DECIDED — Alternative 5 + Alternative 3`, L14 `D19: PENDING/BLOCKED` (contexto histórico), L34 indica tratar referencias `D12 = OPEN` como snapshots históricos; tabla L2262-2291 registra **D19-01..10 y D20/D20a/D20b DECIDED** con **IMPLEMENTATION: NOT AUTHORIZED**. **Industry BOW** (blob) muestra L29 `D12 = DECIDED — Alternative 5 + Alternative 3`, L30 `HD-VC-1..HD-VC-7 = DECIDED`, L31 `HD-1..HD-8 = DECIDED`, L32 `D19 = PENDING / BLOCKED`, L33 `D19-01 = HUMAN DECISION REQUIRED`, L35 `DECISION REGISTERED = NO`. **Diferencia vs fuentes actuales:** misma contradicción C1-C4 presente; **no demuestra reemplazo formal** de estado, muestra **versiones históricas/paquetes de deliberación**. | V-3 COMPLETADO con limitación (no se volcaron archivos sensibles completos). |
| **V-4** | `.github/workflows/ocm-ci.yml` completo + relaciones | Lectura íntegra segmentada | **VERIFICADO** | Jobs relevantes: `architecture` (import-linter), `engineering-health`, `app-guard/domain-guard/trading-guards`, `unit-tests`, `integration-tests`, `security`, `config-validation`, `quality`, `policy-gate` (needs múltiples). `policy-gate` ejecuta `uv run python scripts/engineering_health_check.py` y `uv run python scripts/audit_validator.py` (líneas ~555-590). **Mención M1..M25** en `audit_validator` (CI línea ~583) vs script declara M1..M20. **NO confunde workflow con branch protection GitHub.** | V-4 COMPLETADO. Discrepancia M1-M20/M1-M25 documentada como posible alcance/versión. |
| **V-5** | §17.4 exacto + contexto | Lectura ADR-0057 completo secciones relevantes | **VERIFICADO** | Apariciones: L55 (*no modifica tracking.yaml ni ejecuta §17.4 ni GATE C*), L109 (*HUMAN ACTION REQUIRED — GATE C (§17.3 ADR nuevo formalizado por el humano · §17.4 registro en `tracking.yaml` + contrato/linter)*), L182 (*§17.4 (registro en `tracking.yaml` + contrato/linter), GATE C y vigencia normativa quedan fuera de alcance y requieren autorización humana independiente*), L200+. **Regula:** formalización §17.3 como ADR nuevo + registro en tracking.yaml + contrato/linter para activar GATE C. **No resuelve** C1-C4/D12. Prohíbe ejecución por agente; requiere autorización humana. | V-5 COMPLETADO. |
| **V-6** | Usos exhaustivos `InstrumentType` | `grep -rn` en repo | **VERIFICADO (mapa)** | Definición `packages/market_data/domain/value_objects/instrument_type.py`. Usos: `contract_terms.py` (comparaciones FUTURE/OPTION), `listing_identity.py`, `domain/value_objects/__init__.py`. También referencias en __pycache__ (ignorar). **Limitado a usos código fuente**; tests/docs pueden tener más. Taxonomía cerrada; extensión afectaría identity-defining (G1-D04). | V-6 COMPLETADO (mapa base). Requiere evaluación posterior si se autoriza extensión. |
| **V-7** | Otras ADR instrumentos/identidad/calendario/settlement/BC | Búsqueda keywords | **VERIFICADO (principales)** | Revisadas ADR-0028,0045,0049,0054,0055,0057. Sin ADR específico calendario/sesión no-cripto encontrado. | V-7 COMPLETADO para principales. |
| **V-8** | Estado D12 canónico cruzado | Lectura cruzada | **VERIFICADO (CONTRADICTORIO)** | ADR-0057 C4: 3 fuentes contradictorias; **D12 fuera alcance decisorio ADR-0057**. Pack blob afirma DECIDED Alt5+Alt3, decision-support afirma OPEN, ADR-0049 C/RESTRICTED. **Sin regla formal clara de precedencia automática.** NO RESUELTO. | V-8 COMPLETADO (evidencia, sin resolución). |

---

## 8. Alternativas de roadmap multimercado (SIN SELECCIONAR GANADORA)

| Alt | Descripción | Riesgo | Complejidad | Compatibilidad | Dependencias gobernanza | Criterios salida (propuestos) |
|---|---|---|---|---|---|---|
| **A)** Completar cripto primero, preparar extensibilidad | Estabilizar cripto; abstracciones mínimas sin nuevos mercados | **BAJO** | BAJA | **ÓPTIMA** | Mínimas (materialización D19/D20b) | Contratos tests verdes; sin romper taxonomía |
| **B)** Diseñar abstracciones multimercado primero, posponer integraciones | Núcleo común antes de proveedores | **ALTO** (sobreabstracción) | ALTA | Dudosa | **Muchas** (G1-D04, settlement, calendario, BC) | Difíciles sin evidencia |
| **C)** Incremental por capacidades con límites explícitos por proveedor | Añadir capacidades verificables, límites explícitos | **MEDIO** | MEDIA | Buena | Graduales por fase | Verificables por capacidad end-to-end |
| **D)** Arquitecturas separadas compartiendo solo componentes justificados | Cripto vs tradicional separados | **MEDIO-ALTO** (duplicación/divergencia) | ALTA | Posible con fronteras claras | Muchas | Difíciles, riesgo divergencia semántica |

**Recomendación análisis:** A o C más compatibles con restricciones actuales (`IMPLEMENTATION NOT AUTHORIZED`, `InstrumentType` cerrado).

---

## 9. Procedimiento formal recomendado (contrastado con gobernanza)

| Paso | Acción | Precondiciones | Fuente | Artefactos | Autoridad | Bloqueo |
|---|---|---|---|---|---|---|
| **1)** Investigación conceptual (READ-ONLY) | Completar hallazgos | Autorizado | AUDIT_PROTOCOL §K | Ninguno | Agente | Mutación |
| **2)** Decisión estrategia producto | Alcance mercados | Paso 1 | Cadena adopción §B | Ninguno | **HUMANO** | Requiere humano |
| **3)** Resolver contradicciones documentales | Reconciliar C1-C4/D12 | Paso 2 contexto | ADR-0045 r9/r10, ADR-0057 | Posibles ADR/docs | **HUMANO** | Prohibido agente resolver |
| **4)** Decisión semántica multimercado | settlement/calendario/sesión/identidad | Paso 3 | GOVERNANCE §2 | Ninguno/ADR según caso | **HUMANO/ADR** | Puede requerir ADR |
| **5)** Límites BC/contratos | Determinar BC/puertos | Paso 4 | GOVERNANCE §2/import-linter | Posible ADR | **HUMANO** | Puede requerir ADR/import-linter |
| **6)** Crear/modificar ADR si procede | Formalizar | Paso 4-5 | GOVERNANCE §2,§3 | ADR | **HUMANO autoriza** | Materialización requiere autorización |
| **7)** Decisiones humanas registradas | Cerrar/registrar | Continuo | AUDIT_PROTOCOL §B,N | tracking/ADR | **HUMANO** | Gate |
| **8)** Actualizar Plan Maestro + tracking | Añadir sección si autorizado | Paso 6-7 | PLAN §12,§14, orden | PLAN, tracking | **HUMANO autoriza (fuera modo auditoría)** | AUDIT_PROTOCOL §K en auditoría |
| **9+)** Materialización/implementación | Solo tras autorizaciones | Pasos previos + gates | Orden inviolable | Código/contratos | Autorizado | **IMPLEMENTATION NOT AUTHORIZED**, Gate C BLOCKED, Q2 UNAUTHORIZED |

**Vacío:** No hay procedimiento explícito para corregir contradicciones ni para modificar Plan Maestro. **Propuesta §10.**

---

## 10. Propuesta de corrección de gobernanza (PROPUESTA - NO VIGENTE)

### Problema
- Sin procedimiento para corregir contradicciones documentales (C1-C4/D12)
- Sin procedimiento explícito para modificar Plan Maestro; tensión con AUDIT_PROTOCOL §K
- GOVERNANCE §2 no cubre identity-defining cerrados (G1-D04) ni creación BC → ambigüedad

### Propuesta mínima (documentar, NO aplicar)
1. **GOVERNANCE.md §4:** "Corrección de contradicciones documentales" - inventariar, HUMAN DECISION, no inferir precedencia (ADR-0045), registrar decisión
2. **GOVERNANCE.md §5:** "Modificación documentos LEVEL 4" - fuera modo auditoría, HUMAN DECISION, actualizar §14, seguir cadena
3. **GOVERNANCE.md §2 aclaración:** casos sustanciales (nuevo BC/cambio fronteras/identity-defining cerrados/cambios identidad canónica) requieren evaluación ADR; FAIL-CLOSED si indeterminado
4. **AUDIT_PROTOCOL.md §K aclaración:** prohibición específica a modo auditoría; fuera de auditoría sigue cadena adopción

**Documentos afectados:** GOVERNANCE.md, AUDIT_PROTOCOL.md (posible ADR procedimiento). **Requiere HUMAN DECISION.** **No materializado en esta tarea.**

---

## 11. Matriz de decisiones humanas pendientes

| ID | Pregunta exacta | Alternativas | Consecuencias | Dependencias | Evidencia necesaria |
|---|---|---|---|---|---|
| **DH-1** | ¿Autorizar incorporar roadmap multimercado a **Plan Maestro** como sección, o en documento separado? ¿ADR previo o directo? | A) Sección N1 fuera modo auditoría B) docs/plans/ C) Solo ADR D) Otro | A) Sigue orden; requiere salir modo auditoría B) Menor impacto C) Menor visibilidad en N1 D) Indeterminado | Cadena adopción, AUDIT_PROTOCOL §K | Decisión explícita registrada |
| **DH-2** | ¿Reconciliar **C1-C4/D12**? ¿Cómo corregir contradicciones? | A) Corregir ADR-0049 editorialmente B) Añadir notas reconciliación C) Dejar histórico D) Caso por caso | A) Elimina ambigüedad B) Preserva historia C) Mantiene ambigüedad D) Complejo | ADR-0045 r9/r10 | Decisión por contradicción |
| **DH-3** | ¿Abrir **G1-D04** para no-cripto o mantener cerrado? | A) Mantener cerrado B) Abrir con ADR nuevo C) Gradual D) Modelo separado | A) Respeta estado B) Requiere ADR+cascada C) Requiere criterio D) Complejidad | Identidad G1 | Decisión producto+arquitectura |
| **DH-4** | ¿Separar/unificar **trade/contractual/market settlement**? | A) 3 separados B) Unificar C) 2+1 D) Delegar adaptador | A) Semánticamente correcto B) Riesgo confusión C) Intermedio D) Riesgo fuga | Contratos identidad/trading | Definición semántica |
| **DH-5** | ¿Necesita OCM calendario/sesiones canónico? ¿Dominio/BC? | A) Delegar adaptador B) market_data C) Nuevo BC D) trading E) Solo puertos | A) Mínimo B) Natural C) Justificación fuerte D) Dudoso E) Preparación | Fronteras BC, import-linter | Evidencia necesidad |
| **DH-6** | ¿Autorizar **Q2/materialización D20b** y avanzar hacia **GATE C**? | A) No por ahora B) Sí con plan C) Parcial D) Tras C1-C4/D12 | A) Respeta estado B) Desbloquea C) Complejo D) Más seguro | ADR-0055 Q2, ADR-0057 | Cierre contradicciones + plan |
| **DH-7** | ¿BC para calendario/settlement multimercado? | A) Extender market_data B) Extender trading C) Nuevo BC D) Solo puertos | A) Mínimo B) Solo trade settlement C) ADR probable D) Temporal | GOVERNANCE §2, import-linter | Justificación responsabilidades |
| **DH-8** | ¿Formalizar procedimiento corrección contradicciones/modificación documentos LEVEL 4? | A) Añadir a GOVERNANCE B) Nuevo ADR C) Actualizar AUDIT_PROTOCOL D) Caso a caso | A) Centraliza B) Formal C) Específico D) Ambiguo | AUDIT_PROTOCOL §B,K, ADR-0045 | Aprobar propuesta §10 |

**Nota:** DH-1+DH-2 pueden dividirse en independientes (evitar interpretar ubicación documental como aprobación estrategia).

---

## 12. Historial de actualizaciones del expediente

| Fecha | Acción | Responsable | Notas |
|---|---|---|---|
| 2026-10-19 | Creación expediente consolidado | Sisyphus (agent) | Baseline HEAD df9f0b6, rama chore/kafka-provision, estado tree limpio salvo untracked cobertura. Consolidación hallazgos investigación previa. Sin modificar archivos fuera docs/audits/. Sin operaciones Git mutativas. |
| 2026-10-19 | Registro contradicciones C1-C4/T-PLAN | Sisyphus | Evidencia trazable con ubicaciones exactas |
| 2026-10-19 | Registro estado canónico decisiones | Sisyphus | Corrección: D19-03..10 DECIDED (contradicho por L53-54) |
| 2026-10-19 | Registro verificaciones pendientes V-1..V-8 | Sisyphus | Sin ejecutar validadores mutantes; blob 4ad03eba existe pero lectura completa pendiente |
| 2026-10-19 | Propuestas gobernanza documentadas como PROPUESTAS | Sisyphus | NO vigentes, NO materializadas |

---

## 13. Próximo paso verificable

**Paso inmediato (mínimo accionable):** Resolver **DH-1 + DH-2** como preguntas humanas independientes.

**Acción concreta:**
1. Decidir si reconciliar C1-C4/D12 (DH-2) y con qué mecanismo
2. Decidir ubicación roadmap multimercado (DH-1) **separado** de aprobación estrategia mercados (DH-3)

**Verificaciones a completar progresivamente (read-only):**
- [ ] **V-3:** Leer contenido completo blob `4ad03eba` si accesible (`git show` selectivo) sin generar artefactos
- [ ] **V-5:** Leer §17.4 completo en ADR-0057 con contexto
- [ ] **V-4:** Leer `.github/workflows/ocm-ci.yml` completo
- [ ] **V-6:** Mapear usos `InstrumentType` exhaustivamente
- [ ] **V-7:** Buscar ADR adicionales relacionados
- [ ] **V-1:** Evaluar `scripts/audit_validator.py` impacto (solo ejecutar si read-only puro)

**Bloqueos actuales:** C1-C4/D12 sin resolver; D19/D20b `IMPLEMENTATION NOT AUTHORIZED`; Gate C BLOCKED; Q2 UNAUTHORIZED; `InstrumentType` cerrado (G1-D04).

**Confirmación final:** NO se modificaron archivos fuera de `docs/audits/`. NO se ejecutaron operaciones Git mutativas. NO se materializaron decisiones arquitectónicas. NO se aprobó estrategia multimercado. NO se corrigieron documentos normativos.


---

## FASE 2 — ACTUALIZACIÓN (2026-10-19)

### Verificaciones completadas
| ID | Estado | Evidencia clave |
|---|---|---|
| **V-3** | **COMPLETADO (parcial)** | Blob `4ad03eba` es backup wip (D19 packs + decision-support + identity code/tests). Pack D19 blob registra D19-01..10 + D20/D20a/D20b **DECIDED** con **IMPLEMENTATION: NOT AUTHORIZED**; Industry BOW blob registra D12 DECIDED Alt5+Alt3, HD-VC-1..HD-VC-7/HD-1..HD-8 DECIDED, D19 PENDING/BLOCKED, D19-01 HUMAN DECISION REQUIRED, DECISION REGISTERED NO. **No demuestra reemplazo formal.** |
| **V-4** | **COMPLETADO** | `.github/workflows/ocm-ci.yml`: jobs principales + `policy-gate` (needs architecture/engineering-health/app-guard/domain-guard/trading-guards/unit-tests/security/config-validation/quality) ejecuta `engineering_health_check.py` + `scripts/audit_validator.py`. CI menciona `M1..M25` vs script declara M1..M20. **Workflow != branch protection GitHub.** |
| **V-5** | **COMPLETADO** | `ADR-0057` §17.4 (L55,L109,L182): formalización §17.3 como ADR nuevo + registro en `tracking.yaml` + contrato/linter para activar GATE C. Requiere autorización humana independiente. **No resuelve C1-C4/D12.** |
| **V-6** | **COMPLETADO (mapa)** | `InstrumentType` cerrado SPOT/PERPETUAL/FUTURE/OPTION; usos en `contract_terms`, `listing_identity`, `__init__`. Extensión afectaría identity-defining (G1-D04). |
| **V-1** | **EVALUADO — NO EJECUTADO** | `scripts/audit_validator.py` 1000 líneas, usa subprocess/yaml/json; efectos secundarios no descartados. **Clasificación C**. No ejecutado (limitación justificada). |
| **V-2** | **NO VERIFICABLE LOCALMENTE** | Sin `gh`. Config externa fuera de alcance. |
| **V-7/V-8** | **COMPLETADO/EVIDENCIADO** | Principales ADR revisadas; C4/D12 contradictorio, **sin regla formal de precedencia automática** — **NO RESUELTO**. |

### Estado C1-C4 / D12 (actualizado)
- **C1:** ADR-0049 L53-54 vs L545-551 — **ABIERTA** (interna). Pack/blob confirman D19-03..10 DECIDED en registros deliberativos; contradicción persiste.
- **C2:** Pack blob L841 vs L2262-2291 — **ABIERTA** (histórica)
- **C3:** INDUSTRY/KN_BASE vs ADR-0049 — **ABIERTA (PARCIAL)** (jerarquía vía Q1/ADR-0055, textos contradictorios permanecen)
- **C4/D12:** Pack blob DECIDED Alt5+Alt3, decision-support OPEN, ADR-0049 C/RESTRICTED — **ABIERTA**. **D12 fuera alcance ADR-0057**. Sin regla formal de precedencia → **NO RESUELTO POR INFERENCIA**.

### Confirmación límites
- **Solo `docs/audits/`** modificado. Sin archivos normativos/arquitectónicos/código/tests/CI. Sin operaciones Git mutativas. Sin resolver contradicciones. Sin materializar decisiones. Sin aprobar estrategia/extension.


---

## FASE 3 — INVESTIGACIÓN RECONCILIACIÓN FORMAL (2026-10-19)

### C1 — análisis detallado
- L53-54: *"D19-03..D19-10 permanecen pendientes. NO se anticipa D20–D24."* 
- L544-560: tabla D19-03..10 = DECIDED, D20b-01..06 = DECIDED, D21–D24 BLOCKED
- Ambas en mismo ADR-0049. No hay texto explícito declarando sustitución. 
- **Conclusión:** discrepancia interna sustantiva. No resoluble por inferencia. Requiere HUMAN DECISION: a) corregir L53-54 (marcar superseded), b) añadir nota de reconciliación, c) mantener ambas con preeminencia explícita. Ninguna opción asumida.

### C2-C3 — análisis
- Pack D19 (blob): afirma D19-01..10 + D20/D20a/D20b DECIDED (L2262-2291) pero también L14 "D19: PENDING/BLOCKED" (contexto histórico)
- Industry BOW (blob): D19-01 HUMAN DECISION REQUIRED, D19 PENDING/BLOCKED (L33,L32)
- ADR-0049 vigente (main): D19-01..10 DECIDED
- Q1 (ADR-0055) resolvió jerarquía parcialmente; **textos históricos contradictorios permanecen** (C3). No hay evidencia de reemplazo formal que elimine esos textos históricos. Requiere decisión: preservar como histórico vs actualizar/reconciliar materialmente.

### C4/D12 — análisis
- decision-support: D12 OPEN
- pack: D12 DECIDED Alt5+Alt3
- ADR-0049: G1-D12 C/RESTRICTED (catálogo pendiente)
- ADR-0057: **D12 fuera alcance decisorio** (C4)
- **Conclusión:** D12 tiene 3 formulaciones diferentes. No puede resolverse por inferencia. HUMAN DECISION requerida.

### Procedimientos existentes vs lagunas
- ADR-0045 r9/r10: prohíben inferencia y exigen FAIL-CLOSED + Human Decision Gate — pero no especifica procedimiento para corregir discrepancia interna de ADR ni reconciliar backups/históricos con ADR vigente.
- ADR-0057 documenta inventario C1-C4 pero **no resuelve**; deja D12 fuera alcance.
- §17.4 cubre activación GATE C, no resolución contradicciones.

**Lagunas confirmadas:** procedimiento explícito para corrección de contradicciones documentales; precedencia entre textos deliberativos/backup y ADR vigente; mecanismo para marcar texto obsoleto sin rehacer ADR completo. **Propuestas (no materializadas):** añadir sección a GOVERNANCE "Corrección de contradicciones documentales" (inventario + decisión humana + registro); aclarar diferencia entre "decisión deliberativa", "decisión adoptada formalmente", "registro histórico". Esto requiere HUMAN DECISION.

### Secuencia independiente vs dependiente (DH-1/DH-2)
**Recomendación:** tratar **DH-1** (ubicación roadmap en Plan Maestro) y **DH-2** (reconciliación C1-C4/D12) como **independientes**. Ubicación documental no implica aprobación estrategia (DH-3). Reconciliación puede avanzar como acto separado; sin embargo, rigor de contenido roadmap se beneficia de resolver C1-C4. No bloqueante para investigar ubicación, sí bloqueante para afirmar estados normativos.

**Próximas acciones autorizables (read-only/documentación en audits):** consolidar evidencias completas (V-3 lectura selectiva blobs), documentar precedencia propuesta, formular preguntas DH-1/DH-2 con alternativas concretas — todo dentro de `docs/audits/`. **NO modificar normativos.**

**Confirmación:** FASE 3 completada con análisis formal; sin resolver C1-C4/D12; sin modificar archivos fuera `docs/audits/`; sin operaciones Git mutativas.
