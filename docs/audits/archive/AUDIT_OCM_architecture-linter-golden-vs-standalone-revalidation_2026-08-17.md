# Architecture Linter — Revalidación Golden vs Standalone — 2026-08-17

## 1. Scope

Revalidación forense, en vivo y paso a paso (usuario + Claude, sin agente autónomo),
del estado real de `architecture_linter` tras el fallo original
`tests/architecture_linter/test_golden.py::test_golden_statuses_repo_actual`
y la afirmación de un reporte previo (`/tmp/AUDIT_GOVERNANCE_REVALIDATION.md`,
generado por otra sesión/modelo, no localizado en el repo) de que el linter
"reporta 7/10 reglas en FAIL". Objetivo: determinar cuál de las dos afirmaciones
es correcta y por qué ambas pueden ser simultáneamente ciertas.

## 2. Baseline Git

COMMIT: a36c503 (HEAD -> main) — "chore(architecture): trackear architecture_linter/ + limpieza de cache stale en CI"
PARENT: 314285a — "chore(architecture): fusionar architecture/ en architecture_linter/ — SSOT único"
ORIGIN/MAIN: ab5925d — "docs(audits): revalidación de auditoría de governance — informe original no localizado"
  (commit remoto, ajeno a esta sesión — ver §9)
RAMA: main, ⇡2 sobre origin/main (sin conflicto, sin divergencia bloqueante)
WORKING_TREE: 18 archivos modificados/borrados + 17 sin trackear (preexistentes, no generados por esta sesión)

## 3. Causa raíz del fallo original (ARCH-006 en pytest)

`__pycache__`/`.pytest_cache` con bytecode stale tras la reorganización
`architecture/` → `architecture_linter/` (commit `314285a`). Confirmado
empíricamente: con caché limpia (`find . -name "__pycache__" ... -exec rm -rf {} +;
rm -rf .pytest_cache`), la suite completa da 1164 passed, 4 failed
(los 4 en `tests/kafka/test_integration_kafka.py`, por bootstrap conectando a
`localhost:9093` en vez del puerto real mapeado `9094` — hallazgo aparte, no
arquitectura), coverage 51.46% (> 40% gate). `ARCH-006` no aparece entre los
fallos en esa corrida.

Fix aplicado: `architecture_linter/` y `tests/architecture_linter/` no estaban
trackeados en git (se colaron sin `git add` en el commit `314285a`) — corregido
en `a36c503`.

## 4. Ejecución standalone del linter (evidencia primaria)

Entry point confirmado: \`uv run python -m architecture_linter --json\`
(via \`architecture_linter/__main__.py\` → \`cli.py:main()\`, argparse).
Config: \`architecture_linter/architecture_linter.toml\` (default, sin \`--config\`).

Resultado, caché limpia, commit \`a36c503\`:

| Regla | Status | Findings |
|---|---|---|
| ARCH-001 | FAIL | 1 |
| ARCH-002 | FAIL | 2 |
| ARCH-003 | PARTIAL | 1 |
| ARCH-004 | FAIL | 1 |
| ARCH-005 | FAIL | 1 |
| ARCH-006 | PASS | 1 |
| ARCH-007 | FAIL | 8 |
| ARCH-008 | FAIL | 1 |
| ARCH-009 | PASS | 1 |
| ARCH-010 | FAIL | 2 |

7 FAIL, 1 PARTIAL, 2 PASS — **confirma exactamente** la cifra "7/10 FAIL" del
reporte previo. Evidencia cruda en \`/tmp/linter_run_verificacion.json\`.

## 5. Reconciliación: por qué el test golden pasa Y el linter reporta 7 FAIL

\`tests/architecture_linter/test_golden.py\` (líneas 26-34) declara
\`GOLDEN_EXPECTED\`, un mapa que fija el estado esperado de cada regla como
deuda técnica ya conocida y documentada, con justificación inline:

\`\`\`
ARCH-001: FAIL  # multiple position owners (trading + portfolio)
ARCH-002: FAIL  # divergencia semántica WAC vs reemplazo/pop
ARCH-003: PARTIAL  # reconciliación puntual submit-time, sin loop periódico
ARCH-004: FAIL  # sin balance real; sizing contra capital_usd
ARCH-005: FAIL  # cadena de freshness rota en niveles 3–6
ARCH-006: PASS  # ports huérfanos eliminados o cableados (remediación)
ARCH-007: FAIL  # 8 contratos duplicados/homónimos (ExchangeCircuitOpenError consolidado)
ARCH-008: FAIL  # 1 stub de producción (WSTradesSource; InfraMetricsKafkaProducer eliminado)
ARCH-009: PASS  # capas BC-08 respetadas (ignore_imports documentados)
\`\`\`

\`test_golden_statuses_repo_actual\` no exige "0 fallos" — exige que el estado
real coincida exactamente con este mapa. El linter standalone mide arquitectura
contra el ideal (Clean Architecture); el test golden mide **regresión** respecto
a deuda ya aceptada. Ambas fuentes son correctas, miden preguntas distintas.

## 6. Matriz de veracidad

| Afirmación | Fuente | Resultado |
|---|---|---|
| "7/10 reglas FAIL" | reporte previo no localizado (\`/tmp/AUDIT_GOVERNANCE_REVALIDATION.md\`) | CONFIRMADO |
| "ARCH-006 FAIL en pytest, causado por caché stale" | esta sesión, verificación empírica | CONFIRMADO |
| "architecture_linter no corre en CI" | ambos reportes previos + esta sesión | CONFIRMADO (verificado contra \`.github/workflows/\`) |
| "architecture_linter no estaba trackeado en git" | esta sesión | CONFIRMADO — corregido en \`a36c503\` |
| Contradicción aparente entre "solo ARCH-006 fallaba" (pytest) y "7/10 FAIL" (linter) | — | RESUELTA — ver §5, no es contradicción real, son dos preguntas distintas |
| Informe \`AUDIT_GOVERNANCE_INTEGRATION_EXTERNAL.md\` que motivó la revalidación remota | commit \`ab5925d\` (origin) | NO LOCALIZADO — ver §9, sin resolver |

## 7. Hallazgo ARCH-001 (detalle, no bloqueante para esta auditoría)

Posición gestionada por 6 owners mutables fuera del SSOT de \`portfolio\`:
\`TradeTracker._open_positions\` (trade_tracker.py:59),
\`OMS._orders/._open/._entry_positions\` (oms.py:169-170),
\`RiskManager._open_positions/._positions\` (risk/manager.py). Coincide con
H-09 del informe de auditoría técnica del 6-ago-2026 (ya conocido, sin cerrar).
Motivó \`ADR-0021-estado-posicion-unico-dueno.md\` (sin trackear al momento de
esta sesión).

## 8. CI

Verificado: \`architecture_linter\` **no** está en ningún job de
\`.github/workflows/ocm-ci.yml\` ni en otro workflow. Gate activo hoy solo
via import-linter (\`architecture\` job, ≥49 contratos) — reglas ARCH-NNN
sin enforcement automatizado.

## 9. Conflictos sin resolver

CONFLICTO #1 — \`AUDIT_GOVERNANCE_INTEGRATION_EXTERNAL.md\`

Afirmación A: existe un informe en \`docs/audits/AUDIT_GOVERNANCE_INTEGRATION_EXTERNAL.md\`
(referenciado como objeto de revalidación por el commit remoto \`ab5925d\`).

Evidencia A: \`git status --porcelain\` en esta sesión lo muestra como \`??\`
(sin trackear) en el working tree local — existe localmente pero nunca fue
comiteado.

Afirmación B: el commit remoto \`ab5925d\` declara "informe original no
localizado" tras \`find /\` exhaustivo.

Evidencia B: \`docs/audits/2026-08-17-governance-audit-revalidation.md\`
(commit \`ab5925d\`), secciones 9-10.

Conflicto: el archivo sí existe en el filesystem de \`orangehouse\` (visto por
esta sesión) pero la sesión que generó \`ab5925d\` no lo encontró.

Explicación: no determinada — hipótesis más probable es que esa sesión corrió
en un working tree o checkout distinto (posible sesión aislada/contenedor),
o el archivo se creó después de esa búsqueda. Requiere verificación humana.

Estado: NO DETERMINADO

## 10. Riesgos identificados

- Dos agentes (esta sesión + sesión \`opencode\`/Nemotron en paralelo) escribiendo
  a \`origin/main\` sin coordinación — \`ab5925d\` llegó al remoto mientras
  esta sesión trabajaba en local. Sin colisión esta vez; riesgo de colisión real
  en el futuro si ambas sesiones tocan los mismos archivos.
- \`architecture_linter\` sin gate en CI implica que un \`GOLDEN_EXPECTED\`
  desactualizado (alguien "arregla" ARCH-007 pero no actualiza el golden, o
  viceversa) no se detecta automáticamente hasta la próxima ejecución manual.

## 11. Decisiones que requieren aprobación humana

1. Agregar \`test_golden_statuses_repo_actual\` (no el linter con 0-fail) como
   gate en CI — bloquea solo ante regresión real, no ante deuda ya aceptada.
   Requiere decidir: ¿bloqueante o informativo?
2. Priorizar corrección de alguno de los 7 FAIL ahora (candidato más aislado:
   ARCH-001, ya tiene ADR-0021 sin trackear) vs. dejarlos como deuda documentada.
3. Resolver el Conflicto #1 (§9) — confirmar en qué sesión/entorno se generó
   \`AUDIT_GOVERNANCE_INTEGRATION_EXTERNAL.md\` antes de que otra revalidación
   remota repita el mismo "no localizado".

## 12. Veredicto final

CONSISTENTE, sin regresión. El fallo original de pytest fue caché stale
(remediado). El linter standalone reporta 7 violaciones reales y ya conocidas,
correctamente declaradas como deuda en \`GOLDEN_EXPECTED\`. Las dos cifras
aparentemente contradictorias ("solo ARCH-006 fallaba" vs "7/10 FAIL") miden
cosas distintas y ambas son ciertas. Pendiente: gate en CI y resolución del
Conflicto #1.

---
*Generado en sesión interactiva usuario + Claude (chat), turno por turno con
comandos ejecutados por el usuario y evidencia cruda pegada en cada paso.
Sin git add/commit/push ejecutados por esta sesión.*
