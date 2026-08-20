# AUDIT_OCM_policy-registry-implementation_2026-08-19

**Tipo:** Auditoría de implementación (ADR-0031) — constancia de diseño y ejecución
**Ejecución:** 2026-08-19 (branch `main`)
**Alcance:** Implementación de `policies/registry.yaml` + M22..M26 en `scripts/audit_validator.py` + ticket deuda B-61
**Relación:** ADR-0031 (Aceptado), ADR-0032 (Aceptado)

---

## 1. Executive Summary

Este documento deja constancia de la implementación profesional de ADR-0031
(Policy Registry YAML), incluyendo un **hallazgo de modelado de contrato**
sobre la naturaleza del enforcement de R5–R8.

**Hallazgo central (F-PRI-01):** ADR-0031 modeló todo enforcement como un
*guard pattern* con fixtures de test positivo/negativo en archivo. La
evidencia real del repositorio demuestra que existen **tres modelos de
enforcement distintos**, y el contrato solo contempla uno:

| Modelo | Reglas | Enforcement real | `tests.positive/negative` ¿aplica? |
|---|---|---|---|
| Guard pattern (AST/round-trip) | R1–R4, R9–R16 | Fixtures pos/neg en `tests/` | Sí |
| Tool gate (scanner/umbral CI) | R5 (coverage), R6 (bandit) | Job CI ejecuta herramienta sobre árbol real | No |
| Absence gate (módulo eliminado) | R8 | "el módulo no existe" (rg/git) | No |

**Consecuencia:** el esquema del registry adopta un discriminador
`mechanism_type` (`guard_script | tool_gate | absence_gate | import_linter`)
y la regla M22 es *mechanism-aware*: exige `tests.positive/negative` solo a
guards; a tool/absence gates exige `ci.command`/`evidence`.

**Clasificación de R5–R8:**
- **R5, R6** — NO incumplen ADR-0031; el contrato las modelaba mal. Están
  activas en CI (`activada_en_ci: true`, `backtest: ok`).
- **R8** — NO incumple; resuelta por ausencia (B-13 HECHO, stub eliminado
  `ff67b93`). Registrada como `absence_gate` + `status: DEPRECATED`.
- **R7** — SÍ incumple de verdad: tiene tests (`test_structured_parity.py`)
  pero `backtest: pendiente` y no está activada en CI. Recibe waiver temporal
  + deuda en B-61.

**Renumeración M21..M25 → M22..M26:** M21 ya existe en `audit_validator.py`
(`m21_canonical_audit_filenames`). Las cinco reglas nuevas de ADR-0031 se
implementan como **M22..M26** (M21 permanece intacto).

## 2. Matriz de Findings

| ID | Severity | Classification | Control | Descripción |
|---|---|---|---|---|
| F-PRI-01 | HIGH | NUEVO | DOC-CONTRACT-MODELING | ADR-0031 modeló todo enforcement como guard pattern; R5/R6/R8 son tool/absence gates |
| F-PRI-02 | MEDIUM | NUEVO | DOC-REGISTRY-IMPLEMENTATION | Se crea `policies/registry.yaml` con R1–R16, `mechanism_type` y waiver para R7 |
| F-PRI-03 | MEDIUM | NUEVO | DOC-VALIDATOR-EXTENSION | Se implementan M22–M26 en `audit_validator.py`; M21 intacto |
| F-PRI-04 | LOW | NUEVO | DOC-DEBT-TICKET | Se crea B-61 para migrar R5–R8 a evidencia pos/neg explícita y eliminar waivers |

## 3. Matriz de Controles

| Control | Comando | Resultado | Estado |
|---|---|---|---|
| VALIDATOR | `uv run python scripts/audit_validator.py` | PASS — findings, reglas M1..M26 | **PASS** |
| LINT-IMPORTS | `uv run lint-imports --config architecture_linter/importlinter.toml` | 0 broken | **PASS** |
| RUFF | `uv run ruff check scripts/audit_validator.py` | 0 errores | **PASS** |
| MYPY | `uv run mypy scripts/audit_validator.py` | sin errores | **PASS** |
| PY_COMPILE | `uv run python -m py_compile scripts/audit_validator.py` | ok | **PASS** |
| TESTS | `uv run pytest tests/architecture/test_policy_registry.py -q` | todos verdes | **PASS** |
| GIT_DIFF | `git diff --check` | sin whitespace errors | **PASS** |
| ENGINEERING_HEALTH | `uv run python scripts/engineering_health_check.py` | PASS | **PASS** |

Controles = PASS(8) + FAIL(0) = 8

## 4. Matriz de Decisiones

| ID | Decisión | Impacto |
|---|---|---|
| D-PRI-01 | Adoptar `mechanism_type` como discriminador del enforcement en el registry | Contrato alineado con la realidad; R5/R6/R8 sin waiver innecesario |
| D-PRI-02 | M22 mechanism-aware: guards → tests pos/neg; tool/absence gates → ci.command/evidence | Sin falso incumplimiento de reglas CI puras |
| D-PRI-03 | Renumeración M21..M25 (ADR) → M22..M26 (implementación) porque M21 ya existe | M21 no se renumera ni rompe |
| D-PRI-04 | Waiver temporal solo para R7 (backtest pendiente, no activa en CI) + B-61 | Deuda real representada, no inventada |
| D-PRI-05 | R8 como `absence_gate` + `status: DEPRECATED` (resuelto por eliminación) | Sin waiver forzado donde la regla ya no aplica |

## 5. Integridad

- N7 respetado: no se modifica ninguna auditoría histórica de `docs/audits/`.
- Evidencia de R1–R16 obtenida de tests reales verificados (ver matriz de
  inventario en FASE 0). No se inventan rutas ni nombres de test.
- No se marcan deudas como HECHO. B-61 queda PENDIENTE.
- M21 (`m21_canonical_audit_filenames`) permanece intacto.
- ADR-0032 no se modifica salvo referencia ya existente.

REPRODUCIBILIDAD
- commit: af59819 (feat(policy)) + e777f7d (chore(tracking)) — implementación de ADR-0031
- branch: main
- fecha: 2026-08-19
- protocolo: AUDIT_PROTOCOL v2.2 (M1..M26)
- herramientas: `uv run python scripts/audit_validator.py --versions`
- comandos: los listados en la Matriz de Controles
- resultado: PASS