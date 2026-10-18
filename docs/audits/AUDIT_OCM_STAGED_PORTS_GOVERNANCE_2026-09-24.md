# FASE-A — Staged Ports Gobernados: formalización documental (2026-09-24)

> **Addendum A.1 (2026-09-24) — EXPIRY GOVERNANCE:** tras la investigación exhaustiva
> se clasifica **POLICY DOES NOT EXIST** (no hay regla normativa que compute `expires`);
> la fecha concreta de cada caso es decisión humana; `2026-12-31` es un ejemplo
> documental de ADR-0031 (forma ISO), no una política. Opciones de política y propuesta
> de fecha en la sección "EXPIRY POLICY DECISION REQUIRED" de la siguiente fase.

> **Addendum A.2 (2026-09-24) — GENERAL EXPIRY POLICY FORMALIZADA (DECISIÓN A):**
> política general de expiración documentada en ADR-0044 (§3.1 aplicabilidad / §3.4
> política general) y GOVERNANCE §10: `expires` obligatorio para **todo** Staged Port
> de OCM, ISO-8601 (`YYYY-MM-DD`), futura al autorizar/registrar, sin sustitutos
> (`indefinite`/`never`/`null`/`TBD`/`open-ended`), ausencia = `INVALID STAGED PORT`,
> fecha determinada caso por caso por decisión humana (sin duración universal),
> renovación por reautorización explícita (sin extensión automática), vencimiento →
> FAIL bloqueante (enforcement futuro). **DECISIÓN B (fecha concreta de
> MarketDataViewPort) queda pendiente**; `staged_ports: []` intacto.

> **Addendum A.3 (2026-09-24) — REGISTRO DEL PRIMER STAGED PORT (DECISIÓN B):**
> decisión humana concreta aprobada: `expires: "2026-12-31"` para MarketDataViewPort
> (solo este caso; NO se convierte en duración general). Entrada registrada en
> `policies/registry.yaml`/`staged_ports` con la estructura exacta de ADR-0044 §3.1:
> port/file (`packages/market_data/ports/outbound/market_data_view.py`), recurso ADR-0028
> §3.6 (Fase 1 sin consumidor → B-MD-004 consume en Fase 2), consumer/tracking_item
> B-MD-004 (fase F2, PENDIENTE), owner `market_data`, `expires: "2026-12-31"`,
> justification/revocation según ADR-0044 §3.4/§3.5. `evidence.json` requiere
> regeneración por mecanismo existente (hash previo `6b2a0594…` → nuevo
> `b0401cf0…`). Enforcement técnico (ARCH-006+M26+Golden+tests+CI) sigue PENDIENTE.

## Decisión humana

> **SÍ: OCM adopta el modelo híbrido de Staged Ports Gobernados.**

Un Port sin consumidor continúa siendo considerado huérfano y debe fallar, salvo que
esté explícitamente registrado como Staged Port y cumpla controles de gobernanza
completos, verificables y temporales (ADR → fase → consumidor → tracking → owner →
expiry).

## SAFEOPS baseline (antes de editar)

- Working tree root: `/home/orangemusic/trading/orangecashmachine`
- Branch: `feat/m22b-governance`
- HEAD: `e2130c1804d644743aeff42ecc3ed866f5a6b3f0`
- `git diff --check`: RC=2 (trailing whitespace preexistente en
  `deploy/scripts/install_systemd.sh:64,68,77,79,118`; NO causado por esta fase)
- Working tree preexistente: ADR-0028, tracking.yaml (B-25/fase0_gobernanza),
  book_builder.py (D4), importlinter.toml, deploy/* ; untracked: .vscode/,
  VERIFICATION_FINAL_REPORT.md, docs/audit/, market_data_view.py,
  test_market_data_view.py

Los cambios preexistentes en `docs/plans/tracking.yaml` y
`docs/architecture/decisions/ADR-0028-*` son la cadena de gobernanza D4/ADR-0028
(aceptación 2026-09-24, B-25 EN_PROGRESO, fase0_gobernanza) — **relacionados** con
esta fase, no conflictivos.

## Artefactos

| Archivo | Acción |
|---|---|
| `docs/architecture/decisions/ADR-0044-staged-ports-gobernados.md` | NUEVO — política de Staged Ports Gobernados |
| `docs/architecture/GOVERNANCE.md` | §10 Staged Ports Gobernados |
| `docs/plans/tracking.yaml` | B-MD-004 (consumidor futuro, PENDIENTE) |
| `policies/registry.yaml` | sección `staged_ports` (estructura, sin entrada) + ADR-0044 en related_adrs + modified 2026-09-24 |
| `docs/architecture/decisions/ADR-0028-*` | referencia mínima a ADR-0044 (§3.6 + ADRs relacionados) |

Ningún archivo fuera de alcance fue tocado (ARCH-006, audit_validator, golden, CI,
importlinter, packages, apps, shared, deploy).

## EXPIRY DECISION REQUIRED

No existe en el repositorio una convención suficiente para fijar `expires` del caso
MarketDataViewPort: solo hay un precedente de formato ISO (ejemplo ADR-0031 "2026-12-31")
y ningún hito datado (PLAN Maestro sin fechas por fase, sin referencias 2027). Por regla
de esta fase, NO se inventa una fecha en `policies/registry.yaml`; la entrada del caso no
se registra y queda propuesta razonada para el humano.

**Propuesta razonada (a decisión humana):** `expires: "2026-12-31"`
- Único precedente de expiración existente en el repo (ejemplo de waiver ADR-0031).
- Horizonte ~3 meses desde 2026-09-24: amplio para cablear B-MD-004 (Fase 2) y no
  indefinido.
- Al vencer sin consumidor → FAIL bloqueante + reautorización o retirada del port
  (ADR-0044 §3.4).

En su lugar pueden fijarse hitos concretos de la fase 2 si el humano los define.

## Estado documental de MarketDataViewPort

**NOT YET FULLY REGISTERED** — justifica arquitectónica (ADR-0028 §3.6) y consumidor
(B-MD-004 en tracking.yaml) existentes; owner disponible (market_data); pendiente de
decisión humana el `expires` para completar la entrada staged en registry.

## Evidence

- `policies/registry.yaml` fue modificado. `policies/evidence.json` **NO se regeneró**
  (regla FASE-A §12: no autorizado escribir en esta fase).
- **registry.yaml modified; evidence.json requires regeneration by existing mechanism.**
- Mecanismo existente: CI job `policy-gate-evidence` (`ocm-ci.yml:416-469`) compara
  SHA256 de `policies/registry.yaml` contra el hash en `evidence.json`; en mismatch
  instruye a actualizar `policies/evidence.json`. Herramienta: `sha256sum` + edición
  de `evidence.json` (o CI tras autorización de la fase de implementación).
- Hash real (calculado localmente, read-only) de `policies/registry.yaml` tras esta
  fase: `1e2b542075d08112afdce4a59e13f373d73428c5cacceb55ea774ab78e25e7e5`
  (anterior en `evidence.json`: `6b2a05948ba8f2445068c8a52a4c3d3ebc90e28462fe893467626e15b3a33291`).
  Regenerar `evidence.json` con el nuevo hash mediante el mecanismo existente.

## Validación (read-only, tras los cambios)

- `yaml.safe_load` OK en `docs/plans/tracking.yaml` (scheme version 2, 63 hallazgos,
  B-MD-004 presente: estado PENDIENTE, fase F2) y `policies/registry.yaml`
  (schema_version 1, 16 rules, staged_ports: []).
- `git diff --check` RC=2: solo los espacios preexistentes de
  `deploy/scripts/install_systemd.sh`; sin errores nuevos en los archivos tocados.
- `scripts/audit_validator.py`: FAIL **M11** (golden mismatch ARCH-006 — preexistente,
  esperado hasta la fase de implementación); WARN M25 (ADR-0044 en related_adrs sin
  regla — esperado: el enforcement técnico es fase posterior) y WARN parcial M20/M25
  preexistentes. Sin FAIL mecánico nuevo tras renombrar este informe al patrón canónico.
- `scripts/engineering_health_check.py`: FAIL idéntico al preexistente (B-25
  `EN_PROGRESO` fuera del enum, x2) — sin incoherencias nuevas introducidas.
- ARCH-006 sin cambios (fail como huérfano para MarketDataViewPort); golden y CI sin
  modificar.

## Remaining implementation (fase posterior, requiere autorización)

- `architecture_linter/rules/arch_006.py` — consumo del registro staged (orphan sigue
  FAIL; staged válido → PASS).
- `scripts/audit_validator.py` — M26 (gobernanza del registro staged, bloqueante).
- `tests/architecture_linter/test_golden.py` — pin del caso gobernado (PASS condicionado).
- Tests de regla/audit + wiring CI (jobs unit-tests + policy-gate).
- Registrar la entrada `MarketDataViewPort` en `staged_ports` una vez decidido `expires`.
- Regenerar `policies/evidence.json`.