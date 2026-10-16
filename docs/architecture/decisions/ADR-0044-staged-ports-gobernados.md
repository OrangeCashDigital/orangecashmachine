# ADR-0044: Staged Ports Gobernados (policy para ports sin consumidor, temporalmente permitidos)

**Estado:** Aceptado (2026-09-24)
**Fecha:** 2026-09-24
**Bounded context(s) afectado(s):** ocm (plataforma), market_data, shared, CI/CD

## Contexto

OCM produce Ports de dominio que pueden quedar sin consumidor durante una fase
(por ejemplo, un port de lectura que habilita un consumidor previsto para una fase
posterior). El sistema de enforcement actual (`architecture_linter/rules/arch_006.py`,
regla ARCH-006) considera huérfano todo Protocol bajo `PORTS_DIRS` que no tenga
consumidores reales (`ctx.references`) en los roots indexados, y lo hace `FAIL`.

El caso que motiva esta política es la frontera pública de lectura del BookBuilder:

- **ADR-0028 §3.6 (D4, 2026-09-24)** define `MarketDataViewPort`
  (`packages/market_data/ports/outbound/market_data_view.py`) como frontera pública
  de lectura, **sin consumidor en Fase 1**; el consumidor es **B-MD-004** (market
  validity pre-envío) en Fase 2.
- `ARCH-006` lo reporta hoy como huérfano (0 consumidores en `packages/shared/apps/ocm`;
  la implementación estructural en `book_builder.py` no cuenta como referencia por
  nombre), el `test_golden.py` espera `PASS` para ARCH-006 y `audit_validator` M11
  compara golden↔linter → los gates de CI fallan en estado consistente con la fase.

**Limitación actual de ARCH-006:** detecta el síntoma (0 consumidores) pero **no puede
distinguir** un abandono (orphan) de un port deliberadamente temporal (staged). No
dispone de intención ni de una fuente de gobernanza que lo diferencie.

**Por qué una excepción hardcoded es insuficiente:** añadir el símbolo a un `RESERVED`
(lista por substring de path en el módulo de la regla) resuelve el síntoma sin ADR, sin
fase, sin consumidor previsto, sin owner y sin fecha de expiración; se convierte en una
excepción permanente, opaca y sin accountability. Violaría ADR-0031/ADR-0032 (waiver con
`expires` obligatorio, dueño humano, trazabilidad) y el principio de automatización de la
arquitectura.

## Alternativas evaluadas

1. **Exención hardcoded en ARCH-006 (`RESERVED`)** — Ventaja: cero esfuerzo. Coste:
   no distingue orphan de staged, sin fecha ni dueño, excepción permanente y no
   auditable. Rechazada.
2. **Allowlist en `architecture_linter.toml`** — Ventaja: configurable sin tocar código.
   Coste: duplica una segunda lista fuera de la fuente de gobernanza, sin ADR/tracking/
   expiry. Rechazada (viola fuente única de verdad).
3. **ADR + tracking como única fuente (parsing markdown/yaml en el linter)** — Ventaja:
   evita lista nueva. Coste: parsing del linter (stdlib-only) frágil para ADR markdown.
   Rechazada por robustez.
4. **Registro declarativo gobernado (ELEGIDA)** — `policies/registry.yaml` amplía su
   esquema con una sección `staged_ports`; ARCH-006 sigue siendo guard de huérfanos y
   consume el estado staged válido; la gobernanza del registro se audita en la capa de
   policy validation. Reutiliza el vocabulario ADR-0031 (SSOT YAML, waiver con `expires`
   ISO obligatorio) y ADR-0032 (evidence hash, accountability humana).

## Decisión

> **OCM adopta Staged Ports Gobernados (2026-09-24, decisión humana).**

Un Port sin consumidor continúa siendo considerado huérfano y debe fallar, salvo que
esté explícitamente registrado como Staged Port en **`policies/registry.yaml`** y cumpla
controles de gobernanza completos, verificables y temporales.

**Staged no significa "exento"; significa "temporalmente permitido bajo gobernanza
verificable".**

### 3.1 Policy — contrato mínimo de una entrada staged

Cada entrada de `staged_ports` requiere como mínimo los siguientes campos:

```yaml
port: MarketDataViewPort            # nombre del Protocol/contract
file: packages/market_data/ports/outbound/market_data_view.py
adr: ADR-0028                       # decisión arquitectónica que lo justifica
adr_section: "§3.6"                 # sección del ADR
current_phase: F2                   # fase actual
target_phase: F3                    # fase en la que debe existir el consumidor
consumer: B-MD-004                  # consumidor futuro previsto
tracking_item: B-MD-004             # work item real en docs/plans/tracking.yaml
owner: market_data                  # categoría de equipo existente en el registry
expires: "YYYY-MM-DD"               # ISO-8601, obligatorio, futura, no indefinida
justification: "..."                # por qué se permite sin consumidor
revocation: "..."                   # criterio de salida (consumidor cableado o retirada)
```

No se agregan campos obligatorios sin justificación. `expires` debe poder determinarse
desde una convención del repositorio o por decisión humana explícita; no se inventa.
Esta política aplica a **todo Staged Port de OCM, existente o futuro**: `expires` es una
propiedad de gobernanza de cualquier Staged Port, no una particularidad de un caso.

### 3.2 Orphan rule

> Todo Port sin consumidor y sin registro staged válido es un orphan y debe fallar.

```
PORT SIN CONSUMIDOR
        │
        ├── NO está registrado como staged
        │        ↓
        │     ORPHAN
        │        ↓
        │     FAIL
        │
        └── está registrado como staged
                 │
                 ├── controles incompletos → FAIL
                 │
                 ├── expirado → FAIL
                 │
                 ├── ADR inexistente → FAIL
                 │
                 ├── tracking inexistente → FAIL
                 │
                 └── controles completos → PASS
```

### 3.3 Temporary nature

Staged es un estado **temporal** y nunca una excepción permanente. Todo staged tiene
`expires` y un criterio de salida (`revocation`).

### 3.4 Expiration — política general

Aplica a **todo Staged Port de OCM, existente o futuro**. `expires` es una propiedad de
gobernanza de cualquier Staged Port; MarketDataViewPort es solo el primer caso motivador.

- **Obligatoriedad:** todo registro Staged Port debe contener
  `expires: "YYYY-MM-DD"`. La **ausencia** de `expires` invalida el registro:
  `INVALID STAGED PORT` (no una excepción). No existe una tercera categoría
  "temporalmente permitido sin fecha".
- **Formato:** ISO-8601 `YYYY-MM-DD`. No se aceptan `indefinite`, `never`, `null`,
  `TBD`, `open-ended` ni ningún otro sustituto como expiración válida.
- **Futuridad:** `expires` debe ser una fecha futura en el momento en que el Staged
  Port sea autorizado/registrado.
- **Determinación (caso por caso):** OCM no establece una duración universal. No
  existe regla de "90 días / 3 meses / 6 meses / fin de F2 / fin de F3" ni se deriva
  automáticamente de `2026-12-31`. La fecha concreta de cada caso se decide por
  **decisión humana explícita**. Un ejemplo documental aislado (el `2026-12-31` del
  esquema de ejemplo de ADR-0031) fija la forma (ISO), no la duración.
- **Criterios de justificación:** la fecha debe ser justificable respecto de
  `justification`, `target_phase`, consumidor futuro (`consumer`/`tracking_item`) y el
  horizonte de implementación esperado. La fecha debe ser explícita, futura, limitada,
  trazable y revisable. No se usa una fecha arbitraria solo para satisfacer un gate.
- **Renovación:** la expiración no se extiende automáticamente. Antes de vencer se
  revisa el caso: si el trabajo dejó de ser válido → cerrar/eliminar el Staged Port; si
  sigue siendo válido → nueva autorización con **nueva fecha** y trazabilidad de `adr`,
  `justification`, `tracking_item`, `owner`, `target_phase` y `expires`.
  No existe renovación implícita ni automática.
- **Vencimiento:** al pasar la fecha, el Staged Port deja de considerarse válido. Con
  el enforcement técnico implementado, `expired staged port → FAIL` (bloqueante); sin
  él, la decisión no cambia: el estado deja de ser válido.

### 3.5 Closure

Cuando el consumidor real exista:
1. el Port deja de ser staged;
2. se elimina su entrada de `staged_ports`;
3. ARCH-006 vuelve a tratarlo como Port normal con consumidor (referencias normales).

Cerrar por éxito no deja residuos en el registro; el historial de la entrada queda
documentado en el registry (patrón `history` de ADR-0031).

### 3.6 Source of truth

`policies/registry.yaml` es la **fuente declarativa única** de staged ports. No se
crea una segunda lista equivalente en `architecture_linter.toml` ni en código.

### 3.7 Enforcement (conceptual — implementación posterior)

- **ARCH-006** protege contra Ports huérfanos y, tras esta decisión, consumirá el
  estado staged válido (PASS solo si los controles están completos).
- **audit_validator / M26** validará la gobernanza del registro staged (ADR existente,
  tracking existente, `expires` ISO vigente, campos completos, enforcement en CI).

> **Estado de implementación:** estas capacidades de enforcement NO están implementadas;
> su implementación (ARCH-006 + M26 + golden + tests + CI) es una fase posterior que
> requiere autorización separada. Hasta entonces ARCH-006/M11/Policy Gate pueden reportar
> FAIL por la ausencia del enforcement técnico, no por una decisión distinta de esta ADR.

### 3.8 Golden

El Golden continúa siendo bloqueante. Un staged válido se considera `PASS` condicionado
a esta política. Este ADR no modifica `tests/architecture_linter/test_golden.py`; en la
fase de implementación se añadirá el pin ornamental del caso gobernado.

### 3.9 CI

Los gates de CI permanecen bloqueantes con la siguiente semántica:

```text
orphan            → FAIL
staged inválido   → FAIL
staged expirado   → FAIL
staged legítimo   → PASS
```

No se degrada ningún gate a WARNING/non-blocking ni se modifican umbrales.

### 3.10 Primer caso: MarketDataViewPort (registro con decisión humana)

- `MarketDataViewPort` (frontera pública de lectura de ADR-0028 §3.6, D4) es el primer
  caso que motiva y aplica esta política.
- La relación documental es:

```text
ADR-0028  → justificación arquitectónica (§3.6, D4)
B-MD-004  → consumidor futuro (work item en tracking.yaml, PENDIENTE)
registry   → gobernanza staged (sección staged_ports)
```

- **Este ADR NO afirma que MarketDataViewPort cumpla hoy todos los controles.** Su
  conformidad efectiva (registro completo con `expires` decidido) depende de que existan
  todos los artefactos requeridos: ADR (existe), `tracking_item` B-MD-004 (existe,
  PENDIENTE en F2), fase (F1 → F2), owner (categoría existente) y `expires`.
- **EXPIRY DECIDIDO (2026-09-24):** la expiración de MarketDataViewPort fue decidida
  humanamente (decisión documentada en `docs/audits/AUDIT_OCM_STAGED_PORTS_GOVERNANCE_2026-09-24.md`,
  Addendum A.3 — DECISIÓN B): `expires: "2026-12-31"`. Corresponde **únicamente al caso
  MarketDataViewPort** y **no constituye una duración universal** para Staged Ports.
  Toda renovación futura requiere una decisión humana explícita con nueva fecha (ver
  §3.4). La entrada correspondiente queda **registrada** en `policies/registry.yaml`
  (sección `staged_ports`).

## Justificación técnica

- **Preserva la intención de ARCH-006**: la regla sigue siendo "no huérfanos"; el
  registro introduce la única causa legítima reconocible, sin cambiar el kernel del
  detector.
- **Reutiliza el patrón waiver/expiry de ADR-0031/M24** (`expires` ISO obligatorio,
  adr asociado) y la accountability de ADR-0032: dueño, evidencia y anti-manipulación
  (`evidence.json` ya hasea `policies/registry.yaml`).
- **No toca contratos BC-NN**: BC-56 permanece intacto; DIP se refuerza (el port staged
  es el punto de la frontera pública).
- **Generalizable**: el contrato sirve para futuras fases/casos sin cambios al esquema.
- **Anti-deuda**: `expires` + `revocation` + bloqueo al vencer convierten el staged en
  un préstamo con vencimiento, nunca en una excepción eterna.

## Consecuencias

- **Más fácil:** validar formalmente que un port deliberadamente sin consumidor es
  gobernado y trazable (ADR → fase → consumidor → tracking → owner → expiry).
- **Deuda aceptada:** enforcement técnico pendiente (ARCH-006 consume registro, M26).
  Hasta su implementación, `ARCH-006 = FAIL` para MarketDataViewPort es el estado
  esperado y no debe ocultarse (regla del gate: no ocultar el FAIL modificando golden,
  no añadir el port a `RESERVED`, no degradar CI).
- **Contratos que hacen cumplir:** ARCH-006 (guard de huérfanos), test_golden (gate
  bloqueante), audit_validator M11/M24-M26 futuro, CI policy gate (bloqueante),
  ADR-0031 (registry) y ADR-0032 (evidence hash/waiver).

## Referencias

- Código: `architecture_linter/rules/arch_006.py`, `policies/registry.yaml`,
  `policies/evidence.json`, `scripts/audit_validator.py`, `tests/architecture_linter/test_golden.py`
- ADRs relacionados: ADR-0028 ($3.6 primer caso), ADR-0031, ADR-0032
- Auditorías: `docs/audits/AUDIT_OCM_STAGED_PORTS_GOVERNANCE_2026-09-24.md`