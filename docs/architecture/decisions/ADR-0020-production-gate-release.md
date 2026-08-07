# ADR-0020: Production Gate como gate de release

**Estado:** Aceptado
**Fecha:** 2026-08-06
**Bounded context(s) afectado(s):** OCM (plataforma/CI), shared, packages (todos)

## Contexto

Hasta F2.1 la calidad se "recomendaba" pero no se bloqueaba de forma unificada. Cada gate vivía
en un job separado (ruff, mypy, import-linter, pip-audit) con umbrales que podían vaciarse por
configuración (p.ej. `fail_under = 0` en coverage, B-06/H-04; bandit ausente de CI, B-07/H-07).
La auditoría integral (2026-08) documentó tres problemas estructurales:

1. Un contrato roto o un umbral vacío no rompía el merge: el CI daba verde con gates desactivados.
2. No había una noción formal de "esto es lo que hace un artefacto *releaseable*".
3. Las reglas de calidad no tenían un lugar único que las declarara como obligatorias (estado
   `activada_en_ci` en `tracking.yaml` era solo documental hasta F2.1).

F2.0 (Engineering Health Check) ya valida coherencia Plan↔tracking↔ADR↔contratos↔CI como gate de
entrada. Falta el complemento de **salida**: un gate de release que declare explícitamente qué
chequeos deben pasar para que `main` sea desplegable y operativo (incluida la operación con
capital real en F3).

## Alternativas evaluadas

1. **Confiar en los jobs CI existentes como "gate implícito".** Descartada. Los jobs pueden
   editarse, y los umbrales dentro de ellos no están amarrados a un contrato normativo; un cambio
   silencioso de `fail_under` o la desactivación de bandit no rompería nada perceptible salvo
   revisión manual.
2. **Crear un segundo "mega-job" que repita toda la calidad.** Descartada. Duplica el cómputo y
   el mantenimiento; no añade semántica, solo repetición.
3. **Declarar en `tracking.yaml` el estado `activada_en_ci` de cada regla y hacer que el
   Engineering Health Check (F2.0) lo verifique contra los jobs CI reales. Elegida.** Es la
   formalización mínima: el SSOT de reglas (R5–R10...) gana semántica de gate de release, y el
   health check — que ya valida coherencia — pasa a validar también que toda regla con
   `activada_en_ci: true` tenga su mecanismo presente en `.github/workflows/ocm-ci.yml`.

## Decisión

1. **Production Gate = la suma de las reglas declaradas con `activada_en_ci: true` en
   `tracking.yaml`.** Una regla solo cuenta como gate si su `backtest` es `ok` y su
   `activada_en_ci` es `true`; el mecanismo asociado debe existir como job o step en CI.
2. **El Engineering Health Check (F2.0) es el verificación del Production Gate.** Cada regla
   activa debe tener un mecanismo observable en `.github/workflows/ocm-ci.yml` (nombres de job o
   comandos verificables). Una regla activa sin mecanismo en CI = health check rojo = merge
   bloqueado.
3. **Umbrales mínimos no negociables (R5/R6, fijados sobre medición real en F2.1):**
   - Cobertura `pytest --cov` con `fail_under = 40` (baseline medida 44%, margen 4pts; se sube
     gradualmente en PRs siguientes, nunca de un salto).
   - Bandit `-ll` sin hallazgos High (BLOCKER) sobre `apps ocm packages shared infrastructure`.
   - Import-linter con conteo de contratos ≥ 49 (baseline F2.1).
4. **Nueva regla de release: ningún umbral puede dejarse en un valor "placeholder" que no rompa
   el CI** (regla anti-vacua, en línea con B-06: `fail_under = 0` está prohibido como estado
   final; los umbrales se fijan sobre medición real con margen).
5. **El plan de subida de umbrales es por PRs incrementales**, nunca de un salto (documentado en
   `pyproject.toml [tool.coverage.report]`).

## Justificación técnica

- El health check ya existe, ya valida coherencia y ya corre en CI (job `engineering-health`,
  397459e); añadirle la verificación "regla activa ↔ mecanismo en CI" es un delta pequeño y sin
  costo de runtime significativo.
- El estado `activada_en_ci` ya está en el SSOT de reglas (R5, R6, R9, R10...); esta ADR lo
  convierte de documental a **normativo**: la fuente de verdad no cambia, solo su semántica.
- Fijar umbrales sobre medición real (no a ciegas) evita el fail-fast mal aplicado que la propia
  auditoría señaló en H-04: un umbral irreal rompe CI de inmediato y obliga a desactivarlo, que es
  peor que no tenerlo.

## Consecuencias

- **Más fácil:** saber qué es releaseable se reduce a consultar `tracking.yaml` (reglas con
  `activada_en_ci: true`) y correr el health check; no hay que leer 8 jobs de CI para saber si
  algo quedó gateado.
- **Deuda aceptada:** el health check verifica presencia del mecanismo (nombres/comandos) pero no
  la semántica interna de cada job; un job mal escrito pero presente pasaría la verificación
  nominal. Se mitiga manteniendo la regla anti-vacua (todo umbral debe romper el CI si se cruza).
- **Contratos BC-NN que lo hacen cumplir:** N/A directamente; es gobernanza de plataforma (ocm/).
- **Relación con F2.0:** F2.0 es el gate de entrada (coherencia normativa); ADR-0020 define el
  gate de salida (releaseable). Ambos usan el mismo health check como motor de verificación.

## Referencias

- Código: `scripts/engineering_health_check.py` (verificación), `.github/workflows/ocm-ci.yml`
  (jobs), `pyproject.toml` (`[tool.coverage.report]`, `[tool.bandit]`), `docs/plans/tracking.yaml`
  (SSOT de reglas R5/R6).
- Hallazgos: H-04 (B-06, coverage vacua), H-07 (B-07, bandit fuera de CI), H-20 (B-10, conteo de
  contratos), H-05/H-10/H-12 (paridad de config, dead stubs).
- ADRs relacionados: ADR-0016 (motor live; su requisito nº3 exige health check como gate real),
  ADR-0015 (app-layer guard). Próximo: ADR-0017 (Protocol Discovery Framework, F2.5) — Contract Provenance como componente.
