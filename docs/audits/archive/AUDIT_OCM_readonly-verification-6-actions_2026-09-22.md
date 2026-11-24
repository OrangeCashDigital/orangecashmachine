# CLASIFICACIÓN DE HALLAZGOS — VERIFICACIÓN READ-ONLY
## OrangeCashMachine (OCM) — DEP-005-CLS

- **Fecha**: 2026-09-22
- **Modo**: READ-ONLY absoluto. Cero modificaciones de cualquier tipo.
- **Propósito**: Clasificación definitiva de los 6 hallazgos verificados.

---

## HALLAZGO 1: "Dos procesos market_data.main compitiendo"

### 1. Evidencia concreta
```
ps aux | grep "[m]arket_data.main" → UN SOLO resultado:
  PID 718168, PPID 1, USER orangem+, COMMAND: python -m market_data.main,
  START: 14:36, CPU: 8.6%, MEM: 6.5%, STAT: Ssl

ps aux | grep "[o]cm" → 3 procesos:
  PID 1741: redis-server (docker-init)
  PID 192801: python -m app.cli.streaming_hydra
  PID 718168: python -m market_data.main

systemctl is-active ocm-market-data.service → active
systemctl is-active ocm-streaming.service → active
```

### 2. Estado: **CONTRADICTION**
El reporte original afirma "DOS procesos compitiendo". La evidencia muestra 1 proceso de market-data + 1 proceso de streaming (servicios DIFERENTES). No hay doble instancia.

### 3. Clasificación: **BUG** (en el reporte de auditoría, no en el sistema)
Es un error de interpretación del reporte original, no un defecto del sistema.

### 4. Afecta runtime real: **NO**
El runtime opera correctamente. Un solo `market_data.main` escuchando en puerto 8001. Sin conflicto de puerto. Sin error de sistema.

### 5. Relación con ADR existentes
Ninguna relación directa. El comportamiento de un solo market-data service es consistente con ADR-0024 (dirección arquitectura microservicios) y ADR-0022 (ciclo de vida proceso realtime feeds).

### 6. Decisión arquitectónica requerida: **NINGUNA**
No hay problema arquitectónico. El sistema funciona como está diseñado.

### 7. Por qué NO requiere ADR
No es un cambio de diseño, no es un defecto, no requiere documentación formal. Es simplemente una corrección de un reporte que contenía una premisa falsa.

---

## HALLAZGO 2: "Nice=-20 contradice NoNewPrivileges=true"

### 1. Evidencia concreta
```
deploy/systemd/templates/ocm-streaming.service.template, líneas 31-34:
  Nice=-20
  LimitRTPRIO=99
  LimitMEMLOCK=infinity
  LimitNOFILE=65536

/etc/systemd/system/ocm-streaming.service (ACTIVE):
  NoNewPrivileges=true
  ProtectSystem=strict
  ReadWritePaths=...
  PrivateTmp=true
  ← SIN Nice, SIN LimitRTPRIO, SIN LimitMEMLOCK

deploy/systemd/templates/ocm-market-data.service.template, línea 27:
  Nice=-10

/etc/systemd/system/ocm-market-data.service (ACTIVE):
  ← SIN Nice
```

### 2. Estado: **CONTRADICTION**
La supuesta contradicción existe SOLO en el template, no en el servicio activo. El template tiene `Nice=-20` + `NoNewPrivileges=true`, pero el servicio que realmente corre en el sistema NO tiene `Nice=-20`.

### 3. Clasificación: **DEPLOYMENT**
Es un problema de despliegue: el template fue actualizado pero el servicio activo nunca fue regenerado ni recargado (`systemctl daemon-reload` no ejecutado).

### 4. Afecta runtime real: **NO**
El servicio activo no tiene Nice=-20, por lo tanto la supuesta contradicción no existe en el runtime actual. El sistema opera con la configuración por defecto de systemd.

### 5. Relación con ADR existentes
- ADR-0022: ciclo de vida de proceso realtime feeds — define la estructura de los servicios
- ADR-0024: dirección arquitectura microservicios — define el modelo de deployment
- La discrepancia template/activo es un problema de proceso de deploy, no de diseño arquitectónico

### 6. Decisión arquitectónica requerida: **NINGUNA**
Es un problema operacional de deployment. Requiere:
- Actualizar el servicio activo desde el template (operación de deploy)
- O revertir el template al estado original
- No requiere cambio de diseño arquitectónico

### 7. Por qué NO requiere ADR
No cambia la arquitectura del sistema. Es un problema de proceso operacional entre template y servicio activo. Cualquier persona que ejecute `systemctl daemon-reload` lo resuelve (o lo confirma como intencional si se decidió no aplicar los cambios de bajo-latencia).

---

## HALLAZGO 3: "Secrets placeholder (OCM_API_JWT_SECRET=CHANGE_ME, REDIS_PASSWORD=ocm_local_dev)"

### 1. Evidencia concreta
```
.env:
  OCM_API_JWT_SECRET=CHANGE_ME_generate_with_secrets_token_hex_32
  REDIS_PASSWORD=ocm_local_dev
  BYBIT_API_KEY=<REDACTED>
  BYBIT_API_SECRET=<REDACTED>

scripts/check_production_gates.py:
  G8: "Live trading bloqueado (IS_STUB=true), sin órdenes reales"

apps/api/settings.py:
  jwt_secret: str = Field(env_prefix="OCM_API_")
  @field_validator("jwt_secret") — requiere ≥32 caracteres

packages/portfolio/bootstrap/composition_root.py:
  password=(redis_cfg.password.get_secret_value() if redis_cfg.password else None)

ocm/config/layers/rules.py:
  _rule_production_never_dry_run: production + dry_run=True = ConfigRuleViolation

OCM_ENV=development → dry_run=True por defecto
```

### 2. Estado: **PARTIALLY CONFIRMED**
- CONFIRMADO: `OCM_API_JWT_SECRET` es placeholder (`CHANGE_ME_...`)
- CONFIRMADO: `REDIS_PASSWORD` es valor de desarrollo (`ocm_local_dev`)
- CONFIRMADO: No existe secret provider (Vault, AWS Secrets, etc.)
- CONFIRMADO: `.env` contiene credenciales REALES de BYBIT
- PARTIALMENTE CONFIRMADO: El riesgo real depende de si OCM_ENV=development permite operaciones reales con credenciales BYBIT

### 3. Clasificación: **SECURITY**
Es un riesgo de seguridad real: credenciales reales de exchange en un archivo `.env` que está en desarrollo.

### 4. Afecta runtime real: **PARCIALMENTE**
- `OCM_API_JWT_SECRET` placeholder afecta la autenticación API gateway en producción
- `REDIS_PASSWORD=ocm_local_dev` funciona porque Redis Docker también usa `ocm_local_dev`
- `BYBIT_API_KEY` y `BYBIT_API_SECRET` son REALES pero `OCM_ENV=development` con `dry_run=True` probablemente previene ejecución real de operaciones
- Sin embargo, el riesgo existe: si `dry_run` se desactiva accidentalmente, se operaría con credenciales reales

### 5. Relación con ADR existentes
- ADR-0003: interfaz angosta TradingCompositionRoot — relación con ejecución de órdenes
- ADR-0016: motor de ejecución live — IS_STUB protege contra ejecución real
- ADR-0022: ciclo de vida proceso realtime — relación con credenciales
- No hay ADR específico sobre gestión de secretos o seguridad de credenciales

### 6. Decisión arquitectónica requerida: **POSIBLEMENTE SÍ**
Si se decide implementar un secret provider (Vault, AWS Secrets Manager, etc.), esto sería un cambio arquitectónico que afecta:
- `ocm/config/loader/env_resolver.py` (carga de credenciales)
- `apps/api/settings.py` (resolución de JWT secret)
- `packages/portfolio/bootstrap/composition_root.py` (resolución de Redis password)
- Potencialmente un nuevo bounded context de secrets management

### 7. Por qué podría requerir ADR
Un cambio de `secret management` de `.env` a un proveedor de secretos externo es un cambio de diseño que afecta la arquitectura de seguridad del sistema. Sin embargo, si se mantiene `.env` con un `.env.example` separado y credenciales de desarrollo, no se requiere ADR — es una decisión operacional de seguridad.

---

## HALLAZGO 4: "OCM_ENV=development en .env"

### 1. Evidencia concreta
```
.env: OCM_ENV=development
deploy/host.env: OCM_ENV=development
config/settings.yaml: environment.default_env: development

apps/app/cli/main.py línea 281:
  if os.environ.get("OCM_ENV", "development") == "production":
    _reject_cfg_job_in_production() → SystemExit(2)

ocm/config/layers/rules.py línea 88:
  if cfg.environment.name == "production" and cfg.safety.dry_run:
    raise ConfigRuleViolation("PRODUCTION_DRY_RUN")

ocm/config/loader/env_resolver.py:
  resolve_env() cascade: explicit > env_var > dotenv > settings_yaml > default
  Default: "development"
  ALLOWED_ENVS: {"dev", "development", "staging", "prod", "production", "test"}

apps/app/cli/paper_hydra.py:
  dry_run (default) → config.safety.dry_run
  uv run paper --dry-run

apps/app/cli/streaming_hydra.py:
  uv run streaming --env production --push-interval 15
```

### 2. Estado: **CONFIRMED**
- CONFIRMADO: `OCM_ENV=development` está configurado en `.env` y `host.env`
- CONFIRMADO: El valor `development` tiene efectos específicos y documentados
- CONFIRMADO: `production` bloquea `--cfg job` y bloquea `dry_run=True`
- CONFIRMADO: `development` permite `dry_run=True` (paper trading)

### 3. Clasificación: **CONFIGURATION**
Es una configuración activa del sistema. No es un bug ni un defecto.

### 4. Afecta runtime real: **SÍ**
- `OCM_ENV=development` habilita `dry_run=True` por defecto
- `--cfg job` está permitido (no bloqueado)
- `safety.dry_run` puede ser True
- `LiveExecutor.IS_STUB` probablemente es True
- El sistema opera en modo paper trading
- Sin embargo, las credenciales BYBIT REALES están en `.env`

### 5. Relación con ADR existentes
- ADR-0003: TradingCompositionRoot — define cómo se ensambla el trading engine
- ADR-0012: TradingEngine como runtime puro — relación con IS_STUB
- ADR-0016: Motor de ejecución live — la protección IS_STUB es relevante
- `_reject_cfg_job_in_production()` y `_rule_production_never_dry_run()` son reglas de configuración que enmascaran un problema más profundo: ¿por qué hay credenciales reales en un entorno de desarrollo?

### 6. Decisión arquitectónica requerida: **POSIBLEMENTE SÍ**
La pregunta arquitectónica fundamental es:
**"¿El sistema puede operar en OCM_ENV=development con credenciales BYBIT reales sin riesgo de ejecución real?"**

Si la respuesta es NO (dry_run puede desactivarse accidentalmente), entonces se requiere una decisión arquitectónica para:
- Separar credenciales de desarrollo y producción
- Implementar `IS_STUB=True` forzado en development
- O separar los archivos `.env` en `.env.dev` y `.env.prod`

### 7. Por qué podría requerir ADR
Si se decide que el sistema NO debe tener credenciales reales en development, esto es un cambio de diseño que afecta la arquitectura de configuración y seguridad del sistema. Sin embargo, si se acepta que `OCM_ENV=development` + `dry_run=True` es suficiente como protección, no se requiere ADR — es una decisión operacional.

---

## HALLAZGO 5: "check_production_gates.py es required check que bloquea merge"

### 1. Evidencia concreta
```
scripts/check_production_gates.py (577 líneas, G1-G11)

.github/workflows/ocm-ci.yml:
  semgrep job:
    needs: [architecture, engineering-health]
    steps:
      - run: uv tool run semgrep ... (continue-on-error: true)
      - name: Production gates (code-level, G1/G2/G3/G10/G11)
        run: uv run python scripts/check_production_gates.py --mode gate-ci

  policy-gate job:
    needs: [architecture, engineering-health, app-guard, domain-guard,
            trading-guards, unit-tests, security, config-validation, quality]
    ← NO incluye "semgrep"

  integration-tests job:
    needs: [policy-gate]

  La cadena crítica para merge:
    architecture → engineering-health → [app-guard, domain-guard,
    trading-guards, unit-tests, security, config-validation, quality]
    → policy-gate → integration-tests

  semgrep está PARALELAMENTE a policy-gate, NO es dependencia de nada.
  El job semgrep no tiene "required" explícito en GitHub branch protection
  (no se encontró .github/branch-protection.json).
```

### 2. Estado: **CONTRADICTION**
El reporte original implica que `check_production_gates.py` es un required check que bloquea merge. La evidencia muestra que el job `semgrep` que contiene el check es:
- UN PARALELO job que NO es dependencia de `policy-gate`
- NO está en la cadena crítica de merge
- El semgrep step tiene `continue-on-error: true`
- El paso "Production gates" dentro de semgrep job no tiene `continue-on-error`, pero el job entero no es requerido

### 3. Clasificación: **CI-GOVERNANCE**
Es un problema de gobernanza de CI/CD. El script existe y funciona, pero la integración CI no lo convierte en un gate obligatorio.

### 4. Afecta runtime real: **NO**
El script afecta el pipeline de CI, no el runtime. El sistema funciona correctamente sin que este check sea required.

### 5. Relación con ADR existentes
- ADR-0032: Policy gate — `policy-gate` es el gatekeeper real
- ADR-0034: Semgrep — reglas arquitectónicas declarativas
- `policy-gate` es el ADR de gobernanza que define qué es required para merge
- `check_production_gates.py` NO está en la definición de `policy-gate` como required check

### 6. Decisión arquitectónica requerida: **SÍ, si se quiere que bloquee merge**
Si se decide que `check_production_gates.py` debe ser un required check, se requiere:
- Agregar `semgrep` a las dependencias de `policy-gate` en `ocm-ci.yml`
- O crear un job separado `production-gates` con `needs` y como requisito de merge
- Esto es un cambio de gobernanza de CI que afecta el flujo de desarrollo

### 7. Por qué requiere ADR (o no)
- Si se agrega a `policy-gate`, es un cambio de CI que afecta el flujo de desarrollo — podría requerir un ADR de gobernanza de CI
- Si no se agrega, no requiere nada — simplemente el script existe como verificación opcional
- La decisión es de **proceso**, no de arquitectura, pero tiene implicaciones de gobernanza que justifican documentación formal

---

## HALLAZGO 6: "README dice DETENIDO pero sistema está ACTIVO"

### 1. Evidencia concreta
```
docs/architecture/README.md, línea 45:
  "Estado operativo: actualmente DETENIDO (ni Docker ni local)."

systemctl is-active ocm-market-data.service → active
systemctl is-active ocm-streaming.service → active
curl http://localhost:8001/health → {"status":"healthy","service":"market-data-service"}
curl http://localhost:8001/ready → {"status":"ready","service":"market-data-service"}
ps: PID 718168 (market_data.main) y PID 192801 (streaming_hydra) corriendo
Docker: ocm_redis=healthy, ocm_kafka=healthy, ocm_zookeeper=healthy
```

### 2. Estado: **CONTRADICTION**
El README dice "DETENIDO (ni Docker ni local)" pero:
- Docker está corriendo (redis, kafka, zookeeper healthy)
- systemd services están ACTIVE
- Health endpoint responde healthy
- Procesos corriendo

No es ambigüedad: el texto es explícitamente sobre "Estado operativo" y "ni Docker ni local", pero Docker SÍ está local y corriendo.

### 3. Clasificación: **DOCUMENTATION**
Es un problema de documentación desactualizada. El código y el runtime son correctos; el README no refleja el estado actual.

### 4. Afecta runtime real: **NO**
El sistema funciona correctamente. El README es información descriptiva, no afecta la operación del sistema. Sin embargo, puede llevar a confusiones para nuevos desarrolladores o auditores.

### 5. Relación con ADR existentes
- ADR-0024: dirección arquitectura microservicios — el README referencia ADR-0024
- El README describe la arquitectura actual y futura
- La contradicción es específicamente sobre el estado operativo, no sobre la arquitectura
- No hay ADR sobre el README como documento vivo

### 6. Decisión arquitectónica requerida: **NINGUNA**
Es un problema de documentación. No requiere cambio de diseño. Requiere actualización del README para reflejar el estado actual.

### 7. Por qué NO requiere ADR
Es actualización de documentación, no cambio de arquitectura. Cualquier persona que actualice el README resuelve el problema. No requiere aprobación de ADR ni cambio de diseño. Es una tarea de mantenimiento documental.

---

## TABLA DE CLASIFICACIÓN CONSOLIDADA

| # | Hallazgo | ESTADO | CLASIFICACIÓN | AFECTA RUNTIME | RELACIÓN ADR | DECISIÓN ARQ. REQUERIDA | REQUIERE ADR? |
|---|----------|--------|---------------|----------------|-------------|------------------------|---------------|
| 1 | Dos procesos | **CONTRADICTION** | **BUG** (reporte) | NO | Ninguna | Ninguna | **NO** — premisa falsa |
| 2 | Nice/Limit | **CONTRADICTION** | **DEPLOYMENT** | NO | ADR-0022, ADR-0024 | Ninguna | **NO** — problema operacional |
| 3 | Secrets | **PARTIALLY CONFIRMED** | **SECURITY** | PARCIALMENTE | Ninguna | Posiblemente | **POTENCIALMENTE SÍ** — si se implementa secret provider |
| 4 | OCM_ENV=development | **CONFIRMED** | **CONFIGURATION** | SÍ | ADR-0003, ADR-0012, ADR-0016 | Posiblemente | **POTENCIALMENTE SÍ** — si se separan credenciales |
| 5 | check_production_gates | **CONTRADICTION** | **CI-GOVERNANCE** | NO | ADR-0032, ADR-0034 | SÍ (si se quiere que bloquee) | **SÍ** — cambio de gobernanza CI |
| 6 | README DETENIDO | **CONTRADICTION** | **DOCUMENTATION** | NO | Ninguna | Ninguna | **NO** — mantenimiento documental |

---

## MATRIZ DE RIESGO

| # | Riesgo | NIVEL | MOTIVO |
|---|--------|-------|--------|
| 1 | Ninguno | NINGUNO | Premisa falsa, sistema funciona |
| 2 | Bajo | BAJO | Solo template, no activo |
| 3 | Credenciales reales | MEDIO | BYBIT keys reales en .env con OCM_ENV=development |
| 4 | Desarrollo en producción | MEDIO | OCM_ENV=development + credenciales reales |
| 5 | Falsa sensación de seguridad | BAJO | El check existe pero no bloquea merge |
| 6 | Documentación engañosa | BAJO | README stale, no afecta runtime |

---

## DECISIONES ARQUITECTÓNICAS PENDIENTES (SI SE DECIDE ACTUAR)

### D1: Secret Management
**Problema**: No existe secret provider. Los secretos están en `.env`.
**Decisión**: ¿Se implementa Vault/AWS Secrets? Si SÍ → ADR de seguridad de secretos. Si NO → decisión operacional.

### D2: Credenciales de desarrollo vs producción
**Problema**: `BYBIT_API_KEY` reales en `.env` con `OCM_ENV=development`.
**Decisión**: ¿Se separan credenciales en `.env.dev` y `.env.prod`? Si SÍ → cambio de configuración. Si NO → aceptar el riesgo.

### D3: Production gates como required check
**Problema**: `check_production_gates.py` existe pero no es required en CI.
**Decisión**: ¿Se agrega `semgrep` a las dependencias de `policy-gate`? Si SÍ → cambio de gobernanza CI + posible ADR. Si NO → el script permanece como verificación opcional.

### D4: README como documento vivo
**Problema**: README no refleja estado actual.
**Decisión**: Actualización manual sin ADR.

---

## REGISTRO

| Item | Valor |
|---|---|
| Archivo | `docs/audits/AUDIT_OCM_readonly-verification-6-actions_2026-09-22.md` |
| Archivos modificados | **NINGUNO** |
| Runtime modificado | **NO** |
| ADR modificado | **NINGUNO** |
| Código modificado | **NO** |
| Tests modificados | **NO** |
| Configuración modificada | **NO** |
| Systemd modificado | **NO** |
| Docker modificado | **NO** |
| Kafka modificado | **NO** |
| Iceberg modificado | **NO** |
| PR creado | **NINGUNO** |
| Commit | **NO** |
| Push | **NO** |
| Merge | **NO** |
| Rebase | **NO** |
| Limpieza | **NO** |
| Stash | **NO** |
| `git add` | **NO** |
| `git commit` | **NO** |
| `git push` | **NO** |
| `git merge` | **NO** |
| `git rebase` | **NO** |
| `git clean` | **NO** |
| `kill` / `pkill` | **NO** |
| `systemctl stop/start/restart` | **NO** |
| `docker compose` | **NO** |
| `.env` modificado | **NO** |
| `deploy/host.env` modificado | **NO** |
| `deploy/systemd/templates/` modificado | **NO** |
| `README.md` modificado | **NO** |
| `check_production_gates.py` modificado | **NO** |
| `scripts/` modificado | **NO** |
| `config/` modificado | **NO** |
| `packages/` modificado | **NO** |
| `apps/` modificado | **NO** |
| `shared/` modificado | **NO** |
| `infrastructure/` modificado | **NO** |

**FIN DEL INFORME DE CLASIFICACIÓN.**
