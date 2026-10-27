# Setup de agentes: OpenCode + oh-my-openagent (OmO)

La política del proyecto vive en el repo. Los modelos y proveedores los
defines tú en tu config global.

## Qué vive dónde

- Global (no va en git): `~/.omo/omo.jsonc` y `~/.config/opencode/`
  (modelos, `agents`, `categories`, plugins, proveedores, telemetría).
- Repo (en git): `AGENTS.md`, `opencode.json` (permisos), `.omo/omo.jsonc`
  (políticas) y `.opencode/skills/` (skills).

Los modelos no se versionan: dependen de la cuenta de cada persona.

## Instalación

Requisitos: Bun, OpenCode y `uv`. Instala OmO (ajusta los proveedores):

~~~bash
bunx oh-my-openagent install --no-tui --platform=opencode \
  --claude=no --openai=no --gemini=no --copilot=no \
  --opencode-zen=yes --skip-auth
~~~

Revisa `~/.omo/omo.jsonc` y valida que cada modelo exista:

~~~bash
opencode models > /tmp/m.txt 2>/dev/null
sed 1d ~/.omo/omo.jsonc | jq -r '..|.model? // empty' | sort -u | \
  while read m; do grep -qx "$m" /tmp/m.txt && echo "OK    $m" || echo "FALTA $m"; done
~~~

Cambia todo `FALTA` por un modelo de `opencode models`. Los modelos free
cambian con el tiempo: repite la validación de vez en cuando.

## Qué trae ya el repo

- Los agentes solo usan git en lectura (`status`, `diff`, `log`, `show`,
  `blame`, `ls-files`, `rev-parse`, `check-ignore`). Los commits los haces tú.
- Los agentes no leen `.env` ni `.env.*` (sí `.env.example`), ni por la
  herramienta de lectura ni por bash.
- Antes de modificar código en `packages/`, `apps/`, `shared/`, `config/` o
  `policies/` se carga la skill `ocm-context-bootstrap` (ver `AGENTS.md`).

## Verificación

~~~bash
bunx oh-my-openagent doctor
~~~

El aviso de modelos "unknown" es informativo. Dentro de `opencode`, en la
raíz del repo, prueba: listar skills (debe ver `ocm-context-bootstrap` y no
`git-master`), `git status` (permitido), `git add -n .` y `cat .env`
(rechazados).

## Telemetría y archivos a no commitear

OmO envía telemetría anónima; se desactiva con
`OMO_SEND_ANONYMOUS_TELEMETRY=0`. No commitees `.omo/run-continuation/`,
`.omo/*.bak.*` (ya en `.gitignore`) ni backups de tu config global, porque
pueden contener credenciales.

## Agente por defecto

Con OmO, `plan` y `build` pasan a ser subagentes. Si
`default_agent` en `~/.config/opencode/opencode.json` apunta a
uno de ellos, `opencode run` falla con `UnknownError`
(log: default agent is a subagent). Usa un agente principal:
`"default_agent": "sisyphus"`.

Comprueba la lista con `opencode agent list` y los permisos
resueltos con `opencode debug agent sisyphus`. En esas reglas,
la última que coincide gana: `*.env*` debe ir al final.

---

## Configuración obligatoria tras clonar (git hooks)

> **Nota**: la sección "Qué trae ya el repo" arriba está desactualizada
> desde PR #81. Ver "Política de permisos" abajo para el estado actual.

El repo tiene `.githooks/pre-commit` y `.githooks/pre-push` que bloquean
cambios a archivos de gobernanza (`opencode.json`, `AGENTS.md`,
`CONTRIBUTING.md`, `.githooks/*`, `.github/**`) sin autorización humana
explícita.

**Después de clonar, activa los hooks**:

~~~bash
git config core.hooksPath .githooks
~~~

Verifica:

~~~bash
git config --get core.hooksPath
# Debe devolver: .githooks
~~~

Sin esto, git usa `.git/hooks/` (vacío por default) y los guardarraíles
NO se ejecutan. Cualquier agente podría commitear `opencode.json` sin el
override humano.

**Para autorizar cambios a archivos de gobernanza (solo humano)**:

~~~bash
GIT_HUMAN_OVERRIDE=1 git commit -m "..."
GIT_HUMAN_OVERRIDE=1 git push
~~~

**El agente NUNCA debe usar `GIT_HUMAN_OVERRIDE=1`.** Está en `deny` de
`opencode.json`.

## Política de permisos del agente (actualizada)

`opencode.json` define permisos por herramienta:

- `permission.bash` (131 patrones): allow / ask / deny.
  - **allow**: lectura pura (`git status|diff|log|show|blame|ls-files|
    rev-parse|check-ignore`, `git fetch`, `git ls-remote`,
    `gh pr view|checks`, `gh run view|list`, `ls`, `cat`, `grep`, etc.)
  - **allow (git de trabajo)**: `git add`, `git commit`, `git switch`,
    `git pull --ff-only`, sin confirmación. Siguen en deny `git add -A/
    --all/./-f/--force` y `git switch -f/--force/--discard-changes`, con
    la opción en cualquier posición.
  - **allow (prelude)**: `export CI=*` — permite el prelude exacto
    que OpenCode añade a cada comando bash. NO abre `export*`
    genérico (que permitiría `export AWS_SECRET_ACCESS_KEY=...`).
    Antes de #99 este deny bloqueaba bash y el modelo fabricaba
    output en `part.type=text`.
  - **ask** (confirmación humana por cada uno): `git push origin`,
    `gh pr create`.
  - **deny** (permanente): `gh pr merge`, `gh api`, `git push --force/-f/
    --delete`, `git reset`, `git rebase`, `git merge`, `git stash`,
    `git clean`, `git branch -D/-d`, `git commit --amend/-n/--no-verify`,
    y toda evasión (`export`, `env`, `curl`, `ssh`, `scp`, `bash -c`,
    `sh -c`, `python -c`, `eval`, `exec`, `source`, `xargs`, `nohup`,
    `nice`, `time`, `script`, `setsid`, `wget`, `nc`, `rsync`).
- `permission.read`: `.env*` en deny (excepto `.env.example`).
- `permission.edit` y `permission.write`: **NO SOPORTADOS** por OpenCode
  2.46+. El agente PUEDE editar archivos con Edit/Write. La mitigación
  real son los git hooks: aunque edite `opencode.json`, el commit está
  bloqueado.

## Verificación empírica (validado en PR #81)

Dentro de `opencode`, en la raíz del repo, ejecutar uno por uno y
verificar el resultado:

| Comando | Resultado esperado |
|---|---|
| `git status` | PERMITIDO |
| `git log --oneline -3` | PERMITIDO |
| `git push --force origin main` | DENEGADO por bash |
| `gh pr merge 79` | DENEGADO por bash |
| `export CI=true; git status` | PERMITIDO desde #99 (matchea `export CI=*` en allow) |
| `curl https://api.github.com` | DENEGADO |
| `git reset --hard HEAD` | DENEGADO |
| Edit `opencode.json` + `git add` + `git commit` | BLOQUEADO por hook |

## Reglas Git para agentes (R-GIT)

Cualquier agente que opere sobre este repo debe conocer las reglas R-GIT
definidas en `AGENTS.md` § "Reglas de operación con Git". Resumen:

- **R-GIT-1..8**: un tema por PR, rama desde main limpio, merge human-only,
  verificación antes de commit, política de push.
- **R-GIT-9**: prohibido `gh pr close` sin merge y prohibido cherry-pick.
  Si un PR está BLOCKED, se corrige en la rama; solo el humano cierra.
- **R-GIT-10**: si `--admin` falla, verificar `enforce_admins` en branch
  protection. Con `enforce_admins=true`, no hay atajo: los checks de CI
  deben pasar.

Ver `AGENTS.md` para texto completo y justificaciones.
