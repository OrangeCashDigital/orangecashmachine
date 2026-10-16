# Setup de agentes: OpenCode + oh-my-openagent (OmO)

La política del proyecto vive en el repo. Los modelos y proveedores los
defines tú en tu config global.

## Qué vive dónde

- Global (no va en git): `~/.omo/omo.jsonc` y `~/.config/opencode/`
  (modelos, `agents`, `categories`, plugins, proveedores, telemetría).
- Repo (en git): `AGENTS.md`, `opencode.json` (permisos), `.omo/omo.jsonc`
  (políticas) y `.agents/skills/` (skills).

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
