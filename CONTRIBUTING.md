# Contribuir a OrangeCashMachine

## Principio de seguridad y gobernanza

Los agentes de IA (OpenCode, Claude, etc.) tienen permisos de **solo
lectura** sobre git y GitHub en este repositorio. Toda mutación —
`git add`, `git commit`, `git push`, `gh pr create`, `gh pr merge` —
la ejecuta exclusivamente el humano.

Esto no es una limitación técnica: es una decisión de gobernanza. El
humano es el único autorizado a modificar el remoto.

### Cómo está implementado

1. **`opencode.json`** — `permission.bash` con 86 patrones:
   - `allow`: lectura pura (`git status`, `git diff`, `gh pr view`, …)
   - `ask`: mutaciones controladas (`git add`, `git commit`, `git push`,
     `gh pr create`)
   - `deny`: destructivos y evasión (`gh pr merge`, `git push --force`,
     `curl`, `export`, `bash -c`, etc.)
   - `permission.edit`/`write` **no soportados** por OpenCode 2.46

2. **`.githooks/pre-commit` y `.githooks/pre-push`** — bloquean cambios
   a `opencode.json`, `AGENTS.md`, `CONTRIBUTING.md`, `.githooks/*`,
   `.github/**` sin `GIT_HUMAN_OVERRIDE=1`.

3. **`AGENTS.md`** — reglas no negociables que el agente lee al arrancar.

4. **Branch protection de `main`** — 15 checks obligatorios + no force
   push + no delete.

### Setup obligatorio tras clonar

~~~bash
git config core.hooksPath .githooks
~~~

Sin esto, los hooks no se ejecutan y el agente podría commitear
`opencode.json` sin el override humano.

Ver `docs/agents/opencode-omo-setup.md` para la configuración completa.

### Flujo de contribución

1. Rama desde `main`: `git switch -c feat/mi-cambio`
2. Cambios + tests
3. Commit siguiendo conventional commits (`feat:`, `fix:`, `docs:`, `chore:`)
4. `git push -u origin feat/mi-cambio`
5. PR con `gh pr create`
6. Esperar 15 checks verdes
7. Merge **solo humano** (el agente tiene `gh pr merge` en `deny`)

Si el cambio toca archivos de gobernanza:

~~~bash
GIT_HUMAN_OVERRIDE=1 git commit -m "..."
~~~

**El agente NUNCA debe ejecutar `GIT_HUMAN_OVERRIDE=1`.** Está en `deny`.
