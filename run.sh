#!/usr/bin/env bash
# run.sh — Unified dev entrypoint — OrangeCashMachine
#
# SSOT: los entrypoints aquí deben ser idénticos a [project.scripts]
#       en pyproject.toml. Si cambias uno, cambia el otro.
#
# Principios: SSOT · KISS · Fail-Fast · SafeOps
set -euo pipefail

cd "$(dirname "$0")"

MODE="${1:-}"

if [[ -z "$MODE" ]]; then
  echo "[run.sh] ERROR: Se requiere un modo de ejecución." >&2
  echo "  Uso: ./run.sh <ocm|live|paper|deploy> [args...]" >&2
  echo ""                                                     >&2
  echo "  ocm    → market data pipeline (Hydra)"             >&2
  echo "  live   → live trading ⚠️  capital real"            >&2
  echo "  paper  → paper trading"                             >&2
  echo "  deploy → desplegar flows a Prefect"                 >&2
  exit 1
fi

shift

case "$MODE" in
  # SSOT: app.cli.main:main — idéntico a pyproject.toml [project.scripts]
  ocm)    exec uv run python -m app.cli.main    "$@" ;;
  live)   exec uv run python -m app.cli.live    "$@" ;;
  paper)  exec uv run python -m app.cli.paper   "$@" ;;
  deploy) exec uv run python deploy.py          "$@" ;;
  *)
    echo "[run.sh] ERROR: Modo desconocido '${MODE}'." >&2
    echo "  Válidos: ocm | live | paper | deploy"      >&2
    exit 1
    ;;
esac
