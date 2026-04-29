#!/usr/bin/env bash
# run.sh — Unified dev entrypoint — OrangeCashMachine
# Principios: SSOT · KISS · Fail-Fast · SafeOps
set -euo pipefail

cd "$(dirname "$0")"

MODE="${1:-}"

if [[ -z "$MODE" ]]; then
  echo "[run.sh] ERROR: Se requiere un modo de ejecución." >&2
  echo "  Uso: ./run.sh <ocm|live|paper|deploy> [args...]" >&2
  exit 1
fi

shift

case "$MODE" in
  ocm)    exec uv run python -m app.cli.market_data "$@" ;;
  live)   exec uv run python -m app.cli.live "$@" ;;
  paper)  exec uv run python -m app.cli.paper "$@" ;;
  deploy) exec uv run python deploy.py "$@" ;;
  *)
    echo "[run.sh] ERROR: Modo desconocido '${MODE}'." >&2
    echo "  Válidos: ocm | live | paper | deploy" >&2
    exit 1
    ;;
esac
