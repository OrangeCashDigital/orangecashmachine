#!/usr/bin/env bash
# run.sh — Unified dev entrypoint — OrangeCashMachine
#
# SSOT: los entrypoints aquí deben ser idénticos a [project.scripts]
#       en pyproject.toml. Si cambias uno, cambia el otro.
#
# Modos disponibles:
#   ocm     → market data pipeline (Hydra)
#   live    → live trading ⚠️  capital real
#   paper   → paper trading
#
# Principios: SSOT · KISS · Fail-Fast · SafeOps
set -euo pipefail

cd "$(dirname "$0")"

MODE="${1:-}"

if [[ -z "$MODE" ]]; then
  echo "[run.sh] ERROR: Se requiere un modo de ejecución." >&2
  echo "  Uso: ./run.sh <ocm|live|paper> [args...]" >&2
  echo ""                                                    >&2
  echo "  ocm     → market data pipeline (Hydra)"           >&2
  echo "  live    → live trading ⚠️  capital real"          >&2
  echo "  paper   → paper trading"                           >&2
  exit 1
fi

shift

case "$MODE" in
  # SSOT: idéntico a [project.scripts] en pyproject.toml
  ocm)     exec uv run python -m app.cli.main        "$@" ;;
  live)    exec uv run python -m app.cli.live_hydra  "$@" ;;
  paper)   exec uv run python -m app.cli.paper_hydra "$@" ;;
  *)
    echo "[run.sh] ERROR: Modo desconocido '${MODE}'." >&2
    echo "  Válidos: ocm | live | paper"     >&2
    exit 1
    ;;
esac
