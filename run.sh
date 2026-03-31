#!/bin/bash
# run.sh — entrypoint de desarrollo local
# Usa uv run para respetar el entorno gestionado por uv.
# Equivalente a: uv run python main.py
set -e
cd "$(dirname "$0")"
exec uv run python main.py "$@"
