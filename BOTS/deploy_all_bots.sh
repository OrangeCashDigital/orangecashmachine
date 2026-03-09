#!/usr/bin/env bash
# ========================================================
# OrangeCashMachine - Deploy Maestro
# ========================================================

set -euo pipefail

PAIRS=("BTCUSDT" "XAUTUSDT")
MODE="${1:-production}"

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " OrangeCashMachine • Master Deploy"
echo " Mode: $MODE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# -------------------------
# Verificar Docker
# -------------------------
if ! command -v docker &>/dev/null; then
    echo "❌ Docker no está instalado"
    exit 1
fi

if ! docker info &>/dev/null; then
    echo "❌ Docker daemon no está corriendo"
    exit 1
fi

# -------------------------
# Deploy por par
# -------------------------
deploy_pair() {

    local PAIR=$1
    local PAIR_DIR="$BASE_DIR/$PAIR"
    local DEPLOY_SCRIPT="$PAIR_DIR/deploy_bots_${PAIR,,}.sh"

    echo ""
    echo "🚀 Procesando par: $PAIR"

    # validar carpeta
    if [[ ! -d "$PAIR_DIR" ]]; then
        echo "⚠️ Carpeta no encontrada: $PAIR_DIR"
        return
    fi

    # validar script
    if [[ ! -f "$DEPLOY_SCRIPT" ]]; then
        echo "⚠️ Script no encontrado: $DEPLOY_SCRIPT"
        return
    fi

    # asegurar permisos
    if [[ ! -x "$DEPLOY_SCRIPT" ]]; then
        echo "🔧 Corrigiendo permisos de ejecución"
        chmod +x "$DEPLOY_SCRIPT"
    fi

    # validar config freqtrade
    CONFIG_FILE=$(find "$PAIR_DIR" -name config.json | head -n 1)

    if [[ -z "$CONFIG_FILE" ]]; then
        echo "⚠️ config.json no encontrado para $PAIR"
        return
    fi

    if [[ ! -s "$CONFIG_FILE" ]]; then
        echo "⚠️ config.json está vacío: $CONFIG_FILE"
        return
    fi

    # ejecutar deploy
    if [[ "$MODE" == "dry-run" ]]; then
        echo "⚡ Ejecutando DRY-RUN"
        "$DEPLOY_SCRIPT" dry-run
    else
        echo "⚡ Ejecutando PRODUCCIÓN"
        "$DEPLOY_SCRIPT" production
    fi
}

# -------------------------
# Loop de pares
# -------------------------
for PAIR in "${PAIRS[@]}"; do
    deploy_pair "$PAIR"
done

# -------------------------
# Estado final
# -------------------------
echo ""
echo "📊 Contenedores activos:"
docker ps --filter "name=OCM_" \
--format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "✅ Deploy completado"