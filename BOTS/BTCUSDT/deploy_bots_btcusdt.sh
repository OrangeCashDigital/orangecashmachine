#!/usr/bin/env bash
# ========================================================
# OrangeCashMachine - Deploy BTCUSDT Bots
# Orquestador de bots BTCUSDT
# ========================================================

set -euo pipefail

# -------------------------
# Configuración
# -------------------------
NETWORK_NAME="OCM_BTCUSDT_net"
BOTS=("BTCUSDT_estrategia1" "BTCUSDT_estrategia2" "BTCUSDT_estrategia3")
MODE="${1:-production}"

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " OrangeCashMachine • BTCUSDT Deploy"
echo " Mode: $MODE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# -------------------------
# Verificar Docker
# -------------------------
if ! command -v docker &> /dev/null; then
    echo "❌ Docker no está instalado."
    exit 1
fi

# -------------------------
# Crear red si no existe
# -------------------------
echo "🔹 Verificando red Docker: $NETWORK_NAME"

if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
    echo "🔹 Creando red $NETWORK_NAME"
    docker network create "$NETWORK_NAME"
else
    echo "🔹 Red ya existente"
fi

# -------------------------
# Deploy bots
# -------------------------
for BOT in "${BOTS[@]}"; do

    BOT_PATH="$BASE_DIR/$BOT"

    echo ""
    echo "🚀 Desplegando: $BOT"

    if [[ ! -d "$BOT_PATH" ]]; then
        echo "⚠️  Carpeta no encontrada: $BOT_PATH"
        continue
    fi

    COMPOSE_FILE="$BOT_PATH/docker-compose.yml"

    if [[ ! -f "$COMPOSE_FILE" ]]; then
        echo "⚠️  docker-compose.yml no encontrado en $BOT"
        continue
    fi

    if [[ "$MODE" == "dry-run" ]]; then
        echo "⚡ Ejecutando DRY-RUN para $BOT"

        docker compose -f "$COMPOSE_FILE" run --rm freqtrade \
            trade \
            --config /freqtrade/user_data/config.json \
            --strategy "$BOT" \
            --dry-run

    else
        echo "⚡ Levantando contenedor $BOT"
        docker compose -f "$COMPOSE_FILE" up -d
    fi

done

# -------------------------
# Estado final
# -------------------------
echo ""
echo "📊 Estado de contenedores activos:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "✅ Deploy completado"