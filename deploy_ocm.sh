#!/usr/bin/env bash
set -euo pipefail

# =========================================================
# OrangeCashMachine • System Deploy
# =========================================================

MODE="${1:-production}"

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICES_DIR="$BASE_DIR/services"
BOTS_DIR="$BASE_DIR/BOTS"

cd "$BASE_DIR"

# ---------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------

timestamp() {
    date "+%Y-%m-%d %H:%M:%S"
}

log() {
    echo -e "\n[$(timestamp)] ▶ $1"
}

section() {
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " $1"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

section "OrangeCashMachine • SYSTEM DEPLOY"

echo "Mode : $MODE"
echo "Base : $BASE_DIR"

# ---------------------------------------------------------
# Verificaciones básicas
# ---------------------------------------------------------

log "Verificando dependencias del sistema"

if ! command -v docker >/dev/null 2>&1; then
    echo "❌ Docker no está instalado"
    exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
    echo "❌ Docker Compose no disponible"
    exit 1
fi

log "Docker OK"

# ---------------------------------------------------------
# Infraestructura / servicios auxiliares
# ---------------------------------------------------------

section "Infraestructura y servicios auxiliares"

if [ -d "$SERVICES_DIR" ]; then

    log "Iniciando servicios auxiliares"

    docker compose \
        -f "$SERVICES_DIR/docker-compose.yml" \
        up -d --build --remove-orphans

else

    echo "ℹ️ No hay servicios auxiliares definidos"

fi

# ---------------------------------------------------------
# Deploy de bots
# ---------------------------------------------------------

section "Deploy de Bots de Trading"

if [ -d "$BOTS_DIR" ]; then

    log "Ejecutando deploy de bots"

    "$BOTS_DIR/deploy_all_bots.sh" "$MODE"

else

    echo "⚠️ Directorio BOTS no encontrado"

fi

# ---------------------------------------------------------
# Estado final del sistema
# ---------------------------------------------------------

section "Estado actual de contenedores"

docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "✅ OrangeCashMachine deploy finalizado"