#!/usr/bin/env bash
# ==============================================================================
# OrangeCashMachine — Instalador Reproducible de Unidades Systemd
# ==============================================================================
# Instala las plantillas de systemd renderizadas con la configuración del host
# (/etc/ocm/host.env o deploy/host.env).
#
# Uso:
#   ./deploy/scripts/install_systemd.sh [--dry-run] [--start] [--verify-only]
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Utils ───────────────────────────────────────────────────────────────────
log()  { printf '[install-systemd] %s\n' "$*"; }
warn() { printf '[install-systemd] WARN: %s\n' "$*" >&2; }
die()  { printf '[install-systemd] ERROR: %s\n' "$*" >&2; exit 1; }

# ── Opciones CLI ────────────────────────────────────────────────────────────
DRY_RUN=0
START_SERVICES=0
VERIFY_ONLY=0

for arg in "$@"; do
  case "$arg" in
    --dry-run)     DRY_RUN=1 ;;
    --start)       START_SERVICES=1 ;;
    --verify-only) VERIFY_ONLY=1 ;;
    -h|--help)
      cat <<EOF
Uso: $0 [opciones]

Opciones:
  --dry-run      Muestra las unidades renderizadas sin escribirlas en /etc/systemd/system/
  --verify-only  Renderiza en directorio temporal y ejecuta systemd-analyze verify
  --start        Arranca ocm-market-data.target tras la instalación
EOF
      exit 0
      ;;
    *) die "Opción desconocida: $arg" ;;
  esac
done

# ── Carga de configuración del host ──────────────────────────────────────────
HOST_ENV="/etc/ocm/host.env"
if [ ! -f "$HOST_ENV" ]; then
  if [ -f "$REPO_ROOT/deploy/host.env" ]; then
    HOST_ENV="$REPO_ROOT/deploy/host.env"
    warn "Usando $HOST_ENV del repositorio. En producción se recomienda /etc/ocm/host.env"
  else
    die "No existe $HOST_ENV ni $REPO_ROOT/deploy/host.env. Copie deploy/host.env.example antes de continuar."
  fi
fi

log "Cargando configuración de host desde: $HOST_ENV"
# shellcheck source=/dev/null
source "$HOST_ENV"

# Exportar variables para que envsubst las reconozca correctamente
export OCM_USER OCM_REPO_ROOT OCM_VENV_PATH OCM_ENV OCM_KAFKA_BOOTSTRAP \
       OCM_REDIS_HOST OCM_REDIS_PORT OCM_MARKET_DATA_PORT OCM_PUSHGATEWAY_URL

# ── Validación de variables requeridas ──────────────────────────────────────
: "${OCM_USER:?OCM_USER es obligatoria en host.env}"
: "${OCM_REPO_ROOT:?OCM_REPO_ROOT es obligatoria en host.env}"
: "${OCM_VENV_PATH:?OCM_VENV_PATH es obligatoria en host.env}"
: "${OCM_ENV:?OCM_ENV es obligatoria en host.env}"
: "${OCM_KAFKA_BOOTSTRAP:?OCM_KAFKA_BOOTSTRAP es obligatoria en host.env}"
: "${OCM_REDIS_HOST:?OCM_REDIS_HOST es obligatoria en host.env}"
: "${OCM_REDIS_PORT:?OCM_REDIS_PORT es obligatoria en host.env}"
: "${OCM_MARKET_DATA_PORT:?OCM_MARKET_DATA_PORT es obligatoria en host.env}"
: "${OCM_PUSHGATEWAY_URL:?OCM_PUSHGATEWAY_URL es obligatoria en host.env}"

log "Configuración validada: USER=$OCM_USER REPO=$OCM_REPO_ROOT VENV=$OCM_VENV_PATH ENV=$OCM_ENV"

# ── Renderizado e Instalación ───────────────────────────────────────────────
DEST_DIR="/etc/systemd/system"
TEMP_DIR="$(mktemp -d /tmp/ocm_systemd_XXXXXX)"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

render_file() {
  local src="$1" dest="$2"
  # envsubst expandirá las variables exportadas
  envsubst '$OCM_USER $OCM_REPO_ROOT $OCM_VENV_PATH $OCM_ENV $OCM_KAFKA_BOOTSTRAP $OCM_REDIS_HOST $OCM_REDIS_PORT $OCM_MARKET_DATA_PORT $OCM_PUSHGATEWAY_URL' \
    < "$src" > "$dest"
}

log "Renderizando plantillas en $TEMP_DIR..."

for t in "$REPO_ROOT"/deploy/systemd/templates/*.template; do
  [ -f "$t" ] || continue
  filename="$(basename "$t" .template)"
  render_file "$t" "$TEMP_DIR/$filename"
done

for t in "$REPO_ROOT"/deploy/systemd/targets/*.template; do
  [ -f "$t" ] || continue
  filename="$(basename "$t" .template)"
  render_file "$t" "$TEMP_DIR/$filename"
done

# ── Verificación Sintáctica systemd-analyze ──────────────────────────────────
if command -v systemd-analyze >/dev/null 2>&1; then
  log "Ejecutando systemd-analyze verify sobre las unidades renderizadas..."
  SYSTEMD_LOG_LEVEL=warning systemd-analyze verify "$TEMP_DIR"/*.service "$TEMP_DIR"/*.timer "$TEMP_DIR"/*.target || warn "systemd-analyze detectó advertencias de configuración"
else
  log "systemd-analyze no disponible en el host. Omitiendo verificación sintáctica de systemd."
fi

if [ "$VERIFY_ONLY" -eq 1 ]; then
  log "Verificación completada. Unidades renderizadas en $TEMP_DIR:"
  ls -la "$TEMP_DIR"
  exit 0
fi

if [ "$DRY_RUN" -eq 1 ]; then
  log "Modo --dry-run activo. Muestra de unidades renderizadas:"
  for f in "$TEMP_DIR"/*; do
    printf '\n=== %s ===\n' "$(basename "$f")"
    cat "$f"
  done
  exit 0
fi

# ── Copia a /etc/systemd/system/ ────────────────────────────────────────────
SUDO=""
if [ "$(id -u)" -ne 0 ]; then
  SUDO="sudo"
  log "Se requerirá sudo para escribir en $DEST_DIR"
fi

log "Copiando unidades a $DEST_DIR..."
for f in "$TEMP_DIR"/*; do
  filename="$(basename "$f")"
  $SUDO cp "$f" "$DEST_DIR/$filename"
  $SUDO chmod 644 "$DEST_DIR/$filename"
  log "  ✓ Instalado $DEST_DIR/$filename"
fi

log "Ejecutando daemon-reload..."
$SUDO systemctl daemon-reload

if [ "$START_SERVICES" -eq 1 ]; then
  log "Activando y arrancando ocm-market-data.target..."
  $SUDO systemctl enable --now ocm-market-data.target
  $SUDO systemctl status ocm-market-data.target --no-pager || true
else
  log "Instalación completada exitosamente."
  log "Para activar los servicios ejecute: sudo systemctl enable --now ocm-market-data.target"
fi
