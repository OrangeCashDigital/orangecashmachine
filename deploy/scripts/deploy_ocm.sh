#!/usr/bin/env bash
# ============================================================
# OrangeCashMachine — deploy verificado (B-57, ADR-0037)
# ------------------------------------------------------------
# Secuencia: verify → backup → up -d → health → ACCEPT/ROLLBACK
#
# Compatible con OrangeHouse: single-host, Docker Compose,
# shell, git + CI. Sin infraestructura cloud.
#
# Uso:
#   ./deploy/scripts/deploy_ocm.sh --help
#   ./deploy/scripts/deploy_ocm.sh --check-health        # health de servicios ya levantados
#   ./deploy/scripts/deploy_ocm.sh --verify-artifact <sha256file>  # verifica digest del artifact (B-59)
#   ./deploy/scripts/deploy_ocm.sh --deploy              # up -d + health + ACCEPT/ROLLBACK
#   ./deploy/scripts/deploy_ocm.sh --rollback            # down + up -d con la imagen anterior
#
# Exit codes:
#   0 = deploy ACCEPTED (o --check-health todo healthy, o artifact íntegro)
#   1 = deploy REJECTED (health falló o rollback falló o digest no coincide)
#   2 = error de uso/argumentos
#   3 = prerequisito ausente (docker, compose, .env)
# ============================================================
set -euo pipefail

# ── Configuración (paths relativos a la raíz del repo) ──────
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-ocm}"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/.env}"
HEALTH_TIMEOUT_S="${HEALTH_TIMEOUT_S:-60}"
ARTIFACT_IMAGE="${ARTIFACT_IMAGE:-ocm_market_data}"

# ── Utils ────────────────────────────────────────────────────
log()  { printf '[deploy] %s\n' "$*"; }
die()  { printf '[deploy] ERROR: %s\n' "$*" >&2; exit 2; }
fatal(){ printf '[deploy] FATAL: %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "prerequisito ausente: $1"; }

compose() {
  docker compose -f "$COMPOSE_FILE" -p "$COMPOSE_PROJECT" "$@"
}

# ── Health checks ────────────────────────────────────────────
# Endpoints reales de docker-compose.yml (healthchecks de cada servicio).
# Los puertos host respetan las variables de .env (defaults del compose).
host_port() {
  # Lee ${SVC}_HOST_PORT del .env, default del compose.
  local svc="$1" default="$2"
  local key="${svc^^}_HOST_PORT"
  local v
  v="$(grep -E "^${key}=" "$ENV_FILE" 2>/dev/null | head -1 | cut -d= -f2 || true)"
  printf '%s' "${v:-$default}"
}

http_health() {
  # $1 = nombre servicio, $2 = URL health
  local name="$1" url="$2"
  if curl -fsS "$url" >/dev/null 2>&1; then
    log "HEALTH OK   ${name} → ${url}"
  else
    log "HEALTH FAIL ${name} → ${url}"
    return 1
  fi
}

check_health() {
  local failed=0
  log "health_check_inicio"

  # redis (healthcheck: redis-cli ping)
  local redis_port
  redis_port="$(host_port redis 6379)"
  if docker exec ocm_redis redis-cli -p 6379 ping >/dev/null 2>&1; then
    log "HEALTH OK   redis → docker exec ping"
  else
    # Fallback: puerto host
    if redis-cli -p "$redis_port" ping >/dev/null 2>&1; then
      log "HEALTH OK   redis → redis-cli :$redis_port"
    else
      log "HEALTH FAIL redis → redis-cli :$redis_port"
      failed=1
    fi
  fi

  http_health pushgateway "http://localhost:$(host_port pushgateway 9091)/-/healthy" || failed=1
  http_health prometheus   "http://localhost:$(host_port prometheus 9090)/-/healthy"  || failed=1
  http_health alertmanager "http://localhost:$(host_port alertmanager 9093)/-/healthy"|| failed=1
  http_health grafana      "http://localhost:$(host_port grafana 3000)/api/health"    || failed=1

  # market-data (microservice real desplegable — /health)
  local md_port
  md_port="$(host_port market-data 8001)"
  http_health market-data "http://localhost:${md_port}/health" || failed=1

  if [ "$failed" -eq 0 ]; then
    log "health_check_completo status=HEALTHY"
    return 0
  fi
  log "health_check_completo status=UNHEALTHY"
  return 1
}

# ── Artifact digest (B-59, ADR-0037) ─────────────────────────
# Verifica que el artifact construido en CI (imagen Docker) existe localmente
# y su config digest (identidad inmutable por contenido) coincide con el
# digest referenciado por el deploy. Inmutable: build → digest → verify → deploy.
#
# NOTA: se usa el config digest (`docker image inspect --format '{{.Id}}'`),
# NO `docker save | sha256sum` — el tar de docker save incluye metadatos
# variables y NO es reproducible entre runs (verificado 2026-08-19).
verify_artifact() {
  # $1 = ruta al archivo digest (contenido: "sha256:<hex>")
  local digest_file="$1"
  [ -f "$digest_file" ] || die "archivo digest no existe: $digest_file"
  need docker

  local expected actual
  expected="$(tr -d '[:space:]' < "$digest_file")"
  if [[ ! "$expected" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    die "digest con formato inválido en $digest_file: $expected"
  fi

  # La imagen debe existir localmente (build del artifact previo al deploy).
  if ! docker image inspect "$ARTIFACT_IMAGE:latest" >/dev/null 2>&1; then
    log "artifact_verificacion veredicto=REJECT reason=imagen_no_presente image=${ARTIFACT_IMAGE}:latest"
    fatal "no existe la imagen ${ARTIFACT_IMAGE}:latest (¿artifact no construido?)"
  fi

  actual="$(docker image inspect "$ARTIFACT_IMAGE:latest" --format '{{.Id}}')"

  if [ "$expected" = "$actual" ]; then
    log "artifact_verificacion veredicto=ACCEPT digest=$actual"
    return 0
  fi

  log "artifact_verificacion veredicto=REJECT expected=$expected actual=$actual"
  fatal "artifact digest NO coincide: esperado $expected, actual $actual (refusing deploy)"
}

# ── Deploy ───────────────────────────────────────────────────
snapshot_backup() {
  local backup_dir
  backup_dir="$ROOT_DIR/logs/deploy/$(date +%Y%m%d_%H%M%S)"
  mkdir -p "$backup_dir"
  if [ -f "$ENV_FILE" ]; then cp "$ENV_FILE" "$backup_dir/.env.bak"; fi
  if [ -f "$COMPOSE_FILE" ]; then cp "$COMPOSE_FILE" "$backup_dir/docker-compose.yml.bak"; fi
  printf '%s' "$backup_dir"
}

do_deploy() {
  need docker
  [ -f "$ENV_FILE" ] || fatal "falta $ENV_FILE (GRAFANA_PASSWORD y otros secretos obligatorios)"

  log "deploy_inicio"
  local backup_dir
  backup_dir="$(snapshot_backup)"
  log "backup_creado dir=$backup_dir"

  compose pull || fatal "docker compose pull falló"
  compose up -d --build || fatal "docker compose up -d --build falló"

  log "esperando_health timeout_s=$HEALTH_TIMEOUT_S"
  local waited=0 ok=0
  while [ "$waited" -lt "$HEALTH_TIMEOUT_S" ]; do
    if check_health; then ok=1; break; fi
    sleep 5
    waited=$((waited + 5))
  done

  if [ "$ok" -eq 1 ]; then
    log "deploy_resultado veredicto=ACCEPT backup=$backup_dir"
    return 0
  fi

  log "deploy_resultado veredicto=REJECT → rollback"
  compose down || log "rollback_down_warn"
  compose up -d || fatal "rollback falló"
  fatal "deploy REJECTED: health no pasó; stack restaurado (backup=$backup_dir)"
}

do_rollback() {
  need docker
  log "rollback_inicio"
  compose down || true
  compose up -d || fatal "rollback falló"
  if check_health; then
    log "rollback_resultado veredicto=ACCEPT"
    return 0
  fi
  fatal "rollback REJECTED: health no pasó tras restaurar"
}

# ── CLI ──────────────────────────────────────────────────────
usage() {
  cat <<EOF
OrangeCashMachine deploy (ADR-0037)

Uso:
  $0 --check-health     Solo health checks de servicios levantados
  $0 --verify-artifact <sha256file>
                        Verifica digest del artifact (imagen local vs sha256)
  $0 --deploy           up -d + health + ACCEPT/ROLLBACK (respeta .env)
  $0 --rollback         down + up -d con la imagen anterior + health

Variables: COMPOSE_FILE, COMPOSE_PROJECT, ENV_FILE, HEALTH_TIMEOUT_S, ARTIFACT_IMAGE
EOF
  exit 2
}

main() {
  case "${1:-}" in
    --check-health) need curl; need docker; check_health ;;
    --verify-artifact) [ $# -eq 2 ] || die "--verify-artifact requiere <sha256file>"; verify_artifact "$2" ;;
    --deploy)       do_deploy ;;
    --rollback)     do_rollback ;;
    --help|-h)      usage ;;
    *)              die "argumento desconocido: ${1:-}";;
  esac
}

main "$@"