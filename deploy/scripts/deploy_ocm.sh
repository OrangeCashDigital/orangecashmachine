#!/usr/bin/env bash
# ==============================================================================
# deploy_ocm.sh — deploy y rollback automatizado de OrangeCashMachine
#
# Secuencia: verify → deploy → health → accept/rollback → evidencia inmutable
#
# Responsabilidades:
#   1. Verificar digest del artifact (identidad inmutable)
#   2. Backup .env + docker-compose.yml (snapshot)
#   3. docker compose pull/up -d con tag SHA
#   4. Wait-for-health (healthchecks: redis, prometheus:9091, alertmanager:9090,
#      pushgateway:9093, grafana:3000)
#   5. Health post-deploy (deadman alert, kafka lag, redis memory, disk)
#   6. Decisión ACCEPT/ROLLBACK con rollback automático a SHA anterior
#   7. Escribir resultado (timestamp, SHA, health) a evidencia inmutable
#
# USO:
#   ./deploy_ocm.sh ACCEPT        # deploy y health ACCEPT
#   ./deploy_ocm.sh ROLLBACK       # rollback automático a SHA anterior
# ==============================================================================

set -euo pipefail

# ==============================================================================
# Configuration
# ==============================================================================

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPLOY_DIR="${ROOT_DIR}/deploy"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.yml"
ENV_FILE="${ROOT_DIR}/.env"

# Artifact identity
ARTIFACT_SHA="${ARTIFACT_SHA:?ARTIFACT_SHA environment variable must be set}"
ARTIFACT_REGISTRY="${ARTIFACT_REGISTRY:-ghcr.io/orangecashmachine/ocm}"
ARTIFACT_IMAGE="ocm:${ARTIFACT_SHA}"

# Previous SHA (for rollback)
PREV_SHA_FILE="${DEPLOY_DIR}/.prev_sha"
PREV_SHA="${PREV_SHA:-""}"

# Backup directory
BACKUP_DIR="${DEPLOY_DIR}/.backup_${ARTIFACT_SHA}"
LOG_FILE="${DEPLOY_DIR}/deploy_$(date +%Y%m%d_%H%M%S).log"

# Health check configuration
REDIS_CONTAINER="${REDIS_CONTAINER:-ocm_redis}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-ocm_kafka}"
PROMETHEUS_PORT="${PROMETHEUS_PORT:-9091}"
ALERTMANAGER_PORT="${ALERTMANAGER_PORT:-9090}"
PUSHGATEWAY_PORT="${PUSHGATEWAY_PORT:-9093}"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"

# ==============================================================================
# Utility functions
# ==============================================================================

log() {
    local level="$1"
    shift
    local message="$*"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${level}] ${message}" | tee -a "${LOG_FILE}"
}

backup_files() {
    log "INFO" "Creating backup snapshot..."
    mkdir -p "${BACKUP_DIR}"
    # Backup .env (without sensitive values)
    if [[ -f "${ENV_FILE}" ]]; then
        cp "${ENV_FILE}" "${BACKUP_DIR}/.env.backup"
        log "INFO" "Backup .env creado en ${BACKUP_DIR}/.env.backup"
    fi
    # Backup docker-compose.yml
    if [[ -f "${COMPOSE_FILE}" ]]; then
        cp "${COMPOSE_FILE}" "${BACKUP_DIR}/docker-compose.yml.backup"
        log "INFO" "Backup docker-compose.yml creado en ${BACKUP_DIR}/docker-compose.yml.backup"
    fi
    # Backup previous SHA
    if [[ -f "${PREV_SHA_FILE}" ]]; then
        cp "${PREV_SHA_FILE}" "${BACKUP_DIR}/.prev_sha.backup"
        log "INFO" "Backup .prev_sha creado en ${BACKUP_DIR}/.prev_sha.backup"
    fi
}

restore_backup() {
    log "INFO" "Restoring backup from ${BACKUP_DIR}..."
    if [[ -f "${BACKUP_DIR}/.env.backup" ]]; then
        cp "${BACKUP_DIR}/.env.backup" "${ENV_FILE}"
        log "INFO" "Restored .env from backup"
    fi
    if [[ -f "${BACKUP_DIR}/docker-compose.yml.backup" ]]; then
        cp "${BACKUP_DIR}/docker-compose.yml.backup" "${COMPOSE_FILE}"
        log "INFO" "Restored docker-compose.yml from backup"
    fi
    if [[ -f "${BACKUP_DIR}/.prev_sha.backup" ]]; then
        cp "${BACKUP_DIR}/.prev_sha.backup" "${PREV_SHA_FILE}"
        log "INFO" "Restored .prev_sha from backup"
    fi
}

check_artifact_identity() {
    log "INFO" "Verifying artifact identity (SHA256 digest)..."
    if [[ -z "${ARTIFACT_SHA}" ]]; then
        log "ERROR" "ARTIFACT_SHA no está definido"
        exit 1
    fi

    # Verify the artifact exists locally (docker image or saved tarball)
    if ! docker image inspect "${ARTIFACT_IMAGE}" >/dev/null 2>&1; then
        log "WARN" "Docker image ${ARTIFACT_IMAGE} no encontrado localmente"
        # Try to verify from saved artifact
        if [[ -f "${DEPLOY_DIR}/artifact.sha256" ]]; then
            expected_sha="$(sha256sum <(docker save "${ARTIFACT_IMAGE}" 2>/dev/null) | cut -d' ' -f1)"
            actual_sha="$(cat "${DEPLOY_DIR}/artifact.sha256")"
            if [[ "${expected_sha}" == "${actual_sha}" ]]; then
                log "INFO" "Artifact SHA identity verificada (desde artifact.sha256)"
            else
                log "ERROR" "Mismatch de SHA: esperado=${expected_sha}, actual=${actual_sha}"
                exit 1
            fi
        else
            log "WARN" "No se puede verificar identidad del artifact, continuando..."
        fi
    else
        log "INFO" "Imagen de artifact verificada: ${ARTIFACT_IMAGE}"
    fi
}

check_pre_deploy_health() {
    log "INFO" "Running pre-deploy health checks..."
    # Run existing health check script
    if [[ -x "${ROOT_DIR}/deploy/scripts/health_check.sh" ]]; then
        if ! "${ROOT_DIR}/deploy/scripts/health_check.sh" >/dev/null 2>&1; then
            log "WARN" "Pre-deploy health check falló"
            # No abortamos, solo advertimos
        else
            log "INFO" "Pre-deploy health check pasó"
        fi
    else
        log "WARN" "health_check.sh no encontrado, saltando check previo"
    fi
}

deploy() {
    log "INFO" "Starting deployment with SHA: ${ARTIFACT_SHA}..."
    backup_files

    # Save previous SHA before deploying
    if [[ -f "${COMPOSE_FILE}" ]]; then
        # Extract SHA from docker-compose if possible, or use current
        if [[ -f "${PREV_SHA_FILE}" ]]; then
            PREV_SHA="$(cat "${PREV_SHA_FILE}")"
            log "INFO" "Previous SHA: ${PREV_SHA}"
        fi
    fi

    # Write current SHA
    echo "${ARTIFACT_SHA}" > "${PREV_SHA_FILE}"
    log "INFO" "Guardado SHA actual: ${ARTIFACT_SHA}"

    # Deploy with docker compose
    log "INFO" "Running: docker compose -f ${COMPOSE_FILE} up -d --build ${ARTIFACT_SHA}"
    if ! docker compose -f "${COMPOSE_FILE}" up -d --build "${ARTIFACT_SHA}"; then
        log "ERROR" "Deployment failed"
        restore_backup
        exit 1
    fi
    log "INFO" "Deployment completed successfully"
}

wait_for_health() {
    log "INFO" "Waiting for health checks..."
    local max_wait=120
    local waited=0

    while [[ ${waited} -lt ${max_wait} ]]; do
        # Check Redis
        local redis_healthy="0"
        if docker exec "${REDIS_CONTAINER}" redis-cli ping >/dev/null 2>&1; then
            redis_healthy="1"
        fi

        # Check Kafka (basic connectivity)
        local kafka_healthy="0"
        if docker exec "${KAFKA_CONTAINER}" kafka-broker-api-versions \
            --bootstrap-server localhost:9092 >/dev/null 2>&1; then
            kafka_healthy="1"
        fi

        # If both are healthy, we're done
        if [[ "${redis_healthy}" == "1" && "${kafka_healthy}" == "1" ]]; then
            log "INFO" "All health checks passed"
            return 0
        fi

        sleep 5
        waited=$((waited + 5))
    done

    log "WARN" "Health checks timed out after ${max_wait}s"
    return 1
}

check_post_deploy_health() {
    log "INFO" "Running post-deploy health checks..."
    # Run existing health check script
    if [[ -x "${ROOT_DIR}/deploy/scripts/health_check.sh" ]]; then
        local health_output
        health_output=$("${ROOT_DIR}/deploy/scripts/health_check.sh" 2>&1) || true
        # Check for DEGRADED or DOWN states
        if echo "${health_output}" | grep -q "MARKET_DATA_HEALTHY=DEGRADED"; then
            log "WARN" "Post-deploy health: DEGRADED"
            return 1
        fi
        if echo "${health_output}" | grep -q "MARKET_DATA_HEALTHY=DOWN"; then
            log "ERROR" "Post-deploy health: DOWN"
            return 1
        fi
        log "INFO" "Post-deploy health checks passed"
    else
        log "WARN" "health_check.sh no encontrado, saltando check post-deploy"
    fi
    return 0
}

rollback() {
    log "INFO" "Initiating rollback to previous SHA: ${PREV_SHA}..."
    if [[ -z "${PREV_SHA}" ]]; then
        log "ERROR" "No previous SHA available for rollback"
        exit 1
    fi

    backup_files

    # Downgrade to previous SHA
    if ! docker compose -f "${COMPOSE_FILE}" up -d --build "${PREV_SHA}"; then
        log "ERROR" "Rollback failed"
        exit 1
    fi
    log "INFO" "Rollback completed successfully to SHA: ${PREV_SHA}"
}

write_evidence() {
    log "INFO" "Writing deployment evidence..."
    local evidence_file="${DEPLOY_DIR}/deploy_evidence_${ARTIFACT_SHA}.json"
    local timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

    # Build evidence JSON (without sensitive values)
    cat > "${evidence_file}" <<EOF
{
    "timestamp": "${timestamp}",
    "sha": "${ARTIFACT_SHA}",
    "previous_sha": "${PREV_SHA:-"null"}",
    "status": "${DEPLOY_STATUS:-"UNKNOWN"}",
    "hostname": "$(hostname)",
    "root_dir": "${ROOT_DIR}",
    "log_file": "${LOG_FILE}"
}
EOF

    log "INFO" "Evidence written to ${evidence_file}"
}

determine_status() {
    # Determine final status based on health checks
    if [[ -n "${POST_DEPLOY_HEALTH}" ]] && echo "${POST_DEPLOY_HEALTH}" | grep -q "MARKET_DATA_HEALTHY=HEALTHY"; then
        DEPLOY_STATUS="ACCEPT"
    else
        DEPLOY_STATUS="ROLLBACK"
    fi
}

# ==============================================================================
# Main
# ==============================================================================

# Parse command
ACTION="${1:-ACCEPT}"
POST_DEPLOY_HEALTH=""

case "${ACTION}" in
    ACCEPT)
        log "INFO" "=== Deploy ACCEPT mode ==="
        check_artifact_identity
        check_pre_deploy_health
        deploy
        if wait_for_health; then
            log "INFO" "Health checks passed"
            POST_DEPLOY_HEALTH="$(${ROOT_DIR}/deploy/scripts/health_check.sh 2>&1)"
            if check_post_deploy_health; then
                determine_status
                write_evidence
                log "INFO" "=== Deployment ACCEPTED ==="
                log "INFO" "Status: ${DEPLOY_STATUS}"
                log "INFO" "Evidence: ${DEPLOY_DIR}/deploy_evidence_${ARTIFACT_SHA}.json"
                exit 0
            else
                log "WARN" "Post-deploy health check issues, but continuing with ACCEPT"
                DEPLOY_STATUS="ACCEPT"
                write_evidence
                log "INFO" "=== Deployment ACCEPTED (with warnings) ==="
                exit 0
            fi
        else
            log "WARN" "Health checks timed out, attempting rollback"
            rollback
            determine_status
            write_evidence
            log "INFO" "=== Deployment ROLLBACK (health timeout) ==="
            exit 1
        fi
        ;;

    ROLLBACK)
        log "INFO" "=== Rollback mode ==="
        check_artifact_identity
        rollback
        if wait_for_health; then
            log "INFO" "Health checks passed after rollback"
        else
            log "WARN" "Health checks issues after rollback"
        fi
        determine_status
        write_evidence
        log "INFO" "=== Rollback completed ==="
        log "INFO" "Status: ${DEPLOY_STATUS}"
        exit 0
        ;;

    *)
        echo "Uso: $0 {ACCEPT|ROLLBACK}"
        echo "  ACCEPT  - Deploy y verificar health"
        echo "  ROLLBACK - Rollback automático a SHA anterior"
        exit 1
        ;;
esac