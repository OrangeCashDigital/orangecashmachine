#!/usr/bin/env bash
# =============================================================================
# deploy_ocm.sh — verify → deploy → health → ACCEPT/ROLLBACK → evidencia
# Runtime canónico: systemd (ADR-0039). Docker/Compose = infraestructura, no se toca.
# Secuencia de ADR-0037 adaptada: identidad = commit SHA + sha256(uv.lock).
#
# USO:
#   ./deploy_ocm.sh ACCEPT <sha>     (o ARTIFACT_SHA=<sha> ./deploy_ocm.sh ACCEPT)
#   ./deploy_ocm.sh ROLLBACK         (vuelve al SHA previo registrado)
#
# Opcionales: OCM_DRY_RUN=1 | OCM_SYSTEMD_SCOPE=system|user | OCM_UNITS="..."
#   OCM_STATE_DIR (def. ~/.local/state/ocm/deploy, fuera del repo)
#   OCM_HEALTH_TIMEOUT_S (300) | OCM_ACCEPT_DEGRADED=1 | OCM_REQUIRE_OBS=1
#   OCM_EXPECTED_LOCK_SHA256 | OCM_REQUIRE_SIGNED=1 | UV_SYNC_ARGS (--frozen)
# Exit: 0 aceptado | 1 falló y se revirtió | 2 falló sin poder revertir | 3 precondición
# =============================================================================
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HEALTH_SCRIPT="${REPO_DIR}/deploy/scripts/health_check.sh"
STATE_DIR="${OCM_STATE_DIR:-${XDG_STATE_HOME:-${HOME}/.local/state}/ocm/deploy}"
SCOPE="${OCM_SYSTEMD_SCOPE:-system}"
UNITS="${OCM_UNITS:-ocm-streaming.service ocm-market-data.service}"
HEALTH_TIMEOUT_S="${OCM_HEALTH_TIMEOUT_S:-300}"
UV_SYNC_ARGS="${UV_SYNC_ARGS:---frozen}"

ACTION=""; LOG_FILE=/dev/null; TS_FILE=""; TARGET_FULL=""; TARGET_LOCK=""; PREV_SHA=""
LAST_STATE="UNKNOWN"; LAST_DETAIL=""; OBS_BAD=0
SCQ=(); SCX=(); UNITS_TO_RESTART=()

log() { printf '[%s] [%s] %s\n' "$(date '+%F %T')" "$1" "${*:2}" | tee -a "${LOG_FILE}" >&2; }
die() { log ERROR "$*"; exit 3; }
g()   { git -C "${REPO_DIR}" "$@"; }

verify_identity() {
    local ref="$1"
    log INFO "Verificando identidad de ${ref}..."
    TARGET_FULL="$(g rev-parse --verify --quiet "${ref}^{commit}")" \
        || { log ERROR "commit inexistente: ${ref}"; return 1; }
    g cat-file -e "${TARGET_FULL}:uv.lock" 2>/dev/null \
        || { log ERROR "uv.lock ausente en ${TARGET_FULL}"; return 1; }
    TARGET_LOCK="$(g show "${TARGET_FULL}:uv.lock" | sha256sum | cut -d' ' -f1)"
    if [[ -n "${OCM_EXPECTED_LOCK_SHA256:-}" && "${TARGET_LOCK}" != "${OCM_EXPECTED_LOCK_SHA256}" ]]; then
        log ERROR "uv.lock no coincide: esperado=${OCM_EXPECTED_LOCK_SHA256} actual=${TARGET_LOCK}"
        return 1
    fi
    if [[ "${OCM_REQUIRE_SIGNED:-0}" == 1 ]]; then
        g verify-commit "${TARGET_FULL}" >/dev/null 2>&1 \
            || { log ERROR "commit sin firma válida"; return 1; }
    fi
    log INFO "OK sha=${TARGET_FULL} uv.lock=${TARGET_LOCK:0:16}..."
}

require_clean_tree() {
    if ! g diff --quiet || ! g diff --cached --quiet; then
        die "árbol con cambios sin commitear en ${REPO_DIR}; no se despliega sobre un árbol sucio"
    fi
}

select_units() {
    local u
    UNITS_TO_RESTART=()
    for u in ${UNITS}; do   # solo las que ya están activas/habilitadas (no arranca market-data antes de P2)
        if "${SCQ[@]}" is-active --quiet "${u}" || "${SCQ[@]}" is-enabled --quiet "${u}"; then
            UNITS_TO_RESTART+=("${u}")
        fi
    done
    (( ${#UNITS_TO_RESTART[@]} > 0 )) \
        || die "ninguna unit activa/habilitada (${UNITS}); ¿corriste install_systemd.sh --enable?"
}

apply_sha() {   # explícito con || return: bajo 'if' errexit no aplica
    log INFO "Aplicando $1 ..."
    g checkout --detach --quiet "$1" || return 1
    # shellcheck disable=SC2086
    ( cd "${REPO_DIR}" && uv sync ${UV_SYNC_ARGS} ) || return 1
    "${SCX[@]}" restart "${UNITS_TO_RESTART[@]}" || return 1
}

wait_healthy() {
    local deadline=$(( SECONDS + HEALTH_TIMEOUT_S )) out rc
    LAST_STATE="UNKNOWN"; LAST_DETAIL=""
    while :; do
        rc=0; out="$("${HEALTH_SCRIPT}" 2>&1)" || rc=$?
        LAST_STATE="$(sed -n 's/^MARKET_DATA_HEALTHY=//p' <<<"${out}" | head -1)"
        LAST_STATE="${LAST_STATE:-UNKNOWN}"
        LAST_DETAIL="$(grep -m1 '^# detail:' <<<"${out}" || true)"
        case "${LAST_STATE}" in
            HEALTHY) return 0 ;;
            DEGRADED) if [[ "${OCM_ACCEPT_DEGRADED:-0}" == 1 ]]; then return 0; fi ;;
        esac
        if (( SECONDS >= deadline )); then break; fi
        log INFO "salud=${LAST_STATE} (rc=${rc}); reintento en 10s..."
        sleep 10
    done
    log WARN "salud=${LAST_STATE} tras ${HEALTH_TIMEOUT_S}s"
    return 1
}

check_observability() {   # ADR-0037 §4; solo bloquea con OCM_REQUIRE_OBS=1
    local probe name port path
    OBS_BAD=0
    for probe in \
        "prometheus ${PROMETHEUS_HOST_PORT:-9090} /-/healthy" \
        "pushgateway ${PUSHGATEWAY_HOST_PORT:-9091} /-/healthy" \
        "alertmanager ${ALERTMANAGER_HOST_PORT:-9094} /-/healthy" \
        "grafana ${GRAFANA_HOST_PORT:-3000} /api/health"; do
        read -r name port path <<<"${probe}"
        if curl -fsS -m 3 "http://localhost:${port}${path}" >/dev/null 2>&1; then
            log INFO "obs ${name}: OK"
        else
            log WARN "obs ${name}: sin respuesta (:${port})"; OBS_BAD=$(( OBS_BAD + 1 ))
        fi
    done
    [[ "${OCM_REQUIRE_OBS:-0}" != 1 || ${OBS_BAD} -eq 0 ]]
}

write_evidence() {
    local status="$1" detail file tmp
    detail="${LAST_DETAIL//\"/}"; detail="${detail//\\/}"
    file="${STATE_DIR}/evidence/deploy_${TS_FILE}_${TARGET_FULL:0:12}.json"
    tmp="$(mktemp "${STATE_DIR}/evidence/.tmp.XXXXXX")"
    printf '{\n  "timestamp": "%s",\n  "action": "%s",\n  "status": "%s",\n  "target_sha": "%s",\n  "previous_sha": "%s",\n  "uv_lock_sha256": "%s",\n  "health": "%s",\n  "health_detail": "%s",\n  "observability_failures": %s,\n  "hostname": "%s",\n  "log_file": "%s"\n}\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${ACTION}" "${status}" "${TARGET_FULL}" "${PREV_SHA}" \
        "${TARGET_LOCK}" "${LAST_STATE}" "${detail}" "${OBS_BAD}" "$(hostname)" "${LOG_FILE}" >"${tmp}"
    chmod 0444 "${tmp}"; mv -f "${tmp}" "${file}"
    log INFO "Evidencia: ${file}"
}

set_state() { printf '%s\n' "$1" >"${STATE_DIR}/current_sha"; printf '%s\n' "$2" >"${STATE_DIR}/prev_sha"; }

do_rollback() { log WARN "Revirtiendo a $1 ..."; apply_sha "$1" && wait_healthy; }

# Todo el flujo vive en main() y el script termina con 'exit': bash ya leyó el
# cuerpo completo, así que 'git checkout' de otro SHA no corrompe esta ejecución.
main() {
    local ref="${2:-${ARTIFACT_SHA:-}}" applied=1 c
    ACTION="${1:-}"
    mkdir -p "${STATE_DIR}/evidence"; chmod 700 "${STATE_DIR}"
    TS_FILE="$(date -u +%Y%m%dT%H%M%SZ)"
    LOG_FILE="${STATE_DIR}/deploy_${TS_FILE}.log"
    exec 9>"${STATE_DIR}/.lock"
    flock -n 9 || die "otro deploy en curso (${STATE_DIR}/.lock)"

    for c in git uv flock curl sed grep sha256sum; do
        command -v "${c}" >/dev/null || die "falta el comando: ${c}"
    done
    [[ -x "${HEALTH_SCRIPT}" ]] || die "no ejecutable: ${HEALTH_SCRIPT}"

    if [[ "${SCOPE}" == user ]]; then SCQ=(systemctl --user); SCX=(systemctl --user)
    elif [[ ${EUID} -eq 0 ]]; then SCQ=(systemctl); SCX=(systemctl)
    else SCQ=(systemctl); SCX=(sudo -n systemctl); fi

    PREV_SHA="$(g rev-parse HEAD)"
    case "${ACTION}" in
        ACCEPT)   [[ -n "${ref}" ]] || die "falta SHA: ./deploy_ocm.sh ACCEPT <sha>" ;;
        ROLLBACK) [[ -s "${STATE_DIR}/prev_sha" ]] || die "sin SHA previo en ${STATE_DIR}/prev_sha"
                  ref="$(<"${STATE_DIR}/prev_sha")" ;;
        *) echo "Uso: $0 {ACCEPT <sha>|ROLLBACK}" >&2; exit 3 ;;
    esac

    verify_identity "${ref}" || die "verificación de identidad falló; no se toca nada"
    require_clean_tree
    select_units
    [[ "${TARGET_FULL}" != "${PREV_SHA}" ]] || applied=0

    if [[ "${OCM_DRY_RUN:-0}" == 1 ]]; then
        log INFO "DRY-RUN ${ACTION}: ${PREV_SHA} -> ${TARGET_FULL}; units=${UNITS_TO_RESTART[*]}; sin cambios"
        return 0
    fi

    if [[ "${ACTION}" == ROLLBACK ]]; then
        if { (( applied == 0 )) || apply_sha "${TARGET_FULL}"; } && wait_healthy; then
            (( applied == 0 )) || set_state "${TARGET_FULL}" "${PREV_SHA}"
            write_evidence ROLLBACK_MANUAL_OK; return 0
        fi
        write_evidence ROLLBACK_MANUAL_FAILED; return 2
    fi

    if { (( applied == 0 )) || apply_sha "${TARGET_FULL}"; } && wait_healthy && check_observability; then
        (( applied == 0 )) || set_state "${TARGET_FULL}" "${PREV_SHA}"
        write_evidence ACCEPT
        log INFO "=== ACCEPT ${TARGET_FULL} ==="
        return 0
    fi

    log ERROR "Deploy no saludable (salud=${LAST_STATE})"
    if (( applied == 0 )); then write_evidence UNHEALTHY_NO_CHANGE; return 1; fi
    if do_rollback "${PREV_SHA}"; then
        write_evidence ROLLED_BACK; log WARN "=== ROLLBACK a ${PREV_SHA} OK ==="; return 1
    fi
    write_evidence ROLLBACK_FAILED
    log ERROR "=== ROLLBACK FALLÓ: intervención manual requerida ==="
    return 2
}

main "$@"; exit $?
