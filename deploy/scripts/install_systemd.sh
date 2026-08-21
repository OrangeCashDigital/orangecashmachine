#!/usr/bin/env bash
# ==============================================================================
# install_systemd.sh — renderiza e instala unidades systemd de OCM
# ==============================================================================
# Uso:
#   ./deploy/scripts/install_systemd.sh --verify-only   # solo render + verify
#   ./deploy/scripts/install_systemd.sh                 # render + verify + install + reload
#
# Requiere: deploy/host.env (cp deploy/host.env.example deploy/host.env)
# ==============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEMPLATE_DIR="${REPO_ROOT}/deploy/systemd/templates"
HOST_ENV="${REPO_ROOT}/deploy/host.env"
RENDER_DIR="${REPO_ROOT}/deploy/systemd/rendered"
INSTALL_ONLY=false
[[ "${1:-}" == "--verify-only" ]] && INSTALL_ONLY=true

[[ -f "${HOST_ENV}" ]] || { echo "ERROR: falta ${HOST_ENV} (cp host.env.example host.env)"; exit 1; }

# Exportar variables del host para envsubst (lista explícita en render())
set -a
source "${HOST_ENV}"
set +a

mkdir -p "${RENDER_DIR}"

render() {
    local src="$1" dst="$2"
    # envsubst con lista explícita de variables — nunca exportar el entorno completo
    local vars
    vars=$(grep -oE '^[A-Z_]+=' "${HOST_ENV}" | sed 's/=$//;s/^/$/' | tr '\n' ' ')
    envsubst "${vars}" < "${src}" > "${dst}"
}

UNITS=()
for tpl in "${TEMPLATE_DIR}"/*.template; do
    name="$(basename "${tpl}" .template)"
    out="${RENDER_DIR}/${name}"
    render "${tpl}" "${out}"
    UNITS+=("${out}")
done

echo "[install-systemd] Verificando unidades renderizadas..."
for u in "${UNITS[@]}"; do
    systemd-analyze verify "${u}" || { echo "ERROR: verify falló para ${u}"; exit 1; }
done
echo "[install-systemd] verify OK (0 errores)"

if ${INSTALL_ONLY}; then
    echo "[install-systemd] --verify-only: no se instala nada."
    exit 0
fi

for u in "${UNITS[@]}"; do
    install -m 644 "${u}" "/etc/systemd/system/$(basename "${u}")"
    echo "[install-systemd] instalado: /etc/systemd/system/$(basename "${u}")"
done

systemctl daemon-reload
echo "[install-systemd] daemon-reload OK"
