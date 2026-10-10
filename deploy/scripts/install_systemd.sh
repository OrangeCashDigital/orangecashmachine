#!/usr/bin/env bash
# ==============================================================================
# install_systemd.sh — renderiza, valida e instala unidades systemd de OCM
# ==============================================================================
# Uso:
#   ./deploy/scripts/install_systemd.sh --verify-only   # solo render + verify
#   ./deploy/scripts/install_systemd.sh                # system: install + daemon-reload
#   ./deploy/scripts/install_systemd.sh --user         # user unit (~/.config/systemd/user)
# ==============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEMPLATE_DIR="${REPO_ROOT}/deploy/systemd/templates"
HOST_ENV_EXAMPLE="${REPO_ROOT}/deploy/host.env.example"
HOST_ENV="${REPO_ROOT}/deploy/host.env"
RENDER_DIR="${REPO_ROOT}/deploy/systemd/rendered"

INSTALL_ONLY=false
USER_MODE=false

for arg in "${@:-}"; do
    case "${arg}" in
        --verify-only) INSTALL_ONLY=true ;;
        --user) USER_MODE=true ;;
    esac
done

DEST_DIR="/etc/systemd/system"
if ${USER_MODE}; then
    DEST_DIR="${HOME}/.config/systemd/user"
fi

# ------------------------------------------------------------------------------
# 1. BOOTSTRAP AUTOMÁTICO DE ARCHIVOS DE ENTORNO
# ------------------------------------------------------------------------------
if [[ ! -f "${HOST_ENV}" ]]; then
    if [[ -f "${HOST_ENV_EXAMPLE}" ]]; then
        echo "[install-systemd] host.env no encontrado. Creando desde host.env.example..."
        cp "${HOST_ENV_EXAMPLE}" "${HOST_ENV}"
    else
        echo "[install-systemd] ERROR: No existe ni ${HOST_ENV} ni ${HOST_ENV_EXAMPLE}"
        exit 1
    fi
fi

# Fallbacks de variables de entorno de infraestructura
export OCM_REPO_ROOT="${OCM_REPO_ROOT:-${REPO_ROOT}}"
export OCM_HOST_USER="${OCM_HOST_USER:-$(whoami)}"

# Cargar variables del entorno
set -a
# shellcheck source=deploy/host.env
# shellcheck disable=SC1091
source "${HOST_ENV}"
set +a

mkdir -p "${RENDER_DIR}"

# ------------------------------------------------------------------------------
# 2. RENDERIZADO DE PLANTILLAS
# ------------------------------------------------------------------------------
render() {
    local src="$1" dst="$2"
    
    # Extraer variables del host.env MÁS las variables core (OCM_REPO_ROOT, OCM_HOST_USER)
    local vars
    vars="$(grep -oE '^[A-Z_]+=' "${HOST_ENV}" | sed 's/=$//;s/^/$/' | tr '\n' ' ') \$OCM_REPO_ROOT \$OCM_HOST_USER"
    
    envsubst "${vars}" < "${src}" > "${dst}"
}

UNITS=()
for tpl in "${TEMPLATE_DIR}"/*.template; do
    [[ -e "${tpl}" ]] || continue
    name="$(basename "${tpl}" .template)"
    out="${RENDER_DIR}/${name}"
    
    render "${tpl}" "${out}"
    
    if ${USER_MODE}; then
        # User= y multi-user.target no aplican al user manager de systemd
        sed -i '/^User=/d; s/^WantedBy=multi-user.target/WantedBy=default.target/' "${out}"
    fi
    UNITS+=("${out}")
done

# ------------------------------------------------------------------------------
# 3. VERIFICACIÓN DE SINTAXIS SYSTEMD
# ------------------------------------------------------------------------------
echo "[install-systemd] Verificando unidades renderizadas..."
for u in "${UNITS[@]}"; do
    systemd-analyze verify "${u}" || { echo "ERROR: verify falló para ${u}"; exit 1; }
done
echo "[install-systemd] verify OK (0 errores)"

if ${INSTALL_ONLY}; then
    echo "[install-systemd] --verify-only: Renderizado completado sin instalar nada."
    exit 0
fi

# ------------------------------------------------------------------------------
# 4. INSTALACIÓN Y ACTIVACIÓN
# ------------------------------------------------------------------------------
mkdir -p "${DEST_DIR}"

for u in "${UNITS[@]}"; do
    unit_name="$(basename "${u}")"
    install -m 644 "${u}" "${DEST_DIR}/${unit_name}"
    echo "[install-systemd] Instalado: ${DEST_DIR}/${unit_name}"
done

if ${USER_MODE}; then
    systemctl --user daemon-reload
    echo "[install-systemd] user daemon-reload OK"
else
    systemctl daemon-reload
    echo "[install-systemd] daemon-reload OK"
    
    # Activar y reiniciar los servicios para aplicar los cambios
    for u in "${UNITS[@]}"; do
        unit_name="$(basename "${u}")"
        echo "[install-systemd] Habilitando y reiniciando ${unit_name}..."
        systemctl enable "${unit_name}" --now || true
    done
fi

echo "[install-systemd] PROCESO COMPLETADO CON ÉXITO."