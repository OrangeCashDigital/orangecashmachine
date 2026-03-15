# ==========================================================
# OrangeCashMachine – Market Data Service
# Docker Image
#
# Principios aplicados:
# - KISS: imagen simple y reproducible
# - DRY: minimizar capas innecesarias
# - SafeOps: build determinístico y logs no bufferizados
# - Security: ejecución como usuario no root
# - Performance: uso eficiente de cache Docker
# ==========================================================

# ----------------------------------------------------------
# Base Image
# ----------------------------------------------------------
# Python 3.11 es actualmente la versión más estable
# para stacks de datos (pandas, pyarrow, etc.)

FROM python:3.11-slim-bookworm

# ----------------------------------------------------------
# Environment configuration
# ----------------------------------------------------------

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# ----------------------------------------------------------
# System dependencies
# ----------------------------------------------------------
# build-essential se mantiene porque algunas dependencias
# pueden requerir compilación (ej. pyarrow)

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        git \
    && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------------
# Working directory
# ----------------------------------------------------------

WORKDIR /app

# ----------------------------------------------------------
# Python dependencies
# ----------------------------------------------------------
# Copiar requirements primero para aprovechar cache Docker

COPY requirements.txt .

RUN pip install --upgrade pip setuptools wheel \
    && pip install -r requirements.txt

# ----------------------------------------------------------
# Application source
# ----------------------------------------------------------

COPY . .

# ----------------------------------------------------------
# Security: run as non-root user
# ----------------------------------------------------------

RUN useradd --create-home appuser
USER appuser

# ----------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------
# Inicia el pipeline de orquestación del sistema

CMD ["python", "-m", "market_data.orchestration.entrypoint"]