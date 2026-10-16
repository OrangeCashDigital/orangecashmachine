# ==========================================================
# OrangeCashMachine – Market Data Service
# Multi-stage build: build tools NO llegan a producción
# ==========================================================

# Stage 1: builder — compila dependencias
FROM python:3.11-slim-bookworm AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .

RUN pip install "uv==0.11.14" && uv sync --no-dev --system

# Stage 2: runtime — imagen final limpia
FROM python:3.11-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY --from=builder /usr/local /usr/local
COPY . .

RUN useradd --create-home appuser
USER appuser

# Execution Contract: EP-006 market_data.main = MARKET DATA SERVICE ENTRYPOINT
# (config/execution_registry.yaml; autoridad ADR-0048). CMD = python -m market_data.main
# por contrato explícito, no por conveniencia. Dockerfile = deployment consumer, no SSOT.
CMD ["python", "-m", "market_data.main"]
