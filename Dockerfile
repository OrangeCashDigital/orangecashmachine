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

COPY requirements.txt .

RUN pip install --upgrade pip setuptools wheel \
    && pip install --prefix=/install -r requirements.txt

# Stage 2: runtime — imagen final limpia
FROM python:3.11-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY --from=builder /install /usr/local
COPY . .

RUN useradd --create-home appuser
USER appuser

CMD ["python", "-m", "market_data.orchestration.entrypoint"]
