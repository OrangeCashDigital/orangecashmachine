# ==========================================================
# OrangeCashMachine — Makefile
# Siempre ejecutar desde la raíz del proyecto
# ==========================================================

COMPOSE = docker compose
MONITORING_CONFIGS = \
	infra/monitoring/prometheus.yml \
	infra/monitoring/alertmanager.yml \
	infra/monitoring/alerts.yml

.PHONY: up down restart status preflight repair repair-dry-run validate logs

preflight:
	@echo "→ Verificando configs de monitoreo..."
	@for f in $(MONITORING_CONFIGS); do \
		if [ ! -f "$$f" ]; then \
			echo "ERROR: $$f no existe o no es un archivo regular"; \
			exit 1; \
		fi; \
	done
	@echo "✓ Configs OK"

up: preflight
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

status:
	$(COMPOSE) ps

repair:
	uv run python3 data_platform/repair_silver.py --all

repair-dry-run:
	uv run python3 data_platform/repair_silver.py --all --dry-run

validate:
	uv run python3 data_platform/validate_silver.py

logs:
	$(COMPOSE) logs -f --tail=100
