

# ── Architecture targets ──────────────────────────────────────────────────────

## arch-check: Detecta ciclos y verifica contratos de import
.PHONY: arch-check
arch-check:
	@python3 scripts/arch_metrics.py --strict
	@echo "✓ arch-check OK"

## arch-metrics: Reporte de salud arquitectónica (human-readable)
.PHONY: arch-metrics
arch-metrics:
	@python3 scripts/arch_metrics.py

## arch-metrics-json: Métricas en JSON (para CI/Grafana)
.PHONY: arch-metrics-json
arch-metrics-json:
	@python3 scripts/arch_metrics.py --json

## arch-graph: Reporte de texto del import graph
.PHONY: arch-graph
arch-graph:
	@python3 scripts/arch_graph.py

## arch-graph-svg: Genera SVG del import graph (requiere pydeps)
.PHONY: arch-graph-svg
arch-graph-svg:
	@python3 scripts/arch_graph.py --svg
	@echo "✓ SVG generado en docs/arch/"

## arch-tests: Corre solo los policy tests de arquitectura
.PHONY: arch-tests
arch-tests:
	@.venv/bin/python -m pytest tests/architecture/ -v --tb=short

## ci: Pipeline completo de CI (lint + arch + tests)
.PHONY: ci
ci: arch-check
	@.venv/bin/lint-imports
	@.venv/bin/python -m pytest tests/ -x -q
	@echo "✓ CI completo OK"

## lint: Solo lint-imports
.PHONY: lint
lint:
	@.venv/bin/lint-imports

