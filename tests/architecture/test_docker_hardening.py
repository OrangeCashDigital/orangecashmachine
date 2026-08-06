# -*- coding: utf-8 -*-
"""
tests/architecture/test_docker_hardening.py
============================================

Guard de seguridad docker (B-05/H-14): invariantes estructurales del build/
deploy que impiden hornear secretos o exponer UIs sin auth.

No sustituye el build real (heavy/CI): es el "linter" de hardening.
"""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent.parent

COMPOSE = ROOT / "docker-compose.yml"
DOCKERIGNORE = ROOT / ".dockerignore"


def test_dockerignore_exists_and_excludes_env() -> None:
    assert DOCKERIGNORE.exists(), ".dockerignore es obligatorio (B-05)"
    text = DOCKERIGNORE.read_text()
    assert ".env" in text, ".dockerignore debe excluir .env (no hornear secrets)"


def test_kafka_ui_requires_login() -> None:
    compose = yaml.safe_load(COMPOSE.read_text())
    service = compose["services"]["kafka-ui"]
    env = service["environment"]
    assert env.get("AUTH_TYPE") == "LOGIN_FORM", "kafka-ui debe exigir login (B-05)"
    assert "SPRING_SECURITY_USER_PASSWORD" in env, "kafka-ui debe pedir password"


def test_kafka_ui_not_bound_to_public_interface() -> None:
    compose = yaml.safe_load(COMPOSE.read_text())
    bind = compose["services"]["kafka-ui"]["ports"][0]
    assert bind.startswith("127.0.0.1:"), "kafka-ui debe bindear solo a loopback (B-05)"


def test_grafana_requires_password_not_default() -> None:
    compose = yaml.safe_load(COMPOSE.read_text())
    env = compose["services"]["grafana"]["environment"]
    assert ":?GRAFANA_PASSWORD" in str(env.get("GF_SECURITY_ADMIN_PASSWORD")), (
        "Grafana debe exigir GRAFANA_PASSWORD (no default) (B-05)"
    )
