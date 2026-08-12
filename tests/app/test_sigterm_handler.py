# -*- coding: utf-8 -*-
"""
tests/app/test_sigterm_handler.py
==================================

SafeOps / Guardrail #10 (Engineering Guardrails): el CLI ``ocm``
(``apps/app/cli/main.py``) registra un handler de SIGTERM que traduce la
señal a KeyboardInterrupt, de modo que ``asyncio.run`` cancela la tarea
pendiente graceful (mismo path de exit 130 que SIGINT). Sin este handler,
systemd/k8s mataría el proceso con exit 143 sin cleanup.

R14/H8 (app_layer_guard): los handlers de señal viven en
``app/cli/_bootstrap.py`` — este test ejercita el handler canónico.
"""

from __future__ import annotations

import signal

import pytest
from app.cli._bootstrap import install_sigterm_handler


def test_install_sigterm_handler_registers_for_sigterm(monkeypatch: pytest.MonkeyPatch) -> None:
    registered: list[tuple[int, object]] = []

    def _fake_signal(signum: int, handler: object) -> None:
        registered.append((signum, handler))

    monkeypatch.setattr(signal, "signal", _fake_signal)
    install_sigterm_handler()
    assert len(registered) == 1
    assert registered[0][0] == signal.SIGTERM


def test_sigterm_handler_raises_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    """El handler registrado lanza KeyboardInterrupt (→ exit 130 vía main())."""

    def _fake_signal(signum: int, handler: object) -> None:
        recorded.append(handler)

    recorded: list[object] = []
    monkeypatch.setattr(signal, "signal", _fake_signal)
    install_sigterm_handler()

    assert len(recorded) == 1
    handler = recorded[0]
    assert callable(handler)
    with pytest.raises(KeyboardInterrupt):
        handler(signal.SIGTERM, None)  # type: ignore[operator]
