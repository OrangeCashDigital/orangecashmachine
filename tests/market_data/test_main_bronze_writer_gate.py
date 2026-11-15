# -*- coding: utf-8 -*-
"""
Regresion: con safety.dry_run=True el bronze writer no debe arrancar.

BronzeStorage() sin dry_run escribe a Iceberg de verdad, aunque el resto del
servicio corra en dry_run. El gate evita consumir y commitear offsets de
ohlcv.raw sin escribir a Bronze.
"""

from __future__ import annotations

import pytest
from market_data.main import _should_run_bronze_writer


@pytest.mark.parametrize(("dry_run", "expected"), [(True, False), (False, True)])
def test_bronze_writer_gate(dry_run: bool, expected: bool) -> None:
    assert _should_run_bronze_writer(dry_run) is expected
