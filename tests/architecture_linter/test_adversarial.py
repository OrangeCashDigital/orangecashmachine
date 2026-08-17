"""Batería adversarial del Architecture Governance Linter.

Prueba que el linter NO es una heurística de nombres: mutaciones de renombrado
(attributos, variables, clases) no deben cambiar el veredicto cuando la
semántica estructural se mantiene; y patrones inocentes que imitan señales de
violación (null-objects, streams con terminación normal, single-write) no deben
producir falsos positivos.

Matriz de estados cubierta:
  * violación real golden → FAIL;
  * corregido (single owner coherente) → PASS;
  * mutación sin cambio semántico → mismo resultado (FAIL);
  * FP conocido → PASS;
  * evidencia insuficiente → UNKNOWN;
  * reconciliación parcial → PARTIAL;
  * violación estructural de capa / dependencia prohibida → FAIL.
"""

from __future__ import annotations

from architecture_linter.models import Status
from architecture_linter.rules.arch_001 import Arch001Rule
from architecture_linter.rules.arch_002 import Arch002Rule
from architecture_linter.rules.arch_003 import Arch003Rule
from architecture_linter.rules.arch_006 import Arch006Rule
from architecture_linter.rules.arch_008 import Arch008Rule
from architecture_linter.rules.arch_009 import Arch009Rule, ForbiddenContract, LayerContract
from architecture_linter.rules.arch_010 import Arch010Rule

# ─────────────────────────────────────────────────────────────────────────────
# Mutación A: renombrar el atributo del store (semántica estructural intacta)
# ─────────────────────────────────────────────────────────────────────────────


def test_adversarial_rename_store_attr_still_fails(make_repo) -> None:
    """`_book`/`_ledger` (nada que ver con `_positions`) se detectan por forma/anotación."""
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._book: dict[str, tuple[float, float]] = {}\n"
                "    def buy(self, symbol, qty, avg_price):\n"
                "        prev_qty, prev_avg = self._book.get(symbol, (0.0, 0.0))\n"
                "        new_qty = prev_qty + qty\n"
                "        new_avg = (prev_qty * prev_avg + qty * avg_price) / new_qty\n"
                "        self._book[symbol] = (new_qty, new_avg)\n"
            ),
            "packages/trading/analytics/trade_tracker.py": (
                "class TradeTracker:\n"
                "    def __init__(self):\n"
                "        self._ledger: dict[str, tuple[float, float]] = {}\n"
                "    def _register_open(self, symbol, price):\n"
                "        self._ledger[symbol] = (price, price)\n"
                "    def _register_close(self, symbol):\n"
                "        self._ledger.pop(symbol, None)\n"
            ),
        }
    )
    assert Arch002Rule().run(ctx).status == Status.FAIL
    assert Arch010Rule().run(ctx).status == Status.FAIL


# ─────────────────────────────────────────────────────────────────────────────
# Mutación B: renombrar las variables WAC (semántica, no nombres de variable)
# ─────────────────────────────────────────────────────────────────────────────


def test_adversarial_rename_wac_variables_still_fails(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._entry_positions: dict[str, tuple[float, float]] = {}\n"
                "    def buy(self, symbol, qty, avg_price):\n"
                "        delta, base = self._entry_positions.get(symbol, (0.0, 0.0))\n"
                "        moving_qty = delta + qty\n"
                "        moving_avg = (delta * base + qty * avg_price) / moving_qty\n"
                "        self._entry_positions[symbol] = (moving_qty, moving_avg)\n"
            ),
            "packages/trading/analytics/trade_tracker.py": (
                "class TradeTracker:\n"
                "    def __init__(self):\n"
                "        self._open_positions: dict[str, tuple[float, float]] = {}\n"
                "    def _register_open(self, symbol, price):\n"
                "        self._open_positions[symbol] = (price, price)\n"
                "    def _register_close(self, symbol):\n"
                "        self._open_positions.pop(symbol, None)\n"
            ),
        }
    )
    assert Arch002Rule().run(ctx).status == Status.FAIL


# ─────────────────────────────────────────────────────────────────────────────
# Mutación C: renombrar las clases (múltiples owners siguen siendo múltiples)
# ─────────────────────────────────────────────────────────────────────────────


def test_adversarial_rename_classes_still_fails(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class BookingEngine:\n"
                "    def __init__(self):\n"
                "        self._entry_positions: dict[str, float] = {}\n"
                "    def buy(self, symbol):\n"
                "        self._entry_positions[symbol] = 1.0\n"
            ),
            "packages/trading/risk/manager.py": (
                "class RiskLedger:\n"
                "    def __init__(self):\n"
                "        self._open_positions: dict[str, float] = {}\n"
                "    def track(self, symbol):\n"
                "        self._open_positions[symbol] = 1.0\n"
            ),
        }
    )
    assert Arch001Rule().run(ctx).status == Status.FAIL


# ─────────────────────────────────────────────────────────────────────────────
# Corregido: un solo owner coherente → PASS
# ─────────────────────────────────────────────────────────────────────────────


def test_adversarial_single_consistent_owner_passes(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._entry_positions: dict[str, tuple[float, float]] = {}\n"
                "    def buy(self, symbol, qty, avg_price):\n"
                "        prev_qty, prev_avg = self._entry_positions.get(symbol, (0.0, 0.0))\n"
                "        new_qty = prev_qty + qty\n"
                "        new_avg = (prev_qty * prev_avg + qty * avg_price) / new_qty\n"
                "        self._entry_positions[symbol] = (new_qty, new_avg)\n"
                "    def sell(self, symbol, qty):\n"
                "        prev_qty, prev_avg = self._entry_positions.get(symbol, (0.0, 0.0))\n"
                "        remaining = prev_qty - qty\n"
                "        if remaining > 0:\n"
                "            self._entry_positions[symbol] = (remaining, prev_avg)\n"
                "        else:\n"
                "            self._entry_positions.pop(symbol, None)\n"
            ),
        }
    )
    assert Arch002Rule().run(ctx).status == Status.PASS
    assert Arch001Rule().run(ctx).status == Status.PASS


# ─────────────────────────────────────────────────────────────────────────────
# FP conocido: null-objects, streams reales, single-write → PASS
# ─────────────────────────────────────────────────────────────────────────────


def test_adversarial_known_fp_patterns_pass(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/adapters/inbound/websocket/real_source.py": (
                "class RealSource:\n    async def next(self):\n        return 1\n"
            ),
            "packages/market_data/adapters/inbound/websocket/ws_stream.py": (
                "class LiveStream:\n"
                "    def __init__(self):\n"
                "        self._running = False\n"
                "    async def __aiter__(self):\n"
                "        self._running = True\n"
                "        return self\n"
                "    async def __anext__(self):\n"
                "        data = await self.pull()\n"
                "        if data is None:\n"
                "            raise StopAsyncIteration\n"
                "        return data\n"
                "    async def pull(self):\n"
                "        return await self.fetch()\n"
            ),
            "packages/market_data/ports/outbound/metrics.py": (
                "class NullMetrics:\n"
                "    def inc(self, n):\n"
                "        logger.info('noop')\n"
                "    def observe(self, v):\n"
                "        logger.info('noop')\n"
            ),
            "packages/market_data/infrastructure/storage/iceberg/iceberg_storage.py": (
                "class IcebergStorage:\n"
                "    def write(self):\n"
                "        pass  # pyiceberg stubs: term/literal no declarados\n"
            ),
        }
    )
    assert Arch008Rule().run(ctx).status == Status.PASS


# ─────────────────────────────────────────────────────────────────────────────
# PARTIAL y UNKNOWN de ARCH-003
# ─────────────────────────────────────────────────────────────────────────────


def test_adversarial_partial_reconciliation(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._orders: dict[str, object] = {}\n"
                "    def submit(self, order_id, order):\n"
                "        self._orders[order_id] = order\n"
            ),
            "packages/trading/execution/transport.py": (
                "class OrderTransport:\n    def fetch_state(self, order_id):\n        return {}\n"
            ),
        }
    )
    assert Arch003Rule().run(ctx).status == Status.PARTIAL


def test_adversarial_unknown_no_stores(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": ("class OMS:\n    def __init__(self):\n        self.engine = None\n"),
        }
    )
    assert Arch003Rule().run(ctx).status == Status.UNKNOWN


# ─────────────────────────────────────────────────────────────────────────────
# Violación estructural de capa y dependencia prohibida (ARCH-009)
# ─────────────────────────────────────────────────────────────────────────────


def test_adversarial_layer_violation_fails(make_repo) -> None:
    # Orden real de BC-08: exterior → interior (infrastructure es la capa más externa;
    # las dependencias fluyen solo hacia dentro). domain no puede importar infrastructure.
    contract = LayerContract(
        container="market_data",
        layers=["infrastructure", "adapters", "application", "ports", "domain"],
        name="BC-TEST",
    )
    ctx = make_repo(
        {
            "packages/market_data/domain/entity.py": "class E: pass\n",
            "packages/market_data/infrastructure/db.py": "class DB: pass\n",
            "packages/market_data/domain/bad.py": "from market_data.infrastructure.db import DB\n",
        }
    )
    result = Arch009Rule(contract=contract, forbidden=[]).run(ctx)
    assert result.status == Status.FAIL


def test_adversarial_composition_root_exempt_from_forbidden(make_repo) -> None:
    forbidden = ForbiddenContract(
        name="BC-TEST-FORBIDDEN",
        source_modules=["market_data.application"],
        forbidden_modules=["market_data.infrastructure"],
    )
    # Contenedor ajeno a market_data: no introduce violaciones de capa; el foco
    # es exclusivamente el contrato forbidden y su exención de composition_root.
    dummy_layer = LayerContract(container="other_platform", layers=["infrastructure", "application"], name="BC-DUMMY")
    ctx = make_repo(
        {
            "packages/market_data/application/composition_root.py": ("from market_data.infrastructure.db import DB\n"),
            "packages/market_data/application/pipeline.py": "class P: pass\n",
        }
    )
    result = Arch009Rule(contract=dummy_layer, forbidden=[forbidden]).run(ctx)
    assert result.status == Status.PASS


def test_adversarial_forbidden_violation_fails(make_repo) -> None:
    forbidden = ForbiddenContract(
        name="BC-TEST-FORBIDDEN",
        source_modules=["market_data.application"],
        forbidden_modules=["market_data.infrastructure"],
    )
    ctx = make_repo(
        {
            "packages/market_data/application/pipeline.py": ("from market_data.infrastructure.db import DB\n"),
            "packages/market_data/infrastructure/db.py": "class DB: pass\n",
        }
    )
    result = Arch009Rule(contract=[], forbidden=[forbidden]).run(ctx)
    assert result.status == Status.FAIL


# ─────────────────────────────────────────────────────────────────────────────
# Estado global mutable y port huérfano
# ─────────────────────────────────────────────────────────────────────────────


def test_adversarial_global_state_fails(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "_open_orders: dict[str, object] = {}\n_open_orders['x'] = object()\n"
            ),
        }
    )
    result = Arch010Rule().run(ctx)
    assert result.status == Status.FAIL


def test_adversarial_orphan_port_fails(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/ports/outbound/fetcher.py": (
                "from typing import Protocol\n"
                "class DerivativesFetcherPort(Protocol):\n"
                "    def fetch(self, symbol: str) -> dict: ...\n"
            ),
        }
    )
    result = Arch006Rule().run(ctx)
    assert result.status == Status.FAIL
