"""Tests de reglas del Architecture Governance Linter con fixtures aisladas.

Cada test cubre un invariante: PASS (invariante cumplido), FAIL (violación),
UNKNOWN (no determinable) y ausencia de falso positivo.
"""

from __future__ import annotations

from architecture_linter.models import Status
from architecture_linter.rules.arch_001 import Arch001Rule
from architecture_linter.rules.arch_002 import Arch002Rule
from architecture_linter.rules.arch_003 import Arch003Rule
from architecture_linter.rules.arch_004 import Arch004Rule
from architecture_linter.rules.arch_005 import Arch005Rule
from architecture_linter.rules.arch_006 import Arch006Rule
from architecture_linter.rules.arch_007 import Arch007Rule
from architecture_linter.rules.arch_008 import Arch008Rule
from architecture_linter.rules.arch_009 import Arch009Rule, LayerContract
from architecture_linter.rules.arch_010 import Arch010Rule

# ─────────────────────────────────────────────────────────────────────────────
# ARCH-001 — Multiple Position State Owners
# ─────────────────────────────────────────────────────────────────────────────


def test_arch001_fail_multiple_owners(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._entry_positions: dict[str, float] = {}\n"
                "    def buy(self, symbol):\n"
                "        self._entry_positions[symbol] = 1.0\n"
            ),
            "packages/trading/risk/manager.py": (
                "class RiskManager:\n"
                "    def __init__(self):\n"
                "        self._open_positions: dict[str, float] = {}\n"
                "    def track(self, symbol):\n"
                "        self._open_positions[symbol] = 1.0\n"
            ),
        }
    )
    result = Arch001Rule().run(ctx)
    assert result.status == Status.FAIL


def test_arch001_pass_single_owner(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._entry_positions: dict[str, float] = {}\n"
                "    def buy(self, symbol):\n"
                "        self._entry_positions[symbol] = 1.0\n"
            ),
        }
    )
    result = Arch001Rule().run(ctx)
    assert result.status == Status.PASS


# ─────────────────────────────────────────────────────────────────────────────
# ARCH-002 — Position Semantic Divergence
# ─────────────────────────────────────────────────────────────────────────────


def test_arch002_fail_wac_vs_replace(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._open: dict[str, float] = {}\n"
                "    def buy(self, symbol, qty, avg_price):\n"
                "        new_qty = self._open.get(symbol, 0.0) + qty\n"
                "        new_avg = (self._open.get(symbol, 0.0) * 1 + qty * avg_price) / new_qty\n"
                "        self._open[symbol] = new_avg\n"
            ),
            "packages/trading/analytics/trade_tracker.py": (
                "class TradeTracker:\n"
                "    def __init__(self):\n"
                "        self._open_positions: dict[str, float] = {}\n"
                "    def _register_open(self, symbol, price):\n"
                "        self._open_positions[symbol] = price\n"
                "    def _register_close(self, symbol):\n"
                "        self._open_positions.pop(symbol, None)\n"
            ),
        }
    )
    result = Arch002Rule().run(ctx)
    assert result.status == Status.FAIL


# ─────────────────────────────────────────────────────────────────────────────
# ARCH-003 — Order State Without Reconciliation
# ─────────────────────────────────────────────────────────────────────────────


def test_arch003_fail_no_reconciliation(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._orders: dict[str, object] = {}\n"
                "    def submit(self, order_id, order):\n"
                "        self._orders[order_id] = order\n"
            ),
        }
    )
    result = Arch003Rule().run(ctx)
    assert result.status == Status.FAIL


def test_arch003_pass_with_management(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._orders: dict[str, object] = {}\n"
                "    def manage_open_orders(self):\n"
                "        pass\n"
                "    def fetch_open_orders(self):\n"
                "        pass\n"
            ),
        }
    )
    result = Arch003Rule().run(ctx)
    assert result.status == Status.PASS


def test_arch003_partial_puntual_reconciliation(make_repo) -> None:
    """fetch_state submit-time sin loop periódico => PARTIAL, no FAIL."""
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
    result = Arch003Rule().run(ctx)
    assert result.status == Status.PARTIAL


def test_arch003_unknown_no_stores(make_repo) -> None:
    """Sin almacenes de órdenes en trading => UNKNOWN honesto, nunca PASS/FAIL artificial."""
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n    def __init__(self):\n        self.something_else = []\n"
            ),
        }
    )
    result = Arch003Rule().run(ctx)
    assert result.status == Status.UNKNOWN


# ─────────────────────────────────────────────────────────────────────────────
# ARCH-004 — Balance State
# ─────────────────────────────────────────────────────────────────────────────


def test_arch004_fail_capital_only(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/portfolio/models/position.py": ("class Portfolio:\n    capital_usd: float = 10000.0\n"),
            "packages/trading/risk/manager.py": (
                "class RiskManager:\n    def position_size(self, capital_usd):\n        return capital_usd * 0.1\n"
            ),
        }
    )
    result = Arch004Rule().run(ctx)
    assert result.status == Status.FAIL


def test_arch004_pass_with_balance(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n    def fetch_balance(self):\n        return {'USDT': 100.0}\n"
            ),
        }
    )
    result = Arch004Rule().run(ctx)
    assert result.status == Status.PASS


# ─────────────────────────────────────────────────────────────────────────────
# ARCH-005 — Market Data Freshness Boundary
# ─────────────────────────────────────────────────────────────────────────────


def test_arch005_fail_chain_broken(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/adapters/inbound/websocket/gap_aware_stream.py": (
                "class GapAwareStream:\n"
                "    async def run(self):\n"
                "        import asyncio\n"
                "        try:\n"
                "            await asyncio.wait_for(self._next(), timeout=1.0)\n"
                "        except asyncio.TimeoutError:\n"
                "            await self._handle_silence_gap()\n"
                "    async def _handle_silence_gap(self):\n"
                "        pass\n"
            ),
            "packages/trading/execution/oms.py": ("class OMS:\n    def submit(self, order):\n        pass\n"),
        }
    )
    result = Arch005Rule().run(ctx)
    assert result.status == Status.FAIL


def test_arch005_pass_full_chain(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/adapters/inbound/websocket/gap_aware_stream.py": (
                "class GapAwareStream:\n"
                "    async def run(self):\n"
                "        import asyncio\n"
                "        try:\n"
                "            await asyncio.wait_for(self._next(), timeout=1.0)\n"
                "        except asyncio.TimeoutError:\n"
                "            await self._handle_silence_gap()\n"
                "    async def _handle_silence_gap(self):\n"
                "        await self._run_recovery()\n"
                "    async def _run_recovery(self):\n"
                "        pass\n"
            ),
            "packages/market_data/ports/inbound/trades_source.py": (
                "class TradesSourceProtocol:\n    @property\n    def last_trade_ms(self) -> int:\n        ...\n"
            ),
            "packages/trading/risk/manager.py": (
                "class RiskManager:\n    def check(self, last_trade_ms):\n        return last_trade_ms > 0\n"
            ),
        }
    )
    result = Arch005Rule().run(ctx)
    assert result.status == Status.PASS


# ─────────────────────────────────────────────────────────────────────────────
# ARCH-006 — Orphaned Contract / Port
# ─────────────────────────────────────────────────────────────────────────────


def test_arch006_fail_orphaned_port(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/ports/outbound/market_data_source.py": (
                "from typing import Protocol\nclass MarketDataSourcePort(Protocol):\n    pass\n"
            ),
        }
    )
    result = Arch006Rule().run(ctx)
    assert result.status == Status.FAIL


def test_arch006_pass_consumed_port(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/ports/outbound/market_data_source.py": (
                "from typing import Protocol\nclass MarketDataSourcePort(Protocol):\n    pass\n"
            ),
            "packages/market_data/application/use_cases/loader.py": (
                "from market_data.ports.outbound.market_data_source import MarketDataSourcePort\n"
                "class Loader:\n"
                "    def __init__(self, source: MarketDataSourcePort):\n"
                "        self._source = source\n"
            ),
        }
    )
    result = Arch006Rule().run(ctx)
    assert result.status == Status.PASS


# ─────────────────────────────────────────────────────────────────────────────
# ARCH-007 — Duplicate / Homonymous Contracts
# ─────────────────────────────────────────────────────────────────────────────


def test_arch007_fail_duplicate(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/order.py": (
                "class OrderStatus:\n    PENDING = 'pending'\n    FILLED = 'filled'\n"
            ),
            "packages/trading/execution/transport.py": (
                "class OrderStatus:\n    PENDING = 'pending'\n    FILLED = 'filled'\n"
            ),
        }
    )
    result = Arch007Rule().run(ctx)
    assert result.status == Status.FAIL


def test_arch007_pass_single_definition(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/order.py": ("class OrderStatus:\n    PENDING = 'pending'\n"),
        }
    )
    result = Arch007Rule().run(ctx)
    assert result.status == Status.PASS


def test_arch007_allowlist_excluded(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/infrastructure/bootstrap/composition_root.py": ("class CompositionRoot:\n    pass\n"),
            "packages/portfolio/bootstrap/composition_root.py": ("class CompositionRoot:\n    pass\n"),
        }
    )
    rule = Arch007Rule(allow={"CompositionRoot"})
    result = rule.run(ctx)
    assert result.status == Status.PASS


def test_arch007_mirror_pattern_excluded(make_repo) -> None:
    ctx = make_repo(
        {
            "ocm/config/schema.py": ("class PipelineConfig:\n    realtime: str = 'rest'\n"),
            "ocm/config/structured/pipeline.py": ("class PipelineConfig:\n    realtime: str = 'rest'\n"),
        }
    )
    result = Arch007Rule().run(ctx)
    assert result.status == Status.PASS


# ─────────────────────────────────────────────────────────────────────────────
# ARCH-008 — False Capability / Stub
# ─────────────────────────────────────────────────────────────────────────────


def test_arch008_fail_stub_marker(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/adapters/inbound/websocket/ws_trades_source.py": (
                '"""NOT_IMPLEMENTED"""\n'
                "class WSTradesSource:\n"
                "    async def __anext__(self):\n"
                "        raise StopAsyncIteration\n"
            ),
        }
    )
    result = Arch008Rule().run(ctx)
    assert result.status == Status.FAIL


def test_arch008_pass_clean_class(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/adapters/inbound/websocket/real_source.py": (
                "class RealSource:\n    async def next(self):\n        return 1\n"
            ),
        }
    )
    result = Arch008Rule().run(ctx)
    assert result.status == Status.PASS


def test_arch008_no_false_positive_typing_stub_comment(make_repo) -> None:
    """Comentarios sobre typing-stubs de librerías no deben disparar la regla."""
    ctx = make_repo(
        {
            "packages/market_data/infrastructure/storage/iceberg/iceberg_storage.py": (
                "class IcebergStorage:\n"
                "    def write(self):\n"
                "        pass  # pyiceberg stubs: term/literal no declarados\n"
            ),
        }
    )
    result = Arch008Rule().run(ctx)
    assert result.status == Status.PASS


# ─────────────────────────────────────────────────────────────────────────────
# ARCH-009 — Layer Violation (BC-08)
# ─────────────────────────────────────────────────────────────────────────────


def _make_layer_contract() -> LayerContract:
    return LayerContract(
        container="market_data",
        layers=["infrastructure", "adapters", "application", "ports", "domain"],
        ignore_imports=[
            "market_data.adapters.inbound.rest.ohlcv_fetcher -> market_data.infrastructure.observability.metrics"
        ],
    )


def test_arch009_fail_layer_violation(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/domain/entities/foo.py": (
                "from market_data.infrastructure.storage.iceberg.iceberg_storage import IcebergStorage\n"
                "class Foo:\n"
                "    pass\n"
            ),
        }
    )
    result = Arch009Rule(contract=_make_layer_contract()).run(ctx)
    assert result.status == Status.FAIL


def test_arch009_pass_allowed_direction(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/market_data/application/use_cases/foo.py": (
                "from market_data.ports.outbound.storage import BronzeStoragePort\nclass Foo:\n    pass\n"
            ),
        }
    )
    result = Arch009Rule(contract=_make_layer_contract()).run(ctx)
    assert result.status == Status.PASS


def test_arch009_pass_ignored_import(make_repo) -> None:
    """Deuda técnica documentada en ignore_imports no se marca como violación."""
    ctx = make_repo(
        {
            "packages/market_data/adapters/inbound/rest/ohlcv_fetcher.py": (
                "from market_data.infrastructure.observability.metrics import Metrics\nclass OHLCVFetcher:\n    pass\n"
            ),
        }
    )
    result = Arch009Rule(contract=_make_layer_contract()).run(ctx)
    assert result.status == Status.PASS


# ─────────────────────────────────────────────────────────────────────────────
# ARCH-010 — Duplicated Mutable State
# ─────────────────────────────────────────────────────────────────────────────


def test_arch010_fail_duplicated_position(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._entry_positions: dict[str, float] = {}\n"
                "    def buy(self, symbol):\n"
                "        self._entry_positions[symbol] = 1.0\n"
            ),
            "packages/trading/risk/manager.py": (
                "class RiskManager:\n"
                "    def __init__(self):\n"
                "        self._open_positions: dict[str, float] = {}\n"
                "    def track(self, symbol):\n"
                "        self._open_positions[symbol] = 1.0\n"
            ),
        }
    )
    result = Arch010Rule().run(ctx)
    assert result.status == Status.FAIL


def test_arch010_pass_single_store(make_repo) -> None:
    ctx = make_repo(
        {
            "packages/trading/execution/oms.py": (
                "class OMS:\n"
                "    def __init__(self):\n"
                "        self._entry_positions: dict[str, float] = {}\n"
                "    def buy(self, symbol):\n"
                "        self._entry_positions[symbol] = 1.0\n"
            ),
        }
    )
    result = Arch010Rule().run(ctx)
    assert result.status == Status.PASS
