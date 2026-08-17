"""ARCH-004 — Balance State.

Detecta si Risk/Execution usa balance real consultado al exchange vs capital
configurado estáticamente (capital_usd). Distingue configured capital !=
exchange balance; no marca error solo porque existe capital_usd.
"""

from __future__ import annotations

from architecture_linter.engine import RepoContext
from architecture_linter.models import Evidence, Finding, Status
from architecture_linter.rules.base import Rule

BALANCE_SYMBOLS = (
    "fetch_balance",
    "get_balance",
    "BalancePort",
    "totalAvailableBalance",
    "available_margin",
    "wallet_balance",
    "fetch_wallet",
    "fetch_balances",
)
CAPITAL_SYMBOLS = ("capital_usd", "capital")


class Arch004Rule(Rule):
    rule_id = "ARCH-004"
    rule_name = "Balance State (configured vs exchange)"
    description = (
        "Detecta si Risk/Execution computa contra balance real del exchange o contra capital "
        "configurado estáticamente (capital_usd). configured capital != exchange balance."
    )

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        # Balance real consultado — por símbolo (heurística) o por estructura
        balance_refs: dict[str, list] = {}
        for sym in BALANCE_SYMBOLS:
            refs = ctx.references(sym)
            if refs:
                balance_refs[sym] = refs

        # Evidencia estructural: métodos que devuelven dict con valores numéricos
        # (currency → amount) en adapters/ports/infrastructure = fuente de balance
        structural_balance: list[tuple] = []
        for path, func, line, ret in ctx.dict_returns():
            if any(d in str(path) for d in ("/adapters/", "/ports/", "/infrastructure/")):
                structural_balance.append((path, func, line, ret))

        # capital_usd en risk/portfolio
        capital_refs: dict[str, list] = {}
        for sym in CAPITAL_SYMBOLS:
            refs = ctx.references(sym)
            if refs:
                capital_refs[sym] = refs

        # capital configurado en risk/portfolio con fines de sizing
        capital_in_risk = any(
            "risk" in str(p) or "portfolio" in str(p) for refs in capital_refs.values() for (p, _) in refs
        )

        evidence: list[Evidence] = []
        for sym, refs in balance_refs.items():
            for path, line in refs[:2]:
                evidence.append(self._ev(ctx, path, line, sym))
        for path, func, line, ret in structural_balance:
            evidence.append(self._ev(ctx, path, line, f"{func}() -> {ret}"))

        if balance_refs or structural_balance:
            sources = ", ".join(list(balance_refs.keys()) or [f"{f}()" for _, f, _, _ in structural_balance])
            return [
                self.finding(
                    Status.PASS,
                    f"Existe balance real consultado: {sources}.",
                    evidence=evidence,
                    related_files=sorted(
                        {str(p) for refs in balance_refs.values() for (p, _) in refs}
                        | {str(p) for p, _, _, _ in structural_balance}
                    ),
                    confidence=0.9,
                    concept="balance",
                )
            ]

        if not balance_refs and capital_in_risk:
            cap_ref = capital_refs.get("capital_usd")
            cap_file = cap_ref[0][0] if cap_ref else None
            cap_line = cap_ref[0][1] if cap_ref else None
            return [
                self.finding(
                    Status.FAIL,
                    "No existe balance real consultado al exchange (fetch_balance/get_balance/"
                    "BalancePort ausentes ni fuente estructural currency→amount); Risk/Execution "
                    "computa sizing/drawdown contra capital_usd configurado (capital estático ≠ "
                    "balance del exchange, ADR-0030).",
                    file=str(cap_file) if cap_file else None,
                    line=cap_line,
                    symbol="capital_usd",
                    evidence=evidence,
                    related_files=sorted({str(p) for refs in capital_refs.values() for (p, _) in refs}),
                    related_symbols=["capital_usd", "fetch_balance", "BalancePort"],
                    confidence=0.95,
                    concept="balance",
                    consumer="RiskManager / PortfolioService",
                )
            ]

        return [
            self.finding(
                Status.UNKNOWN,
                "No se detectó ni balance real ni capital configurado con evidencia suficiente.",
                confidence=0.5,
                concept="balance",
            )
        ]

    def _ev(self, ctx: RepoContext, path, line, sym) -> Evidence:
        text = ctx.text(path)
        if not text or line is None or line > len(text.splitlines()):
            return Evidence(str(path), line, sym, "")
        return Evidence(str(path), line, sym, text.splitlines()[line - 1].strip())
