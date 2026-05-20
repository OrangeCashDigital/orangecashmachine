#!/usr/bin/env python3
"""
scripts/arch_graph.py — Visualización del import graph de market_data.

Genera SVG con pydeps (si disponible) y reporte de texto con grimp.

Uso
---
    python3 scripts/arch_graph.py               # reporte texto
    python3 scripts/arch_graph.py --svg         # SVG en docs/arch/
    python3 scripts/arch_graph.py --svg --open  # SVG + abrir en browser

Salida
------
    docs/arch/import_graph_market_data.svg
    docs/arch/import_graph_application.svg
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT    = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "docs" / "arch"


def _pydeps_svg(package_path: Path, out_file: Path, max_bacon: int = 4) -> bool:
    """
    Genera SVG con pydeps.
    Retorna True si tuvo éxito, False si pydeps no está disponible.
    """
    result = subprocess.run(
        [
            sys.executable, "-m", "pydeps",
            str(package_path),
            "--cluster",
            f"--max-bacon={max_bacon}",
            "--rankdir=TB",
            "--noshow",
            "-o", str(out_file),
        ],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    if result.returncode == 0:
        return True
    if "No module named pydeps" in result.stderr:
        return False
    print(f"  ⚠ pydeps error: {result.stderr[:200]}")
    return False


def text_report() -> None:
    """Reporte de texto con estadísticas del grafo via grimp."""
    try:
        import grimp
    except ImportError:
        print("⚠ grimp no disponible")
        return

    print("Construyendo grafo market_data...")
    graph = grimp.build_graph(
        "market_data",
        include_external_packages=False,
        exclude_type_checking_imports=True,
    )

    modules = sorted(str(m) for m in graph.modules)
    layers = {
        "domain":          [m for m in modules if ".domain."         in m],
        "ports":           [m for m in modules if ".ports."          in m],
        "application":     [m for m in modules if ".application."    in m],
        "adapters":        [m for m in modules if ".adapters."       in m],
        "infrastructure":  [m for m in modules if ".infrastructure." in m],
        "shared":          [m for m in modules if m.startswith("shared.")],
    }

    print(f"\nTotales por capa:")
    for layer, mods in layers.items():
        bar = "█" * min(len(mods), 40)
        print(f"  {layer:<16} {len(mods):>3}  {bar}")

    # Top 5 módulos más importados (fan-in)
    print(f"\nTop 10 módulos más importados (fan-in alto = candidatos a abstraer):")
    fan_in: dict[str, int] = {}
    for m in graph.modules:
        try:
            importers = graph.find_modules_that_directly_import(m)
            if importers:
                fan_in[str(m)] = len(importers)
        except Exception:
            pass
    for mod, count in sorted(fan_in.items(), key=lambda x: -x[1])[:10]:
        print(f"  {count:>3}  {mod}")


def main() -> None:
    gen_svg = "--svg" in sys.argv
    do_open = "--open" in sys.argv

    text_report()

    if gen_svg:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        targets = [
            (ROOT / "packages" / "market_data",              "import_graph_market_data.svg",    4),
            (ROOT / "packages" / "market_data" / "application", "import_graph_application.svg", 3),
        ]
        for pkg_path, fname, bacon in targets:
            out = OUT_DIR / fname
            print(f"\nGenerando {fname}...")
            if _pydeps_svg(pkg_path, out, bacon):
                print(f"  ✓ {out}")
                if do_open:
                    subprocess.run(["xdg-open", str(out)])
            else:
                print("  ⚠ pydeps no disponible.")
                print("  Instalar: pip install pydeps")


if __name__ == "__main__":
    main()
