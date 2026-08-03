#!/usr/bin/env python3
"""SSOT enforcement: literales de dominio solo se definen en shared/enums.py.

Gate de CI: falla (exit 1) si un literal de dominio se define fuera de
shared/enums.py. Previene duplicación silenciosa del vocabulario cross-BC.
"""

import re
import sys
from pathlib import Path

LITERALS = ["OrderSide", "PositionSide", "SignalDirection", "DataSource"]
ENUMS_FILE = Path("shared/enums.py")

errors: list[str] = []
for lit in LITERALS:
    pattern = re.compile(rf"^{lit}\s*=", re.MULTILINE)
    for py_file in Path("shared").rglob("*.py"):
        if py_file == ENUMS_FILE:
            continue
        if pattern.search(py_file.read_text(encoding="utf-8")):
            errors.append(f"SSOT violado: {lit} definido también en {py_file}")

if errors:
    print("\n".join(errors))
    sys.exit(1)
print("OK: todos los literales viven en shared/enums.py")
