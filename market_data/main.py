"""
market_data/main.py
===================

Entrypoint del sistema OrangeCashMachine.

Este archivo solo existe como punto de entrada conveniente.
Toda la lógica vive en orchestration/.
"""

from market_data.orchestration.entrypoint import run

if __name__ == "__main__":
    run()
