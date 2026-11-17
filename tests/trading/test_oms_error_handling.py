# -*- coding: utf-8 -*-
"""
tests/trading/test_oms_error_handling.py
========================================

Tests para la robustez del OMS ante fallos de transporte.
"""

from unittest.mock import MagicMock

import pytest
from portfolio.services.portfolio_service import PortfolioService
from trading.application.oms import OMS
from trading.domain.models.order import OrderStatus
from trading.ports.outbound.transport import OrderResult
from trading.risk.manager import RiskManager


@pytest.fixture
def mock_risk():
    return MagicMock(spec=RiskManager)

@pytest.fixture
def mock_executor():
    return MagicMock()

@pytest.fixture
def mock_portfolio():
    return MagicMock(spec=PortfolioService)

class MockSignal:
    def __init__(self, symbol: str, quantity: float):
        self.symbol = symbol
        self.quantity = quantity
        self.direction = "buy"
        self.price = 50000.0

def test_oms_handles_executor_timeout_as_rejected(mock_risk, mock_executor, mock_portfolio):
    # Setup
    mock_risk.validate.return_value = MagicMock(rejected=False, size_pct=0.1)
    oms = OMS(risk_manager=mock_risk, executor=mock_executor, portfolio=mock_portfolio)
    signal = MockSignal("BTC/USDT", 1.0)
    
    # Simular timeout en executor
    mock_executor.execute.side_effect = TimeoutError("Exchange connection timeout")
    
    # Act
    order = oms.submit(signal)
    
    # Assert
    assert order is not None
    assert order.status == OrderStatus.REJECTED
    assert "TimeoutError" in order.reject_reason

def test_oms_handles_executor_rejection_as_rejected(mock_risk, mock_executor, mock_portfolio):
    # Setup
    mock_risk.validate.return_value = MagicMock(rejected=False, size_pct=0.1)
    oms = OMS(risk_manager=mock_risk, executor=mock_executor, portfolio=mock_portfolio)
    signal = MockSignal("BTC/USDT", 1.0)
    
    # Simular rechazo explícito
    mock_executor.execute.return_value = OrderResult(
        accepted=False, 
        reason="Insufficient balance"
    )
    
    # Act
    order = oms.submit(signal)
    
    # Assert
    assert order is not None
    assert order.status == OrderStatus.REJECTED
    assert order.reject_reason == "Insufficient balance"
