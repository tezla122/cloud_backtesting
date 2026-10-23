"""Quantitative backtesting engine package."""

from .backtester import BacktestConfig, BacktestResult, run_backtest
from .metrics import PerformanceMetrics, compute_metrics, floats_to_decimals

__all__ = [
    "BacktestConfig",
    "BacktestResult",
    "PerformanceMetrics",
    "compute_metrics",
    "floats_to_decimals",
    "run_backtest",
]
