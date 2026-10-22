"""Performance metrics for equity curves and trade series."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Sequence


@dataclass(frozen=True)
class PerformanceMetrics:
    total_return: float
    annualized_return: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    trade_count: int
    final_equity: float
    initial_capital: float

    def to_dict(self) -> dict[str, float]:
        return {key: float(value) for key, value in asdict(self).items()}


def _daily_returns(equity_curve: Sequence[float]) -> list[float]:
    returns: list[float] = []
    for previous, current in zip(equity_curve, equity_curve[1:]):
        if previous <= 0:
            returns.append(0.0)
        else:
            returns.append((current - previous) / previous)
    return returns


def _max_drawdown(equity_curve: Sequence[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        if value > peak:
            peak = value
        if peak > 0:
            drawdown = (peak - value) / peak
            if drawdown > max_dd:
                max_dd = drawdown
    return max_dd


def compute_metrics(
    equity_curve: Sequence[float],
    trade_pnls: Sequence[float],
    initial_capital: float,
    trading_days_per_year: int = 252,
) -> PerformanceMetrics:
    """Compute standard risk/return metrics from an equity curve."""
    if not equity_curve:
        return PerformanceMetrics(
            total_return=0.0,
            annualized_return=0.0,
            volatility=0.0,
            sharpe_ratio=0.0,
            max_drawdown=0.0,
            win_rate=0.0,
            trade_count=0,
            final_equity=float(initial_capital),
            initial_capital=float(initial_capital),
        )

    final_equity = float(equity_curve[-1])
    total_return = (final_equity - initial_capital) / initial_capital if initial_capital else 0.0

    returns = _daily_returns(equity_curve)
    n = len(returns)
    if n == 0:
        mean_r = 0.0
        vol = 0.0
    else:
        mean_r = sum(returns) / n
        variance = sum((r - mean_r) ** 2 for r in returns) / n
        vol = math.sqrt(variance)

    annualized_vol = vol * math.sqrt(trading_days_per_year)
    annualized_return = mean_r * trading_days_per_year
    sharpe = (annualized_return / annualized_vol) if annualized_vol > 1e-12 else 0.0

    wins = sum(1 for pnl in trade_pnls if pnl > 0)
    trade_count = len(trade_pnls)
    win_rate = (wins / trade_count) if trade_count else 0.0

    return PerformanceMetrics(
        total_return=round(total_return, 8),
        annualized_return=round(annualized_return, 8),
        volatility=round(annualized_vol, 8),
        sharpe_ratio=round(sharpe, 8),
        max_drawdown=round(_max_drawdown(equity_curve), 8),
        win_rate=round(win_rate, 8),
        trade_count=trade_count,
        final_equity=round(final_equity, 4),
        initial_capital=float(initial_capital),
    )
