"""Institutional quantitative performance metrics."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Mapping, Sequence


@dataclass(frozen=True)
class PerformanceMetrics:
    totalReturnPct: float
    annualizedSharpe: float
    annualizedSortino: float
    maxDrawdownPct: float
    winRatePct: float
    profitFactor: float
    totalTrades: int

    def to_dict(self) -> dict[str, float]:
        return {key: float(value) for key, value in asdict(self).items()}

    def to_decimals(self) -> dict[str, Decimal]:
        """Convert floats to Decimal for DynamoDB persistence."""
        return {
            "totalReturnPct": Decimal(str(self.totalReturnPct)),
            "annualizedSharpe": Decimal(str(self.annualizedSharpe)),
            "annualizedSortino": Decimal(str(self.annualizedSortino)),
            "maxDrawdownPct": Decimal(str(self.maxDrawdownPct)),
            "winRatePct": Decimal(str(self.winRatePct)),
            "profitFactor": Decimal(str(self.profitFactor)),
            "totalTrades": Decimal(str(self.totalTrades)),
        }


def floats_to_decimals(values: Mapping[str, float | int]) -> dict[str, Decimal]:
    return {key: Decimal(str(value)) for key, value in values.items()}


def _period_returns(equity_curve: Sequence[float]) -> list[float]:
    returns: list[float] = []
    for previous, current in zip(equity_curve, equity_curve[1:]):
        if previous <= 0:
            returns.append(0.0)
        else:
            returns.append((current - previous) / previous)
    return returns


def _max_drawdown_pct(equity_curve: Sequence[float]) -> float:
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
    return max_dd * 100.0


def _profit_factor(trade_pnls: Sequence[float]) -> float:
    gross_profit = sum(pnl for pnl in trade_pnls if pnl > 0)
    gross_loss = sum(-pnl for pnl in trade_pnls if pnl < 0)
    if gross_loss < 1e-12:
        return float("inf") if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def compute_metrics(
    equity_curve: Sequence[float],
    trade_pnls: Sequence[float],
    initial_capital: float,
    *,
    periods_per_year: int = 252,
) -> PerformanceMetrics:
    """
    Compute risk/return metrics from an equity curve and closed-trade PnLs.

    Returns percentages for return / drawdown / win-rate (e.g. 12.5 = 12.5%).
    """
    if not equity_curve:
        return PerformanceMetrics(
            totalReturnPct=0.0,
            annualizedSharpe=0.0,
            annualizedSortino=0.0,
            maxDrawdownPct=0.0,
            winRatePct=0.0,
            profitFactor=0.0,
            totalTrades=0,
        )

    final_equity = float(equity_curve[-1])
    total_return_pct = (
        ((final_equity - initial_capital) / initial_capital) * 100.0
        if initial_capital
        else 0.0
    )

    returns = _period_returns(equity_curve)
    n = len(returns)
    if n == 0:
        mean_r = 0.0
        vol = 0.0
        downside = 0.0
    else:
        mean_r = sum(returns) / n
        variance = sum((r - mean_r) ** 2 for r in returns) / n
        vol = math.sqrt(variance)
        downside_sq = sum(min(r, 0.0) ** 2 for r in returns) / n
        downside = math.sqrt(downside_sq)

    annualized_vol = vol * math.sqrt(periods_per_year)
    annualized_downside = downside * math.sqrt(periods_per_year)
    annualized_return = mean_r * periods_per_year

    sharpe = (annualized_return / annualized_vol) if annualized_vol > 1e-12 else 0.0
    sortino = (
        (annualized_return / annualized_downside) if annualized_downside > 1e-12 else 0.0
    )

    trade_count = len(trade_pnls)
    wins = sum(1 for pnl in trade_pnls if pnl > 0)
    win_rate_pct = (wins / trade_count * 100.0) if trade_count else 0.0
    profit_factor = _profit_factor(trade_pnls)
    if math.isinf(profit_factor):
        # Cap for DynamoDB / JSON friendliness while remaining "very large"
        profit_factor = 999.0

    return PerformanceMetrics(
        totalReturnPct=round(total_return_pct, 6),
        annualizedSharpe=round(sharpe, 6),
        annualizedSortino=round(sortino, 6),
        maxDrawdownPct=round(_max_drawdown_pct(equity_curve), 6),
        winRatePct=round(win_rate_pct, 6),
        profitFactor=round(profit_factor, 6),
        totalTrades=trade_count,
    )
