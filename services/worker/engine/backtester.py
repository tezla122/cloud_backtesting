"""Deterministic event-driven backtesting engine with synthetic OHLCV data."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

from .metrics import PerformanceMetrics, compute_metrics

StrategyName = Literal["sma_crossover", "buy_and_hold", "mean_reversion"]


@dataclass(frozen=True)
class Bar:
    timestamp: date
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class BacktestConfig:
    strategy: StrategyName
    symbol: str
    start_date: str
    end_date: str
    initial_capital: float = 100_000.0
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BacktestResult:
    config: BacktestConfig
    metrics: PerformanceMetrics
    equity_curve: list[float]
    trade_count: int
    bars_processed: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy": self.config.strategy,
            "symbol": self.config.symbol,
            "startDate": self.config.start_date,
            "endDate": self.config.end_date,
            "initialCapital": self.config.initial_capital,
            "parameters": self.config.parameters,
            "metrics": self.metrics.to_dict(),
            "equityCurve": self.equity_curve,
            "tradeCount": self.trade_count,
            "barsProcessed": self.bars_processed,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _trading_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _symbol_seed(symbol: str) -> int:
    digest = hashlib.sha256(symbol.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def generate_ohlcv(symbol: str, start_date: str, end_date: str) -> list[Bar]:
    """
    Generate deterministic synthetic OHLCV bars for a symbol/date range.

    Uses a seeded geometric random walk so the same inputs always yield the
    same series — critical for reproducible backtests and unit tests.
    """
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end <= start:
        raise ValueError("end_date must be after start_date")

    days = _trading_days(start, end)
    if not days:
        raise ValueError("no trading days in the requested window")

    seed = _symbol_seed(symbol.upper())
    # Simple LCG for portable deterministic noise (no numpy dependency)
    state = seed % 2147483647 or 1

    def next_unit() -> float:
        nonlocal state
        state = (1103515245 * state + 12345) % (2**31)
        return state / float(2**31)

    price = 50.0 + (seed % 150)
    bars: list[Bar] = []
    for day in days:
        daily_return = (next_unit() - 0.48) * 0.02
        open_px = price
        close_px = max(0.01, open_px * (1.0 + daily_return))
        high_px = max(open_px, close_px) * (1.0 + next_unit() * 0.005)
        low_px = min(open_px, close_px) * (1.0 - next_unit() * 0.005)
        volume = 100_000 + int(next_unit() * 900_000)
        bars.append(
            Bar(
                timestamp=day,
                open=round(open_px, 4),
                high=round(high_px, 4),
                low=round(low_px, 4),
                close=round(close_px, 4),
                volume=float(volume),
            )
        )
        price = close_px
    return bars


def _sma(values: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    if period <= 0:
        raise ValueError("SMA period must be positive")
    running = 0.0
    for i, value in enumerate(values):
        running += value
        if i >= period:
            running -= values[i - period]
        if i >= period - 1:
            out[i] = running / period
    return out


def _signals_sma_crossover(
    closes: list[float],
    fast_period: int,
    slow_period: int,
) -> list[int]:
    fast = _sma(closes, fast_period)
    slow = _sma(closes, slow_period)
    signals = [0] * len(closes)
    position = 0
    for i in range(1, len(closes)):
        if fast[i] is None or slow[i] is None or fast[i - 1] is None or slow[i - 1] is None:
            continue
        prev_diff = (fast[i - 1] or 0.0) - (slow[i - 1] or 0.0)
        curr_diff = (fast[i] or 0.0) - (slow[i] or 0.0)
        if prev_diff <= 0 < curr_diff:
            position = 1
        elif prev_diff >= 0 > curr_diff:
            position = 0
        signals[i] = position
    return signals


def _signals_buy_and_hold(n: int) -> list[int]:
    if n == 0:
        return []
    return [0] + [1] * (n - 1)


def _signals_mean_reversion(
    closes: list[float],
    lookback: int,
    entry_z: float,
    exit_z: float,
) -> list[int]:
    signals = [0] * len(closes)
    position = 0
    for i in range(lookback, len(closes)):
        window = closes[i - lookback : i]
        mean = sum(window) / lookback
        variance = sum((x - mean) ** 2 for x in window) / lookback
        std = math.sqrt(variance) if variance > 0 else 0.0
        if std < 1e-12:
            signals[i] = position
            continue
        z = (closes[i] - mean) / std
        if position == 0 and z <= -entry_z:
            position = 1
        elif position == 1 and z >= -exit_z:
            position = 0
        signals[i] = position
    return signals


def _simulate(
    bars: list[Bar],
    signals: list[int],
    initial_capital: float,
) -> tuple[list[float], list[float]]:
    cash = float(initial_capital)
    shares = 0.0
    equity_curve: list[float] = []
    trade_pnls: list[float] = []
    entry_price = 0.0

    for i, bar in enumerate(bars):
        target = signals[i]
        if target == 1 and shares == 0.0:
            shares = cash / bar.close
            entry_price = bar.close
            cash = 0.0
        elif target == 0 and shares > 0.0:
            trade_pnls.append(shares * (bar.close - entry_price))
            cash = shares * bar.close
            shares = 0.0
            entry_price = 0.0


        equity = cash + shares * bar.close
        equity_curve.append(round(equity, 6))

    return equity_curve, trade_pnls


def run_backtest(config: BacktestConfig) -> BacktestResult:
    """Execute a strategy backtest and return metrics plus equity curve."""
    bars = generate_ohlcv(config.symbol, config.start_date, config.end_date)
    closes = [bar.close for bar in bars]
    params = config.parameters

    if config.strategy == "sma_crossover":
        fast = int(params.get("fastPeriod", params.get("fast_period", 10)))
        slow = int(params.get("slowPeriod", params.get("slow_period", 50)))
        if fast >= slow:
            raise ValueError("fastPeriod must be less than slowPeriod")
        signals = _signals_sma_crossover(closes, fast, slow)
    elif config.strategy == "buy_and_hold":
        signals = _signals_buy_and_hold(len(closes))
    elif config.strategy == "mean_reversion":
        lookback = int(params.get("lookback", 20))
        entry_z = float(params.get("entryZ", params.get("entry_z", 1.5)))
        exit_z = float(params.get("exitZ", params.get("exit_z", 0.25)))
        signals = _signals_mean_reversion(closes, lookback, entry_z, exit_z)
    else:
        raise ValueError(f"Unsupported strategy: {config.strategy}")

    equity_curve, trade_pnls = _simulate(bars, signals, config.initial_capital)
    metrics = compute_metrics(equity_curve, trade_pnls, config.initial_capital)

    # Downsample equity curve for storage (keep endpoints + every Nth point)
    max_points = 500
    if len(equity_curve) > max_points:
        step = math.ceil(len(equity_curve) / max_points)
        sampled = equity_curve[::step]
        if sampled[-1] != equity_curve[-1]:
            sampled.append(equity_curve[-1])
        equity_curve = sampled

    return BacktestResult(
        config=config,
        metrics=metrics,
        equity_curve=equity_curve,
        trade_count=metrics.trade_count,
        bars_processed=len(bars),
    )
