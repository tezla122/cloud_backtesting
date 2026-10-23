"""Event-driven backtesting engine with S3 datasets and synthetic fallback."""

from __future__ import annotations

import csv
import hashlib
import io
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal, Sequence

from .metrics import PerformanceMetrics, compute_metrics

StrategyType = Literal["MEAN_REVERSION", "MOMENTUM_CROSSOVER"]
Timeframe = Literal["1m", "5m", "1h", "1d"]

# Approximate bars per year for annualization by timeframe
_PERIODS_PER_YEAR: dict[str, int] = {
    "1m": 252 * 6.5 * 60,  # equity RTH minutes
    "5m": 252 * 6.5 * 12,
    "1h": 252 * 7,
    "1d": 252,
}


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
    strategy_type: StrategyType
    symbol: str
    timeframe: Timeframe
    start_date: str
    end_date: str
    initial_capital: float = 100_000.0
    commission_bps: float = 0.0
    slippage_bps: float = 0.0
    parameters: dict[str, Any] = field(default_factory=dict)
    dataset_key: str | None = None
    use_synthetic_data: bool = False


@dataclass(frozen=True)
class BacktestResult:
    config: BacktestConfig
    metrics: PerformanceMetrics
    equity_curve: list[float]
    trade_count: int
    bars_processed: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategyType": self.config.strategy_type,
            "symbol": self.config.symbol,
            "timeframe": self.config.timeframe,
            "startDate": self.config.start_date,
            "endDate": self.config.end_date,
            "initialCapital": self.config.initial_capital,
            "commissionBps": self.config.commission_bps,
            "slippageBps": self.config.slippage_bps,
            "parameters": self.config.parameters,
            "datasetKey": self.config.dataset_key,
            "metrics": self.metrics.to_dict(),
            "equityCurve": self.equity_curve,
            "tradeCount": self.trade_count,
            "barsProcessed": self.bars_processed,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }


def _parse_date(value: str) -> date:
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


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

    Seeded geometric random walk — same inputs always yield the same series.
    """
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end <= start:
        raise ValueError("end_date must be after start_date")

    days = _trading_days(start, end)
    if not days:
        raise ValueError("no trading days in the requested window")

    seed = _symbol_seed(symbol.upper())
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


def parse_ohlcv_csv(raw: str | bytes) -> list[Bar]:
    """Parse a CSV with columns: timestamp|date, open, high, low, close, volume."""
    text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("OHLCV CSV is missing a header row")

    # Normalize header keys
    field_map = {name.strip().lower(): name for name in reader.fieldnames}
    required = ("open", "high", "low", "close")
    for col in required:
        if col not in field_map:
            raise ValueError(f"OHLCV CSV missing required column: {col}")

    ts_key = None
    for candidate in ("timestamp", "date", "datetime", "time"):
        if candidate in field_map:
            ts_key = field_map[candidate]
            break
    if ts_key is None:
        raise ValueError("OHLCV CSV missing timestamp/date column")

    volume_key = field_map.get("volume")
    bars: list[Bar] = []
    for row in reader:
        ts = _parse_date(str(row[ts_key]).strip())
        bars.append(
            Bar(
                timestamp=ts,
                open=float(row[field_map["open"]]),
                high=float(row[field_map["high"]]),
                low=float(row[field_map["low"]]),
                close=float(row[field_map["close"]]),
                volume=float(row[volume_key]) if volume_key else 0.0,
            )
        )
    if not bars:
        raise ValueError("OHLCV CSV contained no data rows")
    return bars


def load_bars_from_s3(s3_client: Any, bucket: str, key: str) -> list[Bar]:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return parse_ohlcv_csv(body)


def _filter_bars(bars: Sequence[Bar], start_date: str, end_date: str) -> list[Bar]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    filtered = [bar for bar in bars if start <= bar.timestamp <= end]
    if not filtered:
        raise ValueError("no bars in the requested date window")
    return filtered


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


def _signals_momentum_crossover(
    closes: list[float],
    fast_period: int,
    slow_period: int,
) -> list[int]:
    """Target position after each bar close (0 flat / 1 long)."""
    fast = _sma(closes, fast_period)
    slow = _sma(closes, slow_period)
    signals = [0] * len(closes)
    position = 0
    for i in range(1, len(closes)):
        if fast[i] is None or slow[i] is None or fast[i - 1] is None or slow[i - 1] is None:
            signals[i] = position
            continue
        prev_diff = (fast[i - 1] or 0.0) - (slow[i - 1] or 0.0)
        curr_diff = (fast[i] or 0.0) - (slow[i] or 0.0)
        if prev_diff <= 0 < curr_diff:
            position = 1
        elif prev_diff >= 0 > curr_diff:
            position = 0
        signals[i] = position
    return signals


def _signals_mean_reversion(
    closes: list[float],
    lookback: int,
    entry_z: float,
    exit_z: float,
) -> list[int]:
    signals = [0] * len(closes)
    position = 0
    for i in range(lookback, len(closes)):
        # Window ends at i-1 so the signal at bar i uses only prior closes
        window = closes[i - lookback : i]
        mean = sum(window) / lookback
        variance = sum((x - mean) ** 2 for x in window) / lookback
        std = math.sqrt(variance) if variance > 0 else 0.0
        if std < 1e-12:
            signals[i] = position
            continue
        z = (closes[i] - mean) / std
        if position == 0 and z <= -abs(entry_z):
            position = 1
        elif position == 1 and z >= -abs(exit_z):
            position = 0
        signals[i] = position
    return signals


def _apply_slippage(price: float, *, side: Literal["buy", "sell"], slippage_bps: float) -> float:
    slip = slippage_bps / 10_000.0
    if side == "buy":
        return price * (1.0 + slip)
    return price * (1.0 - slip)


def _commission(notional: float, commission_bps: float) -> float:
    return abs(notional) * (commission_bps / 10_000.0)


def _simulate(
    bars: list[Bar],
    signals: list[int],
    initial_capital: float,
    commission_bps: float,
    slippage_bps: float,
) -> tuple[list[float], list[float]]:
    """
    Simulate fills with no look-ahead bias.

    Signal generated at bar t (close) executes at bar t+1 open with slippage.
    Commission is charged on the fill notional in basis points.
    """
    cash = float(initial_capital)
    shares = 0.0
    equity_curve: list[float] = []
    trade_pnls: list[float] = []
    entry_price = 0.0
    pending_target: int | None = None

    for i, bar in enumerate(bars):
        # Execute yesterday's signal at today's open
        if pending_target is not None:
            target = pending_target
            pending_target = None
            if target == 1 and shares == 0.0 and cash > 0:
                fill = _apply_slippage(bar.open, side="buy", slippage_bps=slippage_bps)
                if fill > 0:
                    # Spend cash net of commission
                    max_notional = cash / (1.0 + commission_bps / 10_000.0)
                    shares = max_notional / fill
                    notional = shares * fill
                    fee = _commission(notional, commission_bps)
                    cash -= notional + fee
                    entry_price = fill
            elif target == 0 and shares > 0.0:
                fill = _apply_slippage(bar.open, side="sell", slippage_bps=slippage_bps)
                notional = shares * fill
                fee = _commission(notional, commission_bps)
                proceeds = notional - fee
                trade_pnls.append(proceeds - (shares * entry_price))
                cash += proceeds
                shares = 0.0
                entry_price = 0.0

        # Mark-to-market at close, then queue next-bar target from today's signal
        equity_curve.append(round(cash + shares * bar.close, 6))
        if i < len(bars) - 1:
            pending_target = signals[i]

    # Flatten any residual position at final close (no future bar to trade)
    if shares > 0.0:
        fill = _apply_slippage(bars[-1].close, side="sell", slippage_bps=slippage_bps)
        notional = shares * fill
        fee = _commission(notional, commission_bps)
        proceeds = notional - fee
        trade_pnls.append(proceeds - (shares * entry_price))
        cash += proceeds
        shares = 0.0
        equity_curve[-1] = round(cash, 6)

    return equity_curve, trade_pnls


def _build_signals(config: BacktestConfig, closes: list[float]) -> list[int]:
    params = config.parameters
    if config.strategy_type == "MOMENTUM_CROSSOVER":
        fast = int(params.get("fastPeriod", params.get("fast_period", 10)))
        slow = int(params.get("slowPeriod", params.get("slow_period", 50)))
        if fast >= slow:
            raise ValueError("fastPeriod must be less than slowPeriod")
        return _signals_momentum_crossover(closes, fast, slow)
    if config.strategy_type == "MEAN_REVERSION":
        lookback = int(params.get("lookbackWindow", params.get("lookback", 20)))
        entry_z = float(params.get("entryZScore", params.get("entryZ", 1.5)))
        exit_z = float(params.get("exitZScore", params.get("exitZ", 0.25)))
        return _signals_mean_reversion(closes, lookback, entry_z, exit_z)
    raise ValueError(f"Unsupported strategyType: {config.strategy_type}")


def run_backtest(
    config: BacktestConfig,
    *,
    s3_client: Any | None = None,
    datasets_bucket: str | None = None,
) -> BacktestResult:
    """
    Execute a strategy backtest.

    Loads OHLCV from S3 unless ``use_synthetic_data`` is true (tests / fallback).
    """
    if config.use_synthetic_data:
        bars = generate_ohlcv(config.symbol, config.start_date, config.end_date)
    else:
        if s3_client is None or not datasets_bucket or not config.dataset_key:
            raise ValueError(
                "S3 client, datasets_bucket, and dataset_key are required "
                "unless useSyntheticData is true"
            )
        bars = load_bars_from_s3(s3_client, datasets_bucket, config.dataset_key)
        bars = _filter_bars(bars, config.start_date, config.end_date)

    closes = [bar.close for bar in bars]
    signals = _build_signals(config, closes)
    equity_curve, trade_pnls = _simulate(
        bars,
        signals,
        config.initial_capital,
        config.commission_bps,
        config.slippage_bps,
    )

    periods = int(_PERIODS_PER_YEAR.get(config.timeframe, 252))
    metrics = compute_metrics(
        equity_curve,
        trade_pnls,
        config.initial_capital,
        periods_per_year=periods,
    )

    # Downsample equity curve for optional artifact payloads
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
        trade_count=metrics.totalTrades,
        bars_processed=len(bars),
    )
