"""Unit tests for the backtesting worker, engine, and idempotency helpers."""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

WORKER_ROOT = Path(__file__).resolve().parents[1]
if str(WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKER_ROOT))

from engine.backtester import (  # noqa: E402
    BacktestConfig,
    generate_ohlcv,
    parse_ohlcv_csv,
    run_backtest,
)
from engine.metrics import compute_metrics  # noqa: E402
from idempotency import IdempotencyConflictError, JobStore, build_execution_token  # noqa: E402
import handler as worker_handler  # noqa: E402


SAMPLE_CSV = """date,open,high,low,close,volume
2022-01-03,100,101,99,100.5,1000
2022-01-04,100.5,102,100,101.5,1100
2022-01-05,101.5,103,101,102.0,1200
2022-01-06,102.0,104,101.5,103.5,1300
2022-01-07,103.5,105,103,104.0,1400
2022-01-10,104.0,106,103.5,105.5,1500
2022-01-11,105.5,107,105,106.0,1600
2022-01-12,106.0,108,105.5,107.5,1700
2022-01-13,107.5,109,107,108.0,1800
2022-01-14,108.0,110,107.5,109.5,1900
2022-01-18,109.5,111,109,110.0,2000
2022-01-19,110.0,112,109.5,111.5,2100
2022-01-20,111.5,113,111,112.0,2200
2022-01-21,112.0,114,111.5,113.5,2300
2022-01-24,113.5,115,113,114.0,2400
2022-01-25,114.0,116,113.5,115.5,2500
2022-01-26,115.5,117,115,116.0,2600
2022-01-27,116.0,118,115.5,117.5,2700
2022-01-28,117.5,119,117,118.0,2800
2022-01-31,118.0,120,117.5,119.5,2900
2022-02-01,119.5,121,119,120.0,3000
2022-02-02,120.0,122,119.5,121.5,3100
2022-02-03,121.5,123,121,122.0,3200
2022-02-04,122.0,124,121.5,123.5,3300
2022-02-07,123.5,125,123,124.0,3400
2022-02-08,124.0,126,123.5,125.5,3500
2022-02-09,125.5,127,125,126.0,3600
2022-02-10,126.0,128,125.5,127.5,3700
2022-02-11,127.5,129,127,128.0,3800
2022-02-14,128.0,130,127.5,129.5,3900
"""


@pytest.fixture
def aws_env(monkeypatch: pytest.MonkeyPatch):
    with mock_aws():
        region = "us-east-1"
        monkeypatch.setenv("AWS_DEFAULT_REGION", region)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")

        dynamodb = boto3.resource("dynamodb", region_name=region)
        table = dynamodb.create_table(
            TableName="BacktestJobs",
            KeySchema=[{"AttributeName": "jobId", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "jobId", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        s3 = boto3.client("s3", region_name=region)
        bucket = "backtest-datasets-test"
        s3.create_bucket(Bucket=bucket)
        s3.put_object(
            Bucket=bucket,
            Key="datasets/SPY/1d.csv",
            Body=SAMPLE_CSV.encode("utf-8"),
        )

        monkeypatch.setenv("JOBS_TABLE_NAME", "BacktestJobs")
        monkeypatch.setenv("DATASETS_BUCKET_NAME", bucket)

        worker_handler._job_store = None
        worker_handler._s3 = None

        yield {"table": table, "bucket": bucket, "s3": s3, "dynamodb": dynamodb}


def _pending_job(job_id: str = "job-1", **overrides) -> dict:
    item = {
        "jobId": job_id,
        "status": "PENDING",
        "strategyConfig": {
            "symbol": "SPY",
            "timeframe": "1d",
            "startDate": "2022-01-01",
            "endDate": "2022-02-28",
            "strategyType": "MOMENTUM_CROSSOVER",
            "parameters": {"fastPeriod": 3, "slowPeriod": 8},
            "initialCapital": 100_000,
            "commissionBps": 1,
            "slippageBps": 2,
        },
        "datasetKey": "datasets/SPY/1d.csv",
        "createdAt": "2022-01-01T00:00:00+00:00",
        "updatedAt": "2022-01-01T00:00:00+00:00",
        "ttl": Decimal("9999999999"),
    }
    item.update(overrides)
    return item


def test_generate_ohlcv_is_deterministic():
    a = generate_ohlcv("AAPL", "2020-01-01", "2020-03-31")
    b = generate_ohlcv("AAPL", "2020-01-01", "2020-03-31")
    assert len(a) == len(b)
    assert a[0].close == b[0].close
    assert a[-1].close == b[-1].close
    assert a[0].timestamp.weekday() < 5


def test_generate_ohlcv_rejects_inverted_window():
    with pytest.raises(ValueError):
        generate_ohlcv("AAPL", "2020-06-01", "2020-01-01")


def test_parse_ohlcv_csv():
    bars = parse_ohlcv_csv(SAMPLE_CSV)
    assert len(bars) == 30
    assert bars[0].open == 100.0
    assert bars[-1].close == 129.5


def test_momentum_crossover_requires_fast_lt_slow():
    with pytest.raises(ValueError, match="fastPeriod"):
        run_backtest(
            BacktestConfig(
                strategy_type="MOMENTUM_CROSSOVER",
                symbol="SPY",
                timeframe="1d",
                start_date="2022-01-01",
                end_date="2022-06-30",
                parameters={"fastPeriod": 50, "slowPeriod": 10},
                use_synthetic_data=True,
            )
        )


def test_momentum_crossover_synthetic_runs():
    result = run_backtest(
        BacktestConfig(
            strategy_type="MOMENTUM_CROSSOVER",
            symbol="SPY",
            timeframe="1d",
            start_date="2022-01-01",
            end_date="2022-12-31",
            parameters={"fastPeriod": 10, "slowPeriod": 40},
            commission_bps=1.0,
            slippage_bps=2.0,
            use_synthetic_data=True,
        )
    )
    assert result.bars_processed > 200
    assert result.metrics.totalTrades >= 0
    assert "annualizedSharpe" in result.metrics.to_dict()
    assert "annualizedSortino" in result.metrics.to_dict()


def test_mean_reversion_synthetic_runs():
    result = run_backtest(
        BacktestConfig(
            strategy_type="MEAN_REVERSION",
            symbol="QQQ",
            timeframe="1d",
            start_date="2023-01-01",
            end_date="2023-06-30",
            parameters={
                "lookbackWindow": 15,
                "entryZScore": 1.2,
                "exitZScore": 0.2,
            },
            use_synthetic_data=True,
        )
    )
    assert result.bars_processed > 50
    assert 0.0 <= result.metrics.winRatePct <= 100.0


def test_s3_dataset_backtest(aws_env):
    result = run_backtest(
        BacktestConfig(
            strategy_type="MOMENTUM_CROSSOVER",
            symbol="SPY",
            timeframe="1d",
            start_date="2022-01-01",
            end_date="2022-02-28",
            parameters={"fastPeriod": 3, "slowPeriod": 8},
            dataset_key="datasets/SPY/1d.csv",
            commission_bps=5.0,
            slippage_bps=5.0,
        ),
        s3_client=aws_env["s3"],
        datasets_bucket=aws_env["bucket"],
    )
    assert result.bars_processed == 30
    assert result.metrics.totalReturnPct is not None


def test_compute_metrics_empty_curve():
    metrics = compute_metrics([], [], initial_capital=10_000)
    assert metrics.totalReturnPct == 0.0
    assert metrics.totalTrades == 0
    assert metrics.profitFactor == 0.0


def test_compute_metrics_drawdown_sharpe_sortino():
    equity = [100.0, 110.0, 105.0, 120.0]
    metrics = compute_metrics(equity, [10.0, -5.0, 15.0], initial_capital=100.0)
    assert metrics.maxDrawdownPct == pytest.approx(((110.0 - 105.0) / 110.0) * 100.0)
    assert metrics.winRatePct == pytest.approx(2 / 3 * 100.0)
    assert metrics.totalReturnPct == pytest.approx(20.0)
    assert metrics.profitFactor == pytest.approx(25.0 / 5.0)
    assert metrics.totalTrades == 3
    decimals = metrics.to_decimals()
    assert isinstance(decimals["annualizedSharpe"], Decimal)


def test_build_execution_token():
    assert build_execution_token("abc", 2) == "abc:2"


def test_job_store_mark_running_and_complete(aws_env):
    store = JobStore("BacktestJobs", dynamodb_resource=aws_env["dynamodb"])
    aws_env["table"].put_item(Item=_pending_job("job-1"))

    running = store.mark_running("job-1")
    assert running["status"] == "RUNNING"

    with pytest.raises(IdempotencyConflictError):
        store.mark_running("job-1")

    done = store.mark_completed(
        "job-1",
        results={"totalReturnPct": Decimal("10.5"), "totalTrades": Decimal("3")},
    )
    assert done["status"] == "COMPLETED"
    assert done["results"]["totalReturnPct"] == Decimal("10.5")

    with pytest.raises(IdempotencyConflictError, match="completed"):
        store.mark_running("job-1")


def test_job_store_allows_retry_from_failed(aws_env):
    store = JobStore("BacktestJobs", dynamodb_resource=aws_env["dynamodb"])
    aws_env["table"].put_item(Item=_pending_job("job-retry", status="FAILED"))

    running = store.mark_running("job-retry")
    assert running["status"] == "RUNNING"


def test_process_job_end_to_end_synthetic(aws_env):
    aws_env["table"].put_item(Item=_pending_job("job-42"))

    outcome = worker_handler.process_job(
        {
            "jobId": "job-42",
            "datasetKey": "datasets/SPY/1d.csv",
            "useSyntheticData": True,
            "strategyConfig": {
                "symbol": "SPY",
                "timeframe": "1d",
                "startDate": "2022-01-01",
                "endDate": "2022-02-28",
                "strategyType": "MOMENTUM_CROSSOVER",
                "parameters": {"fastPeriod": 3, "slowPeriod": 8},
                "initialCapital": 50_000,
                "commissionBps": 1,
                "slippageBps": 1,
            },
        }
    )

    assert outcome["status"] == "COMPLETED"
    assert "annualizedSharpe" in outcome["results"]

    item = aws_env["table"].get_item(Key={"jobId": "job-42"})["Item"]
    assert item["status"] == "COMPLETED"
    assert isinstance(item["results"]["totalReturnPct"], Decimal)


def test_process_job_from_s3(aws_env):
    aws_env["table"].put_item(Item=_pending_job("job-s3"))

    outcome = worker_handler.process_job(
        {
            "jobId": "job-s3",
            "datasetKey": "datasets/SPY/1d.csv",
            "strategyConfig": {
                "symbol": "SPY",
                "timeframe": "1d",
                "startDate": "2022-01-01",
                "endDate": "2022-02-28",
                "strategyType": "MEAN_REVERSION",
                "parameters": {
                    "lookbackWindow": 5,
                    "entryZScore": 1.0,
                    "exitZScore": 0.25,
                },
                "initialCapital": 100_000,
                "commissionBps": 0,
                "slippageBps": 0,
            },
        }
    )
    assert outcome["status"] == "COMPLETED"


def test_handler_partial_batch_failures(aws_env):
    aws_env["table"].put_item(Item=_pending_job("good-job"))

    event = {
        "Records": [
            {
                "messageId": "msg-good",
                "body": json.dumps(
                    {
                        "jobId": "good-job",
                        "datasetKey": "datasets/SPY/1d.csv",
                        "useSyntheticData": True,
                        "strategyConfig": {
                            "symbol": "SPY",
                            "timeframe": "1d",
                            "startDate": "2022-01-01",
                            "endDate": "2022-02-28",
                            "strategyType": "MOMENTUM_CROSSOVER",
                            "parameters": {"fastPeriod": 3, "slowPeriod": 8},
                            "initialCapital": 10_000,
                        },
                    }
                ),
            },
            {
                "messageId": "msg-bad",
                "body": json.dumps({"jobId": "missing-job"}),
            },
        ]
    }

    result = worker_handler.handler(event, None)
    assert {"itemIdentifier": "msg-bad"} in result["batchItemFailures"]
    assert {"itemIdentifier": "msg-good"} not in result["batchItemFailures"]

    failed = aws_env["table"].get_item(Key={"jobId": "missing-job"}).get("Item")
    # missing job raises before mark_failed; ensure good job completed
    good = aws_env["table"].get_item(Key={"jobId": "good-job"})["Item"]
    assert good["status"] == "COMPLETED"
    assert failed is None


def test_handler_marks_failed_and_reports_batch_item(aws_env, monkeypatch):
    aws_env["table"].put_item(Item=_pending_job("fail-job"))

    def boom(*_args, **_kwargs):
        raise RuntimeError("transient engine failure")

    monkeypatch.setattr(worker_handler, "run_backtest", boom)

    event = {
        "Records": [
            {
                "messageId": "msg-fail",
                "body": json.dumps(
                    {
                        "jobId": "fail-job",
                        "useSyntheticData": True,
                        "strategyConfig": {
                            "symbol": "SPY",
                            "timeframe": "1d",
                            "startDate": "2022-01-01",
                            "endDate": "2022-02-28",
                            "strategyType": "MOMENTUM_CROSSOVER",
                            "parameters": {"fastPeriod": 3, "slowPeriod": 8},
                            "initialCapital": 10_000,
                        },
                    }
                ),
            }
        ]
    }

    result = worker_handler.handler(event, None)
    assert result["batchItemFailures"] == [{"itemIdentifier": "msg-fail"}]
    item = aws_env["table"].get_item(Key={"jobId": "fail-job"})["Item"]
    assert item["status"] == "FAILED"
    assert "transient engine failure" in item["errorMessage"]


def test_handler_acks_duplicate_completed(aws_env):
    aws_env["table"].put_item(Item=_pending_job("done-job", status="COMPLETED"))

    event = {
        "Records": [
            {
                "messageId": "msg-dup",
                "body": json.dumps(
                    {
                        "jobId": "done-job",
                        "useSyntheticData": True,
                        "strategyConfig": {
                            "symbol": "SPY",
                            "timeframe": "1d",
                            "startDate": "2022-01-01",
                            "endDate": "2022-02-28",
                            "strategyType": "MOMENTUM_CROSSOVER",
                            "parameters": {"fastPeriod": 3, "slowPeriod": 8},
                            "initialCapital": 10_000,
                        },
                    }
                ),
            }
        ]
    }

    result = worker_handler.handler(event, None)
    assert result["batchItemFailures"] == []
