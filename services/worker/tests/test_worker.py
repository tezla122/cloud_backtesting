"""Unit tests for the backtesting worker, engine, and idempotency helpers."""

from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

WORKER_ROOT = Path(__file__).resolve().parents[1]
if str(WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKER_ROOT))

from engine.backtester import BacktestConfig, generate_ohlcv, run_backtest  # noqa: E402
from engine.metrics import compute_metrics  # noqa: E402
from idempotency import IdempotencyConflictError, JobStore, build_execution_token  # noqa: E402
import handler as worker_handler  # noqa: E402


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
            AttributeDefinitions=[
                {"AttributeName": "jobId", "AttributeType": "S"},
                {"AttributeName": "idempotencyKey", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "IdempotencyKeyIndex",
                    "KeySchema": [{"AttributeName": "idempotencyKey", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()

        s3 = boto3.client("s3", region_name=region)
        bucket = "backtest-results-test"
        s3.create_bucket(Bucket=bucket)

        monkeypatch.setenv("JOBS_TABLE_NAME", "BacktestJobs")
        monkeypatch.setenv("RESULTS_BUCKET_NAME", bucket)
        monkeypatch.setenv("IDEMPOTENCY_TABLE_NAME", "BacktestJobs")

        # Reset cached clients between tests so they pick up moto + env vars
        worker_handler._job_store = None
        worker_handler._s3 = None

        yield {"table": table, "bucket": bucket, "s3": s3, "dynamodb": dynamodb}



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


def test_buy_and_hold_backtest_produces_metrics():
    result = run_backtest(
        BacktestConfig(
            strategy="buy_and_hold",
            symbol="MSFT",
            start_date="2021-01-01",
            end_date="2021-12-31",
            initial_capital=100_000,
        )
    )
    assert result.bars_processed > 200
    assert result.metrics.initial_capital == 100_000
    assert result.metrics.final_equity > 0
    assert isinstance(result.metrics.sharpe_ratio, float)
    assert len(result.equity_curve) > 0


def test_sma_crossover_requires_fast_lt_slow():
    with pytest.raises(ValueError, match="fastPeriod"):
        run_backtest(
            BacktestConfig(
                strategy="sma_crossover",
                symbol="SPY",
                start_date="2022-01-01",
                end_date="2022-06-30",
                parameters={"fastPeriod": 50, "slowPeriod": 10},
            )
        )


def test_sma_crossover_runs():
    result = run_backtest(
        BacktestConfig(
            strategy="sma_crossover",
            symbol="SPY",
            start_date="2022-01-01",
            end_date="2022-12-31",
            parameters={"fastPeriod": 10, "slowPeriod": 40},
        )
    )
    assert result.metrics.trade_count >= 0
    payload = result.to_dict()
    assert payload["strategy"] == "sma_crossover"
    assert "metrics" in payload


def test_mean_reversion_runs():
    result = run_backtest(
        BacktestConfig(
            strategy="mean_reversion",
            symbol="QQQ",
            start_date="2023-01-01",
            end_date="2023-06-30",
            parameters={"lookback": 15, "entryZ": 1.2, "exitZ": 0.2},
        )
    )
    assert result.bars_processed > 50
    assert 0.0 <= result.metrics.win_rate <= 1.0


def test_compute_metrics_empty_curve():
    metrics = compute_metrics([], [], initial_capital=10_000)
    assert metrics.final_equity == 10_000
    assert metrics.total_return == 0.0
    assert metrics.trade_count == 0


def test_compute_metrics_drawdown_and_sharpe():
    equity = [100.0, 110.0, 105.0, 120.0]
    metrics = compute_metrics(equity, [10.0, -5.0, 15.0], initial_capital=100.0)
    assert metrics.max_drawdown == pytest.approx((110.0 - 105.0) / 110.0)
    assert metrics.win_rate == pytest.approx(2 / 3)
    assert metrics.total_return == pytest.approx(0.2)


def test_build_execution_token():
    assert build_execution_token("abc", 2) == "abc:2"


def test_job_store_mark_running_and_succeed(aws_env):
    store = JobStore("BacktestJobs", dynamodb_resource=aws_env["dynamodb"])
    aws_env["table"].put_item(
        Item={
            "jobId": "job-1",
            "status": "QUEUED",
            "strategy": "buy_and_hold",
            "symbol": "AAPL",
            "startDate": "2020-01-01",
            "endDate": "2020-06-30",
            "initialCapital": Decimal("100000"),
            "parameters": {},
            "createdAt": "2020-01-01T00:00:00+00:00",
            "updatedAt": "2020-01-01T00:00:00+00:00",
        }
    )

    running = store.mark_running("job-1")
    assert running["status"] == "RUNNING"

    with pytest.raises(IdempotencyConflictError):
        store.mark_running("job-1")

    done = store.mark_succeeded(
        "job-1",
        metrics={"total_return": Decimal("0.1")},
        result_s3_key="results/job-1/backtest.json",
    )

    assert done["status"] == "SUCCEEDED"
    assert done["resultS3Key"] == "results/job-1/backtest.json"

    # Re-claim after success returns the succeeded record (idempotent skip)
    skipped = store.mark_running("job-1")
    assert skipped["status"] == "SUCCEEDED"


def test_process_job_end_to_end(aws_env):
    aws_env["table"].put_item(
        Item={
            "jobId": "job-42",
            "status": "QUEUED",
            "strategy": "buy_and_hold",
            "symbol": "AAPL",
            "startDate": "2021-01-01",
            "endDate": "2021-03-31",
            "initialCapital": Decimal("50000"),
            "parameters": {},
            "createdAt": "2021-01-01T00:00:00+00:00",
            "updatedAt": "2021-01-01T00:00:00+00:00",
        }
    )

    outcome = worker_handler.process_job(
        {
            "jobId": "job-42",
            "strategy": "buy_and_hold",
            "symbol": "AAPL",
            "startDate": "2021-01-01",
            "endDate": "2021-03-31",
            "initialCapital": 50_000,
            "parameters": {},
        }
    )

    assert outcome["status"] == "SUCCEEDED"
    assert outcome["resultS3Key"].startswith("results/job-42/")

    obj = aws_env["s3"].get_object(Bucket=aws_env["bucket"], Key=outcome["resultS3Key"])
    body = json.loads(obj["Body"].read().decode("utf-8"))
    assert body["symbol"] == "AAPL"
    assert "sharpe_ratio" in body["metrics"]

    item = aws_env["table"].get_item(Key={"jobId": "job-42"})["Item"]
    assert item["status"] == "SUCCEEDED"


def test_handler_partial_batch_failures(aws_env):
    aws_env["table"].put_item(
        Item={
            "jobId": "good-job",
            "status": "QUEUED",
            "strategy": "buy_and_hold",
            "symbol": "IBM",
            "startDate": "2020-01-01",
            "endDate": "2020-02-28",
            "initialCapital": Decimal("10000"),
            "parameters": {},
            "createdAt": "2020-01-01T00:00:00+00:00",
            "updatedAt": "2020-01-01T00:00:00+00:00",
        }
    )

    event = {
        "Records": [
            {
                "messageId": "msg-good",
                "body": json.dumps(
                    {
                        "jobId": "good-job",
                        "strategy": "buy_and_hold",
                        "symbol": "IBM",
                        "startDate": "2020-01-01",
                        "endDate": "2020-02-28",
                        "initialCapital": 10_000,
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
