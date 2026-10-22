"""SQS-triggered Lambda worker that runs quantitative backtests."""

from __future__ import annotations

import json
import logging
import os
import traceback
from decimal import Decimal
from typing import Any

import boto3

from engine.backtester import BacktestConfig, run_backtest
from idempotency import IdempotencyConflictError, JobStore

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

_s3 = None
_job_store: JobStore | None = None


def _get_s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def _get_job_store() -> JobStore:
    global _job_store
    if _job_store is None:
        table_name = os.environ.get("JOBS_TABLE_NAME", "")
        if not table_name:
            raise RuntimeError("JOBS_TABLE_NAME environment variable is required")
        _job_store = JobStore(table_name)
    return _job_store


def _results_bucket() -> str:
    bucket = os.environ.get("RESULTS_BUCKET_NAME", "")
    if not bucket:
        raise RuntimeError("RESULTS_BUCKET_NAME environment variable is required")
    return bucket



def _to_dynamo_numbers(metrics: dict[str, float]) -> dict[str, Decimal]:
    return {key: Decimal(str(value)) for key, value in metrics.items()}


def _parse_message(body: str) -> dict[str, Any]:
    payload = json.loads(body)
    required = ["jobId", "strategy", "symbol", "startDate", "endDate", "initialCapital"]
    missing = [field for field in required if field not in payload]
    if missing:
        raise ValueError(f"Missing required message fields: {', '.join(missing)}")
    return payload


def process_job(payload: dict[str, Any]) -> dict[str, Any]:
    """Process a single backtest job message. Pure-ish core used by tests."""
    job_id = payload["jobId"]
    store = _get_job_store()

    try:
        job = store.mark_running(job_id)
    except IdempotencyConflictError as exc:
        logger.warning("Idempotency conflict for %s: %s", job_id, exc)
        return {"jobId": job_id, "status": "SKIPPED", "reason": str(exc)}
    except KeyError as exc:
        logger.error("Unknown job %s: %s", job_id, exc)
        raise

    if job.get("status") == "SUCCEEDED" and job.get("resultS3Key"):
        return {
            "jobId": job_id,
            "status": "SUCCEEDED",
            "resultS3Key": job["resultS3Key"],
            "deduplicated": True,
        }

    try:
        config = BacktestConfig(
            strategy=payload["strategy"],
            symbol=str(payload["symbol"]).upper(),
            start_date=payload["startDate"],
            end_date=payload["endDate"],
            initial_capital=float(payload["initialCapital"]),
            parameters=dict(payload.get("parameters") or {}),
        )
        result = run_backtest(config)
        result_key = f"results/{job_id}/backtest.json"

        _get_s3().put_object(
            Bucket=_results_bucket(),
            Key=result_key,
            Body=json.dumps(result.to_dict()).encode("utf-8"),
            ContentType="application/json",
        )

        metrics = result.metrics.to_dict()
        store.mark_succeeded(
            job_id,
            metrics=_to_dynamo_numbers(metrics),
            result_s3_key=result_key,
        )

        logger.info(
            "Backtest succeeded",
            extra={
                "jobId": job_id,
                "sharpe": metrics.get("sharpe_ratio"),
                "bars": result.bars_processed,
            },
        )
        return {
            "jobId": job_id,
            "status": "SUCCEEDED",
            "resultS3Key": result_key,
            "metrics": metrics,
        }
    except Exception as exc:
        logger.error("Backtest failed for %s: %s", job_id, exc)
        logger.debug(traceback.format_exc())
        store.mark_failed(job_id, str(exc))
        raise


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """
    Lambda entrypoint for SQS partial-batch failure reporting.

    Returns {"batchItemFailures": [{"itemIdentifier": <messageId>}]} for
    messages that should be retried.
    """
    failures: list[dict[str, str]] = []
    records = event.get("Records", [])

    for record in records:
        message_id = record.get("messageId", "unknown")
        try:
            payload = _parse_message(record["body"])
            process_job(payload)
        except IdempotencyConflictError:
            # Another worker owns this job; do not retry this delivery.
            logger.warning("Dropping conflicting message %s", message_id)
        except Exception:
            logger.exception("Failed processing SQS message %s", message_id)
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}
