"""SQS-triggered Lambda worker that runs quantitative backtests."""

from __future__ import annotations

import json
import logging
import os
import traceback
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


def _datasets_bucket() -> str:
    bucket = os.environ.get("DATASETS_BUCKET_NAME", "")
    if not bucket:
        raise RuntimeError("DATASETS_BUCKET_NAME environment variable is required")
    return bucket


def _parse_message(body: str) -> dict[str, Any]:
    payload = json.loads(body)
    if "jobId" not in payload:
        raise ValueError("Missing required message field: jobId")
    if "strategyConfig" not in payload and "strategyType" not in payload:
        raise ValueError("Missing strategyConfig in message body")
    return payload


def _config_from_payload(payload: dict[str, Any]) -> BacktestConfig:
    """Build BacktestConfig from the API SQS envelope (or flattened test payload)."""
    strategy = payload.get("strategyConfig") or payload
    use_synthetic = bool(
        payload.get("useSyntheticData", strategy.get("useSyntheticData", False))
    )
    dataset_key = payload.get("datasetKey") or strategy.get("datasetKey")

    return BacktestConfig(
        strategy_type=strategy["strategyType"],
        symbol=str(strategy["symbol"]).upper(),
        timeframe=strategy.get("timeframe", "1d"),
        start_date=strategy["startDate"],
        end_date=strategy["endDate"],
        initial_capital=float(strategy.get("initialCapital", 100_000)),
        commission_bps=float(strategy.get("commissionBps", 0)),
        slippage_bps=float(strategy.get("slippageBps", 0)),
        parameters=dict(strategy.get("parameters") or {}),
        dataset_key=dataset_key,
        use_synthetic_data=use_synthetic,
    )


def process_job(payload: dict[str, Any]) -> dict[str, Any]:
    """Process a single backtest job message. Pure-ish core used by tests."""
    job_id = payload["jobId"]
    store = _get_job_store()

    try:
        job = store.mark_running(job_id)
    except IdempotencyConflictError as exc:
        # Duplicate delivery (RUNNING / COMPLETED) — ack without re-executing
        logger.warning(
            "Skipping duplicate delivery for %s: %s",
            job_id,
            exc,
            extra={"jobId": job_id, "event": "duplicate_delivery"},
        )
        return {"jobId": job_id, "status": "SKIPPED", "reason": str(exc)}
    except KeyError as exc:
        logger.error("Unknown job %s: %s", job_id, exc)
        raise

    try:
        config = _config_from_payload(payload)
        # Prefer datasetKey already stored on the job item when present
        if not config.dataset_key and job.get("datasetKey"):
            config = BacktestConfig(
                strategy_type=config.strategy_type,
                symbol=config.symbol,
                timeframe=config.timeframe,
                start_date=config.start_date,
                end_date=config.end_date,
                initial_capital=config.initial_capital,
                commission_bps=config.commission_bps,
                slippage_bps=config.slippage_bps,
                parameters=config.parameters,
                dataset_key=job.get("datasetKey"),
                use_synthetic_data=config.use_synthetic_data,
            )

        result = run_backtest(
            config,
            s3_client=None if config.use_synthetic_data else _get_s3(),
            datasets_bucket=None if config.use_synthetic_data else _datasets_bucket(),
        )
        results = result.metrics.to_decimals()
        store.mark_completed(job_id, results=results)

        logger.info(
            "Backtest completed",
            extra={
                "jobId": job_id,
                "event": "backtest_completed",
                "sharpe": float(result.metrics.annualizedSharpe),
                "bars": result.bars_processed,
            },
        )
        return {
            "jobId": job_id,
            "status": "COMPLETED",
            "results": result.metrics.to_dict(),
        }
    except Exception as exc:
        logger.error("Backtest failed for %s: %s", job_id, exc)
        logger.debug(traceback.format_exc())
        store.mark_failed(job_id, str(exc))
        raise


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """
    Lambda entrypoint for SQS partial-batch failure reporting.

    Transient / unexpected exceptions mark the job FAILED and return
    ``batchItemFailures`` so SQS retries up to maxReceiveCount, then DLQ.
    Duplicate idempotent deliveries are acknowledged (not retried).
    """
    failures: list[dict[str, str]] = []
    records = event.get("Records", [])

    for record in records:
        message_id = record.get("messageId", "unknown")
        try:
            payload = _parse_message(record["body"])
            process_job(payload)
        except IdempotencyConflictError:
            logger.warning(
                "Dropping conflicting message %s (duplicate delivery)",
                message_id,
            )
        except Exception:
            logger.exception("Failed processing SQS message %s", message_id)
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}
