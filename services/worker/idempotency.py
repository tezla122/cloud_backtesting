"""Idempotent job execution helpers backed by DynamoDB conditional updates."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class IdempotencyConflictError(Exception):
    """
    Raised when a duplicate SQS delivery should be acknowledged without
    re-running the backtest (job already RUNNING or COMPLETED).
    """


class JobStore:
    """Thin DynamoDB repository for backtest job lifecycle transitions."""

    def __init__(self, table_name: str, dynamodb_resource: Any | None = None) -> None:
        resource = dynamodb_resource or boto3.resource("dynamodb")
        self._table = resource.Table(table_name)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        response = self._table.get_item(Key={"jobId": job_id})
        return response.get("Item")

    def mark_running(self, job_id: str) -> dict[str, Any]:
        """
        Atomically transition PENDING|FAILED → RUNNING.

        ConditionExpression only allows claiming when status is PENDING or
        FAILED (safe retries) and explicitly excludes COMPLETED. Duplicate
        deliveries that race a COMPLETED/RUNNING job raise
        IdempotencyConflictError so the caller can ack the SQS message.
        """
        existing = self.get_job(job_id)
        if existing is None:
            raise KeyError(f"Job not found: {job_id}")

        status = existing.get("status")
        if status == "COMPLETED":
            logger.info(
                "Duplicate delivery for completed job; skipping",
                extra={"jobId": job_id, "status": status},
            )
            raise IdempotencyConflictError(f"Job already completed: {job_id}")
        if status == "RUNNING":
            logger.info(
                "Duplicate delivery for running job; skipping",
                extra={"jobId": job_id, "status": status},
            )
            raise IdempotencyConflictError(f"Job already running: {job_id}")

        try:
            response = self._table.update_item(
                Key={"jobId": job_id},
                UpdateExpression="SET #s = :running, updatedAt = :ts REMOVE errorMessage",
                # PENDING or FAILED retries only; never claim COMPLETED
                ConditionExpression=(
                    "(#s = :pending OR #s = :failed) AND #s <> :completed"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":running": "RUNNING",
                    ":pending": "PENDING",
                    ":failed": "FAILED",
                    ":completed": "COMPLETED",
                    ":ts": self._now(),
                },
                ReturnValues="ALL_NEW",
            )
            return response["Attributes"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                latest = self.get_job(job_id) or existing
                latest_status = latest.get("status")
                logger.info(
                    "Conditional claim failed; treating as duplicate delivery",
                    extra={"jobId": job_id, "status": latest_status},
                )
                raise IdempotencyConflictError(
                    f"Unable to claim job {job_id}; status={latest_status}"
                ) from exc
            raise

    def mark_completed(
        self,
        job_id: str,
        *,
        results: dict[str, Decimal],
    ) -> dict[str, Any]:
        """Persist Decimal metrics and transition RUNNING → COMPLETED."""
        response = self._table.update_item(
            Key={"jobId": job_id},
            UpdateExpression=(
                "SET #s = :completed, updatedAt = :ts, results = :results "
                "REMOVE errorMessage"
            ),
            ConditionExpression="#s = :running",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":completed": "COMPLETED",
                ":running": "RUNNING",
                ":ts": self._now(),
                ":results": results,
            },
            ReturnValues="ALL_NEW",
        )
        return response["Attributes"]

    def mark_failed(self, job_id: str, error_message: str) -> dict[str, Any]:
        """Mark the job FAILED with a truncated error message (safe for retries)."""
        response = self._table.update_item(
            Key={"jobId": job_id},
            UpdateExpression="SET #s = :failed, updatedAt = :ts, errorMessage = :err",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":failed": "FAILED",
                ":ts": self._now(),
                ":err": error_message[:1000],
            },
            ReturnValues="ALL_NEW",
        )
        return response["Attributes"]


def build_execution_token(job_id: str, attempt: int = 0) -> str:
    """Stable token used for logging / correlation across retries."""
    return f"{job_id}:{attempt}"
