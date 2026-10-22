"""Idempotent job execution helpers backed by DynamoDB conditional updates."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class IdempotencyConflictError(Exception):
    """Raised when a job is already running or completed."""


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
        Transition QUEUED -> RUNNING atomically.

        Idempotent success path: if the job is already SUCCEEDED, return it
        without reprocessing. Concurrent RUNNING/FAILED conflicts raise.
        """
        existing = self.get_job(job_id)
        if existing is None:
            raise KeyError(f"Job not found: {job_id}")

        status = existing.get("status")
        if status == "SUCCEEDED":
            logger.info("Skipping already-succeeded job", extra={"jobId": job_id})
            return existing
        if status == "RUNNING":
            raise IdempotencyConflictError(f"Job already running: {job_id}")
        if status == "FAILED":
            # Allow retry from FAILED by transitioning back to RUNNING
            pass
        elif status != "QUEUED":
            raise IdempotencyConflictError(f"Unexpected job status {status}: {job_id}")

        try:
            response = self._table.update_item(
                Key={"jobId": job_id},
                UpdateExpression="SET #s = :running, updatedAt = :ts",
                ConditionExpression="#s IN (:queued, :failed)",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":running": "RUNNING",
                    ":queued": "QUEUED",
                    ":failed": "FAILED",
                    ":ts": self._now(),
                },
                ReturnValues="ALL_NEW",
            )
            return response["Attributes"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                latest = self.get_job(job_id) or existing
                if latest.get("status") == "SUCCEEDED":
                    return latest
                raise IdempotencyConflictError(
                    f"Unable to claim job {job_id}; status={latest.get('status')}"
                ) from exc
            raise

    def mark_succeeded(
        self,
        job_id: str,
        *,
        metrics: dict[str, float],
        result_s3_key: str,
    ) -> dict[str, Any]:
        response = self._table.update_item(
            Key={"jobId": job_id},
            UpdateExpression=(
                "SET #s = :succeeded, updatedAt = :ts, #m = :metrics, "
                "resultS3Key = :key REMOVE errorMessage"
            ),
            ConditionExpression="#s = :running",
            ExpressionAttributeNames={"#s": "status", "#m": "metrics"},
            ExpressionAttributeValues={
                ":succeeded": "SUCCEEDED",
                ":running": "RUNNING",
                ":ts": self._now(),
                ":metrics": metrics,
                ":key": result_s3_key,
            },
            ReturnValues="ALL_NEW",
        )

        return response["Attributes"]

    def mark_failed(self, job_id: str, error_message: str) -> dict[str, Any]:
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
