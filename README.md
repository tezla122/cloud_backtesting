# Cloud Backtesting Service

Production-ready serverless quantitative backtesting platform on AWS. Submit strategy jobs through an HTTP API; a Python worker executes deterministic event-driven backtests and persists metrics to DynamoDB and S3.

## Architecture

```text
Client
  │  POST /backtests  ·  GET /backtests/{jobId}  ·  GET /health
  ▼
API Gateway (HTTP API)
  ▼
API Lambda (TypeScript / Node.js 20)
  ├── DynamoDB Jobs table (status + idempotency GSI)
  └── SQS Job Queue ──► DLQ
          ▼
     Worker Lambda (Python 3.12)
          ├── Backtesting engine (SMA / buy & hold / mean reversion)
          ├── Idempotent claim (QUEUED → RUNNING → SUCCEEDED|FAILED)
          └── S3 results bucket (JSON artifacts)

CloudWatch alarms + ops dashboard via MonitoringConstruct
```

### Components

| Resource | Purpose |
| --- | --- |
| `HttpApi` | Public HTTP interface |
| `ApiFunction` | Validates payloads (Zod), creates jobs, enqueues work |
| `JobQueue` / DLQ | Decouples API latency from compute; poison-message isolation |
| `WorkerFunction` | Runs strategies, writes results, reports partial batch failures |
| `JobsTable` | Job lifecycle + idempotency key GSI + TTL |
| `ResultsBucket` | Versioned, SSL-only result objects (90-day expiry) |
| `MonitoringConstruct` | Error/latency/DLQ/age alarms + CloudWatch dashboard |

## Strategies

| Name | Parameters | Behavior |
| --- | --- | --- |
| `sma_crossover` | `fastPeriod`, `slowPeriod` | Long when fast SMA crosses above slow |
| `buy_and_hold` | — | Fully invested after first bar |
| `mean_reversion` | `lookback`, `entryZ`, `exitZ` | Buy on deep negative z-score, exit near mean |

Market data is a **deterministic synthetic OHLCV** series seeded by symbol so backtests are reproducible without an external market-data dependency.

## API

### `POST /backtests`

```json
{
  "strategy": "sma_crossover",
  "symbol": "AAPL",
  "startDate": "2020-01-01",
  "endDate": "2023-12-31",
  "initialCapital": 100000,
  "parameters": { "fastPeriod": 10, "slowPeriod": 50 },
  "idempotencyKey": "550e8400-e29b-41d4-a716-446655440000"
}
```

Response `202`:

```json
{ "jobId": "...", "status": "QUEUED", "createdAt": "..." }
```

Pass `Idempotency-Key` as a header or `idempotencyKey` in the body to safely retry submissions.

### `GET /backtests/{jobId}`

Returns job status, parameters, metrics (when succeeded), and `resultS3Key`.

### `GET /health`

Liveness probe for the API Lambda.

## Prerequisites

- Node.js 20+
- Python 3.12+
- AWS CDK CLI (`npx cdk`)
- AWS credentials configured for deploy

## Local setup

```bash
cd cloud-backtesting-service

# TypeScript / CDK
npm install
npm --prefix services/api install
npm test

# Python worker
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r services/worker/requirements.txt
pytest -q services/worker/tests
```

## Deploy

```bash
npm run build
npx cdk bootstrap   # once per account/region
npx cdk deploy
```

Stack outputs include `HttpApiUrl`, `JobsTableName`, `ResultsBucketName`, and `JobQueueUrl`.

## CI/CD

`.github/workflows/ci-cd.yml` runs TypeScript tests + `cdk synth`, Python `pytest`, and (on `main`) OIDC deploy via `AWS_ROLE_ARN`.

## Project layout

```text
bin/app.ts                         CDK app entry
lib/backtesting-stack.ts           Core infrastructure
lib/constructs/monitoring-construct.ts
services/api/                      TypeScript API Lambda
services/worker/                   Python worker + engine
test/backtesting-stack.test.ts     CDK assertions
```

## Security notes

- S3 blocks public access and enforces SSL
- DynamoDB uses AWS-managed encryption and PITR
- Lambdas use least-privilege IAM grants from CDK
- SQS uses managed encryption with a DLQ (`maxReceiveCount=3`)
- X-Ray tracing is enabled on both functions
