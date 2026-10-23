# Cloud Backtesting Service

Production-ready serverless quantitative backtesting platform on AWS. Submit strategy jobs through a REST API; a Python worker executes event-driven backtests (S3 OHLCV or synthetic data) and persists metrics to DynamoDB.

## Architecture

```text
Client
  │  POST /backtests  ·  GET /backtests/{jobId}
  ▼
API Gateway (REST API)
  ▼
API Lambda (TypeScript / Node.js 22 / arm64)
  ├── DynamoDB Jobs table (PENDING → RUNNING → COMPLETED|FAILED + TTL)
  └── SQS BacktestQueue ──► BacktestDLQ
          ▼
     Worker Lambda (Python 3.12 / arm64)
          ├── Idempotent claim (UpdateItem ConditionExpression)
          ├── Engine (MEAN_REVERSION / MOMENTUM_CROSSOVER, t+1 fills)
          └── S3 DatasetsBucket (datasets/{symbol}/{timeframe}.csv)

CloudWatch: DLQDepthAlarm · WorkerErrorAlarm · ApiLatencyAlarm
```

### Components

| Resource | Purpose |
| --- | --- |
| `RestApi` | `POST /backtests`, `GET /backtests/{jobId}` |
| `ApiHandler` | Zod validation, creates `PENDING` jobs, enqueues SQS |
| `BacktestQueue` / `BacktestDLQ` | Decouples API from compute; poison-message isolation |
| `BacktestWorker` | Runs strategies, Decimal metrics, partial batch failures |
| `JobsTable` | Job lifecycle + TTL (`ttl`) |
| `DatasetsBucket` | Private, SSL-only OHLCV CSVs |
| `MonitoringConstruct` | DLQ / worker error / API p99 latency alarms |

## Strategies

| `strategyType` | Parameters | Behavior |
| --- | --- | --- |
| `MOMENTUM_CROSSOVER` | `fastPeriod`, `slowPeriod` | Long when fast SMA crosses above slow |
| `MEAN_REVERSION` | `lookbackWindow`, `entryZScore`, `exitZScore` | Buy on deep negative z-score, exit near mean |

Signals at bar *t* execute at bar *t+1* open with `commissionBps` / `slippageBps`. Pass `useSyntheticData: true` in the SQS payload for deterministic tests without S3 data.

## API

### `POST /backtests`

```json
{
  "symbol": "SPY",
  "timeframe": "1d",
  "startDate": "2020-01-01",
  "endDate": "2023-12-31",
  "strategyType": "MOMENTUM_CROSSOVER",
  "parameters": { "fastPeriod": 10, "slowPeriod": 50 },
  "initialCapital": 100000,
  "commissionBps": 1,
  "slippageBps": 2
}
```

Response `202 Accepted`:

```json
{ "jobId": "...", "status": "PENDING", "pollUrl": "/backtests/{jobId}" }
```

### `GET /backtests/{jobId}`

Returns job status, `strategyConfig`, `datasetKey`, and `results` when `COMPLETED` (or `errorMessage` when `FAILED`). `404` if unknown.

## Prerequisites

- Node.js 22+
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
pytest -q
```

## Deploy

```bash
npm run build
npx cdk bootstrap   # once per account/region
npx cdk deploy
```

Stack outputs include `ApiUrl`, `JobsTableName`, `DatasetsBucketName`, and `BacktestQueueUrl`.

## CI/CD

`.github/workflows/ci-cd.yml`:

1. `test-api-and-cdk` — Node 22, `npm test`, `npx cdk synth`
2. `test-python-worker` — Python 3.12, `pytest`
3. `deploy` — on `main` after both pass; AWS OIDC via `AWS_ROLE_ARN`, then `npx cdk deploy --require-approval never`

## Project layout

```text
bin/app.ts                         CDK app entry
lib/backtesting-stack.ts           Core infrastructure
lib/constructs/monitoring-construct.ts
services/api/                      TypeScript API Lambda
services/worker/                   Python worker + engine
test/backtesting-stack.test.ts     CDK assertions
.github/workflows/ci-cd.yml        CI + OIDC deploy
```

## Security notes

- S3 blocks public access and enforces SSL
- Lambdas use least-privilege IAM grants from CDK
- SQS DLQ with `maxReceiveCount=3`; visibility timeout ≥ 6× worker timeout
- Worker claims jobs atomically; duplicate SQS deliveries are acked without re-running
