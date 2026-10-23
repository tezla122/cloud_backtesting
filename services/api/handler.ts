import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';
import type {
  APIGatewayProxyEvent,
  APIGatewayProxyResult,
  Context,
} from 'aws-lambda';
import { randomUUID } from 'crypto';
import {
  CreateBacktestRequestSchema,
  buildDatasetKey,
  type BacktestJobRecord,
  type CreateBacktestResponse,
  type GetBacktestResponse,
  type StrategyConfig,
} from './schemas';

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});
const sqs = new SQSClient({});

const JOBS_TABLE_NAME = requiredEnv('JOBS_TABLE_NAME');
const QUEUE_URL = requiredEnv('QUEUE_URL');
const JOB_TTL_DAYS = 90;

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function jsonResponse(statusCode: number, body: unknown): APIGatewayProxyResult {
  return {
    statusCode,
    headers: {
      'content-type': 'application/json',
      'access-control-allow-origin': '*',
    },
    body: JSON.stringify(body),
  };
}

interface LogFields {
  level: 'DEBUG' | 'INFO' | 'WARN' | 'ERROR';
  correlationId: string;
  jobId?: string;
  event: string;
  latencyMs?: number;
  [key: string]: unknown;
}

function log(fields: LogFields): void {
  console.log(
    JSON.stringify({
      ...fields,
      timestamp: new Date().toISOString(),
      service: 'backtesting-api',
    }),
  );
}

function correlationIdFrom(event: APIGatewayProxyEvent): string {
  return (
    event.headers?.['x-correlation-id'] ??
    event.headers?.['X-Correlation-Id'] ??
    event.requestContext.requestId ??
    randomUUID()
  );
}

async function createBacktest(
  event: APIGatewayProxyEvent,
  correlationId: string,
): Promise<APIGatewayProxyResult> {
  const started = Date.now();

  if (!event.body) {
    log({
      level: 'WARN',
      correlationId,
      event: 'validation_failed',
      reason: 'missing_body',
      latencyMs: Date.now() - started,
    });
    return jsonResponse(400, { error: 'Request body is required' });
  }

  let raw: unknown;
  try {
    raw = JSON.parse(event.body);
  } catch {
    log({
      level: 'WARN',
      correlationId,
      event: 'validation_failed',
      reason: 'invalid_json',
      latencyMs: Date.now() - started,
    });
    return jsonResponse(400, { error: 'Request body must be valid JSON' });
  }

  const parsed = CreateBacktestRequestSchema.safeParse(raw);
  if (!parsed.success) {
    log({
      level: 'WARN',
      correlationId,
      event: 'validation_failed',
      reason: 'schema',
      details: parsed.error.flatten(),
      latencyMs: Date.now() - started,
    });
    return jsonResponse(400, {
      error: 'Validation failed',
      details: parsed.error.flatten(),
    });
  }

  const request = parsed.data;
  const now = new Date().toISOString();
  const jobId = randomUUID();
  const ttl = Math.floor(Date.now() / 1000) + JOB_TTL_DAYS * 24 * 60 * 60;
  const symbol = request.symbol.toUpperCase();
  const datasetKey = buildDatasetKey(symbol, request.timeframe);

  const strategyConfig: StrategyConfig = {
    symbol,
    timeframe: request.timeframe,
    startDate: request.startDate,
    endDate: request.endDate,
    strategyType: request.strategyType,
    parameters: request.parameters,
    initialCapital: request.initialCapital,
    commissionBps: request.commissionBps,
    slippageBps: request.slippageBps,
  };

  const record: BacktestJobRecord = {
    jobId,
    status: 'PENDING',
    strategyConfig,
    datasetKey,
    createdAt: now,
    updatedAt: now,
    ttl,
  };

  try {
    await ddb.send(
      new PutCommand({
        TableName: JOBS_TABLE_NAME,
        Item: record,
        ConditionExpression: 'attribute_not_exists(jobId)',
      }),
    );
  } catch (error) {
    log({
      level: 'ERROR',
      correlationId,
      jobId,
      event: 'dynamodb_put_failed',
      error: String(error),
      latencyMs: Date.now() - started,
    });
    return jsonResponse(500, { error: 'Failed to create backtest job' });
  }

  try {
    await sqs.send(
      new SendMessageCommand({
        QueueUrl: QUEUE_URL,
        MessageBody: JSON.stringify({
          jobId,
          datasetKey,
          strategyConfig,
        }),
        MessageAttributes: {
          jobId: { DataType: 'String', StringValue: jobId },
          correlationId: { DataType: 'String', StringValue: correlationId },
        },
      }),
    );
  } catch (error) {
    log({
      level: 'ERROR',
      correlationId,
      jobId,
      event: 'sqs_send_failed',
      error: String(error),
      latencyMs: Date.now() - started,
    });
    return jsonResponse(500, { error: 'Failed to enqueue backtest job' });
  }

  const response: CreateBacktestResponse = {
    jobId,
    status: 'PENDING',
    pollUrl: `/backtests/${jobId}`,
  };

  log({
    level: 'INFO',
    correlationId,
    jobId,
    event: 'backtest_created',
    strategyType: strategyConfig.strategyType,
    symbol,
    latencyMs: Date.now() - started,
  });

  return jsonResponse(202, response);
}

async function getBacktest(
  event: APIGatewayProxyEvent,
  correlationId: string,
): Promise<APIGatewayProxyResult> {
  const started = Date.now();
  const jobId = event.pathParameters?.jobId;

  if (!jobId) {
    log({
      level: 'WARN',
      correlationId,
      event: 'validation_failed',
      reason: 'missing_job_id',
      latencyMs: Date.now() - started,
    });
    return jsonResponse(400, { error: 'jobId path parameter is required' });
  }

  const result = await ddb.send(
    new GetCommand({
      TableName: JOBS_TABLE_NAME,
      Key: { jobId },
    }),
  );

  if (!result.Item) {
    log({
      level: 'INFO',
      correlationId,
      jobId,
      event: 'backtest_not_found',
      latencyMs: Date.now() - started,
    });
    return jsonResponse(404, { error: 'Backtest job not found' });
  }

  const job = result.Item as BacktestJobRecord;
  const response: GetBacktestResponse = {
    jobId: job.jobId,
    status: job.status,
    strategyConfig: job.strategyConfig,
    datasetKey: job.datasetKey,
    results: job.results,
    errorMessage: job.errorMessage,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
  };

  log({
    level: 'INFO',
    correlationId,
    jobId,
    event: 'backtest_fetched',
    status: job.status,
    latencyMs: Date.now() - started,
  });

  return jsonResponse(200, response);
}

export async function handler(
  event: APIGatewayProxyEvent,
  _context: Context,
): Promise<APIGatewayProxyResult> {
  const correlationId = correlationIdFrom(event);
  const method = event.httpMethod;
  const resource = event.resource;
  const started = Date.now();

  try {
    if (method === 'POST' && resource === '/backtests') {
      return await createBacktest(event, correlationId);
    }
    if (method === 'GET' && resource === '/backtests/{jobId}') {
      return await getBacktest(event, correlationId);
    }

    // Fallback for proxy/path-based routing (e.g. local invoke)
    const path = event.path ?? '';
    if (method === 'POST' && (path === '/backtests' || path.endsWith('/backtests'))) {
      return await createBacktest(event, correlationId);
    }
    if (method === 'GET' && /\/backtests\/[^/]+$/.test(path)) {
      return await getBacktest(event, correlationId);
    }

    log({
      level: 'WARN',
      correlationId,
      event: 'route_not_found',
      method,
      resource,
      path,
      latencyMs: Date.now() - started,
    });
    return jsonResponse(404, { error: 'Not found' });
  } catch (error) {
    log({
      level: 'ERROR',
      correlationId,
      event: 'unhandled_error',
      method,
      resource,
      error: String(error),
      latencyMs: Date.now() - started,
    });
    return jsonResponse(500, { error: 'Internal server error' });
  }
}
