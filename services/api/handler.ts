import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import type {
  APIGatewayProxyEventV2,
  APIGatewayProxyResultV2,
  Context,
} from 'aws-lambda';
import { randomUUID } from 'crypto';
import {
  CreateBacktestRequestSchema,
  type BacktestJobRecord,
  type CreateBacktestResponse,
  type GetBacktestResponse,
} from './schemas';

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});
const sqs = new SQSClient({});

const JOBS_TABLE_NAME = requiredEnv('JOBS_TABLE_NAME');
const JOB_QUEUE_URL = requiredEnv('JOB_QUEUE_URL');
const JOB_TTL_DAYS = 90;

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function jsonResponse(
  statusCode: number,
  body: unknown,
): APIGatewayProxyResultV2 {
  return {
    statusCode,
    headers: {
      'content-type': 'application/json',
      'access-control-allow-origin': '*',
    },
    body: JSON.stringify(body),
  };
}

function log(level: string, message: string, extra?: Record<string, unknown>): void {
  // Structured logs for CloudWatch Logs Insights
  console.log(
    JSON.stringify({
      level,
      message,
      service: process.env.POWERTOOLS_SERVICE_NAME ?? 'backtesting-api',
      ...extra,
      timestamp: new Date().toISOString(),
    }),
  );
}

async function findByIdempotencyKey(
  idempotencyKey: string,
): Promise<BacktestJobRecord | undefined> {
  const result = await ddb.send(
    new QueryCommand({
      TableName: JOBS_TABLE_NAME,
      IndexName: 'IdempotencyKeyIndex',
      KeyConditionExpression: 'idempotencyKey = :key',
      ExpressionAttributeValues: { ':key': idempotencyKey },
      Limit: 1,
    }),
  );
  const item = result.Items?.[0];
  return item as BacktestJobRecord | undefined;
}

async function createBacktest(
  event: APIGatewayProxyEventV2,
): Promise<APIGatewayProxyResultV2> {
  if (!event.body) {
    return jsonResponse(400, { error: 'Request body is required' });
  }

  let raw: unknown;
  try {
    raw = JSON.parse(event.body);
  } catch {
    return jsonResponse(400, { error: 'Request body must be valid JSON' });
  }

  const headerKey = event.headers?.['idempotency-key'] ?? event.headers?.['Idempotency-Key'];
  if (headerKey && typeof raw === 'object' && raw !== null && !('idempotencyKey' in raw)) {
    (raw as Record<string, unknown>).idempotencyKey = headerKey;
  }

  const parsed = CreateBacktestRequestSchema.safeParse(raw);
  if (!parsed.success) {
    return jsonResponse(400, {
      error: 'Validation failed',
      details: parsed.error.flatten(),
    });
  }

  const request = parsed.data;

  if (request.idempotencyKey) {
    const existing = await findByIdempotencyKey(request.idempotencyKey);
    if (existing) {
      log('INFO', 'Idempotent replay', {
        jobId: existing.jobId,
        idempotencyKey: request.idempotencyKey,
      });
      const response: CreateBacktestResponse = {
        jobId: existing.jobId,
        status: existing.status,
        createdAt: existing.createdAt,
      };
      return jsonResponse(200, response);
    }
  }

  const now = new Date().toISOString();
  const jobId = randomUUID();
  const ttl = Math.floor(Date.now() / 1000) + JOB_TTL_DAYS * 24 * 60 * 60;

  const record: BacktestJobRecord = {
    jobId,
    status: 'QUEUED',
    strategy: request.strategy,
    symbol: request.symbol.toUpperCase(),
    startDate: request.startDate,
    endDate: request.endDate,
    initialCapital: request.initialCapital,
    parameters: request.parameters,
    idempotencyKey: request.idempotencyKey,
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
    log('ERROR', 'Failed to persist job', { jobId, error: String(error) });
    return jsonResponse(500, { error: 'Failed to create backtest job' });
  }

  try {
    await sqs.send(
      new SendMessageCommand({
        QueueUrl: JOB_QUEUE_URL,
        MessageBody: JSON.stringify({
          jobId,
          strategy: record.strategy,
          symbol: record.symbol,
          startDate: record.startDate,
          endDate: record.endDate,
          initialCapital: record.initialCapital,
          parameters: record.parameters,
        }),
        MessageAttributes: {
          jobId: { DataType: 'String', StringValue: jobId },
          strategy: { DataType: 'String', StringValue: record.strategy },
        },
      }),
    );
  } catch (error) {
    log('ERROR', 'Failed to enqueue job', { jobId, error: String(error) });
    return jsonResponse(500, { error: 'Failed to enqueue backtest job' });
  }

  log('INFO', 'Backtest job queued', { jobId, strategy: record.strategy, symbol: record.symbol });

  const response: CreateBacktestResponse = {
    jobId,
    status: 'QUEUED',
    createdAt: now,
  };
  return jsonResponse(202, response);
}

async function getBacktest(
  event: APIGatewayProxyEventV2,
): Promise<APIGatewayProxyResultV2> {
  const jobId = event.pathParameters?.jobId;
  if (!jobId) {
    return jsonResponse(400, { error: 'jobId path parameter is required' });
  }

  const result = await ddb.send(
    new GetCommand({
      TableName: JOBS_TABLE_NAME,
      Key: { jobId },
    }),
  );

  if (!result.Item) {
    return jsonResponse(404, { error: 'Backtest job not found' });
  }

  const job = result.Item as BacktestJobRecord;
  const response: GetBacktestResponse = {
    jobId: job.jobId,
    status: job.status,
    strategy: job.strategy,
    symbol: job.symbol,
    startDate: job.startDate,
    endDate: job.endDate,
    initialCapital: job.initialCapital,
    parameters: job.parameters,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    metrics: job.metrics,
    resultS3Key: job.resultS3Key,
    errorMessage: job.errorMessage,
  };

  return jsonResponse(200, response);
}

function health(): APIGatewayProxyResultV2 {
  return jsonResponse(200, {
    status: 'ok',
    service: 'cloud-backtesting-api',
    timestamp: new Date().toISOString(),
  });
}

export async function handler(
  event: APIGatewayProxyEventV2,
  _context: Context,
): Promise<APIGatewayProxyResultV2> {
  const method = event.requestContext.http.method;
  const path = event.rawPath;

  try {
    if (method === 'GET' && path === '/health') {
      return health();
    }
    if (method === 'POST' && path === '/backtests') {
      return await createBacktest(event);
    }
    if (method === 'GET' && path.startsWith('/backtests/')) {
      return await getBacktest(event);
    }
    return jsonResponse(404, { error: 'Not found' });
  } catch (error) {
    log('ERROR', 'Unhandled API error', { error: String(error), path, method });
    return jsonResponse(500, { error: 'Internal server error' });
  }
}
