import * as cdk from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { BacktestingStack } from '../lib/backtesting-stack';

/** Worker timeout is 150s; SQS visibility must be >= 6× that (900s). */
const WORKER_TIMEOUT_SECONDS = 150;
const MIN_VISIBILITY_TIMEOUT_SECONDS = WORKER_TIMEOUT_SECONDS * 6;

describe('BacktestingStack', () => {
  let template: Template;

  beforeAll(() => {
    const app = new cdk.App();
    const stack = new BacktestingStack(app, 'TestBacktestingStack', {
      env: { account: '111111111111', region: 'us-east-1' },
    });
    template = Template.fromStack(stack);
  });

  test('DynamoDB JobsTable has PAY_PER_REQUEST billing and TTL enabled', () => {
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: Match.arrayWith([
        Match.objectLike({ AttributeName: 'jobId', KeyType: 'HASH' }),
      ]),
      TimeToLiveSpecification: Match.objectLike({
        AttributeName: 'ttl',
        Enabled: true,
      }),
    });
  });

  test('SQS BacktestQueue is wired to DLQ with maxReceiveCount 3', () => {
    template.resourceCountIs('AWS::SQS::Queue', 2);

    template.hasResourceProperties('AWS::SQS::Queue', {
      MessageRetentionPeriod: 1209600, // 14 days
    });

    template.hasResourceProperties('AWS::SQS::Queue', {
      RedrivePolicy: Match.objectLike({
        maxReceiveCount: 3,
      }),
    });
  });

  test('SQS visibility timeout is at least 6x the worker Lambda timeout', () => {
    const queues = template.findResources('AWS::SQS::Queue');
    const mainQueue = Object.values(queues).find(
      (resource) => resource.Properties?.RedrivePolicy !== undefined,
    );
    expect(mainQueue).toBeDefined();
    const visibility = mainQueue!.Properties.VisibilityTimeout as number;
    expect(visibility).toBeGreaterThanOrEqual(MIN_VISIBILITY_TIMEOUT_SECONDS);

    template.hasResourceProperties('AWS::Lambda::Function', {
      Runtime: 'python3.12',
      Timeout: WORKER_TIMEOUT_SECONDS,
    });
  });

  test('CloudWatch alarms exist for DLQ depth and Worker errors', () => {
    template.hasResourceProperties('AWS::CloudWatch::Alarm', {
      AlarmDescription: Match.stringLikeRegexp('DLQ'),
      Threshold: 1,
      ComparisonOperator: 'GreaterThanOrEqualToThreshold',
    });

    template.hasResourceProperties('AWS::CloudWatch::Alarm', {
      AlarmDescription: Match.stringLikeRegexp('Worker'),
      Period: 300,
    });
  });

  test('creates private encrypted DatasetsBucket with SSL enforced', () => {
    template.hasResourceProperties('AWS::S3::Bucket', {
      BucketEncryption: Match.objectLike({
        ServerSideEncryptionConfiguration: Match.arrayWith([
          Match.objectLike({
            ServerSideEncryptionByDefault: Match.objectLike({
              SSEAlgorithm: 'AES256',
            }),
          }),
        ]),
      }),
      PublicAccessBlockConfiguration: Match.objectLike({
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      }),
    });

    template.hasResourceProperties('AWS::S3::BucketPolicy', {
      PolicyDocument: Match.objectLike({
        Statement: Match.arrayWith([
          Match.objectLike({
            Effect: 'Deny',
            Condition: Match.objectLike({
              Bool: Match.objectLike({ 'aws:SecureTransport': 'false' }),
            }),
          }),
        ]),
      }),
    });
  });

  test('creates API Node.js 22 arm64 Lambda and Python 3.12 worker', () => {
    template.hasResourceProperties('AWS::Lambda::Function', {
      Runtime: 'nodejs22.x',
      Architectures: ['arm64'],
      Handler: 'index.handler',
      Environment: Match.objectLike({
        Variables: Match.objectLike({
          JOBS_TABLE_NAME: Match.anyValue(),
          QUEUE_URL: Match.anyValue(),
        }),
      }),
    });

    template.hasResourceProperties('AWS::Lambda::Function', {
      Runtime: 'python3.12',
      Architectures: ['arm64'],
      Handler: 'handler.handler',
      Timeout: 150,
      MemorySize: 1024,
      Environment: Match.objectLike({
        Variables: Match.objectLike({
          JOBS_TABLE_NAME: Match.anyValue(),
          DATASETS_BUCKET_NAME: Match.anyValue(),
        }),
      }),
    });
  });

  test('worker is subscribed to BacktestQueue with batchSize 1', () => {
    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 1,
      FunctionResponseTypes: ['ReportBatchItemFailures'],
    });
  });

  test('creates RestApi with POST /backtests and GET /backtests/{jobId}', () => {
    template.hasResourceProperties('AWS::ApiGateway::RestApi', {
      Name: 'cloud-backtesting-api',
    });

    template.hasResourceProperties('AWS::ApiGateway::Method', {
      HttpMethod: 'POST',
    });
    template.hasResourceProperties('AWS::ApiGateway::Method', {
      HttpMethod: 'GET',
    });
  });

  test('provisions API Gateway p99 latency alarm at 1000ms', () => {
    template.hasResourceProperties('AWS::CloudWatch::Alarm', {
      AlarmDescription: Match.stringLikeRegexp('p99'),
      Threshold: 1000,
    });
    template.resourceCountIs('AWS::CloudWatch::Alarm', 3);
  });

  test('exports the REST API URL', () => {
    template.hasOutput('ApiUrl', {
      Export: Match.objectLike({
        Name: 'TestBacktestingStack-ApiUrl',
      }),
    });
  });
});
