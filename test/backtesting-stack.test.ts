import * as cdk from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { BacktestingStack } from '../lib/backtesting-stack';

describe('BacktestingStack', () => {
  let template: Template;

  beforeAll(() => {
    const app = new cdk.App();
    const stack = new BacktestingStack(app, 'TestBacktestingStack', {
      env: { account: '111111111111', region: 'us-east-1' },
    });
    template = Template.fromStack(stack);
  });

  test('creates DynamoDB jobs table with idempotency GSI', () => {
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      BillingMode: 'PAY_PER_REQUEST',
      KeySchema: Match.arrayWith([
        Match.objectLike({ AttributeName: 'jobId', KeyType: 'HASH' }),
      ]),
      GlobalSecondaryIndexes: Match.arrayWith([
        Match.objectLike({
          IndexName: 'IdempotencyKeyIndex',
          KeySchema: Match.arrayWith([
            Match.objectLike({ AttributeName: 'idempotencyKey', KeyType: 'HASH' }),
          ]),
        }),
      ]),
      TimeToLiveSpecification: Match.objectLike({
        AttributeName: 'ttl',
        Enabled: true,
      }),
    });
  });

  test('creates encrypted results bucket that blocks public access', () => {
    template.hasResourceProperties('AWS::S3::Bucket', {
      BucketEncryption: Match.objectLike({
        ServerSideEncryptionConfiguration: Match.anyValue(),
      }),
      PublicAccessBlockConfiguration: Match.objectLike({
        BlockPublicAcls: true,
        BlockPublicPolicy: true,
        IgnorePublicAcls: true,
        RestrictPublicBuckets: true,
      }),
    });
  });

  test('creates primary queue with DLQ redrive policy', () => {
    template.resourceCountIs('AWS::SQS::Queue', 2);
    template.hasResourceProperties('AWS::SQS::Queue', {
      RedrivePolicy: Match.objectLike({
        maxReceiveCount: 3,
      }),
    });
  });

  test('creates API Node.js Lambda and Python worker Lambda', () => {
    template.hasResourceProperties('AWS::Lambda::Function', {
      Runtime: 'nodejs20.x',
      Handler: 'index.handler',
      Timeout: 29,
      MemorySize: 512,
      TracingConfig: { Mode: 'Active' },
    });

    template.hasResourceProperties('AWS::Lambda::Function', {
      Runtime: 'python3.12',
      Handler: 'handler.handler',
      Timeout: 300,
      MemorySize: 1024,
      ReservedConcurrentExecutions: 20,
      TracingConfig: { Mode: 'Active' },
    });
  });

  test('worker is subscribed to the SQS job queue', () => {
    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 5,
      FunctionResponseTypes: ['ReportBatchItemFailures'],
    });
  });

  test('creates HTTP API with backtest and health routes', () => {
    template.hasResourceProperties('AWS::ApiGatewayV2::Api', {
      Name: 'cloud-backtesting-api',
      ProtocolType: 'HTTP',
    });

    template.resourceCountIs('AWS::ApiGatewayV2::Route', 3);
    template.hasResourceProperties('AWS::ApiGatewayV2::Route', {
      RouteKey: 'POST /backtests',
    });
    template.hasResourceProperties('AWS::ApiGatewayV2::Route', {
      RouteKey: 'GET /backtests/{jobId}',
    });
    template.hasResourceProperties('AWS::ApiGatewayV2::Route', {
      RouteKey: 'GET /health',
    });
  });

  test('provisions CloudWatch alarms for API, worker, and DLQ', () => {
    template.resourceCountIs('AWS::CloudWatch::Alarm', 5);
    template.hasResourceProperties('AWS::CloudWatch::Alarm', {
      AlarmDescription: Match.stringLikeRegexp('DLQ'),
    });
    template.hasResourceProperties('AWS::CloudWatch::Dashboard', {
      DashboardName: Match.stringLikeRegexp('ops'),
    });
  });

  test('exports the HTTP API URL', () => {
    template.hasOutput('HttpApiUrl', {
      Export: Match.objectLike({
        Name: 'TestBacktestingStack-HttpApiUrl',
      }),
    });
  });
});
