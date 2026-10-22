import * as cdk from 'aws-cdk-lib';
import * as apigatewayv2 from 'aws-cdk-lib/aws-apigatewayv2';
import * as apigatewayv2Integrations from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as lambdaNodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import * as path from 'path';
import { MonitoringConstruct } from './constructs/monitoring-construct';

export class BacktestingStack extends cdk.Stack {
  public readonly jobsTable: dynamodb.Table;
  public readonly resultsBucket: s3.Bucket;
  public readonly jobQueue: sqs.Queue;
  public readonly deadLetterQueue: sqs.Queue;
  public readonly apiFunction: lambda.Function;
  public readonly workerFunction: lambda.Function;
  public readonly httpApi: apigatewayv2.HttpApi;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.resultsBucket = new s3.Bucket(this, 'ResultsBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      versioned: true,
      lifecycleRules: [
        {
          id: 'ExpireOldResults',
          enabled: true,
          expiration: cdk.Duration.days(90),
          noncurrentVersionExpiration: cdk.Duration.days(30),
        },
      ],
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    this.jobsTable = new dynamodb.Table(this, 'JobsTable', {
      partitionKey: { name: 'jobId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      timeToLiveAttribute: 'ttl',
    });

    this.jobsTable.addGlobalSecondaryIndex({
      indexName: 'IdempotencyKeyIndex',
      partitionKey: { name: 'idempotencyKey', type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
    });

    this.deadLetterQueue = new sqs.Queue(this, 'JobDeadLetterQueue', {
      retentionPeriod: cdk.Duration.days(14),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
    });

    this.jobQueue = new sqs.Queue(this, 'JobQueue', {
      visibilityTimeout: cdk.Duration.minutes(6),
      retentionPeriod: cdk.Duration.days(4),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: {
        queue: this.deadLetterQueue,
        maxReceiveCount: 3,
      },
    });

    const apiLogGroup = new logs.LogGroup(this, 'ApiLogGroup', {
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const workerLogGroup = new logs.LogGroup(this, 'WorkerLogGroup', {
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.apiFunction = new lambdaNodejs.NodejsFunction(this, 'ApiFunction', {
      runtime: lambda.Runtime.NODEJS_20_X,
      entry: path.join(__dirname, '../services/api/handler.ts'),
      handler: 'handler',
      bundling: {
        minify: true,
        sourceMap: true,
        target: 'node20',
        // AWS SDK v3 is available in the Node.js 20.x Lambda runtime
        externalModules: ['@aws-sdk/*'],
      },
      memorySize: 512,
      timeout: cdk.Duration.seconds(29),
      environment: {
        JOBS_TABLE_NAME: this.jobsTable.tableName,
        JOB_QUEUE_URL: this.jobQueue.queueUrl,
        RESULTS_BUCKET_NAME: this.resultsBucket.bucketName,
        POWERTOOLS_SERVICE_NAME: 'backtesting-api',
        LOG_LEVEL: 'INFO',
      },
      logGroup: apiLogGroup,
      tracing: lambda.Tracing.ACTIVE,
    });

    this.workerFunction = new lambda.Function(this, 'WorkerFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'handler.handler',
      code: lambda.Code.fromAsset(path.join(__dirname, '../services/worker'), {
        exclude: [
          'tests',
          'tests/**',
          '**/__pycache__',
          '**/*.pyc',
          '.pytest_cache',
          '**/.pytest_cache',
          '.venv',
          '.venv/**',
          'venv',
          'venv/**',
          'requirements.txt',
        ],
      }),

      memorySize: 1024,
      timeout: cdk.Duration.minutes(5),
      environment: {
        JOBS_TABLE_NAME: this.jobsTable.tableName,
        RESULTS_BUCKET_NAME: this.resultsBucket.bucketName,
        IDEMPOTENCY_TABLE_NAME: this.jobsTable.tableName,
        POWERTOOLS_SERVICE_NAME: 'backtesting-worker',
        LOG_LEVEL: 'INFO',
      },
      logGroup: workerLogGroup,
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: 20,
    });

    this.workerFunction.addEventSource(
      new lambdaEventSources.SqsEventSource(this.jobQueue, {
        batchSize: 5,
        maxBatchingWindow: cdk.Duration.seconds(5),
        reportBatchItemFailures: true,
      }),
    );

    this.jobsTable.grantReadWriteData(this.apiFunction);
    this.jobsTable.grantReadWriteData(this.workerFunction);
    this.jobQueue.grantSendMessages(this.apiFunction);
    this.resultsBucket.grantReadWrite(this.workerFunction);
    this.resultsBucket.grantRead(this.apiFunction);

    const apiIntegration = new apigatewayv2Integrations.HttpLambdaIntegration(
      'ApiIntegration',
      this.apiFunction,
    );

    this.httpApi = new apigatewayv2.HttpApi(this, 'HttpApi', {
      apiName: 'cloud-backtesting-api',
      description: 'HTTP API for submitting and querying backtests',
      corsPreflight: {
        allowHeaders: ['Content-Type', 'Authorization', 'Idempotency-Key'],
        allowMethods: [
          apigatewayv2.CorsHttpMethod.GET,
          apigatewayv2.CorsHttpMethod.POST,
          apigatewayv2.CorsHttpMethod.OPTIONS,
        ],
        allowOrigins: ['*'],
        maxAge: cdk.Duration.days(1),
      },
    });

    this.httpApi.addRoutes({
      path: '/backtests',
      methods: [apigatewayv2.HttpMethod.POST],
      integration: apiIntegration,
    });

    this.httpApi.addRoutes({
      path: '/backtests/{jobId}',
      methods: [apigatewayv2.HttpMethod.GET],
      integration: apiIntegration,
    });

    this.httpApi.addRoutes({
      path: '/health',
      methods: [apigatewayv2.HttpMethod.GET],
      integration: apiIntegration,
    });

    new MonitoringConstruct(this, 'Monitoring', {
      apiFunction: this.apiFunction,
      workerFunction: this.workerFunction,
      jobQueue: this.jobQueue,
      deadLetterQueue: this.deadLetterQueue,
    });

    new cdk.CfnOutput(this, 'HttpApiUrl', {
      value: this.httpApi.apiEndpoint,
      description: 'Base URL of the backtesting HTTP API',
      exportName: `${this.stackName}-HttpApiUrl`,
    });

    new cdk.CfnOutput(this, 'JobsTableName', {
      value: this.jobsTable.tableName,
      description: 'DynamoDB table for backtest job state',
    });

    new cdk.CfnOutput(this, 'ResultsBucketName', {
      value: this.resultsBucket.bucketName,
      description: 'S3 bucket for backtest result artifacts',
    });

    new cdk.CfnOutput(this, 'JobQueueUrl', {
      value: this.jobQueue.queueUrl,
      description: 'SQS URL for the backtest job queue',
    });
  }
}
