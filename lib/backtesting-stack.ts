import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as lambdaNodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';
import * as path from 'path';
import { MonitoringConstruct } from './constructs/monitoring-construct';

/**
 * Serverless quantitative backtesting stack:
 * RestApi → API Lambda → DynamoDB + SQS → Python Worker → S3 datasets
 */
export class BacktestingStack extends cdk.Stack {
  public readonly datasetsBucket: s3.Bucket;
  public readonly jobsTable: dynamodb.Table;
  public readonly backtestDlq: sqs.Queue;
  public readonly backtestQueue: sqs.Queue;
  public readonly apiFn: lambdaNodejs.NodejsFunction;
  public readonly workerFn: lambda.Function;
  public readonly restApi: apigateway.RestApi;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // --- Amazon S3: historical OHLCV datasets ---
    // Key layout: datasets/{symbol}/{timeframe}.csv
    this.datasetsBucket = new s3.Bucket(this, 'DatasetsBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    // --- Amazon DynamoDB: job lifecycle state ---
    this.jobsTable = new dynamodb.Table(this, 'JobsTable', {
      partitionKey: { name: 'jobId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // --- Amazon SQS: job queue + DLQ ---
    this.backtestDlq = new sqs.Queue(this, 'BacktestDLQ', {
      retentionPeriod: cdk.Duration.days(14),
    });

    // visibilityTimeout = 15 min ≈ 6× Worker timeout (150s) per AWS guidance
    this.backtestQueue = new sqs.Queue(this, 'BacktestQueue', {
      visibilityTimeout: cdk.Duration.minutes(15),
      deadLetterQueue: {
        queue: this.backtestDlq,
        maxReceiveCount: 3,
      },
    });

    // --- API Handler Lambda (TypeScript / Node.js 22 / arm64) ---
    this.apiFn = new lambdaNodejs.NodejsFunction(this, 'ApiHandler', {
      runtime: lambda.Runtime.NODEJS_22_X,
      architecture: lambda.Architecture.ARM_64,
      entry: path.join(__dirname, '../services/api/handler.ts'),
      handler: 'handler',
      bundling: {
        minify: true,
        sourceMap: true,
        target: 'node22',
        // AWS SDK v3 ships with the Node.js 22.x managed runtime
        externalModules: ['@aws-sdk/*'],
      },
      memorySize: 256,
      timeout: cdk.Duration.seconds(29),
      environment: {
        JOBS_TABLE_NAME: this.jobsTable.tableName,
        QUEUE_URL: this.backtestQueue.queueUrl,
      },
    });

    this.jobsTable.grantReadWriteData(this.apiFn);
    this.backtestQueue.grantSendMessages(this.apiFn);

    // --- Amazon API Gateway (REST API) ---
    this.restApi = new apigateway.RestApi(this, 'BacktestingApi', {
      restApiName: 'cloud-backtesting-api',
      description: 'Submit and poll quantitative backtest jobs',
      deployOptions: {
        stageName: 'prod',
        tracingEnabled: true,
        metricsEnabled: true,
      },
      defaultCorsPreflightOptions: {
        allowOrigins: apigateway.Cors.ALL_ORIGINS,
        allowMethods: ['GET', 'POST', 'OPTIONS'],
        allowHeaders: ['Content-Type', 'X-Correlation-Id'],
      },
    });

    const apiIntegration = new apigateway.LambdaIntegration(this.apiFn, {
      proxy: true,
    });

    const backtests = this.restApi.root.addResource('backtests');
    backtests.addMethod('POST', apiIntegration);

    const backtestById = backtests.addResource('{jobId}');
    backtestById.addMethod('GET', apiIntegration);

    // --- Backtest Worker Lambda (Python 3.12 / arm64) ---
    this.workerFn = new lambda.Function(this, 'BacktestWorker', {
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
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
      timeout: cdk.Duration.seconds(150),
      memorySize: 1024,
      environment: {
        JOBS_TABLE_NAME: this.jobsTable.tableName,
        DATASETS_BUCKET_NAME: this.datasetsBucket.bucketName,
      },
    });

    this.workerFn.addEventSource(
      new lambdaEventSources.SqsEventSource(this.backtestQueue, {
        batchSize: 1,
        reportBatchItemFailures: true,
      }),
    );

    this.jobsTable.grantReadWriteData(this.workerFn);
    this.datasetsBucket.grantRead(this.workerFn);

    // --- CloudWatch monitoring ---
    new MonitoringConstruct(this, 'Monitoring', {
      restApi: this.restApi,
      workerFunction: this.workerFn,
      deadLetterQueue: this.backtestDlq,
    });

    new cdk.CfnOutput(this, 'ApiUrl', {
      value: this.restApi.url,
      description: 'Base URL of the backtesting REST API',
      exportName: `${this.stackName}-ApiUrl`,
    });

    new cdk.CfnOutput(this, 'JobsTableName', {
      value: this.jobsTable.tableName,
      description: 'DynamoDB table for backtest job state',
    });

    new cdk.CfnOutput(this, 'DatasetsBucketName', {
      value: this.datasetsBucket.bucketName,
      description: 'S3 bucket for historical OHLCV datasets',
    });

    new cdk.CfnOutput(this, 'BacktestQueueUrl', {
      value: this.backtestQueue.queueUrl,
      description: 'SQS URL for the backtest job queue',
    });
  }
}
