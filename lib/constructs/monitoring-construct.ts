import * as cdk from 'aws-cdk-lib';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

export interface MonitoringConstructProps {
  readonly restApi: apigateway.RestApi;
  readonly workerFunction: lambda.Function;
  readonly deadLetterQueue: sqs.IQueue;
}

/**
 * CloudWatch metric filters and alarms for the backtesting service.
 *
 * Alarms:
 * - DLQDepthAlarm: messages visible on the DLQ
 * - WorkerErrorAlarm: Python worker Lambda errors
 * - ApiLatencyAlarm: API Gateway p99 latency
 */
export class MonitoringConstruct extends Construct {
  public readonly dlqDepthAlarm: cloudwatch.Alarm;
  public readonly workerErrorAlarm: cloudwatch.Alarm;
  public readonly apiLatencyAlarm: cloudwatch.Alarm;

  constructor(scope: Construct, id: string, props: MonitoringConstructProps) {
    super(scope, id);

    const stackName = cdk.Stack.of(this).stackName;

    // Metric filter on the worker log group (created lazily via Function.logGroup)
    const workerLogGroup = logs.LogGroup.fromLogGroupName(
      this,
      'WorkerLogGroup',
      `/aws/lambda/${props.workerFunction.functionName}`,
    );

    new logs.MetricFilter(this, 'WorkerErrorMetricFilter', {
      logGroup: workerLogGroup,
      metricNamespace: 'CloudBacktesting',
      metricName: 'WorkerLoggedErrors',
      filterPattern: logs.FilterPattern.anyTerm('ERROR', '"level":"ERROR"'),
      metricValue: '1',
      defaultValue: 0,
    });

    this.dlqDepthAlarm = new cloudwatch.Alarm(this, 'DLQDepthAlarm', {
      alarmName: `${stackName}-dlq-depth`,
      alarmDescription:
        'BacktestDLQ approximateNumberOfMessagesVisible >= 1 (poison / exhausted retries)',
      metric: props.deadLetterQueue.metricApproximateNumberOfMessagesVisible({
        period: cdk.Duration.minutes(1),
        statistic: 'Maximum',
      }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    this.workerErrorAlarm = new cloudwatch.Alarm(this, 'WorkerErrorAlarm', {
      alarmName: `${stackName}-worker-errors`,
      alarmDescription: 'Python Worker Lambda Errors exceed threshold over 5 minutes',
      metric: props.workerFunction.metricErrors({
        period: cdk.Duration.minutes(5),
        statistic: 'Sum',
      }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    this.apiLatencyAlarm = new cloudwatch.Alarm(this, 'ApiLatencyAlarm', {
      alarmName: `${stackName}-api-latency-p99`,
      alarmDescription: 'API Gateway p99 latency exceeds 1000ms',
      metric: props.restApi.metricLatency({
        period: cdk.Duration.minutes(5),
        statistic: 'p99',
      }),
      threshold: 1000,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
  }
}
