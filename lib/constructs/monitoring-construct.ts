import * as cdk from 'aws-cdk-lib';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cloudwatchActions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Construct } from 'constructs';

export interface MonitoringConstructProps {
  readonly apiFunction: lambda.IFunction;
  readonly workerFunction: lambda.IFunction;
  readonly jobQueue: sqs.IQueue;
  readonly deadLetterQueue: sqs.IQueue;
  readonly alarmTopic?: sns.ITopic;
}

/**
 * CloudWatch alarms and dashboard for the backtesting service.
 */
export class MonitoringConstruct extends Construct {
  public readonly dashboard: cloudwatch.Dashboard;
  public readonly alarmTopic: sns.ITopic;

  constructor(scope: Construct, id: string, props: MonitoringConstructProps) {
    super(scope, id);

    this.alarmTopic =
      props.alarmTopic ??
      new sns.Topic(this, 'AlarmTopic', {
        displayName: 'Cloud Backtesting Alarms',
        topicName: undefined,
      });

    const alarmAction = new cloudwatchActions.SnsAction(this.alarmTopic);

    const apiErrorAlarm = new cloudwatch.Alarm(this, 'ApiErrorAlarm', {
      alarmName: `${cdk.Stack.of(this).stackName}-api-errors`,
      alarmDescription: 'API Lambda error rate is elevated',
      metric: props.apiFunction.metricErrors({
        period: cdk.Duration.minutes(1),
        statistic: 'Sum',
      }),
      threshold: 5,
      evaluationPeriods: 3,
      datapointsToAlarm: 2,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    apiErrorAlarm.addAlarmAction(alarmAction);

    const apiLatencyAlarm = new cloudwatch.Alarm(this, 'ApiLatencyAlarm', {
      alarmName: `${cdk.Stack.of(this).stackName}-api-latency-p99`,
      alarmDescription: 'API Lambda p99 duration exceeds 3 seconds',
      metric: props.apiFunction.metricDuration({
        period: cdk.Duration.minutes(5),
        statistic: 'p99',
      }),
      threshold: 3000,
      evaluationPeriods: 2,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    apiLatencyAlarm.addAlarmAction(alarmAction);

    const workerErrorAlarm = new cloudwatch.Alarm(this, 'WorkerErrorAlarm', {
      alarmName: `${cdk.Stack.of(this).stackName}-worker-errors`,
      alarmDescription: 'Worker Lambda error rate is elevated',
      metric: props.workerFunction.metricErrors({
        period: cdk.Duration.minutes(1),
        statistic: 'Sum',
      }),
      threshold: 3,
      evaluationPeriods: 3,
      datapointsToAlarm: 2,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    workerErrorAlarm.addAlarmAction(alarmAction);

    const dlqDepthAlarm = new cloudwatch.Alarm(this, 'DlqDepthAlarm', {
      alarmName: `${cdk.Stack.of(this).stackName}-dlq-depth`,
      alarmDescription: 'Backtest jobs are landing in the DLQ',
      metric: props.deadLetterQueue.metricApproximateNumberOfMessagesVisible({
        period: cdk.Duration.minutes(1),
        statistic: 'Maximum',
      }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    dlqDepthAlarm.addAlarmAction(alarmAction);

    const queueAgeAlarm = new cloudwatch.Alarm(this, 'QueueAgeAlarm', {
      alarmName: `${cdk.Stack.of(this).stackName}-queue-age`,
      alarmDescription: 'Oldest backtest message is older than 15 minutes',
      metric: props.jobQueue.metricApproximateAgeOfOldestMessage({
        period: cdk.Duration.minutes(5),
        statistic: 'Maximum',
      }),
      threshold: 900,
      evaluationPeriods: 2,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
    queueAgeAlarm.addAlarmAction(alarmAction);

    this.dashboard = new cloudwatch.Dashboard(this, 'Dashboard', {
      dashboardName: `${cdk.Stack.of(this).stackName}-ops`,
    });

    this.dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'API Invocations / Errors',
        left: [props.apiFunction.metricInvocations()],
        right: [props.apiFunction.metricErrors()],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'Worker Invocations / Errors',
        left: [props.workerFunction.metricInvocations()],
        right: [props.workerFunction.metricErrors()],
        width: 12,
      }),
    );

    this.dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'API Duration (p50 / p99)',
        left: [
          props.apiFunction.metricDuration({ statistic: 'p50' }),
          props.apiFunction.metricDuration({ statistic: 'p99' }),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'Worker Duration (p50 / p99)',
        left: [
          props.workerFunction.metricDuration({ statistic: 'p50' }),
          props.workerFunction.metricDuration({ statistic: 'p99' }),
        ],
        width: 12,
      }),
    );

    this.dashboard.addWidgets(
      new cloudwatch.GraphWidget({
        title: 'SQS Queue Depth',
        left: [
          props.jobQueue.metricApproximateNumberOfMessagesVisible(),
          props.deadLetterQueue.metricApproximateNumberOfMessagesVisible({
            label: 'DLQ Visible',
          }),
        ],
        width: 12,
      }),
      new cloudwatch.GraphWidget({
        title: 'Oldest Message Age (seconds)',
        left: [props.jobQueue.metricApproximateAgeOfOldestMessage()],
        width: 12,
      }),
    );
  }
}
