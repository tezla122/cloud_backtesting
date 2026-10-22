#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { BacktestingStack } from '../lib/backtesting-stack';

const app = new cdk.App();

const env = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION ?? 'us-east-1',
};

new BacktestingStack(app, 'CloudBacktestingStack', {
  env,
  description: 'Serverless quantitative backtesting service (API + Worker)',
  tags: {
    Project: 'cloud-backtesting-service',
    ManagedBy: 'cdk',
  },
});

app.synth();
