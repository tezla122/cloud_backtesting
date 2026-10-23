import { z } from 'zod';

export const TimeframeSchema = z.enum(['1m', '5m', '1h', '1d']);
export type Timeframe = z.infer<typeof TimeframeSchema>;

export const StrategyTypeSchema = z.enum(['MEAN_REVERSION', 'MOMENTUM_CROSSOVER']);
export type StrategyType = z.infer<typeof StrategyTypeSchema>;

export const JobStatusSchema = z.enum(['PENDING', 'RUNNING', 'COMPLETED', 'FAILED']);
export type JobStatus = z.infer<typeof JobStatusSchema>;

const MeanReversionParametersSchema = z.object({
  lookbackWindow: z.number().int().positive(),
  entryZScore: z.number(),
  exitZScore: z.number(),
});

const MomentumCrossoverParametersSchema = z.object({
  fastPeriod: z.number().int().positive(),
  slowPeriod: z.number().int().positive(),
});

/**
 * POST /backtests request body.
 * Parameters are strategy-specific; extra keys are rejected via .strict() on the unions.
 */
export const CreateBacktestRequestSchema = z
  .object({
    symbol: z
      .string()
      .trim()
      .min(1)
      .max(32)
      .regex(/^[A-Za-z0-9.\-^]+$/, 'symbol must be alphanumeric (e.g. BTC-USD, SPY)'),
    timeframe: TimeframeSchema,
    startDate: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/, 'startDate must be YYYY-MM-DD'),
    endDate: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/, 'endDate must be YYYY-MM-DD'),
    strategyType: StrategyTypeSchema,
    parameters: z.record(z.union([z.number(), z.string(), z.boolean()])),
    initialCapital: z.number().positive().max(1_000_000_000).default(100_000),
    commissionBps: z.number().min(0).max(10_000).default(0),
    slippageBps: z.number().min(0).max(10_000).default(0),
  })
  .superRefine((value, ctx) => {
    const start = Date.parse(value.startDate);
    const end = Date.parse(value.endDate);
    if (Number.isNaN(start) || Number.isNaN(end)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'startDate and endDate must be valid calendar dates',
      });
      return;
    }
    if (end <= start) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'endDate must be after startDate',
        path: ['endDate'],
      });
    }

    if (value.strategyType === 'MEAN_REVERSION') {
      const parsed = MeanReversionParametersSchema.safeParse(value.parameters);
      if (!parsed.success) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message:
            'MEAN_REVERSION requires lookbackWindow, entryZScore, and exitZScore',
          path: ['parameters'],
        });
      }
    } else if (value.strategyType === 'MOMENTUM_CROSSOVER') {
      const parsed = MomentumCrossoverParametersSchema.safeParse(value.parameters);
      if (!parsed.success) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'MOMENTUM_CROSSOVER requires fastPeriod and slowPeriod',
          path: ['parameters'],
        });
        return;
      }
      if (parsed.data.fastPeriod >= parsed.data.slowPeriod) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          message: 'fastPeriod must be less than slowPeriod',
          path: ['parameters', 'fastPeriod'],
        });
      }
    }
  });

export type CreateBacktestRequest = z.infer<typeof CreateBacktestRequestSchema>;

/** Nested strategy configuration stored on the DynamoDB job item. */
export interface StrategyConfig {
  symbol: string;
  timeframe: Timeframe;
  startDate: string;
  endDate: string;
  strategyType: StrategyType;
  parameters: Record<string, number | string | boolean>;
  initialCapital: number;
  commissionBps: number;
  slippageBps: number;
}

/** Performance metrics written by the worker when status === COMPLETED. */
export interface BacktestResults {
  totalReturnPct: number;
  annualizedSharpe: number;
  annualizedSortino: number;
  maxDrawdownPct: number;
  winRatePct: number;
  profitFactor: number;
  totalTrades: number;
}

export interface BacktestJobRecord {
  jobId: string;
  status: JobStatus;
  strategyConfig: StrategyConfig;
  datasetKey: string;
  results?: BacktestResults;
  errorMessage?: string;
  createdAt: string;
  updatedAt: string;
  ttl: number;
}

export interface CreateBacktestResponse {
  jobId: string;
  status: 'PENDING';
  pollUrl: string;
}

export interface GetBacktestResponse {
  jobId: string;
  status: JobStatus;
  strategyConfig: StrategyConfig;
  datasetKey: string;
  results?: BacktestResults;
  errorMessage?: string;
  createdAt: string;
  updatedAt: string;
}

/** Builds the S3 object key for an OHLCV dataset. */
export function buildDatasetKey(symbol: string, timeframe: Timeframe): string {
  return `datasets/${symbol.toUpperCase()}/${timeframe}.csv`;
}
