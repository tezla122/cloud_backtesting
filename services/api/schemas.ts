import { z } from 'zod';

export const StrategyNameSchema = z.enum([
  'sma_crossover',
  'buy_and_hold',
  'mean_reversion',
]);

export type StrategyName = z.infer<typeof StrategyNameSchema>;

export const CreateBacktestRequestSchema = z
  .object({
    strategy: StrategyNameSchema,
    symbol: z
      .string()
      .trim()
      .min(1)
      .max(16)
      .regex(/^[A-Za-z0-9.\-^]+$/, 'symbol must be alphanumeric'),
    startDate: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/, 'startDate must be YYYY-MM-DD'),
    endDate: z
      .string()
      .regex(/^\d{4}-\d{2}-\d{2}$/, 'endDate must be YYYY-MM-DD'),
    initialCapital: z.number().positive().max(1_000_000_000).default(100_000),
    parameters: z.record(z.union([z.number(), z.string(), z.boolean()])).default({}),
    idempotencyKey: z.string().uuid().optional(),
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
    const spanDays = (end - start) / (1000 * 60 * 60 * 24);
    if (spanDays > 3650) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'backtest window cannot exceed 10 years',
        path: ['endDate'],
      });
    }
  });

export type CreateBacktestRequest = z.infer<typeof CreateBacktestRequestSchema>;

export const JobStatusSchema = z.enum([
  'QUEUED',
  'RUNNING',
  'SUCCEEDED',
  'FAILED',
]);

export type JobStatus = z.infer<typeof JobStatusSchema>;

export interface BacktestJobRecord {
  jobId: string;
  status: JobStatus;
  strategy: StrategyName;
  symbol: string;
  startDate: string;
  endDate: string;
  initialCapital: number;
  parameters: Record<string, number | string | boolean>;
  idempotencyKey?: string;
  createdAt: string;
  updatedAt: string;
  resultS3Key?: string;
  metrics?: Record<string, number>;
  errorMessage?: string;
  ttl?: number;
}

export interface CreateBacktestResponse {
  jobId: string;
  status: JobStatus;
  createdAt: string;
}

export interface GetBacktestResponse {
  jobId: string;
  status: JobStatus;
  strategy: StrategyName;
  symbol: string;
  startDate: string;
  endDate: string;
  initialCapital: number;
  parameters: Record<string, number | string | boolean>;
  createdAt: string;
  updatedAt: string;
  metrics?: Record<string, number>;
  resultS3Key?: string;
  errorMessage?: string;
}
