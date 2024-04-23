import { JobOptions } from './JobOptions'

export enum JobState {
  Pending = 'Pending',
  InProgress = 'InProgress',
  Complete = 'Complete',
  FailedPermanently = 'FailedPermanently',
}

export interface JobRequest<TaskData, _TaskResponse> {
  name: string
  data: TaskData
  opts?: JobOptions
}

export enum JobResult {
  Success = 'Success',
  Cancelled = 'Cancelled',
  FailedRetryable = 'FailedRetryable',
  FailedPermanently = 'FailedPermanently',
}

export interface JobResponse<TaskResponse> {
  status: JobResult
  data?: TaskResponse | null
  reason?: string
  stacktrace?: string[]
}

export interface Job<TaskData, TaskResponse> extends JobRequest<TaskData, TaskResponse> {
  id: number
  opts: JobOptions
  status: JobState
  progress: number | null
  attemptsMade: number
  cancelled?: boolean
  finishedOn?: number
  processedOn?: number
  timestamp: number
  startedAt?: number
  failedReason?: string
  stacktrace?: string[] | null
  returnvalue?: TaskResponse | null
}
