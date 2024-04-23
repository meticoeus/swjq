import { BackoffOptions } from './BackoffOptions'

export interface JobOptions {
  //  default Date.now()
  timestamp?: number

  // Ranges from 1 (highest priority) to MAX_INT  (lowest priority). Note that
  // using priorities has a slight impact on performance,
  // so do not use it if not required.
  priority?: number

  // The total number of attempts to try the job until it completes.
  attempts?: number

  // Backoff setting for automatic retries if the job fails
  backoff?: number | BackoffOptions

  // if true, adds the job to the right of the queue instead of the left (default false)
  lifo?: boolean

  // The number of milliseconds after which the job should be
  // fail with a timeout error [optional]
  timeout?: number

  // If true, removes the job when it successfully completes
  // A number specify the max amount of jobs to keep.
  // Default behavior is to keep the job in the completed set.
  removeOnComplete?: boolean | number

  // If true, removes the job when it fails after all attempts.
  // A number specify the max amount of jobs to keep.
  // Default behavior is to keep the job in the failed set.
  removeOnFail?: boolean | number

  // Limits the amount of stack trace lines that will be recorded in the stacktrace.
  stackTraceLimit?: number
}
