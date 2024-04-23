import { Job } from './Job'

export type TaskEvent<TaskData, TaskResponse> =
  | SuccessTaskEvent<TaskData, TaskResponse>
  | ErrorTaskEvent<TaskData, TaskResponse>

export interface SuccessTaskEvent<TaskData, TaskResponse> {
  job: Job<TaskData, TaskResponse>
  error: null
  result: TaskResponse
}

export interface ErrorTaskEvent<TaskData, TaskResponse> {
  job: Job<TaskData, TaskResponse>
  error: Error | unknown
  result: TaskResponse | null
}

export interface ConcurrentQueueOptions<TaskData = any, TaskResponse = any> {
  concurrency: number
  priorityMode?: boolean
  onProcess: (job: Job<TaskData, TaskResponse>) => Promise<any>
  onDone?: (event: TaskEvent<TaskData, TaskResponse>) => void
  onSuccess?: (event: SuccessTaskEvent<TaskData, TaskResponse>) => void
  onFailure?: (event: ErrorTaskEvent<TaskData, TaskResponse>) => void
  onDrain?: () => void
}

/**
 * Inspired by https://github.com/HowProgrammingWorks/ConcurrentQueue
 */
export class ConcurrentQueue<TaskData = any, TaskResponse = any> {
  protected paused: boolean
  protected processing: Array<Job<TaskData, TaskResponse>>
  protected waiting: Array<Job<TaskData, TaskResponse>>

  constructor(protected opts: ConcurrentQueueOptions<TaskData, TaskResponse>) {
    this.paused = false
    this.processing = []
    this.waiting = []
  }

  get active() {
    return this.processing.length
  }

  getActive() {
    return this.processing
  }

  countWaiting() {
    return this.waiting.length
  }

  getWaiting() {
    return this.waiting
  }

  pause() {
    this.paused = true
  }

  async resume() {
    if (!this.paused) return
    this.paused = false
    const channels = Math.min(this.waiting.length, this.availableChannels())
    const promises: Promise<void>[] = []
    for (let i = 0; i < channels; i++) {
      promises.push(this.takeNext())
    }
    await Promise.all(promises)
  }

  async add(job: Job<TaskData, TaskResponse>) {
    if (!this.paused && this.availableChannels()) {
      await this.next(job)
      return
    }
    this.waiting.push(job)
    if (this.opts.priorityMode) {
      this.waiting.sort(
        (a, b) =>
          (b.opts?.priority || Number.MAX_SAFE_INTEGER) -
          (a.opts?.priority || Number.MAX_SAFE_INTEGER),
      )
    }
  }

  cancel(ids: Array<number>) {
    const toCancel = new Set(ids)
    // Ignore jobs that are currently being processed.
    // for (const job of this.processing) {
    //   if (toCancel.has(job.id)) {
    //   }
    // }
    this.waiting = this.waiting.filter((item) => !toCancel.has(item.id))
  }

  protected async next(job: Job<TaskData, TaskResponse>) {
    const { timeout } = job.opts
    const { onProcess } = this.opts
    this.processing.push(job)
    let timer: ReturnType<typeof setTimeout> | null = null
    let finished = false

    const handleFinish = (event: TaskEvent<TaskData, TaskResponse>) => {
      if (finished) return
      finished = true
      if (timer) clearTimeout(timer)
      this.processing = this.processing.filter((it) => it.id !== job.id)
      this.finish(event)
      if (!this.paused && this.waiting.length > 0) this.takeNext()
    }

    if (timeout && Number.isFinite(timeout)) {
      const error = new Error('Process timed out')
      timer = setTimeout(handleFinish, timeout, { error, job, result: null })
    }

    try {
      const result = await onProcess(job)
      handleFinish({ error: null, job, result })
    } catch (error) {
      handleFinish({ error, job, result: null })
    }
  }

  protected async takeNext() {
    const { waiting } = this
    const job = waiting.shift()
    if (!job) return
    if (this.availableChannels()) await this.next(job)
    return
  }

  protected availableChannels() {
    return Math.max(0, this.opts.concurrency - this.active)
  }

  protected finish(event: TaskEvent<TaskData, TaskResponse>) {
    const { onFailure, onSuccess, onDone, onDrain } = this.opts
    if (event.error) {
      if (onFailure) onFailure(event)
    } else if (onSuccess) {
      onSuccess(event as SuccessTaskEvent<TaskData, TaskResponse>)
    }
    if (onDone) onDone(event)
    if (this.active === 0 && onDrain) onDrain()
  }
}
