import { IDBPDatabase } from 'idb'
import { ConcurrentQueue } from './ConcurrentQueue'
import {
  Connection,
  ConnectionOptions,
  DBTables,
  InternalConnectionOptions,
  QueueDBSchema,
} from './Connection'
import { Job, JobResponse, JobResult, JobState } from './Job'

export interface WorkerOptions extends ConnectionOptions {
  maintenanceDelay?: number
  concurrency?: number
  maxAllowedDbErrors?: number
  maxAllowedNetworkErrors?: number
}

export interface InternalWorkerOptions extends InternalConnectionOptions {
  maintenanceDelay: number
  concurrency: number
  maxAllowedDbErrors: number
  maxAllowedNetworkErrors: number
}

interface RunSession {
  dbErrors: number
  networkErrors: number
}

const defaultOptions = {
  concurrency: 3,
  maintenanceDelay: 1000,
  maxAllowedDbErrors: 100,
  maxAllowedNetworkErrors: 5,
}

export class Worker<TaskData = any, TaskResponse = any> extends Connection<TaskData, TaskResponse> {
  protected paused: boolean | undefined
  protected session: RunSession
  protected runner: Promise<any> | undefined
  protected queue: ConcurrentQueue<TaskData, TaskResponse> | undefined
  protected receivingNewJobs: boolean | undefined
  protected canClose: boolean | undefined
  protected opts: InternalWorkerOptions
  protected handleClose?: (v?: any | PromiseLike<any>) => void
  protected cancelledJobs: Set<number>

  constructor(
    name: string,
    protected processFn: (
      job: Job<TaskData, TaskResponse>,
      db: IDBPDatabase<QueueDBSchema<TaskData, TaskResponse>>,
    ) => Promise<JobResponse<TaskResponse>>,
    opts: WorkerOptions = {},
  ) {
    super(name, opts)
    // @ts-ignore
    const superOpts: InternalWorkerOptions = this.opts

    this.opts = {
      ...defaultOptions,
      ...superOpts,
      ...opts,
    }

    this.session = {
      dbErrors: 0,
      networkErrors: 0,
    }

    this.cancelledJobs = new Set()

    this.processJob = this.processJob.bind(this)
  }

  protected modifyJobUpdate(
    _job: Job<TaskData, TaskResponse>,
    nextJob: Job<TaskData, TaskResponse>,
  ): Job<TaskData, TaskResponse> {
    return nextJob
  }

  async run() {
    if (!this.runner) {
      this.runner = this.init()
    }
    return this.runner
  }

  async receiveNewJobs() {
    if (!this.queue || this.paused) return
    this.receivingNewJobs = true
    let hasNewJobs = false

    try {
      // get all new jobs that have come in since the last check
      const allJobs = await this.getJobsByStatus(JobState.Pending)
      const waiting = this.queue.getWaiting()
      const waitingIds = new Set<number>(waiting.map((job) => job.id))

      const promises: Promise<void>[] = []
      for (const job of allJobs) {
        if (!waitingIds.has(job.id)) {
          hasNewJobs = true
          promises.push(this.queue.add(job))
        }
      }
      await Promise.all(promises)

    } catch (e) {
      console.error(e)
    } finally {
      this.receivingNewJobs = false
      // if there are no new jobs after all and queue is already drained, close
      if (!hasNewJobs && this.canClose) {
        await this.onClose()
      }
    }
  }

  async cancelJobs(ids: Array<number>) {
    // 3 states:
    // jobs are being actively processed
    // jobs are in the queue
    // jobs haven't been loaded yet (typically due to cold start of queue to remove jobs)
    const db = await this.client()
    const tx = db.transaction(DBTables.queue, 'readwrite')

    const active = new Set(this.queue?.getActive()?.map((job) => job.id) || [])

    if (this.queue) {
      this.queue.cancel(ids)
    }

    const promises: Array<any> = []
    for (const id of ids) {
      if (active.has(id)) {
        this.cancelledJobs.add(id)
      } else {
        // just delete the job
        promises.push(tx.store.delete(id))
      }
    }

    await Promise.all([...promises, tx.done])

    await this.postAction({
      type: 'mutate',
    })
  }

  pause() {
    if (this.paused) return
    this.paused = true
    if (this.queue) {
      this.queue.pause()
    }
  }

  async resume() {
    if (!this.paused) return
    this.paused = false
    if (this.queue) {
      this.startSession()
      await this.queue.resume()
    }
  }

  async updateProgress(
    job: Job<TaskData, TaskResponse>,
    progress: number | null,
  ): Promise<Job<TaskData, TaskResponse> | undefined> {
    const db = await this.client()
    const tx = db.transaction(DBTables.queue, 'readwrite')
    const existingJob = await tx.store.get(job.id)
    if (existingJob?.status !== JobState.InProgress) {
      return existingJob
    }
    const nextJob = this.modifyJobUpdate(job, {
      ...existingJob,
      progress,
    })
    await Promise.all([tx.store.put(nextJob), tx.done])
    return nextJob
  }

  protected startSession() {
    if (!this.paused) {
      // in case we paused the queue due to exceeding network errors limit
      this.queue?.resume()
    }
    this.session = {
      dbErrors: 0,
      networkErrors: 0,
    }
  }

  protected init() {
    return new Promise<void>(async (resolve) => {
      this.handleClose = resolve
      this.receivingNewJobs = false
      this.canClose = false
      this.queue = new ConcurrentQueue({
        concurrency: this.opts.concurrency,
        onProcess: this.processJob,

        // TODO: review output and handle permanent or retryable errors
        onFailure: (_e) => null,

        onDrain: () => {
          this.canClose = true

          if (this.receivingNewJobs) {
            return
          }

          if (this.paused || this.queue?.countWaiting() === 0) {
            this.onClose()
          }
        },
      })
      this.startSession()

      await this.open()
      const meta = await this.getMeta()
      if (meta.paused) {
        await this.onClose()
        return
      }

      const jobs = await this.initQueue()
      if (jobs.length) {
        await this.setMeta({
          processing: true,
        })
        jobs.forEach((job) => this.queue?.add(job))
      } else {
        await this.onClose()
        return
      }

      await this.postAction({
        type: 'run',
      })
    })
  }

  protected async onClose() {
    try {
      await this.postAction({
        type: 'stop',
      })
    } catch (e) {
      console.error('[SWJQ] Failed post action "stop"', e)
    }
    await this.setMeta({
      processing: false,
    })

    this.handleClose && this.handleClose()
    this.handleClose = void 0
    this.receivingNewJobs = false
    this.canClose = false
    this.queue = undefined
  }

  protected async initQueue() {
    return this.getJobsByStatuses([JobState.Pending, JobState.InProgress])
  }

  // TODO: review. we probably need to handle retry logic and so forth here
  protected async processJob(job: Job<TaskData, TaskResponse>) {
    let db
    if (this.session.networkErrors >= this.opts.maxAllowedNetworkErrors) {
      this.queue?.pause()
      return
    }
    try {
      // claim and get the latest job
      const data = await this.claimJob(job)
      if (!data.job) {
        console.warn('failed to process job. it no longer exists', { job })
        return
      }

      db = data.db
      job = data.job
    } catch (e) {
      if (++this.session.dbErrors < this.opts.maxAllowedDbErrors) {
        // just requeue
        this.queue?.add(job)
      }
      return
    }

    try {
      const response = await this.processFn(job, db)

      // If a cancelled job failed, just silently remove it
      // TODO: jobs that succeed may be a concern
      if (this.cancelledJobs.has(job.id)) {
        switch (response.status) {
          case JobResult.FailedPermanently:
          case JobResult.FailedRetryable:
            try {
              await this.deleteJob(job.id)
            } catch (e) {
              this.session.dbErrors++
            }
            return
        }
      }

      switch (response.status) {
        case JobResult.Cancelled:
          await this.deleteJob(job.id)
          break
        case JobResult.FailedPermanently:
          await this.onJobFailure(job, response)
          break
        case JobResult.FailedRetryable:
          const releasedJob = await this.onJobFailure(job, response)
          if (releasedJob) {
            if (this.session.networkErrors < this.opts.maxAllowedNetworkErrors) {
              this.queue?.add(releasedJob)
            } else {
              this.queue?.pause()
            }
          }
          break
        case JobResult.Success:
          await this.onJobComplete(job, response)
          break
      }
    } catch (error) {
      const e = error as any
      this.session.networkErrors++
      // TODO: should we treat as retryable or not?
      await this.onJobFailure(job, {
        status: JobResult.FailedPermanently,
        reason: e.message,
        stacktrace: e.stacktrace || e.stack,
      })
    }
  }

  protected async onJobComplete(
    job: Job<TaskData, TaskResponse>,
    result: JobResponse<TaskResponse>,
  ): Promise<Job<TaskData, TaskResponse> | undefined> {
    try {
      const db = await this.client()
      const tx = db.transaction(DBTables.queue, 'readwrite')
      const existingJob: Job<TaskData, TaskResponse> | undefined = await tx.store.get(job.id)
      if (!existingJob) return existingJob
      const nextJob = this.modifyJobUpdate(job, {
        ...existingJob,
        returnvalue: result.data,
        status: JobState.Complete,
        finishedOn: Date.now(),
        processedOn: Date.now(),
      })
      await Promise.all([tx.store.put(nextJob), tx.done])
      return nextJob
    } catch (err2) {
      console.error(new Error(`Fatal error updating job ${job.id}`))
      this.session.dbErrors++
      return void 0
    }
  }

  protected async onJobFailure(
    job: Job<TaskData, TaskResponse>,
    response: JobResponse<TaskResponse>,
  ): Promise<Job<TaskData, TaskResponse> | undefined> {
    try {
      const db = await this.client()
      const tx = db.transaction(DBTables.queue, 'readwrite')
      const existingJob: Job<TaskData, TaskResponse> | undefined = await tx.store.get(job.id)
      if (!existingJob) return existingJob
      const nextJob = this.modifyJobUpdate(job, {
        ...existingJob,
        status:
          response.status === JobResult.FailedRetryable
            ? JobState.Pending
            : JobState.FailedPermanently,
        processedOn: Date.now(),
        failedReason: response.reason ?? void 0,
        stacktrace: response.stacktrace ?? void 0,
      })
      await Promise.all([tx.store.put(nextJob), tx.done])
      return nextJob
    } catch (err2) {
      console.error(new Error(`Fatal error updating job ${job.id}`))
      this.session.dbErrors++
      return void 0
    }
  }

  protected async claimJob(job: Job<TaskData, TaskResponse>) {
    const db = await this.client()
    const tx = db.transaction(DBTables.queue, 'readwrite')
    const existingJob: Job<TaskData, TaskResponse> | undefined = await tx.store.get(job.id)
    if (!existingJob) return { db, job: void 0 }
    const nextJob = this.modifyJobUpdate(job, {
      ...existingJob,
      attemptsMade: existingJob.attemptsMade + 1,
      status: JobState.InProgress,
      startedAt: Date.now(),
      processedOn: void 0,
      failedReason: void 0,
      stacktrace: void 0,
    })
    await Promise.all([tx.store.put(nextJob), tx.done])
    return { db, job: nextJob }
  }
}
