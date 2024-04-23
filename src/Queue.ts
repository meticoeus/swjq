import { Connection, DBTables } from './Connection'
import { Job, JobRequest, JobState } from './Job'
import { deleteJobs } from './utils'

export interface Message {
  type: string
  payload?: any
}

export class Queue<TaskData = any, TaskResponse = any> extends Connection<TaskData, TaskResponse> {
  async getWorker(): Promise<ServiceWorker | null> {
    const reg = await navigator.serviceWorker.ready
    return reg.active
  }

  async postMessage(message: Message) {
    const sw = await this.getWorker()
    sw?.postMessage(message)
  }

  async addJob(job: JobRequest<TaskData, TaskResponse>) {
    const db = await this.client()

    const tx = db.transaction(DBTables.queue, 'readwrite', {
      durability: 'strict',
    })

    // @ts-ignore - https://github.com/jakearchibald/idb/issues/150
    const add = tx.store.add(this.initializeJob(job))
    const [id, _] = await Promise.all([add, tx.done])

    await Promise.all([
      this.postMessage({
        type: `${this.name}:Add`,
        payload: { count: 1 },
      }),
      this.postAction({
        type: 'mutate',
      }),
    ])

    return id
  }

  async addJobBatch(jobs: Array<JobRequest<TaskData, TaskResponse>>): Promise<Array<number>> {
    const db = await this.client()

    const tx = db.transaction(DBTables.queue, 'readwrite', {
      durability: 'strict',
    })

    const adds: Array<Promise<any>> = []

    for (const job of jobs) {
      // see https://github.com/jakearchibald/idb/issues/150
      const add = tx.store.add(this.initializeJob(job) as unknown as Job<TaskData, TaskResponse>)
      adds.push(add)
    }
    const [ids] = await Promise.all([Promise.all(adds), tx.done])

    await Promise.all([
      this.postMessage({
        type: `${this.name}:Add`,
        payload: { count: jobs.length },
      }),
      this.postAction({
        type: 'mutate',
      }),
    ])

    return ids
  }

  async getJob(id: number): Promise<Job<TaskData, TaskResponse> | undefined> {
    const db = await this.client()
    const tx = db.transaction(DBTables.queue)
    const existingJob = await tx.store.get(id)
    await tx.done
    return existingJob
  }

  async updateJob(
    id: number,
    update: (job?: Job<TaskData, TaskResponse>) => Job<TaskData, TaskResponse> | null,
  ): Promise<Job<TaskData, TaskResponse> | undefined> {
    const db = await this.client()
    const tx = db.transaction(DBTables.queue, 'readwrite')
    const existingJob = await tx.store.get(id)
    const nextJob = update(existingJob)
    if (!nextJob) {
      return existingJob
    }
    await Promise.all([tx.store.put(nextJob), tx.done])

    await this.postAction({
      type: 'mutate',
    })

    return nextJob
  }

  /**
   * Add a job to the log that failed client side validation.
   */
  async addJobError(job: JobRequest<TaskData, TaskResponse>, reason?: string) {
    const db = await this.client()

    const tx = db.transaction(DBTables.queue, 'readwrite', {
      durability: 'strict',
    })

    const jobData = this.initializeJob(job)
    jobData.status = JobState.FailedPermanently
    jobData.failedReason = reason
    jobData.attemptsMade = 0

    // @ts-ignore - https://github.com/jakearchibald/idb/issues/150
    const add = tx.store.add(jobData)
    const [id, _] = await Promise.all([add, tx.done])

    await this.postAction({
      type: 'mutate',
    })

    return id
  }

  async pause() {
    const { paused } = await this.getMeta()
    if (paused) return

    await this.setMeta({ paused: true })

    // We need to post this here instead of the worker since the worker is never executed if it isn't already running
    await Promise.all([
      this.postAction({
        type: 'pause',
      }),
      this.postMessage({
        type: `${this.name}:Pause`,
      }),
    ])
  }

  async resume() {
    const { paused } = await this.getMeta()
    if (!paused) return

    await this.setMeta({ paused: false })

    // Be consistent with pause for faster client UI feedback
    await Promise.all([
      this.postAction({
        type: 'resume',
      }),
      this.postMessage({
        type: `${this.name}:Resume`,
      }),
    ])
  }

  private initializeJob(
    job: JobRequest<TaskData, TaskResponse>,
  ): Omit<Job<TaskData, TaskResponse>, 'id'> {
    const opts = job.opts || {}
    return {
      ...job,
      opts,
      status: JobState.Pending,
      progress: null,
      attemptsMade: 0,
      timestamp: opts.timestamp || Date.now(),
    }
  }

  async cancelJobs(ids: Array<number>) {
    if (ids.length) {
      const sw = await this.getWorker()
      if (sw) {
        await this.postMessage({
          type: `${this.name}:CancelJobs`,
          payload: {
            ids,
          },
        })
      } else {
        try {
          await Promise.all(ids.map((id) => this.deleteJob(id)))
        } catch (e) {}
      }
    }
  }

  async clearFailedJobs() {
    const db = await this.client()
    await deleteJobs<TaskData, TaskResponse>(
      db,
      (job) => job.status === JobState.FailedPermanently,
    )

    await this.postAction({
      type: 'mutate',
    })
  }
}
