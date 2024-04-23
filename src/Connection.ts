import { BroadcastChannel } from 'broadcast-channel'
import EventEmitter from 'eventemitter3'
import { DBSchema, IDBPDatabase, openDB } from 'idb'
import { Job, JobState } from './Job'
import { QueueEvent } from './QueueEvents'
import { queryJobs } from './utils'

export interface ConnectionOptions {
  prefix?: string
  version?: number
}

export interface InternalConnectionOptions {
  prefix: string
  version: number
}

export enum DBTables {
  queue = 'queue',
  meta = 'meta',
}

export enum QueueIndexes {
  /** technically, 'id' is just the store itself but it's helpful for the query options to define here */
  id = 'id',
  status = 'status',
}

export interface QueueDBSchema<TaskData = any, TaskResponse = any> extends DBSchema {
  [DBTables.queue]: {
    key: number
    value: Job<TaskData, TaskResponse>
    indexes: {
      [QueueIndexes.status]: JobState
    }
  }
  [DBTables.meta]: {
    key: DBTables.meta
    value: QueueMeta
  }
}

export interface QueueMeta {
  id: DBTables.meta
  processing: boolean
  paused: boolean
}

export const initialMeta: QueueMeta = {
  id: DBTables.meta,
  processing: false,
  paused: false,
}

export class Connection<TaskData = any, TaskResponse = any> extends EventEmitter<{
  message: (e: QueueEvent) => void
}> {
  protected db: IDBPDatabase<QueueDBSchema<TaskData, TaskResponse>> | undefined
  protected opts: InternalConnectionOptions
  protected connection: Promise<IDBPDatabase<QueueDBSchema<TaskData, TaskResponse>>> | undefined
  protected channel: BroadcastChannel<QueueEvent>

  constructor(
    public readonly name: string,
    opts: ConnectionOptions = {},
  ) {
    super()
    this.opts = {
      prefix: 'swjq',
      version: 1,
      ...opts,
    }

    this.channel = new BroadcastChannel(`${this.opts.prefix}:${name}`, {
      webWorkerSupport: true,
    })
  }

  protected async client() {
    return this.open()
  }

  async postAction(action: QueueEvent) {
    this.emit('message', action)
    await this.channel.postMessage(action)
  }

  async open(): Promise<IDBPDatabase<QueueDBSchema<TaskData, TaskResponse>>> {
    if (this.db) {
      return this.db
    }

    if (!this.connection) {
      this.connection = openDB(this.name, this.opts.version, {
        upgrade: (database, _oldVersion, _newVersion, _transaction) => {
          if (!database.objectStoreNames.contains(DBTables.queue)) {
            const store = database.createObjectStore(DBTables.queue, {
              keyPath: 'id',
              autoIncrement: true,
            })

            store.createIndex(QueueIndexes.status, 'status', {
              unique: false,
            })
            // TODO: create indexes as needed
          }

          if (!database.objectStoreNames.contains(DBTables.meta)) {
            const store = database.createObjectStore(DBTables.meta, { keyPath: 'id' })
            store.put(initialMeta)
          }
        },
      })
      this.db = await this.connection
    }
    return this.connection
  }

  async close() {
    if (!this.connection) return
    const db = await this.client()
    db.close()
    this.connection = undefined
    this.db = undefined
  }

  async deleteJob(id: number) {
    const db = await this.client()
    return db.delete(DBTables.queue, id)
  }

  async getMeta(): Promise<QueueMeta> {
    const db = await this.client()
    const data = (await db.get(DBTables.meta, DBTables.meta)) as QueueMeta
    return data || initialMeta
  }

  protected async setMeta(meta: Omit<Partial<QueueMeta>, 'id'>) {
    const db = await this.client()

    const tx = db.transaction(DBTables.meta, 'readwrite')
    const existing = await tx.store.get(DBTables.meta)
    // TODO: this may be a good spot to post messages based on the diff btw input and existing
    await tx.store.put({
      ...(existing || initialMeta),
      ...meta,
      id: DBTables.meta,
    })
    await tx.done
  }

  protected async getJobsByStatuses(statuses: Array<JobState>) {
    const set = new Set(statuses)
    const db = await this.client()
    const jobs = await queryJobs<TaskData, TaskResponse>(db)
    return jobs.filter((job) => set.has(job.status))
  }

  protected async getJobsById(ids: Array<number>) {
    const db = await this.client()
    return queryJobs<TaskData, TaskResponse>(db, { property: QueueIndexes.id, ids })
  }

  protected async getJobsByStatus(status: JobState) {
    const db = await this.client()
    return queryJobs<TaskData, TaskResponse>(db, { property: QueueIndexes.status, status })
  }
}
