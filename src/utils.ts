import {DBTables, QueueDBSchema, QueueIndexes} from './Connection'
import { Job, JobState } from './Job'
import { IDBPCursorWithValue, IDBPDatabase, IDBPIndex } from 'idb'

export type QueryJobsOptions = QueryJobsById | QueryJobsByStatus

interface QueryJobsById {
  property: QueueIndexes.id
  ids: Array<number>
}

interface QueryJobsByStatus {
  property: QueueIndexes.status
  status: JobState
}

export async function queryJobs<TaskData, TaskResponse>(
  db: IDBPDatabase<QueueDBSchema<TaskData, TaskResponse>>,
  opts?: QueryJobsOptions,
): Promise<Array<Job<TaskData, TaskResponse>>> {
  const tx = db.transaction(DBTables.queue)
  const store = tx.objectStore(DBTables.queue)
  const items: Array<Job<TaskData, TaskResponse>> = []

  let source: IDBPIndex<QueueDBSchema<TaskData, TaskResponse>, [DBTables.queue], DBTables.queue, QueueIndexes.status>
  let cursor: IDBPCursorWithValue<QueueDBSchema<TaskData, TaskResponse>, [DBTables.queue], DBTables.queue> | null = null

  if (opts) {
    switch (opts.property) {
      case QueueIndexes.id: {
        const ids = new Set(opts.ids)
        cursor = await store.openCursor()

        while (cursor) {
          const job = cursor.value
          if (ids.has(job.id)) {
            items.push(cursor.value)
          }
          cursor = await cursor.continue()
        }
        break
      }
      case QueueIndexes.status:
        source = store.index(QueueIndexes.status)
        cursor = await source.openCursor(opts.status)
        break
    }
  } else {
    if (store.getAll) {
      return store.getAll()
    } else {
      cursor = await store.openCursor()
    }
  }

  while (cursor) {
    items.push(cursor.value)
    cursor = await cursor.continue()
  }

  return items
}

export async function deleteJobs<TaskData, TaskResponse>(
  db: IDBPDatabase<QueueDBSchema<TaskData, TaskResponse>>,
  filter: (job: Job<TaskData, TaskResponse>) => boolean,
): Promise<void> {
  const tx = db.transaction(DBTables.queue, 'readwrite')

  let cursor = await tx.store.openCursor()
  const items: Array<Promise<void>> = []

  while (cursor) {
    if (filter(cursor.value)) {
      items.push(cursor.delete())
    }
    cursor = await cursor.continue()
  }

  await Promise.all([...items, tx.done])
}
