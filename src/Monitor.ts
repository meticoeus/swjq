import isEqual from 'lodash.isequal'
import { defer, from, fromEvent, interval, merge, Observable, of } from 'rxjs'
import {
  concatMap,
  delay,
  distinctUntilChanged,
  filter,
  map,
  mapTo,
  mergeMap,
  retry,
  startWith,
  switchMap,
  takeUntil,
  throttleTime,
} from 'rxjs/operators'
import { Connection, QueueMeta } from './Connection'
import { Job, JobState } from './Job'
import { QueueEvent } from './QueueEvents'
import { queryJobs, QueryJobsOptions } from './utils'

const ObserveInterval = 1000

export interface MonitorStats {
  pending: number
  errors: number
  inProgress: number
  complete: number
  total: number
}

export class Monitor<TaskData = any, TaskResponse = any> extends Connection<
  TaskData,
  TaskResponse
> {
  job$(opts?: QueryJobsOptions): Observable<Array<Job<TaskData, TaskResponse>>> {
    const { mutate$, start$, stop$ } = this.buildObservables()

    return from(this.client()).pipe(
      mergeMap((db) =>
        merge(
          start$.pipe(
            switchMap(() => merge(of(null), interval(ObserveInterval).pipe(takeUntil(stop$)))),
          ),
          mutate$,
          stop$,
          // Delay the stop event to re-query jobs in case of race-condition problems.
          stop$.pipe(delay(2 * ObserveInterval)),
        ).pipe(mapTo(db)),
      ),
      throttleTime(ObserveInterval, void 0, { leading: true, trailing: true }),
      concatMap((db) => queryJobs<TaskData, TaskResponse>(db, opts)),
      retry(500),
    )

    // This may be the safer option...
    // return from(this.client())
    //   .pipe(mergeMap((db) =>
    //     interval(ObserveInternal)
    //       .pipe(
    //         concatMap(() => queryJobs<Job<TaskData, TaskResponse>>(db, opts)),
    //       )
    //   ))
    //   .pipe(
    //     retry(5),
    //   );
  }

  jobWithStats$(opts?: QueryJobsOptions) {
    return this.job$(opts).pipe(
      map((jobs) => ({
        loading: false,
        error: null,
        ...sortJobs(jobs),
      })),
    )
  }

  meta$(): Observable<QueueMeta> {
    const { meta$, start$, stop$ } = this.buildObservables()

    return merge(
      start$.pipe(
        switchMap(() => merge(of(null), interval(ObserveInterval * 10).pipe(takeUntil(stop$)))),
      ),
      stop$,
    ).pipe(
      concatMap(() => meta$),
      retry(500),
      distinctUntilChanged(isEqual),
    )
  }

  protected buildObservables() {
    const message$: Observable<QueueEvent> = merge(
      fromEvent(this, 'message'),
      fromEvent(this.channel, 'message'),
    )
    const meta$ = defer(() => this.getMeta())
    const start$ = message$.pipe(
      filter((action) => action.type === 'run' || action.type === 'resume'),
      startWith({ type: 'run' } as QueueEvent),
    )

    const mutate$ = message$.pipe(filter((action) => action.type === 'mutate'))

    const stop$ = merge(
      message$.pipe(filter((action) => action.type === 'pause' || action.type === 'stop')),
      meta$.pipe(
        filter((meta) => !meta.processing),
        mapTo({ type: 'stop' } as QueueEvent),
      ),
    )

    return {
      message$,
      meta$,
      mutate$,
      start$,
      stop$,
    }
  }
}

export function sortJobs<TaskData, TaskResponse>(jobs: Array<Job<TaskData, TaskResponse>>) {
  jobs.sort((a, b) => b.timestamp - a.timestamp)
  const stats: MonitorStats = {
    pending: 0,
    inProgress: 0,
    errors: 0,
    complete: 0,
    total: jobs.length,
  }
  for (const job of jobs) {
    switch (job.status) {
      case JobState.Pending:
        stats.pending++
        break
      case JobState.InProgress:
        stats.inProgress++
        break
      case JobState.FailedPermanently:
        stats.errors++
        break
      case JobState.Complete:
        stats.complete++
        break
    }
  }

  return {
    jobs,
    stats,
  }
}
