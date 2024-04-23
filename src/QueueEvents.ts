export type QueueEvent =
  | QueueRunEvent
  | QueueStopEvent
  | QueuePauseEvent
  | QueueResumeEvent
  | QueueMutateEvent

export interface QueueRunEvent {
  type: 'run'
}

export interface QueueStopEvent {
  type: 'stop'
}

export interface QueuePauseEvent {
  type: 'pause'
}

export interface QueueResumeEvent {
  type: 'resume'
}

export interface QueueMutateEvent {
  type: 'mutate'
}
