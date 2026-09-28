import { NativeEventEmitter, NativeModule, Platform } from 'react-native'
import type { DownloadTask } from './DownloadTask'
import type { Headers } from './types'
import type { GroupSnapshotEvent } from './NativeRNBackgroundDownloader'

/** `paused`: stopped by pauseAll() / the Android notification; keeps finished tasks until resumeAll(). */
export type GroupState = 'queued' | 'running' | 'retrying' | 'paused' | 'done' | 'failed' | 'canceled'

export interface GroupTaskSpec {
  id: string
  url: string
  destination: string
  headers?: Headers
}

export interface GroupSpec {
  id: string
  name?: string
  tasks: GroupTaskSpec[]
  /** 0..1 JPEG re-encode quality applied to every finished image (0 = keep as is). */
  compressValue?: number
}

export interface GroupSnapshot {
  id: string
  name: string
  state: GroupState
  /** 0-based retry pass the group is on. */
  attempt: number
  total: number
  completed: number
  failed: number
  failedTaskIds: string[]
}

export interface GroupQueueConfig {
  /** How many groups download at the same time (1 = strictly one after another). Default 1. */
  maxConcurrentGroups?: number
  /** Extra passes over a group's failed tasks before the group is reported `failed`. Default 2. */
  maxRetries?: number
  /** Pause before each retry pass (last value repeats). Default [3000, 10000]. */
  retryDelaysMs?: number[]
  /**
   * Android: texts of the download notification (it shows chapters/groups of the current batch and has
   * pause / resume / cancel actions). Placeholders: {done} {total} {failed} {name}.
   * Keys: title, progress, paused, finished, finishedWithErrors, resumeOnLaunch, actionPause, actionResume, actionCancel.
   */
  notificationTexts?: Record<string, string>
}

type Listener = (snapshot: GroupSnapshot) => void

export interface GroupQueueNative {
  enqueueGroup?: (group: GroupSpec) => void
  cancelGroup?: (id: string) => Promise<void>
  acknowledgeGroup?: (id: string) => void
  getGroups?: () => Promise<GroupSnapshot[]>
  setGroupQueueConfig?: (config: GroupQueueConfig) => void
  pauseAllGroups?: () => void
  resumeAllGroups?: () => void
  cancelAllGroups?: () => void
  onGroupState?: (handler: (s: GroupSnapshotEvent) => void) => unknown
  onGroupProgress?: (handler: (s: GroupSnapshotEvent) => void) => unknown
}

interface Backend {
  enqueue (group: GroupSpec): void
  cancel (id: string): Promise<void>
  acknowledge (id: string): void
  getAll (): Promise<GroupSnapshot[]>
  configure (config: GroupQueueConfig): void
  pauseAll (): void
  resumeAll (): void
  cancelAll (): void
}

const stateListeners = new Set<Listener>()
const progressListeners = new Set<Listener>()
const emitState = (s: GroupSnapshot) => stateListeners.forEach(l => l(s))
const emitProgress = (s: GroupSnapshot) => progressListeners.forEach(l => l(s))

/** The queue lives in native code (GroupQueue.kt / RNBGDGroupQueue.mm); JS only forwards calls and events. */
function createNativeBackend (native: GroupQueueNative & NativeModule): Backend {
  if (Platform.OS === 'ios' && typeof native.onGroupState === 'function' && typeof native.onGroupProgress === 'function') {
    // iOS new architecture: events come through the TurboModule's typed emitters.
    native.onGroupState(s => emitState(s as GroupSnapshot))
    native.onGroupProgress(s => emitProgress(s as GroupSnapshot))
  } else {
    const emitter = new NativeEventEmitter(native)
    emitter.addListener('groupState', emitState)
    emitter.addListener('groupProgress', emitProgress)
  }
  return {
    enqueue: group => native.enqueueGroup!(group),
    cancel: id => native.cancelGroup!(id),
    acknowledge: id => native.acknowledgeGroup!(id),
    getAll: () => native.getGroups!(),
    configure: config => native.setGroupQueueConfig!(config),
    pauseAll: () => native.pauseAllGroups?.(),
    resumeAll: () => native.resumeAllGroups?.(),
    cancelAll: () => native.cancelAllGroups?.(),
  }
}

/**
 * Same contract in JS on top of per-task downloads — fallback for a native build that predates the
 * native queue. Scheduling the next group needs JS, so the queue stalls while JS is suspended.
 */
function createJsBackend (createTask: (spec: GroupTaskSpec) => DownloadTask): Backend {
  interface G { spec: GroupSpec, snap: GroupSnapshot, done: Set<string>, pending: Map<string, DownloadTask> }
  const groups = new Map<string, G>()
  let cfg: Required<Omit<GroupQueueConfig, 'notificationTexts'>> = { maxConcurrentGroups: 1, maxRetries: 2, retryDelaysMs: [3000, 10000] }

  const snap = (g: G): GroupSnapshot => ({ ...g.snap, completed: g.done.size, failedTaskIds: [...g.snap.failedTaskIds] })
  const isTerminal = (s: GroupState) => s === 'done' || s === 'failed' || s === 'canceled'

  const schedule = () => {
    let running = [...groups.values()].filter(g => g.snap.state === 'running' || g.snap.state === 'retrying').length
    for (const g of groups.values()) {
      if (running >= cfg.maxConcurrentGroups) break
      if (g.snap.state !== 'queued') continue
      running++
      startAttempt(g)
    }
  }

  const settleAttempt = (g: G) => {
    if (g.snap.state === 'paused') return
    if (g.snap.failedTaskIds.length && g.snap.attempt < cfg.maxRetries) {
      const delay = cfg.retryDelaysMs[g.snap.attempt] ?? cfg.retryDelaysMs[cfg.retryDelaysMs.length - 1] ?? 0
      g.snap.attempt++
      g.snap.state = 'retrying'
      emitState(snap(g))
      setTimeout(() => { if (g.snap.state === 'retrying') startAttempt(g) }, delay)
      return
    }
    g.snap.state = g.snap.failedTaskIds.length ? 'failed' : 'done'
    emitState(snap(g))
    schedule()
  }

  const startAttempt = (g: G) => {
    if (isTerminal(g.snap.state) || g.snap.state === 'paused') return
    g.snap.state = 'running'
    g.snap.failedTaskIds = []
    g.snap.failed = 0
    emitState(snap(g))
    const todo = g.spec.tasks.filter(t => !g.done.has(t.id))
    if (!todo.length) return settleAttempt(g)
    for (const t of todo) {
      const task = createTask(t)
      g.pending.set(t.id, task)
      const settle = (ok: boolean) => {
        if (!g.pending.delete(t.id)) return
        if (ok) g.done.add(t.id)
        else { g.snap.failedTaskIds.push(t.id); g.snap.failed++ }
        if (g.pending.size === 0) settleAttempt(g)
        else emitProgress(snap(g))
      }
      task.done(() => settle(true)).error(() => settle(false))
      task.start()
    }
  }

  return {
    enqueue (spec) {
      const existing = groups.get(spec.id)
      if (existing && !isTerminal(existing.snap.state)) return
      const g: G = {
        spec,
        done: new Set(),
        pending: new Map(),
        snap: { id: spec.id, name: spec.name ?? spec.id, state: 'queued', attempt: 0, total: spec.tasks.length, completed: 0, failed: 0, failedTaskIds: [] },
      }
      groups.set(spec.id, g)
      emitState(snap(g))
      schedule()
    },
    async cancel (id) {
      const g = groups.get(id)
      if (!g || isTerminal(g.snap.state)) return
      g.snap.state = 'canceled'
      const pending = [...g.pending.values()]
      g.pending.clear()
      await Promise.all(pending.map(t => t.stop()))
      emitState(snap(g))
      groups.delete(id)
      schedule()
    },
    acknowledge (id) {
      const g = groups.get(id)
      if (g && isTerminal(g.snap.state)) groups.delete(id)
    },
    getAll: async () => [...groups.values()].map(snap),
    configure (config) {
      const { notificationTexts: _texts, ...queueConfig } = config
      cfg = { ...cfg, ...Object.fromEntries(Object.entries(queueConfig).filter(([, v]) => v !== undefined)) }
      schedule()
    },
    pauseAll () {
      for (const g of groups.values()) {
        if (isTerminal(g.snap.state) || g.snap.state === 'paused') continue
        g.snap.state = 'paused'
        const pending = [...g.pending.values()]
        g.pending.clear()
        pending.forEach(t => { t.stop() })
        emitState(snap(g))
      }
    },
    resumeAll () {
      for (const g of groups.values()) {
        if (g.snap.state !== 'paused') continue
        g.snap.state = 'queued'
        g.snap.attempt = 0
        emitState(snap(g))
      }
      schedule()
    },
    cancelAll () {
      for (const id of [...groups.keys()]) this.cancel(id)
    },
  }
}

let backend: Backend | null = null

/** @internal wired by index.ts once the native module is initialized. */
export function initGroupQueue (native: GroupQueueNative & NativeModule, createTask: (spec: GroupTaskSpec) => DownloadTask) {
  if (backend) return
  backend = typeof native.enqueueGroup === 'function'
    ? createNativeBackend(native)
    : createJsBackend(createTask)
}

function getBackend (): Backend {
  if (!backend) throw new Error('[RNBackgroundDownloader] groupQueue used before the native module was initialized')
  return backend
}

/**
 * Queue of download groups (e.g. one group per manga chapter). Enqueue everything at once; the queue
 * runs `maxConcurrentGroups` groups at a time, retries failed tasks and reports group-level events only.
 * The queue is native — it keeps going while JS is backgrounded or busy.
 */
export const groupQueue = {
  enqueue: (group: GroupSpec) => getBackend().enqueue(group),
  cancel: (id: string) => getBackend().cancel(id),
  /** The host recorded a finished group's result — the queue may forget it. */
  acknowledge: (id: string) => getBackend().acknowledge(id),
  /** Every group the queue still knows, incl. ones that settled while JS wasn't listening. */
  getAll: () => getBackend().getAll(),
  configure: (config: GroupQueueConfig) => getBackend().configure(config),
  /** Stops every unfinished group (finished tasks are kept); nothing starts until resumeAll(). */
  pauseAll: () => getBackend().pauseAll(),
  resumeAll: () => getBackend().resumeAll(),
  /** Cancels every unfinished group — each reports `canceled`; the host acknowledges them. */
  cancelAll: () => getBackend().cancelAll(),
  onState (listener: Listener) {
    stateListeners.add(listener)
    return { remove: () => stateListeners.delete(listener) }
  },
  onProgress (listener: Listener) {
    progressListeners.add(listener)
    return { remove: () => progressListeners.delete(listener) }
  },
}
