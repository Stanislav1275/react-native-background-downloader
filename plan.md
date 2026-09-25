# Fork Fix Plan

> Working directory: `/Users/cocktail/WebstormProjects/react-native-background-downloader`
> App consuming it: `/Users/cocktail/WebstormProjects/app/fsd/features/download/`

---

## Bugs Analyzed

### #164 — Android 11: `getExistingDownloadTasks()` returns `[]` with internal destination path
**Root cause (3 parts):**
1. When `destination` is an internal app path (e.g. `RNFS.DocumentDirectoryPath`), Android's `DownloadManager.setDestinationUri()` throws `SecurityException`.
2. Library catches it → falls back to `ResumableDownloader` (downloads work, progress events work).
3. `getExistingDownloadTasks()` Phase 4 was a no-op — active `ResumableDownloader` tasks were never included in results.

### #159 — Android: `getExistingDownloadTasks()` returns `[]` after force-stop if download is active
**Root cause:** After force-stop, Android `DownloadManager` may reassign new download IDs. The persisted `downloadId → config` map in MMKV becomes stale. Phase 1 found the download row in `DownloadManager` but couldn't match it to any config → incorrectly cancelled it.

### #161 — iOS: `setConfig({ allowsCellularAccess })` crashes app (SIGSEGV)
**Root cause:** `_setAllowsCellularAccessInternal:` is a void method that calls `[urlSession invalidateAndCancel]` which can throw `NSException`. The TurboModule bridge catches unhandled exceptions from void methods on background queues and calls `convertNSExceptionToJSError`, which accesses Hermes VM from `com.eko.backgrounddownloader` queue — Hermes is not thread-safe → SIGSEGV. Same pattern affects any void method that throws on this queue.

---

## Fixes Applied

### ✅ Fix 1 — Android Phase 4: active ResumableDownloader tasks in `getExistingDownloadTasks()`
**Files:** `android/src/main/java/com/eko/ResumableDownloader.kt`, `RNBackgroundDownloaderModuleImpl.kt`

- Added `getActiveDownloads(): Map<String, DownloadState>` to `ResumableDownloader`
- Phase 4 now iterates active downloads (non-cancelled, non-paused), builds task info using `configIdToMetadata[configId]` for metadata

**Fixes:** #164 (app still running, download active), partially #159 (Android 16+ path where ResumableDownloader is always used)

**Limitation:** Does NOT fix force-stop with ResumableDownloader — state is in-memory only and lost after process kill. Paused downloads survive force-stop (state is written to disk on pause).

### ✅ Fix 2 — Android Phase 1: destination path matching fallback
**File:** `android/src/main/java/com/eko/RNBackgroundDownloaderModuleImpl.kt`

- In Phase 1, when `downloadId` not found in `downloadIdToConfig` map, try to match by normalized local URI vs `config.destination`
- On match: restore both `downloadIdToConfig[downloadId]` and `configIdToDownloadId[configId]`, persist via `saveDownloadIdToConfigMap()`
- On no match: cancel orphan download (unchanged behavior)

**Fixes:** #159 (active DownloadManager download, force-stop, new download ID assigned)

### ✅ Fix 3 — iOS: wrap void config methods in `@try/@catch`
**File:** `ios/RNBackgroundDownloader.mm`

- Wrapped `_setAllowsCellularAccessInternal:` and `_setMaxParallelDownloadsInternal:` in `@try/@catch`
- Exceptions caught and logged via `DLog` — not propagated to TurboModule bridge → no Hermes thread-safety violation

**Fixes:** #161 (SIGSEGV on `setConfig({ allowsCellularAccess })` with New Architecture)

---

## Remaining / Not Fixed

### 🔶 #159 — ResumableDownloader force-stop (active → lost)
After force-stop, active `ResumableDownloader` downloads (Android 16+, or internal-path fallback) lose all in-memory state. `getExistingDownloadTasks()` cannot recover them.

**Options to fix:**
1. **Persist active ResumableDownloader state to MMKV on progress tick** (e.g. every N bytes). On restart, load persisted state and surface as `SUSPENDED` in `getExistingDownloadTasks()`. User would need to call `resumeTask()`.
2. **Restore temp-file-then-move pattern for DownloadManager** (matches 4.3.2 behavior). Always download to `getExternalFilesDir()` temp file, move to final destination on completion. Avoids ResumableDownloader fallback for internal-path cases on Android < 16.

Option 2 is safer (matches old working behavior), Option 1 is more complete.

### 🔶 #161 — Other void methods potentially affected
`download:` (the main download start method) and `setNotificationGroupingConfig:` also run on the background queue and could theoretically crash if they throw. Lower risk but worth wrapping.

---

## Docs Updated

- `docs/PLATFORM_NOTES.md` — added Troubleshooting entries for #164, #159, #161 with root cause + fix + workaround for older versions

---

## App Integration Notes (`app/fsd/features/download/`)

The app uses `KeshaTaskFactory` (`model/infrastructure/kesha-task-factory.ts`) and calls `getExistingTasks()` via `restoreActiveTasks()` in `DownloadManager`. With Fix 1 applied, active ResumableDownloader downloads (Android 11 fallback, Android 16+) will now appear in restore.

**Potential behavior change:** `restoreActiveTasks()` will now see previously invisible active tasks. Ensure session restore handles `state = TASK_RUNNING` tasks correctly (not just `TASK_SUSPENDED`). Review `TitleDownloadSession.restore()` to confirm it doesn't assume restored tasks are always paused.

---

## 🔴 Throttling & completion live in app JS, not in native (2026-09-25)

**Concern:** chapter-level orchestration of downloads is in the app's JS thread
(`app/fsd/features/chapter-downloader/model/download-manager.ts`), so downloads cannot be reliably
continued/finished when the app is backgrounded (JS suspended) or killed. Not verified on a device yet —
this is a code-reading analysis.

**What is JS-only today:**

| Piece | Where | What breaks when JS isn't running |
|---|---|---|
| Chapter queue, `MAX_CONCURRENT_CHAPTERS = 6` + `pump()` | app `download-manager.ts` (in-memory `queue`) | Queued chapters never start after the running ones finish; on kill the queue is lost entirely. The "6" itself has no measurement behind it (commit `68006d16`, "fix: try fix"). |
| Chapter completion (counting `group.tasks[].state`) | app `KeshaChapterDownloader` | Native may finish every image in background, but `status: 'downloaded'` + `saveChapterOfflineData` are written only by JS → chapter stays `pending` forever (downloaded-chapters screen shows it as not downloaded → "Повторить"). |
| Image retries `MAX_IMAGE_RETRIES = 2` | app `KeshaChapterDownloader` | No retry in background. |
| Stall watchdog `STALL_TIMEOUT_MS` (3 min, `setTimeout`) | app `KeshaChapterDownloader` | Timer doesn't fire while JS is suspended. |
| Grouping (`groupingApi` / `GroupTask`) | lib `src/GroupTask.ts` — JS, not native | Group state is not persisted; nothing survives a process death. |
| Restore on launch | — | App no longer calls `getExistingDownloadTasks()` at all (the "App Integration Notes" above point to the old `fsd/features/download/` path — outdated). |

Native today only throttles *images* (Android: `maxParallelDownloads` → UIDT on 14+, hard pool of 3 on ≤13;
iOS: `HTTPMaximumConnectionsPerHost`), so the real cap is up to 6 chapters × up to 6 images in parallel.

**Direction (to discuss):**
1. Native, persisted queue for groups: JS enqueues all groups at once (`maxConcurrentGroups`), native starts
   the next one itself when a group settles — works in background / after restart.
2. Native group completion + persisted per-group result (DONE / FAILED image ids), native retry of failed images
   (`maxRetries`) and native stall timeout.
3. On app launch: reconcile — JS reads persisted group results (`getExistingDownloadTasks` / new `getGroups()`),
   flips chapters `pending → downloaded/error`, writes offline data for groups that finished while JS was dead.
4. Single connection budget instead of two multiplied limits (chapters × images) — measure 2 / 4 / 6 on a
   large batch (time + timeout-error count) before picking a value.

**Open questions:** iOS background `URLSession` finishes tasks but JS wakes only via
`handleEventsForBackgroundURLSession` — does the lib surface group completion there? UIDT job per image or per
group on Android 14+?

---

## 🚧 Native group queue (2026-09-25)

**Why:** measured on Redmi / Android 11 — the app feeds chapters one by one from JS: ~50 `download()` bridge
calls per chapter, per-image progress/complete events back into JS, JS-side completion accounting.
JS stalls 0.5–1.3 s on every chapter start/finish; and any JS pause (background, kill) stalls the queue.

**API (fork, Android native; iOS keeps a JS implementation of the same API for now):**

```ts
groupQueue.enqueue({ id, name?, tasks: [{ id, url, destination, headers? }], compressValue? })  // one bridge call per group
groupQueue.cancel(id): Promise<void>
groupQueue.getAll(): Promise<GroupSnapshot[]>     // reconcile after restart
groupQueue.acknowledge(id)                         // host recorded the result → native forgets it
groupQueue.onState(cb) / onProgress(cb)            // group-level events only
setConfig({ maxConcurrentGroups, groupMaxRetries, groupRetryDelaysMs })
```

**Native (Kotlin `GroupQueue`):** persisted queue (SharedPreferences, one key per group); runs at most
`maxConcurrentGroups` groups; tasks of queued groups go through the resumable path; per-task begin/progress/
complete events are NOT sent to JS for group tasks; group settles when every task is DONE/FAILED → failed
tasks retried `groupMaxRetries` times with `groupRetryDelaysMs` back-off → `groupState {done|failed,
failedTaskIds}`; next group starts natively, no JS round-trip. On module init: running groups → re-queued,
their finished tasks kept.
