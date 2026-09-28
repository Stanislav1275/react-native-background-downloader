package com.eko

import android.content.Context
import android.content.SharedPreferences
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.os.Handler
import android.os.Looper
import org.json.JSONArray
import org.json.JSONObject

/**
 * Native queue of download groups (e.g. one group = one manga chapter's pages).
 *
 * The host enqueues whole groups in one bridge call; this class decides when their tasks start
 * (at most [maxConcurrentGroups] groups at a time), accounts per-task completion, retries failed
 * tasks, and reports only group-level events. Nothing here waits for JS: when a group settles the
 * next one starts immediately, so a backgrounded / busy / reloading JS thread no longer stalls the
 * queue, and JS no longer receives per-image events.
 *
 * State is persisted per group (SharedPreferences), so after a process death [restore] re-queues
 * unfinished groups (already finished tasks are kept) and [snapshots] still reports groups that
 * settled while JS was not listening, until the host [acknowledge]s them.
 */
class GroupQueue(
  context: Context,
  private val startTask: (task: Task, compressValue: Float) -> Unit,
  private val stopTask: (taskId: String) -> Unit,
  private val emit: (event: String, payload: JSONObject) -> Unit,
) {
  data class Task(val id: String, val url: String, val destination: String, val headers: Map<String, String>)

  private class Group(
    val id: String,
    val name: String,
    val compressValue: Float,
    val tasks: List<Task>,
    /** Local image file for the notification (e.g. the title cover); optional. */
    val image: String? = null,
    var state: String = STATE_QUEUED,
    var attempt: Int = 0,
    val done: MutableSet<String> = mutableSetOf(),
    val failed: MutableSet<String> = mutableSetOf(),
    /** Tasks started in the current attempt and not settled yet. */
    val pending: MutableSet<String> = mutableSetOf(),
    /** Bumped on every start / pause / cancel: a delayed retry from an older attempt is dropped. */
    var token: Int = 0,
  ) {
    val isTerminal get() = state == STATE_DONE || state == STATE_FAILED || state == STATE_CANCELED
  }

  companion object {
    private const val TAG = "RNBGDGroupQueue"
    private const val PREFS = "rnbgd_group_queue"
    private const val PROGRESS_EMIT_MIN_INTERVAL_MS = 300L

    const val STATE_QUEUED = "queued"
    const val STATE_RUNNING = "running"
    const val STATE_RETRY_WAIT = "retrying"
    const val STATE_DONE = "done"
    const val STATE_FAILED = "failed"
    const val STATE_CANCELED = "canceled"
    /** Stopped by the user (notification / host); keeps finished tasks, not scheduled until resumed. */
    const val STATE_PAUSED = "paused"

    /** Set by a notification action while the process had no queue (it died while paused). */
    private const val KEY_PENDING_ACTION = "pending_action"
    private const val KEY_BATCH_TOTAL = "batch_total"
    private const val KEY_BATCH_DONE = "batch_done"
    private const val KEY_BATCH_FAILED = "batch_failed"
    const val ACTION_RESUME = "resume"
    const val ACTION_CANCEL = "cancel"

    /** A notification action that arrived while no queue existed; applied by the next [restore]. */
    fun setPendingActionStatic(context: Context, action: String) {
      context.getSharedPreferences(PREFS, Context.MODE_PRIVATE).edit().putString(KEY_PENDING_ACTION, action).apply()
    }
  }

  /** Chapters of the current batch (since the queue was last idle) — what the notification shows. */
  data class Summary(
    val total: Int, val done: Int, val failed: Int, val paused: Boolean, val active: Boolean,
    val currentName: String?, val currentImage: String? = null,
  )

  /** Called (main thread) whenever [summary] may have changed. */
  @Volatile var onChanged: (() -> Unit)? = null

  /**
   * Set when the owning module goes away (JS reload / host teardown). The next module instance
   * restores the groups from prefs; this queue must not start, persist or report anything after that.
   */
  @Volatile private var detached = false

  fun detach() {
    detached = true
  }
  private var batchTotal = 0
  private var batchDone = 0
  private var batchFailed = 0

  private val lock = Any()
  private val prefs: SharedPreferences = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
  private val handler = Handler(Looper.getMainLooper())
  private val connectivity = context.getSystemService(ConnectivityManager::class.java)
  /** Groups whose last attempt failed while offline: retried when a network appears, attempts not spent. */
  private val waitingForNetwork = LinkedHashSet<String>()
  private var networkCallbackRegistered = false

  /** Insertion-ordered: the queue order is the enqueue order. */
  private val groups = LinkedHashMap<String, Group>()
  private val taskToGroup = HashMap<String, String>()
  private val lastProgressEmit = HashMap<String, Long>()

  @Volatile var maxConcurrentGroups: Int = 1
    set(value) { field = value.coerceAtLeast(1); schedule() }
  @Volatile var maxRetries: Int = 2
  @Volatile var retryDelaysMs: LongArray = longArrayOf(3_000L, 10_000L)

  fun owns(taskId: String): Boolean = synchronized(lock) { taskToGroup.containsKey(taskId) }

  fun hasWork(): Boolean = synchronized(lock) { groups.values.any { !it.isTerminal } }

  /** Work that needs the foreground service (paused groups do not). */
  fun hasActiveWork(): Boolean = synchronized(lock) { groups.values.any { !it.isTerminal && it.state != STATE_PAUSED } }

  fun summary(): Summary = synchronized(lock) {
    val live = groups.values.filter { !it.isTerminal }
    Summary(
      total = batchTotal,
      done = batchDone,
      failed = batchFailed,
      paused = live.isNotEmpty() && live.all { it.state == STATE_PAUSED },
      active = live.any { it.state != STATE_PAUSED },
      currentName = (live.firstOrNull { it.state == STATE_RUNNING } ?: live.firstOrNull())?.name,
      currentImage = (live.firstOrNull { it.state == STATE_RUNNING } ?: live.firstOrNull())?.image,
    )
  }

  // ─── host API ────────────────────────────────────────────────────────────────

  fun enqueue(id: String, name: String, tasks: List<Task>, compressValue: Float, image: String? = null) {
    if (detached) return
    synchronized(lock) {
      val existing = groups[id]
      if (existing != null && !existing.isTerminal) {
        RNBackgroundDownloaderModuleImpl.logD(TAG, "enqueue: $id already queued/running, ignored")
        return
      }
      existing?.let { forget(it) }
      if (groups.values.none { !it.isTerminal }) resetBatch()
      batchTotal++
      val group = Group(id, name, compressValue, tasks, image = image)
      groups[id] = group
      for (t in tasks) taskToGroup[t.id] = id
      persist(group)
      emitState(group)
    }
    schedule()
  }

  /**
   * @param keepUntilAck keep the canceled record (persisted) until the host [acknowledge]s it — for
   * cancels the host did not ask for (notification action), so it can clean up after the fact even
   * if JS was not listening when it happened.
   */
  fun cancel(id: String, keepUntilAck: Boolean = false) {
    val toStop: List<String>
    synchronized(lock) {
      val group = groups[id] ?: return
      if (group.isTerminal) return
      toStop = group.pending.toList()
      group.pending.clear()
      group.token++
      group.state = STATE_CANCELED
      batchTotal = (batchTotal - 1).coerceAtLeast(batchDone + batchFailed)
      persist(group)
      emitState(group)
      if (!keepUntilAck) forget(group)
    }
    for (taskId in toStop) stopTask(taskId)
    schedule()
    notifyChanged()
  }

  /** Stops every running/queued group; finished tasks are kept, nothing is scheduled until [resumeAll]. */
  fun pauseAll() {
    if (detached) return
    val toStop = mutableListOf<String>()
    synchronized(lock) {
      for (group in groups.values) {
        if (group.isTerminal || group.state == STATE_PAUSED) continue
        toStop.addAll(group.pending)
        group.pending.clear()
        group.failed.clear()
        group.token++
        group.state = STATE_PAUSED
        persist(group)
        emitState(group)
      }
    }
    for (taskId in toStop) stopTask(taskId)
    notifyChanged()
  }

  fun resumeAll() {
    if (detached) return
    synchronized(lock) {
      for (group in groups.values) {
        if (group.state != STATE_PAUSED) continue
        group.state = STATE_QUEUED
        group.attempt = 0
        persist(group)
        emitState(group)
      }
    }
    schedule()
    notifyChanged()
  }

  /** User cancel from the notification: records stay until the host acknowledges them. */
  fun cancelAll() {
    val ids = synchronized(lock) { groups.values.filter { !it.isTerminal }.map { it.id } }
    for (id in ids) cancel(id, keepUntilAck = true)
  }


  fun acknowledge(id: String) {
    synchronized(lock) {
      val group = groups[id] ?: return
      if (!group.isTerminal) return
      forget(group)
    }
  }

  fun snapshots(): JSONArray = synchronized(lock) {
    JSONArray().apply { for (g in groups.values) put(snapshotOf(g)) }
  }

  /** Re-reads persisted groups (process restart): unfinished ones go back to the queue. */
  fun restore() {
    synchronized(lock) {
      for ((key, raw) in prefs.all) {
        if (!key.startsWith("g:") || raw !is String) continue
        val group = runCatching { fromJson(JSONObject(raw)) }.getOrNull() ?: continue
        if (!group.isTerminal && group.state != STATE_PAUSED) {
          group.state = STATE_QUEUED
          group.pending.clear()
        }
        if (!group.isTerminal) batchTotal++
        else if (group.state == STATE_DONE) { batchTotal++; batchDone++ }
        else if (group.state == STATE_FAILED) { batchTotal++; batchFailed++ }
        groups[group.id] = group
        for (t in group.tasks) taskToGroup[t.id] = group.id
      }
      if (groups.values.any { !it.isTerminal }) {
        batchTotal = maxOf(batchTotal, prefs.getInt(KEY_BATCH_TOTAL, 0))
        batchDone = maxOf(batchDone, prefs.getInt(KEY_BATCH_DONE, 0))
        batchFailed = maxOf(batchFailed, prefs.getInt(KEY_BATCH_FAILED, 0))
      }
      RNBackgroundDownloaderModuleImpl.logD(TAG, "restored ${groups.size} group(s)")
    }
    when (prefs.getString(KEY_PENDING_ACTION, null)) {
      ACTION_RESUME -> resumeAll()
      ACTION_CANCEL -> cancelAll()
    }
    prefs.edit().remove(KEY_PENDING_ACTION).apply()
    schedule()
    notifyChanged()
  }

  // ─── task callbacks from the download listeners ──────────────────────────────

  fun onTaskProgress(taskId: String) {
    if (detached) return
    val payload = synchronized(lock) {
      val group = groups[taskToGroup[taskId] ?: return] ?: return
      val now = System.currentTimeMillis()
      if (now - (lastProgressEmit[group.id] ?: 0L) < PROGRESS_EMIT_MIN_INTERVAL_MS) return
      lastProgressEmit[group.id] = now
      snapshotOf(group)
    }
    emit("groupProgress", payload)
  }

  fun onTaskSettled(taskId: String, success: Boolean) {
    if (detached) return
    var settledGroup: Group? = null
    synchronized(lock) {
      val group = groups[taskToGroup[taskId] ?: return] ?: return
      if (!group.pending.remove(taskId)) return
      if (success) {
        group.done.add(taskId)
        group.failed.remove(taskId)
      } else {
        group.failed.add(taskId)
      }
      if (group.pending.isEmpty()) settledGroup = group
      else lastProgressEmit[group.id] = 0L // force the next progress emit to go out
    }
    val group = settledGroup
    if (group == null) {
      onTaskProgress(taskId)
      return
    }
    onAttemptSettled(group)
  }

  // ─── internals ───────────────────────────────────────────────────────────────

  private fun onAttemptSettled(group: Group) {
    synchronized(lock) {
      if (group.isTerminal) return
      if (group.failed.isNotEmpty() && !isOnline()) {
        // No network: failures say nothing about the chapter. Hold the slot and retry on reconnect
        // instead of burning the retry budget (3 s + 10 s) during a longer outage.
        group.state = STATE_RETRY_WAIT
        group.token++
        waitingForNetwork.add(group.id)
        persist(group)
        emitState(group)
        RNBackgroundDownloaderModuleImpl.logD(TAG, "${group.id}: ${group.failed.size} task(s) failed offline, waiting for network")
        ensureNetworkCallback()
        return
      }
      if (group.failed.isNotEmpty() && group.attempt < maxRetries) {
        val delay = retryDelaysMs.getOrElse(group.attempt) { retryDelaysMs.lastOrNull() ?: 0L }
        group.attempt++
        val token = group.token
        group.state = STATE_RETRY_WAIT
        persist(group)
        emitState(group)
        RNBackgroundDownloaderModuleImpl.logD(TAG, "${group.id}: ${group.failed.size} task(s) failed, retry #${group.attempt} in ${delay}ms")
        // A group waiting for its retry keeps its slot: retries are for transient network loss,
        // starting other groups meanwhile would only fail them too.
        handler.postDelayed({ if (group.token == token) startAttempt(group) }, delay)
        return
      }
      group.state = if (group.failed.isEmpty()) STATE_DONE else STATE_FAILED
      if (group.state == STATE_DONE) batchDone++ else batchFailed++
      persist(group)
      emitState(group)
    }
    schedule()
    notifyChanged()
  }

  private fun schedule() {
    if (detached) return
    val toStart = mutableListOf<Group>()
    synchronized(lock) {
      var running = groups.values.count { it.state == STATE_RUNNING || it.state == STATE_RETRY_WAIT }
      for (group in groups.values) {
        if (running >= maxConcurrentGroups) break
        if (group.state != STATE_QUEUED) continue
        group.state = STATE_RUNNING
        running++
        toStart.add(group)
      }
    }
    for (group in toStart) startAttempt(group)
    if (toStart.isNotEmpty()) notifyChanged()
  }

  private fun isOnline(): Boolean {
    val cm = connectivity ?: return true
    val caps = cm.getNetworkCapabilities(cm.activeNetwork ?: return false) ?: return false
    return caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
  }

  private fun ensureNetworkCallback() {
    if (networkCallbackRegistered) return
    val cm = connectivity ?: return
    try {
      cm.registerDefaultNetworkCallback(object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) {
          // Give the network a moment to become usable (DNS etc.) before hitting the CDN again.
          handler.postDelayed({ retryWaitingForNetwork() }, 1_500L)
        }
      })
      networkCallbackRegistered = true
    } catch (e: Exception) {
      RNBackgroundDownloaderModuleImpl.logE(TAG, "network callback failed: ${e.message}")
    }
  }

  private fun retryWaitingForNetwork() {
    if (detached) return
    val toRetry = synchronized(lock) {
      val list = waitingForNetwork.mapNotNull { groups[it] }.filter { it.state == STATE_RETRY_WAIT }
      waitingForNetwork.clear()
      list
    }
    if (toRetry.isNotEmpty()) RNBackgroundDownloaderModuleImpl.logD(TAG, "network back: retrying ${toRetry.size} group(s)")
    for (group in toRetry) startAttempt(group)
  }

  private fun resetBatch() {
    batchTotal = 0
    batchDone = 0
    batchFailed = 0
  }

  private fun notifyChanged() {
    if (detached) return
    // Batch counters survive a restart: acknowledged groups are gone from prefs, so recounting
    // on restore would shrink "N of M".
    synchronized(lock) {
      prefs.edit().putInt(KEY_BATCH_TOTAL, batchTotal).putInt(KEY_BATCH_DONE, batchDone).putInt(KEY_BATCH_FAILED, batchFailed).apply()
    }
    val cb = onChanged ?: return
    if (Looper.myLooper() == Looper.getMainLooper()) cb() else handler.post(cb)
  }

  private fun startAttempt(group: Group) {
    if (detached) return
    val tasks: List<Task>
    synchronized(lock) {
      if (group.isTerminal || group.state == STATE_PAUSED) return
      group.state = STATE_RUNNING
      group.token++
      tasks = group.tasks.filter { it.id !in group.done }
      group.failed.clear()
      group.pending.clear()
      group.pending.addAll(tasks.map { it.id })
      persist(group)
      emitState(group)
    }
    if (tasks.isEmpty()) {
      onAttemptSettled(group)
      return
    }
    for (task in tasks) {
      try {
        startTask(task, group.compressValue)
      } catch (e: Exception) {
        RNBackgroundDownloaderModuleImpl.logE(TAG, "start ${task.id} failed: ${e.message}")
        onTaskSettled(task.id, false)
      }
    }
  }

  private fun forget(group: Group) {
    groups.remove(group.id)
    lastProgressEmit.remove(group.id)
    for (t in group.tasks) if (taskToGroup[t.id] == group.id) taskToGroup.remove(t.id)
    prefs.edit().remove("g:${group.id}").apply()
  }

  private fun emitState(group: Group) {
    if (!detached) emit("groupState", snapshotOf(group))
  }

  private fun snapshotOf(g: Group) = JSONObject().apply {
    put("id", g.id)
    put("name", g.name)
    put("state", g.state)
    put("attempt", g.attempt)
    put("total", g.tasks.size)
    put("completed", g.done.size)
    put("failed", g.failed.size)
    put("failedTaskIds", JSONArray(g.failed.toList()))
  }

  private fun persist(g: Group) {
    if (detached) return
    val json = JSONObject().apply {
      put("id", g.id)
      put("name", g.name)
      put("compressValue", g.compressValue.toDouble())
      g.image?.let { put("image", it) }
      put("state", g.state)
      put("attempt", g.attempt)
      put("done", JSONArray(g.done.toList()))
      put("failed", JSONArray(g.failed.toList()))
      put("tasks", JSONArray().apply {
        for (t in g.tasks) put(JSONObject().apply {
          put("id", t.id)
          put("url", t.url)
          put("destination", t.destination)
          put("headers", JSONObject(t.headers as Map<*, *>))
        })
      })
    }
    prefs.edit().putString("g:${g.id}", json.toString()).apply()
  }

  private fun fromJson(json: JSONObject): Group {
    val tasksJson = json.getJSONArray("tasks")
    val tasks = (0 until tasksJson.length()).map { i ->
      val t = tasksJson.getJSONObject(i)
      val h = t.optJSONObject("headers") ?: JSONObject()
      Task(t.getString("id"), t.getString("url"), t.getString("destination"), h.keys().asSequence().associateWith { h.getString(it) })
    }
    fun set(key: String): MutableSet<String> {
      val arr = json.optJSONArray(key) ?: return mutableSetOf()
      return (0 until arr.length()).map { arr.getString(it) }.toMutableSet()
    }
    return Group(
      id = json.getString("id"),
      name = json.optString("name"),
      compressValue = json.optDouble("compressValue", 0.0).toFloat(),
      tasks = tasks,
      image = json.optString("image").takeIf { it.isNotEmpty() },
      state = json.optString("state", STATE_QUEUED),
      attempt = json.optInt("attempt", 0),
      done = set("done"),
      failed = set("failed"),
    )
  }
}
