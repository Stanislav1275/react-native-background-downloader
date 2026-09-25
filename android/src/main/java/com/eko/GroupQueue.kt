package com.eko

import android.content.Context
import android.content.SharedPreferences
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
    var state: String = STATE_QUEUED,
    var attempt: Int = 0,
    val done: MutableSet<String> = mutableSetOf(),
    val failed: MutableSet<String> = mutableSetOf(),
    /** Tasks started in the current attempt and not settled yet. */
    val pending: MutableSet<String> = mutableSetOf(),
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
  }

  private val lock = Any()
  private val prefs: SharedPreferences = context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
  private val handler = Handler(Looper.getMainLooper())

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

  // ─── host API ────────────────────────────────────────────────────────────────

  fun enqueue(id: String, name: String, tasks: List<Task>, compressValue: Float) {
    synchronized(lock) {
      val existing = groups[id]
      if (existing != null && !existing.isTerminal) {
        RNBackgroundDownloaderModuleImpl.logD(TAG, "enqueue: $id already queued/running, ignored")
        return
      }
      existing?.let { forget(it) }
      val group = Group(id, name, compressValue, tasks)
      groups[id] = group
      for (t in tasks) taskToGroup[t.id] = id
      persist(group)
      emitState(group)
    }
    schedule()
  }

  fun cancel(id: String) {
    val toStop: List<String>
    synchronized(lock) {
      val group = groups[id] ?: return
      if (group.isTerminal) return
      toStop = group.pending.toList()
      group.pending.clear()
      group.state = STATE_CANCELED
      persist(group)
      emitState(group)
      forget(group)
    }
    for (taskId in toStop) stopTask(taskId)
    schedule()
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
        if (!group.isTerminal) {
          group.state = STATE_QUEUED
          group.pending.clear()
        }
        groups[group.id] = group
        for (t in group.tasks) taskToGroup[t.id] = group.id
      }
      RNBackgroundDownloaderModuleImpl.logD(TAG, "restored ${groups.size} group(s)")
    }
    schedule()
  }

  // ─── task callbacks from the download listeners ──────────────────────────────

  fun onTaskProgress(taskId: String) {
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
      if (group.failed.isNotEmpty() && group.attempt < maxRetries) {
        val delay = retryDelaysMs.getOrElse(group.attempt) { retryDelaysMs.lastOrNull() ?: 0L }
        group.attempt++
        group.state = STATE_RETRY_WAIT
        persist(group)
        emitState(group)
        RNBackgroundDownloaderModuleImpl.logD(TAG, "${group.id}: ${group.failed.size} task(s) failed, retry #${group.attempt} in ${delay}ms")
        // A group waiting for its retry keeps its slot: retries are for transient network loss,
        // starting other groups meanwhile would only fail them too.
        handler.postDelayed({ startAttempt(group) }, delay)
        return
      }
      group.state = if (group.failed.isEmpty()) STATE_DONE else STATE_FAILED
      persist(group)
      emitState(group)
    }
    schedule()
  }

  private fun schedule() {
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
  }

  private fun startAttempt(group: Group) {
    val tasks: List<Task>
    synchronized(lock) {
      if (group.isTerminal) return
      group.state = STATE_RUNNING
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

  private fun emitState(group: Group) = emit("groupState", snapshotOf(group))

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
    val json = JSONObject().apply {
      put("id", g.id)
      put("name", g.name)
      put("compressValue", g.compressValue.toDouble())
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
      state = json.optString("state", STATE_QUEUED),
      attempt = json.optInt("attempt", 0),
      done = set("done"),
      failed = set("failed"),
    )
  }
}
