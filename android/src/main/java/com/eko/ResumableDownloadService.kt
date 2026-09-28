package com.eko

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Binder
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.PowerManager
import androidx.core.app.NotificationCompat
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * A foreground service that manages resumable downloads in the background.
 * This ensures downloads continue even when the app is in the background or the screen is off.
 */
class ResumableDownloadService : Service() {

  companion object {
    private const val TAG = "ResumableDownloadSvc"
    private const val WAKELOCK_TAG = "ResumableDownloadService::WakeLock"

    // Notification group for grouping all download notifications together
    private const val NOTIFICATION_GROUP_KEY = "com.eko.DOWNLOAD_GROUP"

    /** Minimum gap between summary-notification refreshes; every task begin/finish asked for one. */
    private const val NOTIFICATION_UPDATE_MIN_INTERVAL_MS = 1000L

    /**
     * Bounded pool for all resumable downloads of this service. Process-wide rather than per
     * service instance so setMaxParallelDownloads() works before the service is bound, and so the
     * limit survives the service being stopped and recreated between batches. Idle threads time out.
     */
    private val downloadPool: ThreadPoolExecutor = ThreadPoolExecutor(
      DownloadConstants.DOWNLOAD_THREAD_POOL_SIZE,
      DownloadConstants.DOWNLOAD_THREAD_POOL_SIZE,
      30L,
      TimeUnit.SECONDS,
      LinkedBlockingQueue()
    ).apply { allowCoreThreadTimeOut(true) }

    fun setMaxParallelDownloads(max: Int) {
      val size = max.coerceAtLeast(1)
      synchronized(downloadPool) {
        // Core may never exceed max: grow max first, shrink core first.
        if (size >= downloadPool.maximumPoolSize) {
          downloadPool.maximumPoolSize = size
          downloadPool.corePoolSize = size
        } else {
          downloadPool.corePoolSize = size
          downloadPool.maximumPoolSize = size
        }
      }
    }

    // Action constants for Intent
    const val ACTION_START_DOWNLOAD = "com.eko.action.START_DOWNLOAD"
    const val ACTION_PAUSE_DOWNLOAD = "com.eko.action.PAUSE_DOWNLOAD"
    const val ACTION_RESUME_DOWNLOAD = "com.eko.action.RESUME_DOWNLOAD"
    const val ACTION_CANCEL_DOWNLOAD = "com.eko.action.CANCEL_DOWNLOAD"
    const val ACTION_STOP_SERVICE = "com.eko.action.STOP_SERVICE"
    // Notification actions over the whole group queue
    const val ACTION_PAUSE_ALL = "com.eko.action.PAUSE_ALL"
    const val ACTION_RESUME_ALL = "com.eko.action.RESUME_ALL"
    const val ACTION_CANCEL_ALL = "com.eko.action.CANCEL_ALL"

    /** The running service, for queue-change callbacks from the module. */
    @Volatile var instance: ResumableDownloadService? = null
      private set

    /** Set by the module: the notification shows the queue's chapters and controls it. */
    @Volatile var groupQueue: GroupQueue? = null

    /**
     * Texts for the queue notification, from JS (`setGroupQueueConfig({ notificationTexts })`).
     * Placeholders: {done} {total} {failed} {name}.
     */
    @Volatile var queueTexts: Map<String, String> = mapOf(
      "title" to "Downloads",
      "progress" to "{done} of {total}",
      "paused" to "Paused · {done} of {total}",
      "finished" to "Downloaded {done} of {total}",
      "finishedWithErrors" to "Downloaded {done} of {total}, failed {failed}",
      "resumeOnLaunch" to "Will continue when the app is opened",
      "actionPause" to "Pause",
      "actionResume" to "Resume",
      "actionCancel" to "Cancel",
    )

    /** Separate id: the paused notification outlives the service (it is not a foreground one). */
    private const val PAUSED_NOTIFICATION_ID = DownloadConstants.NOTIFICATION_ID + 1

    // Extra keys
    const val EXTRA_DOWNLOAD_ID = "download_id"
    const val EXTRA_URL = "url"
    const val EXTRA_DESTINATION = "destination"
    const val EXTRA_HEADERS = "headers"
    const val EXTRA_START_BYTE = "start_byte"
    const val EXTRA_TOTAL_BYTES = "total_bytes"
  }

  private val binder = LocalBinder()
  @Volatile private var isForeground = false
  private val mainHandler = Handler(Looper.getMainLooper())
  private val idleStopRunnable = Runnable { stopNowIfIdle() }

  fun isInForeground(): Boolean = isForeground
  @Volatile private var lastNotificationUpdate = 0L
  private val activeDownloads = ConcurrentHashMap<String, DownloadJob>()
  private var wakeLock: PowerManager.WakeLock? = null
  private var listener: ResumableDownloader.DownloadListener? = null

  // Generation counter per download ID - incremented each time a new download starts
  // This is separate from sessionToken to ensure we can detect stale events even
  // across cancel/restart cycles where the job is removed and re-added
  private val downloadGeneration = ConcurrentHashMap<String, Long>()

  // Throttle progress logging to reduce log noise
  private val lastProgressLogTime = ConcurrentHashMap<String, Long>()

  // Shared ResumableDownloader instance
  val resumableDownloader = ResumableDownloader(downloadPool)

  inner class LocalBinder : Binder() {
    fun getService(): ResumableDownloadService = this@ResumableDownloadService
  }

  /**
   * Creates a validating listener wrapper that checks session token and generation
   * before delegating events to the actual listener. This prevents stale events
   * from old download sessions from being processed.
   *
   * @param id The download ID
   * @param sessionToken The session token from when the download was started
   * @param generation The generation counter from when the download was started
   * @param cleanupOnTerminal Whether to clean up state on complete/error (true for start, false for resume since job already exists)
   */
  private fun createValidatingListener(
    id: String,
    sessionToken: Long,
    generation: Long,
    cleanupOnTerminal: Boolean = true
  ): ResumableDownloader.DownloadListener {
    return object : ResumableDownloader.DownloadListener {

      private fun isValid(): Boolean {
        val currentJob = activeDownloads[id]
        val currentGeneration = downloadGeneration[id] ?: 0
        return currentJob != null &&
               currentJob.sessionToken == sessionToken &&
               currentGeneration == generation
      }

      private fun logStale(event: String) {
        val currentJob = activeDownloads[id]
        val currentGeneration = downloadGeneration[id] ?: 0
        RNBackgroundDownloaderModuleImpl.logD(TAG, "Ignoring stale $event for $id (session: my=$sessionToken vs job=${currentJob?.sessionToken}, gen: my=$generation vs current=$currentGeneration)")
      }

      override fun onBegin(id: String, expectedBytes: Long, headers: Map<String, String>) {
        if (isValid()) {
          listener?.onBegin(id, expectedBytes, headers)
          updateNotification()
        } else {
          logStale("onBegin")
        }
      }

      override fun onProgress(id: String, bytesDownloaded: Long, bytesTotal: Long) {
        val currentJob = activeDownloads[id]
        val currentGeneration = downloadGeneration[id] ?: 0

        // Throttle progress logging to reduce noise
        val now = System.currentTimeMillis()
        val lastLogTime = lastProgressLogTime[id] ?: 0L
        if (now - lastLogTime >= DownloadConstants.PROGRESS_LOG_INTERVAL_MS) {
          RNBackgroundDownloaderModuleImpl.logD(TAG, "onProgress: id=$id, bytes=$bytesDownloaded, myToken=$sessionToken, jobToken=${currentJob?.sessionToken}, myGen=$generation, currentGen=$currentGeneration")
          lastProgressLogTime[id] = now
        }

        if (isValid()) {
          listener?.onProgress(id, bytesDownloaded, bytesTotal)
        } else {
          logStale("onProgress")
        }
      }

      override fun onComplete(id: String, location: String, bytesDownloaded: Long, bytesTotal: Long) {
        if (isValid()) {
          listener?.onComplete(id, location, bytesDownloaded, bytesTotal)
          if (cleanupOnTerminal) {
            activeDownloads.remove(id)
            lastProgressLogTime.remove(id)
          }
          stopServiceIfIdle()
        } else {
          logStale("onComplete")
        }
      }

      override fun onError(id: String, error: String, errorCode: Int) {
        if (isValid()) {
          listener?.onError(id, error, errorCode)
          if (cleanupOnTerminal) {
            activeDownloads.remove(id)
            lastProgressLogTime.remove(id)
          }
          stopServiceIfIdle()
        } else {
          logStale("onError")
        }
      }
    }
  }

  data class DownloadJob(
    val id: String,
    val url: String,
    val destination: String,
    val headers: Map<String, String>,
    val startByte: Long,
    val totalBytes: Long,
    val sessionToken: Long = System.nanoTime() // Unique token for this download session
  )

  override fun onCreate() {
    super.onCreate()
    RNBackgroundDownloaderModuleImpl.logD(TAG, "Service created")
    instance = this
    createNotificationChannel()
  }

  /**
   * Extract headers from Intent, handling deprecated API for backward compatibility.
   */
  @Suppress("UNCHECKED_CAST", "DEPRECATION")
  private fun getHeadersFromIntent(intent: Intent): HashMap<String, String> {
    return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
      intent.getSerializableExtra(EXTRA_HEADERS, HashMap::class.java) as? HashMap<String, String> ?: HashMap()
    } else {
      @Suppress("DEPRECATION")
      intent.getSerializableExtra(EXTRA_HEADERS) as? HashMap<String, String> ?: HashMap()
    }
  }

  override fun onBind(intent: Intent?): IBinder {
    return binder
  }

  override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
    RNBackgroundDownloaderModuleImpl.logD(TAG, "onStartCommand: action=${intent?.action}")

    when (intent?.action) {
      ACTION_START_DOWNLOAD -> {
        val id = intent.getStringExtra(EXTRA_DOWNLOAD_ID)
        val url = intent.getStringExtra(EXTRA_URL)
        val destination = intent.getStringExtra(EXTRA_DESTINATION)
        val headers = getHeadersFromIntent(intent)
        val startByte = intent.getLongExtra(EXTRA_START_BYTE, 0)
        val totalBytes = intent.getLongExtra(EXTRA_TOTAL_BYTES, -1)

        if (id != null && url != null && destination != null) {
          startDownloadInternal(id, url, destination, headers, startByte, totalBytes)
        }
      }
      ACTION_PAUSE_DOWNLOAD -> {
        val id = intent.getStringExtra(EXTRA_DOWNLOAD_ID)
        if (id != null) {
          pauseDownload(id)
        }
      }
      ACTION_RESUME_DOWNLOAD -> {
        val id = intent.getStringExtra(EXTRA_DOWNLOAD_ID)
        if (id != null) {
          resumeDownload(id)
        }
      }
      ACTION_CANCEL_DOWNLOAD -> {
        val id = intent.getStringExtra(EXTRA_DOWNLOAD_ID)
        if (id != null) {
          cancelDownload(id)
        }
      }
      ACTION_STOP_SERVICE -> {
        stopServiceIfIdle()
      }
      ACTION_PAUSE_ALL -> onPauseAll()
      ACTION_RESUME_ALL -> onResumeAll()
      ACTION_CANCEL_ALL -> onCancelAll()
    }

    return START_STICKY
  }

  /**
   * Android 15+: a dataSync FGS gets 6 h per 24 h. On timeout the service must stop within seconds
   * or the app crashes — pause the queue instead, so the user can resume it from the notification
   * (resuming from a notification action starts a fresh foreground window).
   */
  override fun onTimeout(startId: Int, fgsType: Int) {
    RNBackgroundDownloaderModuleImpl.logE(TAG, "FGS timeout (type=$fgsType), pausing the queue")
    val queue = groupQueue
    if (queue != null) {
      queue.pauseAll()
      stopForPause()
    } else {
      releaseWakeLock()
      stopForeground(STOP_FOREGROUND_REMOVE)
      isForeground = false
      stopSelf()
    }
  }

  override fun onDestroy() {
    RNBackgroundDownloaderModuleImpl.logD(TAG, "Service destroyed")
    if (instance === this) instance = null
    mainHandler.removeCallbacks(idleStopRunnable)
    releaseWakeLock()
    isForeground = false
    super.onDestroy()
  }

  fun setDownloadListener(listener: ResumableDownloader.DownloadListener?) {
    this.listener = listener
  }

  fun startDownload(
    id: String,
    url: String,
    destination: String,
    headers: Map<String, String>,
    startByte: Long = 0,
    totalBytes: Long = -1
  ) {
    startDownloadInternal(id, url, destination, headers, startByte, totalBytes)
  }

  private fun startDownloadInternal(
    id: String,
    url: String,
    destination: String,
    headers: Map<String, String>,
    startByte: Long,
    totalBytes: Long
  ) {
    RNBackgroundDownloaderModuleImpl.logD(TAG, "Starting download: $id from byte $startByte")

    // Start foreground service if not already — once, not per task: this ran for every image of
    // every chapter (an AMS round-trip plus a notification build each time), largely on the main
    // thread when the operations queued before binding are flushed in onServiceConnected.
    mainHandler.removeCallbacks(idleStopRunnable)
    if (!isForeground) startForegroundWithNotification()
    acquireWakeLock()

    // Increment generation counter for this download ID
    // This ensures we can detect stale events even if job was removed and re-added
    val generation = (downloadGeneration[id] ?: 0) + 1
    downloadGeneration[id] = generation
    RNBackgroundDownloaderModuleImpl.logD(TAG, "Download $id starting with generation $generation")

    val job = DownloadJob(id, url, destination, headers, startByte, totalBytes)
    activeDownloads[id] = job

    // Create a validating listener wrapper
    val serviceListener = createValidatingListener(id, job.sessionToken, generation, cleanupOnTerminal = true)

    // Start the download using ResumableDownloader
    resumableDownloader.startDownload(
      id = id,
      url = url,
      destination = destination,
      headers = headers,
      listener = serviceListener,
      startByte = startByte,
      totalBytes = totalBytes
    )
  }

  fun pauseDownload(id: String): Boolean {
    RNBackgroundDownloaderModuleImpl.logD(TAG, "Pausing download: $id")
    val result = resumableDownloader.pause(id)
    if (result) {
      updateNotification()
      // Don't stop service - keep it alive for potential resume
    }
    return result
  }

  fun resumeDownload(id: String): Boolean {
    RNBackgroundDownloaderModuleImpl.logD(TAG, "Resuming download: $id")

    if (this.listener == null) return false
    val currentJob = activeDownloads[id] ?: return false
    val sessionToken = currentJob.sessionToken
    val generation = downloadGeneration[id] ?: 0
    RNBackgroundDownloaderModuleImpl.logD(TAG, "Resuming $id with session=$sessionToken, generation=$generation")

    // Create a validating listener wrapper (don't cleanup since job already exists)
    val serviceListener = createValidatingListener(id, sessionToken, generation, cleanupOnTerminal = false)

    // Make sure service is in foreground
    startForegroundWithNotification()
    acquireWakeLock()

    return resumableDownloader.resume(id, serviceListener)
  }

  fun cancelDownload(id: String): Boolean {
    RNBackgroundDownloaderModuleImpl.logD(TAG, "Cancelling download: $id")
    activeDownloads.remove(id)
    val result = resumableDownloader.cancel(id)
    stopServiceIfIdle()
    return result
  }

  // ─── queue controls (notification actions) ───────────────────────────────────

  private fun onPauseAll() {
    val queue = groupQueue ?: return stopNowIfIdle()
    queue.pauseAll()
    if (!queue.hasActiveWork()) stopForPause()
  }

  private fun onResumeAll() {
    notificationManager().cancel(PAUSED_NOTIFICATION_ID)
    val queue = groupQueue
    if (queue == null) {
      // The process died while paused: the queue lives in the RN module, which only the app starts.
      GroupQueue.setPendingActionStatic(this, GroupQueue.ACTION_RESUME)
      notificationManager().notify(PAUSED_NOTIFICATION_ID, buildQueueNotification(ongoing = false, textOverride = text("resumeOnLaunch")))
      stopSelf()
      return
    }
    // Started from a notification action: allowed to go foreground, and must right away.
    mainHandler.removeCallbacks(idleStopRunnable)
    startForegroundWithNotification()
    acquireWakeLock()
    queue.resumeAll()
    if (!queue.hasActiveWork()) stopNowIfIdle()
  }

  private fun onCancelAll() {
    notificationManager().cancel(PAUSED_NOTIFICATION_ID)
    val queue = groupQueue
    if (queue == null) {
      GroupQueue.setPendingActionStatic(this, GroupQueue.ACTION_CANCEL)
      stopSelf()
      return
    }
    queue.cancelAll()
    stopNowIfIdle()
  }

  /** Paused: no foreground service needed; leave a dismissable notification to resume / cancel from. */
  private fun stopForPause() {
    mainHandler.removeCallbacks(idleStopRunnable)
    releaseWakeLock()
    if (isForeground) stopForeground(STOP_FOREGROUND_REMOVE)
    isForeground = false
    notificationManager().notify(PAUSED_NOTIFICATION_ID, buildQueueNotification(ongoing = false))
    stopSelf()
  }

  /** Queue state changed (called by the module on the main thread). */
  fun onQueueChanged() {
    val queue = groupQueue ?: return
    if (queue.summary().paused && !queue.hasActiveWork()) {
      stopForPause()
      return
    }
    updateNotification(force = true)
  }

  private fun text(key: String): String = queueTexts[key] ?: ""

  private fun format(template: String, s: GroupQueue.Summary): String =
    template.replace("{done}", s.done.toString())
      .replace("{total}", s.total.toString())
      .replace("{failed}", s.failed.toString())
      .replace("{name}", s.currentName ?: "")

  private fun actionIntent(action: String, requestCode: Int): PendingIntent =
    PendingIntent.getService(
      this, requestCode,
      Intent(this, ResumableDownloadService::class.java).setAction(action),
      PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
    )

  private fun contentIntent(): PendingIntent? {
    val launch = packageManager.getLaunchIntentForPackage(packageName) ?: return null
    return PendingIntent.getActivity(this, 0, launch, PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE)
  }

  /** Chapter-level notification driven by the group queue, with pause / resume / cancel. */
  private fun buildQueueNotification(ongoing: Boolean, textOverride: String? = null): Notification {
    val s = groupQueue?.summary() ?: GroupQueue.Summary(0, 0, 0, paused = true, active = false, currentName = null)
    val settled = s.done + s.failed
    val finished = !s.active && !s.paused
    val contentText = textOverride ?: when {
      s.paused -> format(text("paused"), s)
      finished && s.failed > 0 -> format(text("finishedWithErrors"), s)
      finished -> format(text("finished"), s)
      else -> format(text("progress"), s)
    }
    val builder = NotificationCompat.Builder(this, DownloadConstants.NOTIFICATION_CHANNEL_ID)
      .setContentTitle(text("title"))
      .setContentText(contentText)
      .setSubText(if (s.active) s.currentName else null)
      .setSmallIcon(if (s.active) android.R.drawable.stat_sys_download else android.R.drawable.stat_sys_download_done)
      .setPriority(NotificationCompat.PRIORITY_LOW)
      .setOnlyAlertOnce(true)
      .setOngoing(ongoing)
      .setAutoCancel(!ongoing)
      .setContentIntent(contentIntent())
    largeIconFor(s.currentImage)?.let { builder.setLargeIcon(it) }
    if (!finished && s.total > 0) builder.setProgress(s.total, settled, false)
    if (s.active) {
      builder.addAction(0, text("actionPause"), actionIntent(ACTION_PAUSE_ALL, 1))
      builder.addAction(0, text("actionCancel"), actionIntent(ACTION_CANCEL_ALL, 3))
    } else if (s.paused && textOverride == null) {
      builder.addAction(0, text("actionResume"), actionIntent(ACTION_RESUME_ALL, 2))
      builder.addAction(0, text("actionCancel"), actionIntent(ACTION_CANCEL_ALL, 3))
    }
    return builder.build()
  }

  // Large icon cache: the same cover is shown for a whole title, decode it once.
  private var largeIconPath: String? = null
  private var largeIcon: android.graphics.Bitmap? = null

  private fun largeIconFor(path: String?): android.graphics.Bitmap? {
    if (path.isNullOrEmpty()) return null
    if (path == largeIconPath && largeIcon != null) return largeIcon
    val file = java.io.File(path.removePrefix("file://"))
    if (!file.exists()) return null // cover still downloading: try again on the next update
    val bitmap = try {
      val bounds = android.graphics.BitmapFactory.Options().apply { inJustDecodeBounds = true }
      android.graphics.BitmapFactory.decodeFile(file.path, bounds)
      val target = (64 * resources.displayMetrics.density).toInt().coerceAtLeast(1)
      var sample = 1
      while (bounds.outWidth / (sample * 2) >= target && bounds.outHeight / (sample * 2) >= target) sample *= 2
      android.graphics.BitmapFactory.decodeFile(file.path, android.graphics.BitmapFactory.Options().apply { inSampleSize = sample })
    } catch (e: Exception) {
      null
    } ?: return null
    largeIconPath = path
    largeIcon = bitmap
    return bitmap
  }

  private fun notificationManager() = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager

  fun isPaused(id: String): Boolean = resumableDownloader.isPaused(id)

  fun getState(id: String) = resumableDownloader.getState(id)

  private fun startForegroundWithNotification() {
    val notification = createNotification()
    try {
      if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
        startForeground(DownloadConstants.NOTIFICATION_ID, notification, android.content.pm.ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
      } else {
        startForeground(DownloadConstants.NOTIFICATION_ID, notification)
      }
      isForeground = true
    } catch (e: Exception) {
      RNBackgroundDownloaderModuleImpl.logE(TAG, "Failed to start foreground service: ${e.message}")
    }
  }

  private fun createNotificationChannel() {
    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
      val name = "Background Downloads"
      val descriptionText = "Shows download progress for background downloads"
      val importance = NotificationManager.IMPORTANCE_LOW
      val channel = NotificationChannel(DownloadConstants.NOTIFICATION_CHANNEL_ID, name, importance).apply {
        description = descriptionText
        setShowBadge(false)
      }

      val notificationManager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
      notificationManager.createNotificationChannel(channel)
    }
  }

  private fun createNotification(): Notification {
    if (groupQueue?.summary()?.let { it.total > 0 } == true) return buildQueueNotification(ongoing = true)
    val activeCount = activeDownloads.size
    val pausedCount = activeDownloads.keys.count { resumableDownloader.isPaused(it) }
    val runningCount = activeCount - pausedCount

    val contentText = when {
      runningCount > 0 && pausedCount > 0 -> "$runningCount downloading, $pausedCount paused"
      runningCount > 0 -> "$runningCount download${if (runningCount > 1) "s" else ""} in progress"
      pausedCount > 0 -> "$pausedCount download${if (pausedCount > 1) "s" else ""} paused"
      else -> "Download service running"
    }

    // Use download icon when actively downloading, pause icon when all paused
    val icon = if (runningCount > 0) {
      android.R.drawable.stat_sys_download
    } else {
      android.R.drawable.stat_sys_download_done
    }

    // No queue attached (e.g. between two module instances on a JS reload): keep the host's title.
    if (groupQueue == null && queueTexts["title"] != null) {
      return NotificationCompat.Builder(this, DownloadConstants.NOTIFICATION_CHANNEL_ID)
        .setContentTitle(text("title"))
        .setSmallIcon(android.R.drawable.stat_sys_download)
        .setPriority(NotificationCompat.PRIORITY_LOW)
        .setOngoing(true)
        .setOnlyAlertOnce(true)
        .setProgress(0, 0, true)
        .setContentIntent(contentIntent())
        .build()
    }
    return NotificationCompat.Builder(this, DownloadConstants.NOTIFICATION_CHANNEL_ID)
      .setContentTitle("Background Download")
      .setContentText(contentText)
      .setSmallIcon(icon)
      .setPriority(NotificationCompat.PRIORITY_LOW)
      .setOngoing(true)
      .setGroup(NOTIFICATION_GROUP_KEY)
      .setGroupSummary(true)
      .build()
  }

  private fun updateNotification(force: Boolean = false) {
    if (!isForeground) return
    val now = System.currentTimeMillis()
    if (!force && now - lastNotificationUpdate < NOTIFICATION_UPDATE_MIN_INTERVAL_MS) return
    lastNotificationUpdate = now
    val notificationManager = getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
    notificationManager.notify(DownloadConstants.NOTIFICATION_ID, createNotification())
  }

  private fun acquireWakeLock() {
    if (wakeLock == null) {
      val powerManager = getSystemService(Context.POWER_SERVICE) as PowerManager
      wakeLock = powerManager.newWakeLock(
        PowerManager.PARTIAL_WAKE_LOCK,
        WAKELOCK_TAG
      ).apply {
        setReferenceCounted(false)
        acquire(DownloadConstants.WAKELOCK_TIMEOUT_MS)
      }
      RNBackgroundDownloaderModuleImpl.logD(TAG, "WakeLock acquired")
    }
  }

  private fun releaseWakeLock() {
    wakeLock?.let {
      if (it.isHeld) {
        it.release()
        RNBackgroundDownloaderModuleImpl.logD(TAG, "WakeLock released")
      }
    }
    wakeLock = null
  }

  /**
   * Whether the service still has work. Used by [Downloader] to decide when it can drop its
   * binding: while a client is bound, stopServiceIfIdle()'s stopSelf() is a no-op and the
   * process stays at service adj, out of reach of the platform's background trim callbacks.
   */
  fun hasActiveWork(): Boolean = activeDownloads.isNotEmpty()

  private fun hasWork(): Boolean =
    activeDownloads.isNotEmpty() || activeDownloads.keys.any { resumableDownloader.getState(it) != null }

  /** Defers the actual stop by IDLE_STOP_GRACE_MS — see DownloadConstants for why. */
  private fun stopServiceIfIdle() {
    if (hasWork()) {
      RNBackgroundDownloaderModuleImpl.logD(TAG, "Service has active downloads, keeping alive")
      updateNotification()
      return
    }
    // The last task settled: show the final text now, not up to a throttle interval / grace later.
    updateNotification(force = true)
    mainHandler.removeCallbacks(idleStopRunnable)
    mainHandler.postDelayed(idleStopRunnable, DownloadConstants.IDLE_STOP_GRACE_MS)
  }

  private fun stopNowIfIdle() {
    val queue = groupQueue
    if (!hasWork() && queue != null && queue.summary().paused) {
      stopForPause()
      return
    }
    if (!hasWork() && queue?.hasActiveWork() != true) {
      RNBackgroundDownloaderModuleImpl.logD(TAG, "No active downloads for ${DownloadConstants.IDLE_STOP_GRACE_MS}ms, stopping service")
      releaseWakeLock()
      stopForeground(STOP_FOREGROUND_REMOVE)
      isForeground = false
      stopSelf()
    } else {
      RNBackgroundDownloaderModuleImpl.logD(TAG, "Service has active downloads, keeping alive")
      updateNotification()
    }
  }
}
