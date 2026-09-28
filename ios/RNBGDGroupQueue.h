#import <Foundation/Foundation.h>

NS_ASSUME_NONNULL_BEGIN

/// Starts one task of a group: `task` is {id, url, destination, headers?}. Returns NO if it could not be started.
typedef BOOL (^RNBGDGroupStartTask)(NSDictionary *task, CGFloat compressValue);
typedef void (^RNBGDGroupStopTask)(NSString *taskId);
/// `event` is "groupState" or "groupProgress", `payload` a group snapshot.
typedef void (^RNBGDGroupEmit)(NSString *event, NSDictionary *payload);

/**
 * Native queue of download groups (e.g. one group = one manga chapter's pages) — the iOS side of the
 * contract implemented by GroupQueue.kt on Android.
 *
 * The host enqueues whole groups in one bridge call; this class decides when their tasks start (at
 * most `maxConcurrentGroups` groups at a time), accounts per-task completion, retries failed tasks and
 * reports only group-level events. The next group starts from the URLSession delegate, without JS.
 *
 * iOS specifics:
 * - Once the app is suspended nothing can start the next group in time (tasks created from the
 *   background are discretionary), so on entering the background `flushForBackground` hands every
 *   queued group to the background session at once and fires pending retries immediately; the
 *   session's per-host connection limit still bounds the actual parallelism.
 * - Background session tasks survive process death. `restore` keeps running groups running with
 *   their persisted pending tasks; `adoptLiveTaskIds:` then restarts only the tasks the session no
 *   longer has, instead of downloading the whole group again.
 *
 * State is persisted per group (MMKV), settled groups stay reported by `snapshots` until the host
 * acknowledges them.
 */
@interface RNBGDGroupQueue : NSObject

@property (nonatomic, assign) NSInteger maxConcurrentGroups;
@property (nonatomic, assign) NSInteger maxRetries;
@property (nonatomic, copy) NSArray<NSNumber *> *retryDelaysMs;

- (instancetype)initWithStartTask:(RNBGDGroupStartTask)startTask
                         stopTask:(RNBGDGroupStopTask)stopTask
                             emit:(RNBGDGroupEmit)emit;

- (BOOL)owns:(NSString *)taskId;
- (BOOL)hasWork;

/// `spec` is {id, name?, compressValue?, tasks: [{id, url, destination, headers?}]}.
- (void)enqueue:(NSDictionary *)spec;
- (void)cancel:(NSString *)groupId;
/// Stops every unfinished group (finished tasks are kept); nothing is scheduled until -resumeAll.
- (void)pauseAll;
- (void)resumeAll;
/// Cancels every unfinished group; the canceled records stay until the host acknowledges them.
- (void)cancelAll;
- (void)acknowledge:(NSString *)groupId;
- (NSArray<NSDictionary *> *)snapshots;

/// Re-reads persisted groups (process restart). Call before the session delivers delegate callbacks.
- (void)restore;
/// The owning module is going away (JS reload): from now on the queue does nothing — no persisting,
/// no events, no task starts. The next module instance restores the groups from storage.
- (void)detach;
/// Called once the session is activated with the ids of the download tasks it still runs.
- (void)adoptLiveTaskIds:(NSSet<NSString *> *)liveTaskIds;
/// App is going to be suspended: start every queued group and every pending retry now.
- (void)flushForBackground;
- (void)onTaskProgress:(NSString *)taskId;
- (void)onTaskSettled:(NSString *)taskId success:(BOOL)success;

@end

NS_ASSUME_NONNULL_END
