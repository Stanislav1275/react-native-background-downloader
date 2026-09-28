#import "RNBGDGroupQueue.h"
#import <MMKV/MMKV.h>
#import <QuartzCore/QuartzCore.h>
#include <atomic>

static NSString *const kStateQueued = @"queued";
static NSString *const kStateRunning = @"running";
static NSString *const kStateRetryWait = @"retrying";
static NSString *const kStateDone = @"done";
static NSString *const kStateFailed = @"failed";
static NSString *const kStateCanceled = @"canceled";
static NSString *const kStatePaused = @"paused";

static NSString *const kGroupKeyPrefix = @"g:";
static const CFTimeInterval kProgressEmitMinInterval = 0.3;

@interface RNBGDGroup : NSObject
@property (nonatomic, copy) NSString *groupId;
@property (nonatomic, copy) NSString *name;
@property (nonatomic, assign) CGFloat compressValue;
/// [{id, url, destination, headers?}]
@property (nonatomic, copy) NSArray<NSDictionary *> *tasks;
@property (nonatomic, copy) NSString *state;
@property (nonatomic, assign) NSInteger attempt;
@property (nonatomic, strong) NSMutableSet<NSString *> *done;
@property (nonatomic, strong) NSMutableSet<NSString *> *failed;
/// Tasks started in the current attempt and not settled yet.
@property (nonatomic, strong) NSMutableSet<NSString *> *pending;
/// Restored after process death with tasks that may still live in the background session.
@property (nonatomic, assign) BOOL awaitingAdoption;
/// Bumped on every attempt start, so a stale retry timer does nothing.
@property (nonatomic, assign) NSUInteger attemptToken;
@property (nonatomic, assign) CFTimeInterval lastProgressEmit;
/// Monotonic enqueue sequence number, persisted so restore keeps the queue order.
@property (nonatomic, assign) NSInteger order;
@end

@implementation RNBGDGroup
- (instancetype)init {
    if (self = [super init]) {
        _state = kStateQueued;
        _done = [NSMutableSet set];
        _failed = [NSMutableSet set];
        _pending = [NSMutableSet set];
    }
    return self;
}

- (BOOL)isTerminal {
    return [_state isEqualToString:kStateDone] || [_state isEqualToString:kStateFailed] || [_state isEqualToString:kStateCanceled];
}
@end

@implementation RNBGDGroupQueue {
    RNBGDGroupStartTask _startTask;
    RNBGDGroupStopTask _stopTask;
    RNBGDGroupEmit _emit;
    MMKV *_mmkv;
    dispatch_queue_t _timerQueue;
    /// Insertion-ordered: the queue order is the enqueue order.
    NSMutableArray<NSString *> *_order;
    NSMutableDictionary<NSString *, RNBGDGroup *> *_groups;
    NSMutableDictionary<NSString *, NSString *> *_taskToGroup;
    NSInteger _nextOrder;
    /// Set by -detach from the module's invalidate; read on the session delegate / timer queues.
    std::atomic<bool> _detached;
}

- (instancetype)initWithStartTask:(RNBGDGroupStartTask)startTask stopTask:(RNBGDGroupStopTask)stopTask emit:(RNBGDGroupEmit)emit {
    if (self = [super init]) {
        _detached = false;
        _startTask = [startTask copy];
        _stopTask = [stopTask copy];
        _emit = [emit copy];
        _mmkv = [MMKV mmkvWithID:@"RNBackgroundDownloaderGroupQueue"];
        _timerQueue = dispatch_queue_create("com.eko.backgrounddownloader.groupqueue", DISPATCH_QUEUE_SERIAL);
        _order = [NSMutableArray array];
        _groups = [NSMutableDictionary dictionary];
        _taskToGroup = [NSMutableDictionary dictionary];
        _maxConcurrentGroups = 1;
        _maxRetries = 2;
        _retryDelaysMs = @[@3000, @10000];
    }
    return self;
}

- (void)setMaxConcurrentGroups:(NSInteger)value {
    @synchronized (self) {
        _maxConcurrentGroups = MAX(1, value);
    }
    [self schedule];
}

- (BOOL)owns:(NSString *)taskId {
    if (taskId == nil) return NO;
    @synchronized (self) {
        return _taskToGroup[taskId] != nil;
    }
}

- (BOOL)hasWork {
    @synchronized (self) {
        for (RNBGDGroup *g in _groups.allValues) {
            if (![g isTerminal]) return YES;
        }
        return NO;
    }
}

#pragma mark - Host API

- (void)enqueue:(NSDictionary *)spec {
    if (_detached) return;
    NSString *groupId = spec[@"id"];
    NSArray *tasks = spec[@"tasks"];
    if (![groupId isKindOfClass:[NSString class]] || ![tasks isKindOfClass:[NSArray class]]) return;

    NSDictionary *snapshot;
    @synchronized (self) {
        RNBGDGroup *existing = _groups[groupId];
        if (existing != nil && ![existing isTerminal]) {
            NSLog(@"[RNBGDGroupQueue] enqueue: %@ already queued/running, ignored", groupId);
            return;
        }
        if (existing != nil) [self forget:existing];

        RNBGDGroup *group = [RNBGDGroup new];
        group.groupId = groupId;
        group.name = [spec[@"name"] isKindOfClass:[NSString class]] ? spec[@"name"] : groupId;
        group.compressValue = [spec[@"compressValue"] isKindOfClass:[NSNumber class]] ? [spec[@"compressValue"] doubleValue] : 0;
        NSMutableArray *valid = [NSMutableArray arrayWithCapacity:tasks.count];
        for (NSDictionary *t in tasks) {
            if (![t isKindOfClass:[NSDictionary class]]) continue;
            if (![t[@"id"] isKindOfClass:[NSString class]] || ![t[@"url"] isKindOfClass:[NSString class]] || ![t[@"destination"] isKindOfClass:[NSString class]]) continue;
            NSMutableDictionary *task = [@{@"id": t[@"id"], @"url": t[@"url"], @"destination": t[@"destination"]} mutableCopy];
            if ([t[@"headers"] isKindOfClass:[NSDictionary class]]) task[@"headers"] = t[@"headers"];
            [valid addObject:task];
        }
        group.tasks = valid;
        group.order = _nextOrder++;
        _groups[groupId] = group;
        [_order addObject:groupId];
        for (NSDictionary *t in valid) _taskToGroup[t[@"id"]] = groupId;
        [self persist:group];
        snapshot = [self snapshotOf:group];
    }
    [self emitEvent:@"groupState" payload:snapshot];
    [self schedule];
}

- (void)cancel:(NSString *)groupId {
    [self cancel:groupId keepUntilAck:NO];
}

- (void)cancel:(NSString *)groupId keepUntilAck:(BOOL)keep {
    NSArray<NSString *> *toStop;
    NSDictionary *snapshot;
    @synchronized (self) {
        RNBGDGroup *group = _groups[groupId];
        if (group == nil || [group isTerminal]) return;
        toStop = group.pending.allObjects;
        [group.pending removeAllObjects];
        group.attemptToken++;
        group.state = kStateCanceled;
        snapshot = [self snapshotOf:group];
        if (keep) [self persist:group];
        else [self forget:group];
    }
    [self emitEvent:@"groupState" payload:snapshot];
    for (NSString *taskId in toStop) _stopTask(taskId);
    [self schedule];
}

- (void)pauseAll {
    NSMutableArray<NSString *> *toStop = [NSMutableArray array];
    NSMutableArray<NSDictionary *> *snapshots = [NSMutableArray array];
    @synchronized (self) {
        for (NSString *groupId in _order) {
            RNBGDGroup *group = _groups[groupId];
            if ([group isTerminal] || [group.state isEqualToString:kStatePaused]) continue;
            [toStop addObjectsFromArray:group.pending.allObjects];
            [group.pending removeAllObjects];
            [group.failed removeAllObjects];
            group.awaitingAdoption = NO;
            group.attemptToken++;
            group.state = kStatePaused;
            [self persist:group];
            [snapshots addObject:[self snapshotOf:group]];
        }
    }
    for (NSDictionary *snapshot in snapshots) [self emitEvent:@"groupState" payload:snapshot];
    for (NSString *taskId in toStop) _stopTask(taskId);
}

- (void)resumeAll {
    NSMutableArray<NSDictionary *> *snapshots = [NSMutableArray array];
    @synchronized (self) {
        for (NSString *groupId in _order) {
            RNBGDGroup *group = _groups[groupId];
            if (![group.state isEqualToString:kStatePaused]) continue;
            group.state = kStateQueued;
            group.attempt = 0;
            [self persist:group];
            [snapshots addObject:[self snapshotOf:group]];
        }
    }
    for (NSDictionary *snapshot in snapshots) [self emitEvent:@"groupState" payload:snapshot];
    [self schedule];
}

- (void)cancelAll {
    NSArray<NSString *> *ids;
    @synchronized (self) {
        NSMutableArray<NSString *> *live = [NSMutableArray array];
        for (NSString *groupId in _order) {
            if (![_groups[groupId] isTerminal]) [live addObject:groupId];
        }
        ids = live;
    }
    for (NSString *groupId in ids) [self cancel:groupId keepUntilAck:YES];
}

- (void)acknowledge:(NSString *)groupId {
    @synchronized (self) {
        RNBGDGroup *group = _groups[groupId];
        if (group == nil || ![group isTerminal]) return;
        [self forget:group];
    }
}

- (NSArray<NSDictionary *> *)snapshots {
    @synchronized (self) {
        NSMutableArray *result = [NSMutableArray arrayWithCapacity:_order.count];
        for (NSString *groupId in _order) [result addObject:[self snapshotOf:_groups[groupId]]];
        return result;
    }
}

- (void)restore {
    @synchronized (self) {
        NSMutableArray<RNBGDGroup *> *restored = [NSMutableArray array];
        for (NSString *key in [_mmkv allKeys]) {
            if (![key hasPrefix:kGroupKeyPrefix]) continue;
            RNBGDGroup *group = [self groupFromJson:[_mmkv getStringForKey:key]];
            if (group == nil) continue;
            if ([group.state isEqualToString:kStateRunning] && group.pending.count > 0) {
                // Its tasks may still be running (or already finished) in the background session.
                group.awaitingAdoption = YES;
            } else if (![group isTerminal] && ![group.state isEqualToString:kStatePaused]) {
                group.state = kStateQueued;
                [group.pending removeAllObjects];
            }
            [restored addObject:group];
        }
        // MMKV keys are unordered; keep the enqueue order persisted with each group.
        [restored sortUsingComparator:^NSComparisonResult(RNBGDGroup *a, RNBGDGroup *b) {
            return [@(a.order) compare:@(b.order)];
        }];
        for (RNBGDGroup *group in restored) {
            _nextOrder = MAX(_nextOrder, group.order + 1);
            _groups[group.groupId] = group;
            [_order addObject:group.groupId];
            for (NSDictionary *t in group.tasks) _taskToGroup[t[@"id"]] = group.groupId;
        }
        NSLog(@"[RNBGDGroupQueue] restored %lu group(s)", (unsigned long)restored.count);
    }
    // Groups awaiting adoption hold their slots; queued ones wait for them like any other.
    [self schedule];
}

- (void)adoptLiveTaskIds:(NSSet<NSString *> *)liveTaskIds {
    NSMutableArray<NSArray *> *toRestart = [NSMutableArray array];
    NSMutableArray<RNBGDGroup *> *settled = [NSMutableArray array];
    @synchronized (self) {
        for (NSString *groupId in _order) {
            RNBGDGroup *group = _groups[groupId];
            if (!group.awaitingAdoption) continue;
            group.awaitingAdoption = NO;
            if ([group isTerminal]) continue;
            if (group.pending.count == 0) {
                [settled addObject:group];
                continue;
            }
            for (NSDictionary *task in group.tasks) {
                NSString *taskId = task[@"id"];
                if ([group.pending containsObject:taskId] && ![liveTaskIds containsObject:taskId]) {
                    [toRestart addObject:@[task, @(group.compressValue)]];
                }
            }
        }
    }
    NSLog(@"[RNBGDGroupQueue] adopt: %lu live task(s), restarting %lu lost one(s)", (unsigned long)liveTaskIds.count, (unsigned long)toRestart.count);
    for (NSArray *pair in toRestart) {
        NSDictionary *task = pair[0];
        if (!_startTask(task, [pair[1] doubleValue])) [self onTaskSettled:task[@"id"] success:NO];
    }
    for (RNBGDGroup *group in settled) [self onAttemptSettled:group];
}

- (void)flushForBackground {
    if (_detached) return;
    NSMutableArray<RNBGDGroup *> *toStart = [NSMutableArray array];
    @synchronized (self) {
        for (NSString *groupId in _order) {
            RNBGDGroup *group = _groups[groupId];
            if ([group.state isEqualToString:kStateQueued] || [group.state isEqualToString:kStateRetryWait]) {
                group.state = kStateRunning;
                [toStart addObject:group];
            }
        }
    }
    if (toStart.count) NSLog(@"[RNBGDGroupQueue] background: starting %lu group(s) now", (unsigned long)toStart.count);
    for (RNBGDGroup *group in toStart) [self startAttempt:group];
}

#pragma mark - Task callbacks

- (void)onTaskProgress:(NSString *)taskId {
    if (_detached) return;
    NSDictionary *snapshot;
    @synchronized (self) {
        RNBGDGroup *group = [self groupOfTask:taskId];
        if (group == nil) return;
        CFTimeInterval now = CACurrentMediaTime();
        if (now - group.lastProgressEmit < kProgressEmitMinInterval) return;
        group.lastProgressEmit = now;
        snapshot = [self snapshotOf:group];
    }
    [self emitEvent:@"groupProgress" payload:snapshot];
}

- (void)onTaskSettled:(NSString *)taskId success:(BOOL)success {
    if (_detached) return;
    RNBGDGroup *settledGroup = nil;
    @synchronized (self) {
        RNBGDGroup *group = [self groupOfTask:taskId];
        if (group == nil || ![group.pending containsObject:taskId]) return;
        [group.pending removeObject:taskId];
        if (success) {
            [group.done addObject:taskId];
            [group.failed removeObject:taskId];
        } else {
            [group.failed addObject:taskId];
        }
        if (group.pending.count == 0) {
            settledGroup = group;
        } else {
            // Keeps pending/done current for adoption after a process death.
            [self persist:group];
            group.lastProgressEmit = 0; // force the next progress emit to go out
        }
    }
    if (settledGroup == nil) {
        [self onTaskProgress:taskId];
        return;
    }
    [self onAttemptSettled:settledGroup];
}

#pragma mark - Internals

- (RNBGDGroup *)groupOfTask:(NSString *)taskId {
    NSString *groupId = taskId ? _taskToGroup[taskId] : nil;
    return groupId ? _groups[groupId] : nil;
}

- (void)onAttemptSettled:(RNBGDGroup *)group {
    NSDictionary *snapshot;
    @synchronized (self) {
        if ([group isTerminal]) return;
        if (group.failed.count > 0 && group.attempt < _maxRetries) {
            NSNumber *delayMs = group.attempt < (NSInteger)_retryDelaysMs.count ? _retryDelaysMs[group.attempt] : (_retryDelaysMs.lastObject ?: @0);
            group.attempt++;
            group.state = kStateRetryWait;
            NSUInteger token = group.attemptToken;
            [self persist:group];
            snapshot = [self snapshotOf:group];
            NSLog(@"[RNBGDGroupQueue] %@: %lu task(s) failed, retry #%ld in %@ms", group.groupId, (unsigned long)group.failed.count, (long)group.attempt, delayMs);
            // A group waiting for its retry keeps its slot: retries are for transient network loss,
            // starting other groups meanwhile would only fail them too.
            __weak RNBGDGroupQueue *weakSelf = self;
            dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)([delayMs doubleValue] * NSEC_PER_MSEC)), _timerQueue, ^{
                RNBGDGroupQueue *strongSelf = weakSelf;
                if (strongSelf == nil) return;
                @synchronized (strongSelf) {
                    // Already restarted by flushForBackground, canceled, or re-enqueued.
                    if (group.attemptToken != token || ![group.state isEqualToString:kStateRetryWait]) return;
                }
                [strongSelf startAttempt:group];
            });
        } else {
            group.state = group.failed.count == 0 ? kStateDone : kStateFailed;
            [self persist:group];
            snapshot = [self snapshotOf:group];
        }
    }
    [self emitEvent:@"groupState" payload:snapshot];
    if ([group isTerminal]) [self schedule];
}

- (void)schedule {
    if (_detached) return;
    NSMutableArray<RNBGDGroup *> *toStart = [NSMutableArray array];
    @synchronized (self) {
        NSInteger running = 0;
        for (RNBGDGroup *g in _groups.allValues) {
            if ([g.state isEqualToString:kStateRunning] || [g.state isEqualToString:kStateRetryWait]) running++;
        }
        for (NSString *groupId in _order) {
            if (running >= _maxConcurrentGroups) break;
            RNBGDGroup *group = _groups[groupId];
            if (![group.state isEqualToString:kStateQueued]) continue;
            group.state = kStateRunning;
            running++;
            [toStart addObject:group];
        }
    }
    for (RNBGDGroup *group in toStart) [self startAttempt:group];
}

- (void)startAttempt:(RNBGDGroup *)group {
    if (_detached) return;
    NSMutableArray<NSDictionary *> *tasks = [NSMutableArray array];
    CGFloat compressValue;
    NSDictionary *snapshot;
    @synchronized (self) {
        if ([group isTerminal] || [group.state isEqualToString:kStatePaused] || _groups[group.groupId] != group) return;
        group.state = kStateRunning;
        group.attemptToken++;
        for (NSDictionary *t in group.tasks) {
            if (![group.done containsObject:t[@"id"]]) [tasks addObject:t];
        }
        [group.failed removeAllObjects];
        [group.pending removeAllObjects];
        for (NSDictionary *t in tasks) [group.pending addObject:t[@"id"]];
        compressValue = group.compressValue;
        [self persist:group];
        snapshot = [self snapshotOf:group];
    }
    [self emitEvent:@"groupState" payload:snapshot];
    if (tasks.count == 0) {
        [self onAttemptSettled:group];
        return;
    }
    for (NSDictionary *task in tasks) {
        if (!_startTask(task, compressValue)) {
            NSLog(@"[RNBGDGroupQueue] start %@ failed", task[@"id"]);
            [self onTaskSettled:task[@"id"] success:NO];
        }
    }
}

- (void)forget:(RNBGDGroup *)group {
    [_groups removeObjectForKey:group.groupId];
    [_order removeObject:group.groupId];
    for (NSDictionary *t in group.tasks) {
        if ([_taskToGroup[t[@"id"]] isEqualToString:group.groupId]) [_taskToGroup removeObjectForKey:t[@"id"]];
    }
    [_mmkv removeValueForKey:[kGroupKeyPrefix stringByAppendingString:group.groupId]];
}

- (NSDictionary *)snapshotOf:(RNBGDGroup *)g {
    return @{
        @"id": g.groupId,
        @"name": g.name ?: g.groupId,
        @"state": g.state,
        @"attempt": @(g.attempt),
        @"total": @(g.tasks.count),
        @"completed": @(g.done.count),
        @"failed": @(g.failed.count),
        @"failedTaskIds": g.failed.allObjects,
    };
}

- (void)detach {
    _detached = true;
}

- (void)emitEvent:(NSString *)event payload:(NSDictionary *)payload {
    if (_detached) return;
    _emit(event, payload);
}

- (void)persist:(RNBGDGroup *)g {
    if (_detached) return;
    NSDictionary *json = @{
        @"id": g.groupId,
        @"name": g.name ?: g.groupId,
        @"compressValue": @(g.compressValue),
        @"state": g.state,
        @"attempt": @(g.attempt),
        @"order": @(g.order),
        @"done": g.done.allObjects,
        @"failed": g.failed.allObjects,
        @"pending": g.pending.allObjects,
        @"tasks": g.tasks,
    };
    NSData *data = [NSJSONSerialization dataWithJSONObject:json options:0 error:nil];
    if (data == nil) return;
    [_mmkv setString:[[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding] forKey:[kGroupKeyPrefix stringByAppendingString:g.groupId]];
}

- (nullable RNBGDGroup *)groupFromJson:(nullable NSString *)raw {
    if (raw == nil) return nil;
    NSDictionary *json = [NSJSONSerialization JSONObjectWithData:[raw dataUsingEncoding:NSUTF8StringEncoding] options:0 error:nil];
    if (![json isKindOfClass:[NSDictionary class]] || ![json[@"id"] isKindOfClass:[NSString class]] || ![json[@"tasks"] isKindOfClass:[NSArray class]]) return nil;
    RNBGDGroup *g = [RNBGDGroup new];
    g.groupId = json[@"id"];
    g.name = [json[@"name"] isKindOfClass:[NSString class]] ? json[@"name"] : g.groupId;
    g.compressValue = [json[@"compressValue"] doubleValue];
    g.tasks = json[@"tasks"];
    g.state = [json[@"state"] isKindOfClass:[NSString class]] ? json[@"state"] : kStateQueued;
    g.attempt = [json[@"attempt"] integerValue];
    g.order = [json[@"order"] integerValue];
    if ([json[@"done"] isKindOfClass:[NSArray class]]) [g.done addObjectsFromArray:json[@"done"]];
    if ([json[@"failed"] isKindOfClass:[NSArray class]]) [g.failed addObjectsFromArray:json[@"failed"]];
    if ([json[@"pending"] isKindOfClass:[NSArray class]]) [g.pending addObjectsFromArray:json[@"pending"]];
    return g;
}

@end
