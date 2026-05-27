package core

// Yield labels: a controlled vocabulary identifying every cooperative-yield
// checkpoint in the handlers. Each yield() call passes one of these labels;
// the test runner records it in the trace so an agent reading a failure can
// identify which checkpoint executed last, what state the DB was in, and
// what code path the fiber took.
//
// Naming: <operation>.<phase>[.<target>]
//
//	operation = the request kind for RPC handlers (promise.create, task.acquire, ...),
//	            or a stable identifier for background processors and helpers
//	            (promise_timeout, task_retry_timeout, enqueue_resume, debug.snap, ...).
//
//	phase     = one of EIGHT closed verbs describing what just happened:
//	              read       — after a single-row SELECT/Scan
//	              scan       — after Iter() opens a multi-row cursor
//	              scan_row   — per-iteration work in a multi-row loop
//	              preinsert  — non-LWT INSERT that prepares state ahead of the main commit
//	              commit     — state-changing write (LWT or non-LWT UPDATE)
//	              rollback   — DELETE undoing a preinsert when the LWT did not apply
//	              cleanup    — DELETE for state hygiene (not rollback)
//	              batch      — BATCH statement completed
//
//	target    = added only when (operation, phase) recurs on different tables/rows
//	            in one operation. Allowed values: table names (promises, tasks,
//	            schedules, promise_timeouts, task_timeouts, schedule_timeouts,
//	            callbacks, listeners, timeouts) or semantic roles
//	            (awaited, awaiter) when distinguishing rows of the same table.
//	            For task_timeouts, an additional .retry or .lease suffix identifies
//	            which of the two timeout types the call targets:
//	              .retry — type 0: next scheduled retry for an unacquired/released task
//	              .lease — type 1: in-flight lease expiry for an actively acquired task
//	            Exception: scan labels covering a full-table scan of both types are
//	            intentionally left generic (no .retry/.lease suffix).
//
// Production background processors (TickAt etc.) and their debug-mode
// counterparts (debugTickAt) emit DIFFERENT labels: the debug variant
// carries a ".debug" suffix on the otherwise identical label. They have
// very different reachability — production ticks run on a background
// goroutine outside the fiber runner; debug ticks run inside a fiber
// driven by a debug.tick request. Distinguishing them aids forensics.
//
// Adding a new label: declare here, then reference at the yield call site.
// The compiler will catch typos; the grep handle for any agent inspecting
// a trace is the label string itself.
const (
	// ─── promise.read (helper readPromise) ──────────────────────────────────
	LabelPromiseRead = "promise.read"

	// ─── promise.create ─────────────────────────────────────────────────────
	LabelPromiseCreatePreinsertPromiseTimeouts   = "promise.create.preinsert.promise_timeouts"
	LabelPromiseCreatePreinsertTaskTimeoutsRetry = "promise.create.preinsert.task_timeouts.retry"
	LabelPromiseCreateCommit                     = "promise.create.commit"
	LabelPromiseCreateRollbackPromiseTimeouts    = "promise.create.rollback.promise_timeouts"
	LabelPromiseCreateRollbackTaskTimeoutsRetry  = "promise.create.rollback.task_timeouts.retry"

	// ─── promise.register_callback ──────────────────────────────────────────
	LabelPromiseRegisterCallbackCommitAwaited   = "promise.register_callback.commit.awaited"
	LabelPromiseRegisterCallbackResumePreinsert = "promise.register_callback.resume.preinsert"
	LabelPromiseRegisterCallbackResumeCommit    = "promise.register_callback.resume.commit"
	LabelPromiseRegisterCallbackResumeRollback  = "promise.register_callback.resume.rollback"

	// ─── promise.register_listener ──────────────────────────────────────────
	LabelPromiseRegisterListenerCommit = "promise.register_listener.commit"

	// ─── promise.settle ─────────────────────────────────────────────────────
	LabelPromiseSettleCleanupPromiseTimeouts   = "promise.settle.cleanup.promise_timeouts"
	LabelPromiseSettleCleanupTaskTimeoutsRetry = "promise.settle.cleanup.task_timeouts.retry"
	LabelPromiseSettleCleanupTaskTimeoutsLease = "promise.settle.cleanup.task_timeouts.lease"

	// ─── task.create (initial path + reacquire path + conflict path) ────────
	LabelTaskCreatePreinsertPromiseTimeouts   = "task.create.preinsert.promise_timeouts"
	LabelTaskCreatePreinsertTaskTimeoutsLease = "task.create.preinsert.task_timeouts.lease"
	LabelTaskCreateCommit                     = "task.create.commit"
	LabelTaskCreateRollbackPromiseTimeouts    = "task.create.rollback.promise_timeouts"
	LabelTaskCreateRollbackTaskTimeoutsLease  = "task.create.rollback.task_timeouts.lease"
	LabelTaskCreateCleanupTaskTimeoutsRetry   = "task.create.cleanup.task_timeouts.retry"

	// ─── task.acquire ───────────────────────────────────────────────────────
	LabelTaskAcquirePreinsertTaskTimeoutsLease = "task.acquire.preinsert.task_timeouts.lease"
	LabelTaskAcquireCommit                     = "task.acquire.commit"
	LabelTaskAcquireRollbackTaskTimeoutsLease  = "task.acquire.rollback.task_timeouts.lease"
	LabelTaskAcquireCleanupTaskTimeoutsRetry   = "task.acquire.cleanup.task_timeouts.retry"

	// ─── task.release ───────────────────────────────────────────────────────
	LabelTaskReleasePreinsertTaskTimeoutsRetry = "task.release.preinsert.task_timeouts.retry"
	LabelTaskReleaseCommit                     = "task.release.commit"
	LabelTaskReleaseRollbackTaskTimeoutsRetry  = "task.release.rollback.task_timeouts.retry"
	LabelTaskReleaseCleanupTaskTimeoutsLease   = "task.release.cleanup.task_timeouts.lease"

	// ─── task.heartbeat (loop body — one batch of yields per task) ──────────
	LabelTaskHeartbeatPreinsertTaskTimeoutsLease = "task.heartbeat.preinsert.task_timeouts.lease"
	LabelTaskHeartbeatCommit                     = "task.heartbeat.commit"
	LabelTaskHeartbeatCleanupTaskTimeoutsLease   = "task.heartbeat.cleanup.task_timeouts.lease"
	LabelTaskHeartbeatRollbackTaskTimeoutsLease  = "task.heartbeat.rollback.task_timeouts.lease"

	// ─── task.suspend ───────────────────────────────────────────────────────
	LabelTaskSuspendReadAwaiters             = "task.suspend.read.awaiters"
	LabelTaskSuspendClearResumes             = "task.suspend.clear.resumes"
	LabelTaskSuspendCommit                   = "task.suspend.commit"
	LabelTaskSuspendCleanupTaskTimeoutsLease = "task.suspend.cleanup.task_timeouts.lease"

	// ─── task.fulfill ───────────────────────────────────────────────────────
	LabelTaskFulfillCleanupPromiseTimeouts   = "task.fulfill.cleanup.promise_timeouts"
	LabelTaskFulfillCleanupTaskTimeoutsLease = "task.fulfill.cleanup.task_timeouts.lease"

	// ─── task.halt ───────────────────────────────────────────────────────────
	LabelTaskHaltCommit                   = "task.halt.commit"
	LabelTaskHaltCleanupTaskTimeoutsLease = "task.halt.cleanup.task_timeouts.lease"
	LabelTaskHaltCleanupTaskTimeoutsRetry = "task.halt.cleanup.task_timeouts.retry"

	// ─── task.continue ──────────────────────────────────────────────────────
	LabelTaskContinuePreinsertTaskTimeoutsRetry = "task.continue.preinsert.task_timeouts.retry"
	LabelTaskContinueCommit                     = "task.continue.commit"
	LabelTaskContinueRollbackTaskTimeoutsRetry  = "task.continue.rollback.task_timeouts.retry"

	// ─── schedule.read (helper readScheduleRow) ─────────────────────────────
	LabelScheduleRead = "schedule.read"

	// ─── schedule.create ────────────────────────────────────────────────────
	LabelScheduleCreatePreinsertScheduleTimeouts = "schedule.create.preinsert.schedule_timeouts"
	LabelScheduleCreateCommit                    = "schedule.create.commit"
	LabelScheduleCreateRollbackScheduleTimeouts  = "schedule.create.rollback.schedule_timeouts"

	// ─── schedule.delete ────────────────────────────────────────────────────
	LabelScheduleDeleteRead                    = "schedule.delete.read"
	LabelScheduleDeleteCommit                  = "schedule.delete.commit"
	LabelScheduleDeleteCleanupScheduleTimeouts = "schedule.delete.cleanup.schedule_timeouts"

	// ─── schedule_timeout (onScheduleTimeout) ───────────────────────────────
	LabelScheduleTimeoutCleanupScheduleTimeouts    = "schedule_timeout.cleanup.schedule_timeouts"
	LabelScheduleTimeoutPreinsertScheduleTimeouts  = "schedule_timeout.preinsert.schedule_timeouts"
	LabelScheduleTimeoutCommitSchedules            = "schedule_timeout.commit.schedules"
	LabelScheduleTimeoutPreinsertPromiseTimeouts   = "schedule_timeout.preinsert.promise_timeouts"
	LabelScheduleTimeoutPreinsertTaskTimeoutsRetry = "schedule_timeout.preinsert.task_timeouts.retry"
	LabelScheduleTimeoutCommitPromises             = "schedule_timeout.commit.promises"
	LabelScheduleTimeoutRollbackPromiseTimeouts    = "schedule_timeout.rollback.promise_timeouts"
	LabelScheduleTimeoutRollbackTaskTimeoutsRetry  = "schedule_timeout.rollback.task_timeouts.retry"

	// ─── promise_timeout (production: TickAt + onPromiseTimeout) ────────────
	LabelPromiseTimeoutScanPromiseTimeouts      = "promise_timeout.scan.promise_timeouts"
	LabelPromiseTimeoutScanRowPromiseTimeouts   = "promise_timeout.scan_row.promise_timeouts"
	LabelPromiseTimeoutRead                     = "promise_timeout.read"
	LabelPromiseTimeoutCleanupPromiseTimeouts   = "promise_timeout.cleanup.promise_timeouts"
	LabelPromiseTimeoutCleanupTaskTimeoutsRetry = "promise_timeout.cleanup.task_timeouts.retry"
	LabelPromiseTimeoutCleanupTaskTimeoutsLease = "promise_timeout.cleanup.task_timeouts.lease"

	// ─── promise_timeout debug variant (debugTickAt) ────────────────────────
	LabelPromiseTimeoutScanPromiseTimeoutsDebug    = "promise_timeout.scan.promise_timeouts.debug"
	LabelPromiseTimeoutScanRowPromiseTimeoutsDebug = "promise_timeout.scan_row.promise_timeouts.debug"

	// ─── task_timeout (production: TickAt scan) ─────────────────────────────
	LabelTaskTimeoutScanTaskTimeouts    = "task_timeout.scan.task_timeouts"
	LabelTaskTimeoutScanRowTaskTimeouts = "task_timeout.scan_row.task_timeouts"

	// ─── task_timeout debug variant (debugTickAt) ───────────────────────────
	LabelTaskTimeoutScanTaskTimeoutsDebug    = "task_timeout.scan.task_timeouts.debug"
	LabelTaskTimeoutScanRowTaskTimeoutsDebug = "task_timeout.scan_row.task_timeouts.debug"

	// ─── task_retry_timeout (onTaskRetryTimeout) ────────────────────────────
	LabelTaskRetryTimeoutRead                       = "task_retry_timeout.read"
	LabelTaskRetryTimeoutPreinsertTaskTimeoutsRetry = "task_retry_timeout.preinsert.task_timeouts.retry"
	LabelTaskRetryTimeoutCleanupTaskTimeoutsRetry   = "task_retry_timeout.cleanup.task_timeouts.retry"
	LabelTaskRetryTimeoutCommit                     = "task_retry_timeout.commit"
	LabelTaskRetryTimeoutRollbackTaskTimeoutsRetry  = "task_retry_timeout.rollback.task_timeouts.retry"

	// ─── task_lease_timeout (onTaskLeaseTimeout) ────────────────────────────
	LabelTaskLeaseTimeoutRead                       = "task_lease_timeout.read"
	LabelTaskLeaseTimeoutPreinsertTaskTimeoutsRetry = "task_lease_timeout.preinsert.task_timeouts.retry"
	LabelTaskLeaseTimeoutCommit                     = "task_lease_timeout.commit"
	LabelTaskLeaseTimeoutRollbackTaskTimeoutsRetry  = "task_lease_timeout.rollback.task_timeouts.retry"
	LabelTaskLeaseTimeoutCleanupTaskTimeoutsLease   = "task_lease_timeout.cleanup.task_timeouts.lease"

	// ─── schedule_timeout (production: TickAt + debug variant) ──────────────
	LabelScheduleTimeoutScanScheduleTimeouts         = "schedule_timeout.scan.schedule_timeouts"
	LabelScheduleTimeoutScanRowScheduleTimeouts      = "schedule_timeout.scan_row.schedule_timeouts"
	LabelScheduleTimeoutScanScheduleTimeoutsDebug    = "schedule_timeout.scan.schedule_timeouts.debug"
	LabelScheduleTimeoutScanRowScheduleTimeoutsDebug = "schedule_timeout.scan_row.schedule_timeouts.debug"

	// ─── enqueue_resume ──────────────────────────────────────────────────────────
	LabelEnqueueResumeReadAwaiters               = "settle_and_notify.read.awaiters"
	LabelEnqueueResumePreinsertTaskTimeoutsRetry = "settle_and_notify.preinsert.task_timeouts.retry"
	LabelEnqueueResumeCommit                     = "settle_and_notify.commit"
	LabelEnqueueResumeRollbackTaskTimeoutsRetry  = "settle_and_notify.rollback.task_timeouts.retry"

	// ─── debug.snap (handler_snap.go) ───────────────────────────────────────
	LabelDebugSnapScanPromises         = "debug.snap.scan.promises"
	LabelDebugSnapScanPromiseTimeouts  = "debug.snap.scan.promise_timeouts"
	LabelDebugSnapScanTaskTimeouts     = "debug.snap.scan.task_timeouts"
	LabelDebugSnapScanSchedules        = "debug.snap.scan.schedules"
	LabelDebugSnapScanScheduleTimeouts = "debug.snap.scan.schedule_timeouts"
)
