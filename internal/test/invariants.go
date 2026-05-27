package test

import (
	"fmt"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
)

// InvariantS is a predicate over a system snapshot.
// It returns an error describing the violation, or nil if the invariant holds.
type InvariantS func(snap core.DebugSnapResData) error

// InvariantT is a predicate over two (non-consecutive) system snapshots.
// It returns an error describing the violation, or nil if the invariant holds.
type InvariantT func(old, new core.DebugSnapResData) error

// NamedInvariantS pairs an InvariantS with its canonical function name.
type NamedInvariantS struct {
	Name  string
	Check func(snap core.DebugSnapResData) error
}

// NamedInvariantT pairs an InvariantT with its canonical function name.
type NamedInvariantT struct {
	Name  string
	Check func(old, new core.DebugSnapResData) error
}

// AllInvariantsS returns all static (single-snapshot) invariants.
func AllInvariantsS() []NamedInvariantS {
	return []NamedInvariantS{
		// Section 1: Promise-Task Coupling
		{Name: "PromiseWithTargetHasTask", Check: PromiseWithTargetHasTask},
		{Name: "TaskHasPromise", Check: TaskHasPromise},
		{Name: "ActivePromiseHasActiveTask", Check: ActivePromiseHasActiveTask},
		{Name: "ActiveTaskHasActivePromise", Check: ActiveTaskHasActivePromise},
		{Name: "SettledPromiseHasFulfilledTask", Check: SettledPromiseHasFulfilledTask},
		{Name: "FulfilledTaskHasSettledPromise", Check: FulfilledTaskHasSettledPromise},
		{Name: "SettledPromiseWithTaskIsFulfilled", Check: SettledPromiseWithTaskIsFulfilled},
		{Name: "PromiseNoTargetHasNoTask", Check: PromiseNoTargetHasNoTask},
		// Section 2: Promise Structure
		{Name: "PendingPromiseHasTimeout", Check: PendingPromiseHasTimeout},
		{Name: "TimeoutHasPendingPromise", Check: TimeoutHasPendingPromise},
		{Name: "SettledPromiseHasNoTimeout", Check: SettledPromiseHasNoTimeout},
		{Name: "SettledPromiseHasNoCallbacks", Check: SettledPromiseHasNoCallbacks},
		{Name: "CallbackNotSelfReferential", Check: CallbackNotSelfReferential},
		{Name: "CallbackAwaiterHasTarget", Check: CallbackAwaiterHasTarget},
		{Name: "CallbackAwaitedExists", Check: CallbackAwaitedExists},
		// Section 4: Timer Promises
		{Name: "TimerNeverRejectedTimedout", Check: TimerNeverRejectedTimedout},
		{Name: "TimerSettledByTimeoutIsResolved", Check: TimerSettledByTimeoutIsResolved},
		{Name: "RejectedTimedoutSettledAtEqualsTimeoutAt", Check: RejectedTimedoutSettledAtEqualsTimeoutAt},
		{Name: "SettledCreatedAtLeSettledAt", Check: SettledCreatedAtLeSettledAt},
		// Section 5: Task Structure
		{Name: "SuspendedTaskHasCallbacks", Check: SuspendedTaskHasCallbacks},
		// CallbackAwaiterIsSuspendedOrHaltedOrFulfilled is disabled: standalone
		// promise.register_callback in chaosOp can register a callback for a
		// task-ful awaiter in "pending" state without atomically suspending it,
		// creating a kill window between the LWT commit and the subsequent
		// task.suspend call.
		// {Name: "CallbackAwaiterIsSuspendedOrHaltedOrFulfilled", Check: CallbackAwaiterIsSuspendedOrHaltedOrFulfilled},
		{Name: "TaskVersionNonNegative", Check: TaskVersionNonNegative},
		{Name: "NonAcquiredTaskNoPidOrTtl", Check: NonAcquiredTaskNoPidOrTtl},
		// Section 7: Task Timeouts
		{Name: "PendingTaskHasRetryTimeout", Check: PendingTaskHasRetryTimeout},
		{Name: "AcquiredTaskHasLeaseTimeout", Check: AcquiredTaskHasLeaseTimeout},
		{Name: "LeaseTimeoutHasValidPidAndTtl", Check: LeaseTimeoutHasValidPidAndTtl},
		{Name: "SuspendedTaskHasNoTimeout", Check: SuspendedTaskHasNoTimeout},
		{Name: "HaltedTaskHasNoTimeout", Check: HaltedTaskHasNoTimeout},
		{Name: "FulfilledTaskHasNoTimeout", Check: FulfilledTaskHasNoTimeout},
		{Name: "LeaseTimeoutOnlyForAcquiredTask", Check: LeaseTimeoutOnlyForAcquiredTask},
		{Name: "RetryTimeoutOnlyForPendingTask", Check: RetryTimeoutOnlyForPendingTask},
		// Section 8: Listeners
		{Name: "SettledPromiseHasNoListeners", Check: SettledPromiseHasNoListeners},
		{Name: "PendingListenerPromiseIsPending", Check: PendingListenerPromiseIsPending},
		// Section 9: Schedules
		{Name: "ScheduleNextRunAtPositive", Check: ScheduleNextRunAtPositive},
		{Name: "ScheduleLastRunBeforeNextRun", Check: ScheduleLastRunBeforeNextRun},
		{Name: "ScheduleHasTimeout", Check: ScheduleHasTimeout},
		{Name: "ScheduleTimeoutHasSchedule", Check: ScheduleTimeoutHasSchedule},
	}
}

// AllInvariantsT returns all transition (two-snapshot) invariants.
func AllInvariantsT() []NamedInvariantT {
	return []NamedInvariantT{
		// Section 3: Promise Immutability
		{Name: "PromiseNeverDeleted", Check: PromiseNeverDeleted},
		{Name: "PromiseTargetImmutable", Check: PromiseTargetImmutable},
		{Name: "PromiseCreatedAtImmutable", Check: PromiseCreatedAtImmutable},
		{Name: "SettledPromiseImmutable", Check: SettledPromiseImmutable},
		{Name: "PromiseTimeoutAtImmutable", Check: PromiseTimeoutAtImmutable},
		// Section 6: Task Version
		{Name: "TaskNeverDeleted", Check: TaskNeverDeleted},
		{Name: "FulfilledTaskImmutable", Check: FulfilledTaskImmutable},
		{Name: "NewTaskStartsAtVersion0", Check: NewTaskStartsAtVersion0},
		{Name: "TaskVersionMonotonic", Check: TaskVersionMonotonic},
		{Name: "TaskResumeVersionUnchanged", Check: TaskResumeVersionUnchanged},
		{Name: "TaskAcquireBumpsVersion", Check: TaskAcquireBumpsVersion},
		{Name: "SuspendPreservesVersion", Check: SuspendPreservesVersion},
		{Name: "HaltPreservesVersion", Check: HaltPreservesVersion},
		{Name: "ReleasePreservesVersion", Check: ReleasePreservesVersion},
		// Section 9: Schedules
		{Name: "ScheduleNextRunAtMonotonic", Check: ScheduleNextRunAtMonotonic},
		{Name: "ScheduleLastRunAtMonotonic", Check: ScheduleLastRunAtMonotonic},
	}
}

// =============================================================================
// helpers
// =============================================================================

// promiseKey returns a composite key for a PromiseRecord that is unique across
// all (origin, id) pairs. Promises with the same id but different origins are
// distinct rows in the DB and must be tracked independently by invariants.
func promiseKey(p core.PromiseRecord) string {
	origin := ""
	if p.Origin != nil {
		origin = *p.Origin
	}
	return origin + "\x00" + p.ID
}

// taskKey returns a composite key for a TaskRecord that is unique across all
// (origin, id) pairs.
func taskKey(t core.TaskRecord) string {
	origin := ""
	if t.Origin != nil {
		origin = *t.Origin
	}
	return origin + "\x00" + t.ID
}

// timeoutKey returns a composite key for a promise TimeoutEntry.
func timeoutKey(pt core.TimeoutEntry) string {
	return pt.Origin + "\x00" + pt.ID
}

// taskTimeoutKey returns a composite key for a TaskTimeoutEntry.
func taskTimeoutKey(tt core.TaskTimeoutEntry) string {
	return tt.Origin + "\x00" + tt.ID
}

// callbackAwaitedKey returns the composite key for the awaited side of a callback.
func callbackAwaitedKey(cb core.CallbackEntry) string {
	origin := ""
	if cb.Origin != nil {
		origin = *cb.Origin
	}
	return origin + "\x00" + cb.Awaited
}

// callbackAwaiterKey returns the composite key for the awaiter side of a callback.
func callbackAwaiterKey(cb core.CallbackEntry) string {
	origin := ""
	if cb.Origin != nil {
		origin = *cb.Origin
	}
	return origin + "\x00" + cb.Awaiter
}

// listenerPromiseKey returns the composite promise key for a ListenerEntry.
func listenerPromiseKey(le core.ListenerEntry) string {
	origin := ""
	if le.Origin != nil {
		origin = *le.Origin
	}
	return origin + "\x00" + le.ID
}

// scheduleKey returns a composite key for a ScheduleRecord.
func scheduleKey(s core.ScheduleRecord) string {
	origin := ""
	if s.Origin != nil {
		origin = *s.Origin
	}
	return origin + "\x00" + s.ID
}

func isSettled(state string) bool {
	return state == "resolved" || state == "rejected" ||
		state == "rejected_canceled" || state == "rejected_timedout"
}

func isActiveTask(state string) bool {
	return state == "pending" || state == "acquired" ||
		state == "suspended" || state == "halted"
}

func valueEqual(a, b core.Value) bool {
	return a.Data == b.Data && stringMapEqual(a.Headers, b.Headers)
}

func stringMapEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func intPtrEqual(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func int64PtrEqual(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func int64PtrStr(p *int64) string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d", *p)
}

// =============================================================================
// Section 1: Promise-Task Coupling (InvariantS)
// =============================================================================

// PromiseWithTargetHasTask: every promise whose tags include "resonate:target"
// must have a corresponding TaskRecord in the snapshot.
func PromiseWithTargetHasTask(snap core.DebugSnapResData) error {
	taskKeys := make(map[string]bool, len(snap.Tasks))
	for _, t := range snap.Tasks {
		taskKeys[taskKey(t)] = true
	}
	for _, p := range snap.Promises {
		if p.Tags["resonate:target"] != "" && !taskKeys[promiseKey(p)] {
			return fmt.Errorf("PromiseWithTargetHasTask: promise %q has resonate:target but no task record", p.ID)
		}
	}
	return nil
}

// TaskHasPromise: every TaskRecord must have a corresponding PromiseRecord.
func TaskHasPromise(snap core.DebugSnapResData) error {
	promiseKeys := make(map[string]bool, len(snap.Promises))
	for _, p := range snap.Promises {
		promiseKeys[promiseKey(p)] = true
	}
	for _, t := range snap.Tasks {
		if !promiseKeys[taskKey(t)] {
			return fmt.Errorf("TaskHasPromise: task %q has no corresponding promise record", t.ID)
		}
	}
	return nil
}

// ActivePromiseHasActiveTask: a pending promise with a target must have a task
// in Pending, Acquired, Suspended, or Halted state.
func ActivePromiseHasActiveTask(snap core.DebugSnapResData) error {
	tasksByKey := make(map[string]core.TaskRecord, len(snap.Tasks))
	for _, t := range snap.Tasks {
		tasksByKey[taskKey(t)] = t
	}
	for _, p := range snap.Promises {
		if p.State != "pending" || p.Tags["resonate:target"] == "" {
			continue
		}
		t, ok := tasksByKey[promiseKey(p)]
		if !ok {
			return fmt.Errorf("ActivePromiseHasActiveTask: pending promise %q with target has no task", p.ID)
		}
		if !isActiveTask(t.State) {
			return fmt.Errorf("ActivePromiseHasActiveTask: pending promise %q has task in non-active state %q", p.ID, t.State)
		}
	}
	return nil
}

// ActiveTaskHasActivePromise: an active task (Pending/Acquired/Suspended/Halted)
// must have a pending promise.
func ActiveTaskHasActivePromise(snap core.DebugSnapResData) error {
	promiseByKey := make(map[string]core.PromiseRecord, len(snap.Promises))
	for _, p := range snap.Promises {
		promiseByKey[promiseKey(p)] = p
	}
	for _, t := range snap.Tasks {
		if !isActiveTask(t.State) {
			continue
		}
		p, ok := promiseByKey[taskKey(t)]
		if !ok {
			return fmt.Errorf("ActiveTaskHasActivePromise: active task %q has no promise", t.ID)
		}
		if p.State != "pending" {
			return fmt.Errorf("ActiveTaskHasActivePromise: active task %q has non-pending promise state %q", t.ID, p.State)
		}
	}
	return nil
}

// SettledPromiseHasFulfilledTask: a settled promise with a target must have a
// fulfilled task.
func SettledPromiseHasFulfilledTask(snap core.DebugSnapResData) error {
	tasksByKey := make(map[string]core.TaskRecord, len(snap.Tasks))
	for _, t := range snap.Tasks {
		tasksByKey[taskKey(t)] = t
	}
	for _, p := range snap.Promises {
		if !isSettled(p.State) || p.Tags["resonate:target"] == "" {
			continue
		}
		t, ok := tasksByKey[promiseKey(p)]
		if !ok {
			return fmt.Errorf("SettledPromiseHasFulfilledTask: settled promise %q with target has no task", p.ID)
		}
		if t.State != "fulfilled" {
			return fmt.Errorf("SettledPromiseHasFulfilledTask: settled promise %q has task in state %q, want fulfilled", p.ID, t.State)
		}
	}
	return nil
}

// FulfilledTaskHasSettledPromise: a fulfilled task must have a settled promise.
func FulfilledTaskHasSettledPromise(snap core.DebugSnapResData) error {
	promiseByKey := make(map[string]core.PromiseRecord, len(snap.Promises))
	for _, p := range snap.Promises {
		promiseByKey[promiseKey(p)] = p
	}
	for _, t := range snap.Tasks {
		if t.State != "fulfilled" {
			continue
		}
		p, ok := promiseByKey[taskKey(t)]
		if !ok {
			return fmt.Errorf("FulfilledTaskHasSettledPromise: fulfilled task %q has no promise", t.ID)
		}
		if !isSettled(p.State) {
			return fmt.Errorf("FulfilledTaskHasSettledPromise: fulfilled task %q has non-settled promise state %q", t.ID, p.State)
		}
	}
	return nil
}

// SettledPromiseWithTaskIsFulfilled: any settled promise that has a task must
// have a fulfilled task.
func SettledPromiseWithTaskIsFulfilled(snap core.DebugSnapResData) error {
	tasksByKey := make(map[string]core.TaskRecord, len(snap.Tasks))
	for _, t := range snap.Tasks {
		tasksByKey[taskKey(t)] = t
	}
	for _, p := range snap.Promises {
		if !isSettled(p.State) {
			continue
		}
		t, ok := tasksByKey[promiseKey(p)]
		if !ok {
			continue // no task — OK
		}
		if t.State != "fulfilled" {
			return fmt.Errorf("SettledPromiseWithTaskIsFulfilled: settled promise %q has task in state %q, want fulfilled", p.ID, t.State)
		}
	}
	return nil
}

// PromiseNoTargetHasNoTask: any promise (regardless of state) without a
// resonate:target tag must have no corresponding task record.
func PromiseNoTargetHasNoTask(snap core.DebugSnapResData) error {
	taskKeys := make(map[string]bool, len(snap.Tasks))
	for _, t := range snap.Tasks {
		taskKeys[taskKey(t)] = true
	}
	for _, p := range snap.Promises {
		if p.Tags["resonate:target"] != "" {
			continue
		}
		if taskKeys[promiseKey(p)] {
			return fmt.Errorf("PromiseNoTargetHasNoTask: promise %q (state=%s) has no resonate:target but has a task record", p.ID, p.State)
		}
	}
	return nil
}

// =============================================================================
// Section 2: Promise Structure (InvariantS)
// =============================================================================

// PendingPromiseHasTimeout: every pending promise must have a promise_timeouts entry.
func PendingPromiseHasTimeout(snap core.DebugSnapResData) error {
	timeoutKeys := make(map[string]bool, len(snap.PromiseTimeouts))
	for _, pt := range snap.PromiseTimeouts {
		timeoutKeys[timeoutKey(pt)] = true
	}
	for _, p := range snap.Promises {
		if p.State == "pending" && !timeoutKeys[promiseKey(p)] {
			return fmt.Errorf("PendingPromiseHasTimeout: pending promise %q has no promise_timeouts entry", p.ID)
		}
	}
	return nil
}

// TimeoutHasPendingPromise: every promise_timeouts entry must reference a
// pending promise.
func TimeoutHasPendingPromise(snap core.DebugSnapResData) error {
	pendingKeys := make(map[string]bool, len(snap.Promises))
	for _, p := range snap.Promises {
		if p.State == "pending" {
			pendingKeys[promiseKey(p)] = true
		}
	}
	for _, pt := range snap.PromiseTimeouts {
		if !pendingKeys[timeoutKey(pt)] {
			return fmt.Errorf("TimeoutHasPendingPromise: promise_timeouts entry for %q does not reference a pending promise", pt.ID)
		}
	}
	return nil
}

// SettledPromiseHasNoTimeout: settled promises must have no promise_timeouts entry.
func SettledPromiseHasNoTimeout(snap core.DebugSnapResData) error {
	timeoutKeys := make(map[string]bool, len(snap.PromiseTimeouts))
	for _, pt := range snap.PromiseTimeouts {
		timeoutKeys[timeoutKey(pt)] = true
	}
	for _, p := range snap.Promises {
		if isSettled(p.State) && timeoutKeys[promiseKey(p)] {
			return fmt.Errorf("SettledPromiseHasNoTimeout: settled promise %q has a promise_timeouts entry", p.ID)
		}
	}
	return nil
}

// SettledPromiseHasNoCallbacks: settled promises must have no registered callbacks.
func SettledPromiseHasNoCallbacks(snap core.DebugSnapResData) error {
	settledKeys := make(map[string]bool, len(snap.Promises))
	for _, p := range snap.Promises {
		if isSettled(p.State) {
			settledKeys[promiseKey(p)] = true
		}
	}
	for _, cb := range snap.Callbacks {
		if settledKeys[callbackAwaitedKey(cb)] {
			return fmt.Errorf("SettledPromiseHasNoCallbacks: settled promise %q has callback (awaiter=%q)", cb.Awaited, cb.Awaiter)
		}
	}
	return nil
}

// CallbackNotSelfReferential: no promise registers itself as its own callback awaiter.
func CallbackNotSelfReferential(snap core.DebugSnapResData) error {
	for _, cb := range snap.Callbacks {
		if cb.Awaiter == cb.Awaited {
			return fmt.Errorf("CallbackNotSelfReferential: promise %q is its own callback awaiter", cb.Awaiter)
		}
	}
	return nil
}

// CallbackAwaiterHasTarget: every awaiter ID in any callbacks set belongs to a
// promise with a target (i.e., has a TaskRecord).
func CallbackAwaiterHasTarget(snap core.DebugSnapResData) error {
	taskKeys := make(map[string]bool, len(snap.Tasks))
	for _, t := range snap.Tasks {
		taskKeys[taskKey(t)] = true
	}
	for _, cb := range snap.Callbacks {
		if !taskKeys[callbackAwaiterKey(cb)] {
			return fmt.Errorf("CallbackAwaiterHasTarget: awaiter %q in callback has no task (no target)", cb.Awaiter)
		}
	}
	return nil
}

// CallbackAwaitedExists: for every callback entry, the awaited promise ID must
// refer to a promise that exists in the snapshot.
func CallbackAwaitedExists(snap core.DebugSnapResData) error {
	promiseKeys := make(map[string]bool, len(snap.Promises))
	for _, p := range snap.Promises {
		promiseKeys[promiseKey(p)] = true
	}
	for _, cb := range snap.Callbacks {
		if !promiseKeys[callbackAwaitedKey(cb)] {
			return fmt.Errorf("CallbackAwaitedExists: callback references awaited promise %q which does not exist in snapshot", cb.Awaited)
		}
	}
	return nil
}

// =============================================================================
// Section 4: Timer Promises (InvariantS)
// =============================================================================

// TimerNeverRejectedTimedout: timer promises (resonate:timer=true) never reach
// rejected_timedout state.
func TimerNeverRejectedTimedout(snap core.DebugSnapResData) error {
	for _, p := range snap.Promises {
		if p.Tags["resonate:timer"] == "true" && p.State == "rejected_timedout" {
			return fmt.Errorf("TimerNeverRejectedTimedout: timer promise %q is in rejected_timedout state", p.ID)
		}
	}
	return nil
}

// TimerSettledByTimeoutIsResolved: a timer promise whose settledAt == timeoutAt
// must be in Resolved state.
func TimerSettledByTimeoutIsResolved(snap core.DebugSnapResData) error {
	for _, p := range snap.Promises {
		if p.Tags["resonate:timer"] != "true" || p.SettledAt == nil {
			continue
		}
		if *p.SettledAt == p.TimeoutAt && p.State != "resolved" {
			return fmt.Errorf("TimerSettledByTimeoutIsResolved: timer promise %q settled at timeoutAt=%d but state is %q", p.ID, p.TimeoutAt, p.State)
		}
	}
	return nil
}

// RejectedTimedoutSettledAtEqualsTimeoutAt: rejected_timedout promises must have
// settledAt == timeoutAt.
func RejectedTimedoutSettledAtEqualsTimeoutAt(snap core.DebugSnapResData) error {
	for _, p := range snap.Promises {
		if p.State != "rejected_timedout" {
			continue
		}
		if p.SettledAt == nil || *p.SettledAt != p.TimeoutAt {
			settledAt := int64(-1)
			if p.SettledAt != nil {
				settledAt = *p.SettledAt
			}
			return fmt.Errorf("RejectedTimedoutSettledAtEqualsTimeoutAt: promise %q has settledAt=%d, timeoutAt=%d", p.ID, settledAt, p.TimeoutAt)
		}
	}
	return nil
}

// SettledCreatedAtLeSettledAt: createdAt <= settledAt for every settled promise.
func SettledCreatedAtLeSettledAt(snap core.DebugSnapResData) error {
	for _, p := range snap.Promises {
		if p.SettledAt == nil {
			continue
		}
		if p.CreatedAt > *p.SettledAt {
			return fmt.Errorf("SettledCreatedAtLeSettledAt: promise %q has createdAt=%d > settledAt=%d", p.ID, p.CreatedAt, *p.SettledAt)
		}
	}
	return nil
}

// =============================================================================
// Section 5: Task Structure (InvariantS)
// =============================================================================

// SuspendedTaskHasCallbacks: every suspended task must have at least one
// registered callback on some promise.
func SuspendedTaskHasCallbacks(snap core.DebugSnapResData) error {
	awaiterKeys := make(map[string]bool, len(snap.Callbacks))
	for _, cb := range snap.Callbacks {
		awaiterKeys[callbackAwaiterKey(cb)] = true
	}
	for _, t := range snap.Tasks {
		if t.State == "suspended" && !awaiterKeys[taskKey(t)] {
			return fmt.Errorf("SuspendedTaskHasCallbacks: suspended task %q has no registered callbacks", t.ID)
		}
	}
	return nil
}

// TaskVersionNonNegative: all task versions must be >= 0.
func TaskVersionNonNegative(snap core.DebugSnapResData) error {
	for _, t := range snap.Tasks {
		if t.Version < 0 {
			return fmt.Errorf("TaskVersionNonNegative: task %q has version %d < 0", t.ID, t.Version)
		}
	}
	return nil
}

// NonAcquiredTaskNoPidOrTtl: only acquired tasks may carry a PID or TTL.
func NonAcquiredTaskNoPidOrTtl(snap core.DebugSnapResData) error {
	for _, t := range snap.Tasks {
		if t.State == "acquired" {
			continue
		}
		if t.PID != "" {
			return fmt.Errorf("NonAcquiredTaskNoPidOrTtl: task %q (state=%s) has non-empty pid %q", t.ID, t.State, t.PID)
		}
		if t.TTL != nil {
			return fmt.Errorf("NonAcquiredTaskNoPidOrTtl: task %q (state=%s) has non-nil ttl", t.ID, t.State)
		}
	}
	return nil
}

// CallbackAwaiterIsSuspendedOrHaltedOrFulfilled: every callback's awaiter task must be in
// suspended, halted, or fulfilled state. Fulfilled is permitted because when an
// awaiter settles before the awaited promise does, its ID is not removed from
// the awaited promise's callbacks column (by design, to avoid cross-partition
// writes); it is silently skipped when the awaited promise eventually settles.
func CallbackAwaiterIsSuspendedOrHaltedOrFulfilled(snap core.DebugSnapResData) error {
	taskStateByKey := make(map[string]string, len(snap.Tasks))
	for _, t := range snap.Tasks {
		taskStateByKey[taskKey(t)] = t.State
	}
	for _, cb := range snap.Callbacks {
		state, ok := taskStateByKey[callbackAwaiterKey(cb)]
		if !ok {
			continue // covered by CallbackAwaiterHasTarget
		}
		if state != "suspended" && state != "halted" && state != "fulfilled" {
			return fmt.Errorf("CallbackAwaiterIsSuspendedOrHaltedOrFulfilled: callback awaiter %q is in state %q (want suspended, halted, or fulfilled)", cb.Awaiter, state)
		}
	}
	return nil
}

// =============================================================================
// Section 7: Task Timeouts (InvariantS)
// =============================================================================

// AcquiredTaskHasLeaseTimeout: acquired tasks must have a lease (type=1) timeout.
func AcquiredTaskHasLeaseTimeout(snap core.DebugSnapResData) error {
	leaseKeys := make(map[string]bool)
	for _, tt := range snap.TaskTimeouts {
		if tt.Type == 1 {
			leaseKeys[taskTimeoutKey(tt)] = true
		}
	}
	for _, t := range snap.Tasks {
		if t.State == "acquired" && !leaseKeys[taskKey(t)] {
			return fmt.Errorf("AcquiredTaskHasLeaseTimeout: acquired task %q has no lease timeout entry", t.ID)
		}
	}
	return nil
}

// LeaseTimeoutHasValidPidAndTtl: lease (type=1) timeout entries must correspond
// to a task with a non-empty PID and positive TTL.
func LeaseTimeoutHasValidPidAndTtl(snap core.DebugSnapResData) error {
	tasksByKey := make(map[string]core.TaskRecord, len(snap.Tasks))
	for _, t := range snap.Tasks {
		tasksByKey[taskKey(t)] = t
	}
	for _, tt := range snap.TaskTimeouts {
		if tt.Type != 1 {
			continue
		}
		t, ok := tasksByKey[taskTimeoutKey(tt)]
		if !ok {
			return fmt.Errorf("LeaseTimeoutHasValidPidAndTtl: lease timeout for unknown task %q", tt.ID)
		}
		if t.PID == "" {
			return fmt.Errorf("LeaseTimeoutHasValidPidAndTtl: lease timeout for task %q has empty pid", tt.ID)
		}
		if t.TTL == nil || *t.TTL <= 0 {
			return fmt.Errorf("LeaseTimeoutHasValidPidAndTtl: lease timeout for task %q has invalid ttl", tt.ID)
		}
	}
	return nil
}

// PendingTaskHasRetryTimeout: pending tasks must have a retry (type=0) timeout.
func PendingTaskHasRetryTimeout(snap core.DebugSnapResData) error {
	retryKeys := make(map[string]bool)
	for _, tt := range snap.TaskTimeouts {
		if tt.Type == 0 {
			retryKeys[taskTimeoutKey(tt)] = true
		}
	}
	for _, t := range snap.Tasks {
		if t.State == "pending" && !retryKeys[taskKey(t)] {
			return fmt.Errorf("PendingTaskHasRetryTimeout: pending task %q has no retry timeout entry", t.ID)
		}
	}
	return nil
}

// SuspendedTaskHasNoTimeout: suspended tasks must have no task_timeouts entry.
func SuspendedTaskHasNoTimeout(snap core.DebugSnapResData) error {
	timeoutKeys := make(map[string]bool)
	for _, tt := range snap.TaskTimeouts {
		timeoutKeys[taskTimeoutKey(tt)] = true
	}
	for _, t := range snap.Tasks {
		if t.State == "suspended" && timeoutKeys[taskKey(t)] {
			return fmt.Errorf("SuspendedTaskHasNoTimeout: suspended task %q has a task_timeouts entry", t.ID)
		}
	}
	return nil
}

// HaltedTaskHasNoTimeout: halted tasks must have no task_timeouts entry.
func HaltedTaskHasNoTimeout(snap core.DebugSnapResData) error {
	timeoutKeys := make(map[string]bool)
	for _, tt := range snap.TaskTimeouts {
		timeoutKeys[taskTimeoutKey(tt)] = true
	}
	for _, t := range snap.Tasks {
		if t.State == "halted" && timeoutKeys[taskKey(t)] {
			return fmt.Errorf("HaltedTaskHasNoTimeout: halted task %q has a task_timeouts entry", t.ID)
		}
	}
	return nil
}

// FulfilledTaskHasNoTimeout: fulfilled tasks must have no task_timeouts entry.
func FulfilledTaskHasNoTimeout(snap core.DebugSnapResData) error {
	timeoutKeys := make(map[string]bool)
	for _, tt := range snap.TaskTimeouts {
		timeoutKeys[taskTimeoutKey(tt)] = true
	}
	for _, t := range snap.Tasks {
		if t.State == "fulfilled" && timeoutKeys[taskKey(t)] {
			return fmt.Errorf("FulfilledTaskHasNoTimeout: fulfilled task %q has a task_timeouts entry", t.ID)
		}
	}
	return nil
}

// LeaseTimeoutOnlyForAcquiredTask: lease (type=1) timeouts must only exist for
// acquired tasks.
func LeaseTimeoutOnlyForAcquiredTask(snap core.DebugSnapResData) error {
	tasksByKey := make(map[string]core.TaskRecord, len(snap.Tasks))
	for _, t := range snap.Tasks {
		tasksByKey[taskKey(t)] = t
	}
	for _, tt := range snap.TaskTimeouts {
		if tt.Type != 1 {
			continue
		}
		t, ok := tasksByKey[taskTimeoutKey(tt)]
		if !ok {
			return fmt.Errorf("LeaseTimeoutOnlyForAcquiredTask: lease timeout for unknown task %q", tt.ID)
		}
		if t.State != "acquired" {
			return fmt.Errorf("LeaseTimeoutOnlyForAcquiredTask: lease timeout for task %q in state %q (want acquired)", tt.ID, t.State)
		}
	}
	return nil
}

// RetryTimeoutOnlyForPendingTask: retry (type=0) timeouts must only exist for
// pending tasks.
func RetryTimeoutOnlyForPendingTask(snap core.DebugSnapResData) error {
	tasksByKey := make(map[string]core.TaskRecord, len(snap.Tasks))
	for _, t := range snap.Tasks {
		tasksByKey[taskKey(t)] = t
	}
	for _, tt := range snap.TaskTimeouts {
		if tt.Type != 0 {
			continue
		}
		t, ok := tasksByKey[taskTimeoutKey(tt)]
		if !ok {
			return fmt.Errorf("RetryTimeoutOnlyForPendingTask: retry timeout for unknown task %q", tt.ID)
		}
		if t.State != "pending" {
			return fmt.Errorf("RetryTimeoutOnlyForPendingTask: retry timeout for task %q in state %q (want pending)", tt.ID, t.State)
		}
	}
	return nil
}

// =============================================================================
// Section 8: Listeners (InvariantS)
// =============================================================================

// SettledPromiseHasNoListeners: settled promises must have no listener entries.
func SettledPromiseHasNoListeners(snap core.DebugSnapResData) error {
	settledKeys := make(map[string]bool, len(snap.Promises))
	for _, p := range snap.Promises {
		if isSettled(p.State) {
			settledKeys[promiseKey(p)] = true
		}
	}
	for _, l := range snap.Listeners {
		if settledKeys[listenerPromiseKey(l)] {
			return fmt.Errorf("SettledPromiseHasNoListeners: settled promise %q has a listener entry", l.ID)
		}
	}
	return nil
}

// PendingListenerPromiseIsPending: every listener entry must reference a pending promise.
func PendingListenerPromiseIsPending(snap core.DebugSnapResData) error {
	pendingKeys := make(map[string]bool, len(snap.Promises))
	for _, p := range snap.Promises {
		if p.State == "pending" {
			pendingKeys[promiseKey(p)] = true
		}
	}
	for _, l := range snap.Listeners {
		if !pendingKeys[listenerPromiseKey(l)] {
			return fmt.Errorf("PendingListenerPromiseIsPending: listener references promise %q which is not pending", l.ID)
		}
	}
	return nil
}

// =============================================================================
// Section 9: Schedules (InvariantS)
// =============================================================================

// ScheduleNextRunAtPositive: every schedule's nextRunAt must be a positive value.
func ScheduleNextRunAtPositive(snap core.DebugSnapResData) error {
	for _, s := range snap.Schedules {
		if s.NextRunAt <= 0 {
			return fmt.Errorf("ScheduleNextRunAtPositive: schedule %q has nextRunAt=%d (<= 0)", s.ID, s.NextRunAt)
		}
	}
	return nil
}

// ScheduleLastRunBeforeNextRun: when lastRunAt is set, it must be strictly less
// than nextRunAt.
func ScheduleLastRunBeforeNextRun(snap core.DebugSnapResData) error {
	for _, s := range snap.Schedules {
		if s.LastRunAt != nil && *s.LastRunAt >= s.NextRunAt {
			return fmt.Errorf("ScheduleLastRunBeforeNextRun: schedule %q has lastRunAt=%d >= nextRunAt=%d", s.ID, *s.LastRunAt, s.NextRunAt)
		}
	}
	return nil
}

// ScheduleHasTimeout: every schedule must have a schedule_timeouts entry.
func ScheduleHasTimeout(snap core.DebugSnapResData) error {
	timeoutKeys := make(map[string]bool, len(snap.ScheduleTimeouts))
	for _, st := range snap.ScheduleTimeouts {
		timeoutKeys[timeoutKey(st)] = true
	}
	for _, s := range snap.Schedules {
		if !timeoutKeys[scheduleKey(s)] {
			return fmt.Errorf("ScheduleHasTimeout: schedule %q has no schedule_timeouts entry", s.ID)
		}
	}
	return nil
}

// ScheduleTimeoutHasSchedule: every schedule_timeouts entry must reference an
// existing schedule.
func ScheduleTimeoutHasSchedule(snap core.DebugSnapResData) error {
	scheduleKeys := make(map[string]bool, len(snap.Schedules))
	for _, s := range snap.Schedules {
		scheduleKeys[scheduleKey(s)] = true
	}
	for _, st := range snap.ScheduleTimeouts {
		if !scheduleKeys[timeoutKey(st)] {
			return fmt.Errorf("ScheduleTimeoutHasSchedule: schedule_timeouts entry for %q has no corresponding schedule", st.ID)
		}
	}
	return nil
}

// =============================================================================
// Section 3: Promise Immutability (InvariantT)
// =============================================================================

// PromiseNeverDeleted: every promise that exists in old must still exist in new.
func PromiseNeverDeleted(old, new core.DebugSnapResData) error {
	newIDs := make(map[string]bool, len(new.Promises))
	for _, p := range new.Promises {
		newIDs[promiseKey(p)] = true
	}
	for _, p := range old.Promises {
		if !newIDs[promiseKey(p)] {
			return fmt.Errorf("PromiseNeverDeleted: promise %q existed in old snapshot but not in new", p.ID)
		}
	}
	return nil
}

// PromiseTargetImmutable: the presence (or absence) of a task for an existing
// promise must not change — target is set at creation and never altered.
func PromiseTargetImmutable(old, new core.DebugSnapResData) error {
	oldPromiseKeys := make(map[string]bool, len(old.Promises))
	for _, p := range old.Promises {
		oldPromiseKeys[promiseKey(p)] = true
	}
	oldTaskKeys := make(map[string]bool, len(old.Tasks))
	for _, t := range old.Tasks {
		oldTaskKeys[taskKey(t)] = true
	}
	newTaskKeys := make(map[string]bool, len(new.Tasks))
	for _, t := range new.Tasks {
		newTaskKeys[taskKey(t)] = true
	}
	// A promise that had a task in old must still have a task in new.
	for key := range oldTaskKeys {
		if oldPromiseKeys[key] && !newTaskKeys[key] {
			return fmt.Errorf("PromiseTargetImmutable: promise+task %q had a task (target) in old but not in new", key)
		}
	}
	// A promise that existed in old without a task must not gain a task in new.
	for key := range newTaskKeys {
		if oldPromiseKeys[key] && !oldTaskKeys[key] {
			return fmt.Errorf("PromiseTargetImmutable: promise %q gained a task (target) between snapshots", key)
		}
	}
	return nil
}

// PromiseCreatedAtImmutable: createdAt must not change for any promise that
// exists in both snapshots.
func PromiseCreatedAtImmutable(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]core.PromiseRecord, len(old.Promises))
	for _, p := range old.Promises {
		oldByKey[promiseKey(p)] = p
	}
	for _, p := range new.Promises {
		if o, ok := oldByKey[promiseKey(p)]; ok {
			if o.CreatedAt != p.CreatedAt {
				return fmt.Errorf("PromiseCreatedAtImmutable: promise %q changed createdAt from %d to %d", p.ID, o.CreatedAt, p.CreatedAt)
			}
		}
	}
	return nil
}

// SettledPromiseImmutable: once a promise is observed as settled, every field
// must remain unchanged in subsequent snapshots. Settled promises are
// externalized to clients (returned by promise.get, promise.search, embedded
// in unblock messages), so any post-settlement mutation would break the
// promise contract.
func SettledPromiseImmutable(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]core.PromiseRecord, len(old.Promises))
	for _, p := range old.Promises {
		oldByKey[promiseKey(p)] = p
	}
	for _, n := range new.Promises {
		o, ok := oldByKey[promiseKey(n)]
		if !ok || !isSettled(o.State) {
			continue
		}
		if n.State != o.State {
			return fmt.Errorf("SettledPromiseImmutable: promise %q state changed from %q to %q", n.ID, o.State, n.State)
		}
		if !valueEqual(o.Value, n.Value) {
			return fmt.Errorf("SettledPromiseImmutable: promise %q value changed", n.ID)
		}
		if !int64PtrEqual(o.SettledAt, n.SettledAt) {
			return fmt.Errorf("SettledPromiseImmutable: promise %q settledAt changed from %s to %s", n.ID, int64PtrStr(o.SettledAt), int64PtrStr(n.SettledAt))
		}
		if !valueEqual(o.Param, n.Param) {
			return fmt.Errorf("SettledPromiseImmutable: promise %q param changed", n.ID)
		}
		if !stringMapEqual(o.Tags, n.Tags) {
			return fmt.Errorf("SettledPromiseImmutable: promise %q tags changed", n.ID)
		}
		if o.TimeoutAt != n.TimeoutAt {
			return fmt.Errorf("SettledPromiseImmutable: promise %q timeoutAt changed from %d to %d", n.ID, o.TimeoutAt, n.TimeoutAt)
		}
		if o.CreatedAt != n.CreatedAt {
			return fmt.Errorf("SettledPromiseImmutable: promise %q createdAt changed from %d to %d", n.ID, o.CreatedAt, n.CreatedAt)
		}
	}
	return nil
}

// =============================================================================
// Section 6: Task Version (InvariantT)
// =============================================================================

// TaskNeverDeleted: every task that exists in old must still exist in new.
func TaskNeverDeleted(old, new core.DebugSnapResData) error {
	newKeys := make(map[string]bool, len(new.Tasks))
	for _, t := range new.Tasks {
		newKeys[taskKey(t)] = true
	}
	for _, t := range old.Tasks {
		if !newKeys[taskKey(t)] {
			return fmt.Errorf("TaskNeverDeleted: task %q existed in old snapshot but not in new", t.ID)
		}
	}
	return nil
}

// FulfilledTaskImmutable: once a task is observed as fulfilled, its version,
// PID, TTL, and state must remain unchanged in subsequent snapshots.
func FulfilledTaskImmutable(old, new core.DebugSnapResData) error {
	newByKey := make(map[string]core.TaskRecord, len(new.Tasks))
	for _, t := range new.Tasks {
		newByKey[taskKey(t)] = t
	}
	for _, o := range old.Tasks {
		if o.State != "fulfilled" {
			continue
		}
		n, ok := newByKey[taskKey(o)]
		if !ok {
			continue // covered by TaskNeverDeleted
		}
		if n.State != "fulfilled" {
			return fmt.Errorf("FulfilledTaskImmutable: task %q state changed from fulfilled to %q", o.ID, n.State)
		}
		if n.Version != o.Version {
			return fmt.Errorf("FulfilledTaskImmutable: task %q version changed from %d to %d", o.ID, o.Version, n.Version)
		}
		if n.PID != o.PID {
			return fmt.Errorf("FulfilledTaskImmutable: task %q pid changed from %q to %q", o.ID, o.PID, n.PID)
		}
		if !intPtrEqual(o.TTL, n.TTL) {
			return fmt.Errorf("FulfilledTaskImmutable: task %q ttl changed", o.ID)
		}
	}
	return nil
}

// NewTaskStartsAtVersion0: any task that is new in new (absent in old) must
// start at version 0 (pending, from task.acquire or implicit create) or
// version 1 (acquired, from task.create which atomically creates+acquires).
func NewTaskStartsAtVersion0(old, new core.DebugSnapResData) error {
	oldKeys := make(map[string]bool, len(old.Tasks))
	for _, t := range old.Tasks {
		oldKeys[taskKey(t)] = true
	}
	for _, t := range new.Tasks {
		if !oldKeys[taskKey(t)] {
			// v0 pending: created but not yet acquired.
			if t.Version == 0 {
				continue
			}
			// v1 acquired: task.create atomically creates and acquires.
			if t.Version == 1 && t.State == "acquired" {
				continue
			}
			return fmt.Errorf("NewTaskStartsAtVersion0: new task %q starts at version %d (state=%s), want 0 or 1-acquired",
				t.ID, t.Version, t.State)
		}
	}
	return nil
}

// TaskVersionMonotonic: task version must increase by at most 1 between snapshots.
func TaskVersionMonotonic(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]core.TaskRecord, len(old.Tasks))
	for _, t := range old.Tasks {
		oldByKey[taskKey(t)] = t
	}
	for _, t := range new.Tasks {
		o, ok := oldByKey[taskKey(t)]
		if !ok {
			continue
		}
		delta := t.Version - o.Version
		if delta < 0 {
			return fmt.Errorf("TaskVersionMonotonic: task %q version decreased from %d to %d", t.ID, o.Version, t.Version)
		}
		if delta > 1 {
			return fmt.Errorf("TaskVersionMonotonic: task %q version jumped from %d to %d (delta %d > 1)", t.ID, o.Version, t.Version, delta)
		}
	}
	return nil
}

// TaskResumeVersionUnchanged: a Suspended→Pending or Halted→Pending
// transition must leave the version unchanged (version bumps only on pending→acquired).
func TaskResumeVersionUnchanged(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]core.TaskRecord, len(old.Tasks))
	for _, t := range old.Tasks {
		oldByKey[taskKey(t)] = t
	}
	for _, t := range new.Tasks {
		o, ok := oldByKey[taskKey(t)]
		if !ok {
			continue
		}
		if (o.State == "suspended" || o.State == "halted") && t.State == "pending" {
			if t.Version != o.Version {
				return fmt.Errorf("TaskResumeVersionUnchanged: task %q transitioned %s→pending; version should be %d (unchanged), got %d",
					t.ID, o.State, o.Version, t.Version)
			}
		}
	}
	return nil
}

// TaskAcquireBumpsVersion: a Pending→Acquired transition must bump the task
// version by exactly 1.
func TaskAcquireBumpsVersion(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]core.TaskRecord, len(old.Tasks))
	for _, t := range old.Tasks {
		oldByKey[taskKey(t)] = t
	}
	for _, t := range new.Tasks {
		o, ok := oldByKey[taskKey(t)]
		if !ok {
			continue
		}
		if o.State == "pending" && t.State == "acquired" {
			if t.Version != o.Version+1 {
				return fmt.Errorf("TaskAcquireBumpsVersion: task %q transitioned pending→acquired; version should be %d, got %d",
					t.ID, o.Version+1, t.Version)
			}
		}
	}
	return nil
}

// SuspendPreservesVersion: an Acquired→Suspended transition must not change the
// task version.
func SuspendPreservesVersion(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]core.TaskRecord, len(old.Tasks))
	for _, t := range old.Tasks {
		oldByKey[taskKey(t)] = t
	}
	for _, t := range new.Tasks {
		o, ok := oldByKey[taskKey(t)]
		if !ok {
			continue
		}
		if o.State == "acquired" && t.State == "suspended" && t.Version != o.Version {
			return fmt.Errorf("SuspendPreservesVersion: task %q transitioned acquired→suspended; version should be %d (unchanged), got %d",
				t.ID, o.Version, t.Version)
		}
	}
	return nil
}

// HaltPreservesVersion: any transition into halted state must not change the
// task version.
func HaltPreservesVersion(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]core.TaskRecord, len(old.Tasks))
	for _, t := range old.Tasks {
		oldByKey[taskKey(t)] = t
	}
	for _, t := range new.Tasks {
		o, ok := oldByKey[taskKey(t)]
		if !ok {
			continue
		}
		if t.State == "halted" && o.State != "halted" && t.Version != o.Version {
			return fmt.Errorf("HaltPreservesVersion: task %q transitioned %s→halted; version should be %d (unchanged), got %d",
				t.ID, o.State, o.Version, t.Version)
		}
	}
	return nil
}

// ReleasePreservesVersion: an Acquired→Pending transition must not change the
// task version.
func ReleasePreservesVersion(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]core.TaskRecord, len(old.Tasks))
	for _, t := range old.Tasks {
		oldByKey[taskKey(t)] = t
	}
	for _, t := range new.Tasks {
		o, ok := oldByKey[taskKey(t)]
		if !ok {
			continue
		}
		if o.State == "acquired" && t.State == "pending" && t.Version != o.Version {
			return fmt.Errorf("ReleasePreservesVersion: task %q transitioned acquired→pending; version should be %d (unchanged), got %d",
				t.ID, o.Version, t.Version)
		}
	}
	return nil
}

// =============================================================================
// Section 3 additions: Promise Immutability (InvariantT)
// =============================================================================

// PromiseTimeoutAtImmutable: timeoutAt must not change for any promise present
// in both snapshots.
func PromiseTimeoutAtImmutable(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]int64, len(old.Promises))
	for _, p := range old.Promises {
		oldByKey[promiseKey(p)] = p.TimeoutAt
	}
	for _, p := range new.Promises {
		if oldTimeoutAt, ok := oldByKey[promiseKey(p)]; ok && p.TimeoutAt != oldTimeoutAt {
			return fmt.Errorf("PromiseTimeoutAtImmutable: promise %q timeoutAt changed from %d to %d", p.ID, oldTimeoutAt, p.TimeoutAt)
		}
	}
	return nil
}

// =============================================================================
// Section 9: Schedules (InvariantT)
// =============================================================================

// ScheduleNextRunAtMonotonic: nextRunAt must never decrease between snapshots.
func ScheduleNextRunAtMonotonic(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]int64, len(old.Schedules))
	for _, s := range old.Schedules {
		oldByKey[scheduleKey(s)] = s.NextRunAt
	}
	for _, s := range new.Schedules {
		if oldNextRunAt, ok := oldByKey[scheduleKey(s)]; ok && s.NextRunAt < oldNextRunAt {
			return fmt.Errorf("ScheduleNextRunAtMonotonic: schedule %q nextRunAt decreased from %d to %d", s.ID, oldNextRunAt, s.NextRunAt)
		}
	}
	return nil
}

// ScheduleLastRunAtMonotonic: lastRunAt must never decrease or regress to nil
// between snapshots.
func ScheduleLastRunAtMonotonic(old, new core.DebugSnapResData) error {
	oldByKey := make(map[string]*int64, len(old.Schedules))
	for i := range old.Schedules {
		oldByKey[scheduleKey(old.Schedules[i])] = old.Schedules[i].LastRunAt
	}
	for _, s := range new.Schedules {
		oldLastRunAt, ok := oldByKey[scheduleKey(s)]
		if !ok {
			continue
		}
		if oldLastRunAt != nil && s.LastRunAt == nil {
			return fmt.Errorf("ScheduleLastRunAtMonotonic: schedule %q lastRunAt regressed to nil (was %d)", s.ID, *oldLastRunAt)
		}
		if oldLastRunAt != nil && s.LastRunAt != nil && *s.LastRunAt < *oldLastRunAt {
			return fmt.Errorf("ScheduleLastRunAtMonotonic: schedule %q lastRunAt decreased from %d to %d", s.ID, *oldLastRunAt, *s.LastRunAt)
		}
	}
	return nil
}
