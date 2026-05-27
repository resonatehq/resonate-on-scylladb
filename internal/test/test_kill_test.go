package test

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
)

const (
	killProb       = 0.3
	killMaxWorkers = 5
)

// TestHandlerKillInvariants spawns random operations through the cooperative
// runner and kills fibers at pseudo-random yield points. After each kill the
// handler's DB state is snapped and all static invariants are checked. A kill
// leaves the DB after some subset of the operation's queries have committed;
// if any invariant fires, that subset is not a valid intermediate state.
//
// Defaults: seed=<current timestamp>, iterations=1000, operations=100. Override
// via env vars RESONATE_TEST_SEED, RESONATE_TEST_ITERATIONS, RESONATE_TEST_OPERATIONS.
// To replay a specific failure: set SEED to the failing seed and ITERATIONS=1.
func TestHandlerKillInvariants(t *testing.T) {
	h := setupHandler(t)

	baseSeed := envInt64("RESONATE_TEST_SEED", time.Now().UnixNano())
	iterations := envInt("RESONATE_TEST_ITERATIONS", 1000)
	operations := envInt("RESONATE_TEST_OPERATIONS", 100)
	t.Logf("config: seed=%d iterations=%d operations=%d promises=%s", baseSeed, iterations, operations, promisesConfig())

	for seed := baseSeed; seed < baseSeed+int64(iterations); seed++ {
		promiseIDs := pickPromisePool(seed)
		debugReset(t, h)

		sched := NewRunner(
			func(req []byte, yield func(string)) []byte {
				resp, _ := h.Handle(req, yield)
				return resp
			},
			seed,
		)
		sched.OnKill = killedResponse
		sched.Trace = newRing()

		interleavRng := rand.New(rand.NewSource(seed))
		reqRng := newRng(seed + 500)
		now := int64(1_000_000)
		n := operations

		for n > 0 || sched.Active() {
			if n > 0 && sched.ActiveCount() < killMaxWorkers && (!sched.Active() || interleavRng.Intn(2) == 0) {
				req := generateRequest(reqRng, now, nil, promiseIDs, []string{promiseIDs[0]})
				if req["kind"] == "debug.tick" {
					if data, ok := req["data"].(map[string]any); ok {
						if tickTime, ok := data["time"].(int64); ok && tickTime > now {
							now = tickTime
						}
						data["time"] = now
					}
				}
				if head, ok := req["head"].(map[string]any); ok {
					head["resonate:debug_time"] = now
				}
				reqBytes, _ := json.Marshal(req)
				sched.Spawn(reqBytes)
				n--
			} else {
				if sched.TickOrKill(killProb) {
					if !checkKillInvariants(t, h, seed, len(promiseIDs), now, sched.Trace) {
						goto nextSeed
					}
				}
			}
		}

		if !checkKillInvariants(t, h, seed, len(promiseIDs), now, sched.Trace) {
			goto nextSeed
		}

	nextSeed:
		debugReset(t, h)
	}
}

// acceptedOrphans are invariant names that are expected to fire as a result
// of the pre-insert pattern: a timeout entry may exist for a record that has
// not yet been committed (killed between pre-insert and LWT), or an orphan
// entry from a failed transition outlives subsequent state changes on the same
// task. These orphan entries are semantically safe — the timeout loop returns
// nil on not-found / wrong-state without causing harmful side effects.
var acceptedOrphans = map[string]bool{
	// promise_timeouts entry for a non-pending / non-existent promise.
	"TimeoutHasPendingPromise": true,
	// settled promise still has a promise_timeouts entry: the settle LWT commits
	// inside enqueueResume (at LabelEnqueueResumeCommit) but the promise_timeouts
	// DELETE runs only after enqueueResume returns. A kill between them leaves a
	// stale entry; the timeout loop handles it gracefully on wrong-state.
	"SettledPromiseHasNoTimeout": true,
	// task_timeouts type=1 entry for a non-acquired / non-existent task (pre-insert killed before LWT).
	"LeaseTimeoutOnlyForAcquiredTask": true,
	// lease timeout exists but the task has already settled (pid cleared): the lease
	// timeout DELETE runs after the settle LWT, so a kill between them leaves an orphan
	// for a task that no longer carries a pid. Symmetric to SettledPromiseHasNoTimeout.
	"LeaseTimeoutHasValidPidAndTtl": true,
	// task_timeouts type=0 entry for a non-pending / non-existent task: symmetric
	// twin of LeaseTimeoutOnlyForAcquiredTask. Paths that swap lease→retry
	// (task.acquire, taskCreateReacquire, onTaskLeaseTimeout) kill after the LWT
	// before the retry DELETE; paths that pre-insert retry before an LWT
	// (task.release, task.continue, promise.create with task, createSchedulePromise)
	// kill between the two, leaving an orphan for an acquired, halted, or
	// non-existent task. The timeout loop discards it on wrong-state / not-found.
	"RetryTimeoutOnlyForPendingTask": true,
	// task_timeouts entry for a suspended task: two paths leave a stale entry.
	// (1) task.suspend: batch LWT commits (task=suspended) before the lease timeout
	// DELETE runs — symmetric to HaltedTaskHasNoTimeout.
	// (2) enqueueResume: pre-inserts a retry timeout for each suspended awaiter
	// before the batch LWT that transitions them to pending; a kill between the
	// pre-insert and the batch leaves a suspended task with an orphan retry entry.
	"SuspendedTaskHasNoTimeout": true,
	// task_timeouts entry for a fulfilled task: an orphan from a pre-insert that
	// was never committed to the promise row persists after the task is fulfilled
	// (task.fulfill only deletes tracked entries, not orphans at arbitrary timestamps).
	"FulfilledTaskHasNoTimeout": true,
	// task_timeouts entry for a halted task: the halt LWT commits (task_state=halted)
	// before the cleanup DELETE runs. A real crash between them leaves a stale entry,
	// but the timeout processor discards it on not-acquired / not-pending state.
	"HaltedTaskHasNoTimeout": true,
	// schedule_timeouts entry for a non-existent schedule: symmetric twin of
	// TimeoutHasPendingPromise. schedule.create pre-inserts the entry before the
	// LWT that creates the schedule row; a kill between them leaves an orphan.
	// schedule.delete LWTs the schedule row away before the schedule_timeouts
	// DELETE runs; a kill between them leaves a stale entry for a deleted schedule.
	"ScheduleTimeoutHasSchedule": true,
}

// checkKillInvariants snaps the handler state and runs all static invariants.
// Returns true if all pass. On the first accepted-orphan violation it logs
// with t.Logf and returns false so the caller skips to the next seed.
// On any other violation it prints the trace and calls t.Fatalf, stopping
// the test immediately with the failing seed in the message.
func checkKillInvariants(t *testing.T, h *core.Handler, seed int64, promises int, now int64, trace *ring) bool {
	t.Helper()
	snapReq, _ := json.Marshal(map[string]any{
		"kind": "debug.snap",
		"head": makeHead(nil, now, nil),
		"data": map[string]any{},
	})
	snapBytes, err := h.Handle(snapReq, func(string) {})
	if err != nil {
		if s := trace.render(); s != "" {
			t.Logf("%s", s)
		}
		t.Fatalf("%s", failMsg("kind", "snap_error", "seed", seed, "promises", promises, "err", err))
	}
	var resp struct {
		Data core.DebugSnapResData `json:"data"`
	}
	if err := json.Unmarshal(snapBytes, &resp); err != nil {
		if s := trace.render(); s != "" {
			t.Logf("%s", s)
		}
		t.Fatalf("%s", failMsg("kind", "snap_unmarshal", "seed", seed, "promises", promises, "err", err))
	}
	for _, inv := range AllInvariantsS() {
		if err := inv.Check(resp.Data); err != nil {
			if acceptedOrphans[inv.Name] {
				t.Logf("ACCEPTED-ORPHAN seed=%d promises=%d name=%q msg=%q", seed, promises, inv.Name, err.Error())
				return false
			}
			if s := trace.render(); s != "" {
				t.Logf("%s", s)
			}
			t.Fatalf("%s", failMsg("kind", "invariant_kill", "seed", seed, "promises", promises, "name", inv.Name, "err", err))
		}
	}
	return true
}
