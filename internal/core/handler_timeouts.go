package core

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"log"
	"sort"

	"github.com/gocql/gocql"
)

// resumeAwaiter carries the fields needed to call sendExecute for a suspended
// awaiter that is now pending after a settle.
type resumeAwaiter struct {
	target      string
	id          string
	taskVersion int
}

// settledData carries the settled state returned from enqueueResume and tryTimeout.
type settledData struct {
	State     string
	ValHdrs   map[string]string
	ValData   string
	SettledAt int64
}

// RetryTimeout is the delay between a task retry and the next execute message: 30 seconds in ms.
const RetryTimeout = int64(30_000)

// BucketFor returns the bucket identifier for timestamp t under the handler's
// configured BucketWidth.
func (h *Handler) BucketFor(t int64) int64 {
	return t / h.BucketWidth
}

// shardFor returns a stable shard index in [0, numShards) for the given id
// using FNV-32a. Returns 0 when Shards is 0 or 1.
func (h *Handler) shardFor(id string) int16 {
	if h.Shards <= 1 {
		return 0
	}
	f := fnv.New32a()
	f.Write([]byte(id))
	return int16(f.Sum32() % uint32(h.Shards))
}

// bucketsToScan returns the bucket IDs TickAt should query for due-by-now
// timeouts: the current bucket plus h.BucketLookback past buckets, clamped at 0.
func (h *Handler) bucketsToScan(t int64) []int64 {
	cur := h.BucketFor(t)
	start := cur - int64(h.BucketLookback)
	if start < 0 {
		start = 0
	}
	out := make([]int64, 0, cur-start+1)
	for b := start; b <= cur; b++ {
		out = append(out, b)
	}
	return out
}

// TickPromiseTimeoutsAt scans promise_timeouts for a single shard and fires
// all handlers with timeout_at ≤ t. Checks ctx.Done() between rows so the
// coordinator can cancel a goroutine cleanly during a shard rebalance.
func (h *Handler) TickPromiseTimeoutsAt(ctx context.Context, t int64, shard int16, yield func(string)) {
	for _, b := range h.bucketsToScan(t) {
		iter := h.Session.Query(
			`SELECT timeout_at, promise_id, origin FROM promise_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at <= ?`,
			b, shard, t,
		).Iter()
		yield(LabelPromiseTimeoutScanPromiseTimeouts)

		var ptTimeout int64
		var ptID, ptOrigin string
		for iter.Scan(&ptTimeout, &ptID, &ptOrigin) {
			yield(LabelPromiseTimeoutScanRowPromiseTimeouts)
			select {
			case <-ctx.Done():
				iter.Close()
				return
			default:
			}
			if err := h.onPromiseTimeout(ptOrigin, ptID, ptTimeout, yield); err != nil {
				log.Printf("TickPromiseTimeoutsAt shard %d: onPromiseTimeout(%s): %v", shard, ptID, err)
			}
		}
		if err := iter.Close(); err != nil {
			log.Printf("TickPromiseTimeoutsAt: promise_timeouts scan bucket %d shard %d: %v", b, shard, err)
		}
	}
}

// TickTaskTimeoutsAt scans task_timeouts for a single shard and fires all
// handlers with timeout_at ≤ t. Checks ctx.Done() between rows.
func (h *Handler) TickTaskTimeoutsAt(ctx context.Context, t int64, shard int16, yield func(string)) {
	for _, b := range h.bucketsToScan(t) {
		iter := h.Session.Query(
			`SELECT timeout_at, timeout_type, task_id, origin, promise_timeout_at FROM task_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at <= ?`,
			b, shard, t,
		).Iter()
		yield(LabelTaskTimeoutScanTaskTimeouts)

		var ttTimeout int64
		var ttType int8
		var ttID, ttOrigin string
		var ttPromiseTimeoutAt int64
		for iter.Scan(&ttTimeout, &ttType, &ttID, &ttOrigin, &ttPromiseTimeoutAt) {
			yield(LabelTaskTimeoutScanRowTaskTimeouts)
			select {
			case <-ctx.Done():
				iter.Close()
				return
			default:
			}
			if ttOrigin == "" {
				ttOrigin = ttID
			}
			if ttType == 0 {
				if err := h.onTaskRetryTimeout(ttOrigin, ttID, ttTimeout, ttPromiseTimeoutAt, t, yield); err != nil {
					log.Printf("TickTaskTimeoutsAt shard %d: onTaskRetryTimeout(%s): %v", shard, ttID, err)
				}
			} else {
				if err := h.onTaskLeaseTimeout(ttOrigin, ttID, ttTimeout, ttPromiseTimeoutAt, t, yield); err != nil {
					log.Printf("TickTaskTimeoutsAt shard %d: onTaskLeaseTimeout(%s): %v", shard, ttID, err)
				}
			}
		}
		if err := iter.Close(); err != nil {
			log.Printf("TickTaskTimeoutsAt: task_timeouts scan bucket %d shard %d: %v", b, shard, err)
		}
	}
}

// TickScheduleTimeoutsAt scans schedule_timeouts for a single shard and fires
// all handlers with timeout_at ≤ t. Checks ctx.Done() between rows.
func (h *Handler) TickScheduleTimeoutsAt(ctx context.Context, t int64, shard int16, yield func(string)) {
	for _, b := range h.bucketsToScan(t) {
		iter := h.Session.Query(
			`SELECT timeout_at, schedule_id, origin, create_token FROM schedule_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at <= ?`,
			b, shard, t,
		).Iter()
		yield(LabelScheduleTimeoutScanScheduleTimeouts)

		var stTimeout int64
		var stID, stOrigin string
		var stToken gocql.UUID
		for iter.Scan(&stTimeout, &stID, &stOrigin, &stToken) {
			yield(LabelScheduleTimeoutScanRowScheduleTimeouts)
			select {
			case <-ctx.Done():
				iter.Close()
				return
			default:
			}
			if stOrigin == "" {
				stOrigin = stID
			}
			if err := h.onScheduleTimeout(stOrigin, stID, stTimeout, stToken, t, yield); err != nil {
				log.Printf("TickScheduleTimeoutsAt shard %d: onScheduleTimeout(%s): %v", shard, stID, err)
			}
		}
		if err := iter.Close(); err != nil {
			log.Printf("TickScheduleTimeoutsAt: schedule_timeouts scan bucket %d shard %d: %v", b, shard, err)
		}
	}
}

// DebugTick fires all expired timeout handlers for entries with timeout_at ≤ req.Time.
// It uses full-table scans with ALLOW FILTERING instead of the bucketed path so that
// arbitrary test timestamps (e.g. year 2050) do not generate millions of CQL queries.
func (h *Handler) DebugTick(head RequestHead, req DebugTickData, now int64, yield func(string)) Res[[]DebugTickAction] {
	h.debugTickAt(req.Time, yield)
	if req.Time > h.maxDebugTick {
		h.maxDebugTick = req.Time
	}
	return Res[[]DebugTickAction]{
		Kind: "debug.tick",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: []DebugTickAction{},
	}
}

// debugTickAt is the debug-only tick implementation that avoids the bucketed scan.
// It performs full-table ALLOW FILTERING queries, which is fine for test workloads.
//
// All three timeout tables are collected-then-sorted before processing because
// ALLOW FILTERING returns rows in partition-token order. Token assignment is
// chosen randomly when a ScyllaDB cluster first starts, so without sorting the
// processing order would differ across docker compose runs even with identical
// data — breaking the seed-based reproducibility contract.
func (h *Handler) debugTickAt(t int64, yield func(string)) {
	// ── promise_timeouts ───────────────────────────────────────────────────────
	// Sort by (timeout_at, promise_id) for deterministic processing.
	{
		type ptEntry struct {
			timeoutAt int64
			origin    string
			id        string
		}
		var due []ptEntry

		iter := h.Session.Query(
			`SELECT timeout_at, promise_id, origin FROM promise_timeouts WHERE timeout_at <= ? ALLOW FILTERING`,
			t,
		).Iter()
		yield(LabelPromiseTimeoutScanPromiseTimeoutsDebug)
		var ptTimeout int64
		var ptID string
		var ptOrigin string
		for iter.Scan(&ptTimeout, &ptID, &ptOrigin) {
			yield(LabelPromiseTimeoutScanRowPromiseTimeoutsDebug)
			due = append(due, ptEntry{ptTimeout, ptOrigin, ptID})
		}
		if err := iter.Close(); err != nil {
			log.Printf("debugTickAt: promise_timeouts scan: %v", err)
		}

		sort.Slice(due, func(i, j int) bool {
			if due[i].timeoutAt != due[j].timeoutAt {
				return due[i].timeoutAt < due[j].timeoutAt
			}
			return due[i].id < due[j].id
		})

		for _, e := range due {
			if err := h.onPromiseTimeout(e.origin, e.id, e.timeoutAt, yield); err != nil {
				log.Printf("debugTickAt: onPromiseTimeout(%s): %v", e.id, err)
			}
		}
	}

	// ── task_timeouts ──────────────────────────────────────────────────────────
	// Sort by (timeout_at, timeout_type, task_id) for deterministic processing.
	{
		type ttEntry struct {
			timeoutAt        int64
			ttype            int8
			id               string
			origin           string
			promiseTimeoutAt int64
		}
		var due []ttEntry

		iter := h.Session.Query(
			`SELECT timeout_at, timeout_type, task_id, origin, promise_timeout_at FROM task_timeouts WHERE timeout_at <= ? ALLOW FILTERING`,
			t,
		).Iter()
		yield(LabelTaskTimeoutScanTaskTimeoutsDebug)
		var ttTimeout int64
		var ttType int8
		var ttID string
		var ttOrigin string
		var ttPromiseTimeoutAt int64
		for iter.Scan(&ttTimeout, &ttType, &ttID, &ttOrigin, &ttPromiseTimeoutAt) {
			yield(LabelTaskTimeoutScanRowTaskTimeoutsDebug)
			if ttOrigin == "" {
				ttOrigin = ttID
			}
			due = append(due, ttEntry{ttTimeout, ttType, ttID, ttOrigin, ttPromiseTimeoutAt})
		}
		if err := iter.Close(); err != nil {
			log.Printf("debugTickAt: task_timeouts scan: %v", err)
		}

		sort.Slice(due, func(i, j int) bool {
			if due[i].timeoutAt != due[j].timeoutAt {
				return due[i].timeoutAt < due[j].timeoutAt
			}
			if due[i].ttype != due[j].ttype {
				return due[i].ttype < due[j].ttype
			}
			return due[i].id < due[j].id
		})

		for _, e := range due {
			if e.ttype == 0 {
				if err := h.onTaskRetryTimeout(e.origin, e.id, e.timeoutAt, e.promiseTimeoutAt, t, yield); err != nil {
					log.Printf("debugTickAt: onTaskRetryTimeout(%s): %v", e.id, err)
				}
			} else {
				if err := h.onTaskLeaseTimeout(e.origin, e.id, e.timeoutAt, e.promiseTimeoutAt, t, yield); err != nil {
					log.Printf("debugTickAt: onTaskLeaseTimeout(%s): %v", e.id, err)
				}
			}
		}
	}

	// ── schedule_timeouts ──────────────────────────────────────────────────────
	// Collect into a slice and sort by (timeout_at, schedule_id, token) so
	// that processing order is deterministic regardless of ScyllaDB row order.
	{
		type schedEntry struct {
			timeoutAt  int64
			scheduleID string
			origin     string
			token      gocql.UUID
		}
		var due []schedEntry

		iter := h.Session.Query(
			`SELECT timeout_at, schedule_id, origin, create_token FROM schedule_timeouts WHERE timeout_at <= ? ALLOW FILTERING`,
			t,
		).Iter()
		yield(LabelScheduleTimeoutScanScheduleTimeoutsDebug)
		var stTimeout int64
		var stID string
		var stOrigin string
		var stToken gocql.UUID
		for iter.Scan(&stTimeout, &stID, &stOrigin, &stToken) {
			yield(LabelScheduleTimeoutScanRowScheduleTimeoutsDebug)
			if stOrigin == "" {
				stOrigin = stID
			}
			due = append(due, schedEntry{stTimeout, stID, stOrigin, stToken})
		}
		if err := iter.Close(); err != nil {
			log.Printf("debugTickAt: schedule_timeouts scan: %v", err)
		}

		sort.Slice(due, func(i, j int) bool {
			if due[i].timeoutAt != due[j].timeoutAt {
				return due[i].timeoutAt < due[j].timeoutAt
			}
			if due[i].scheduleID != due[j].scheduleID {
				return due[i].scheduleID < due[j].scheduleID
			}
			return due[i].token.String() < due[j].token.String()
		})

		for _, e := range due {
			if err := h.onScheduleTimeout(e.origin, e.scheduleID, e.timeoutAt, e.token, t, yield); err != nil {
				log.Printf("debugTickAt: onScheduleTimeout(%s): %v", e.scheduleID, err)
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// onPromiseTimeout
// ─────────────────────────────────────────────────────────────────────────────

// promiseTimeoutInput holds all fields needed to eagerly settle a timed-out promise.
type promiseTimeoutInput struct {
	Origin     string
	ID         string
	Tags       map[string]string
	Target     string
	TimeoutAt  int64
	CreatedAt  int64
	TaskState  string
	TaskTRetry *int64
	TaskTLease *int64
	Listeners  []string
	Callbacks  []string
}

// onPromiseTimeout settles a pending promise that has reached its deadline.
// Uses tryTimeout to atomically settle the promise and notify awaiters.
func (h *Handler) onPromiseTimeout(origin string, id string, timeoutAt int64, yield func(string)) error {
	// Read all columns we need for side effects before the batch.
	var (
		state            string
		tags             map[string]string
		target           string
		createdAt        int64
		promiseTimeoutAt int64
		taskState        string
		taskTimeoutRetry *int64
		taskTimeoutLease *int64
		listeners        []string
		callbacks        []string
	)

	err := h.Session.Query(
		`SELECT state, tags, target, created_at, timeout_at, task_state,
		        task_timeout_retry, task_timeout_lease,
		        listeners, callbacks
		 FROM promises WHERE origin = ? AND id = ?`,
		origin, id,
	).Scan(
		&state, &tags, &target, &createdAt, &promiseTimeoutAt, &taskState,
		&taskTimeoutRetry, &taskTimeoutLease,
		&listeners, &callbacks,
	)
	yield(LabelPromiseTimeoutRead)
	if err == gocql.ErrNotFound {
		// Stale — explicitly delete promise_timeouts entry.
		if delErr := h.Session.Query(
			`DELETE FROM promise_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?`,
			h.BucketFor(timeoutAt), h.shardFor(id), timeoutAt, origin, id,
		).Exec(); delErr != nil {
			log.Printf("onPromiseTimeout: delete promise_timeouts(%s): %v", id, delErr)
		}
		yield(LabelPromiseTimeoutCleanupPromiseTimeouts)
		return nil
	}
	if err != nil {
		return err
	}
	if state != "pending" {
		// Already settled — delete promise_timeouts entry.
		if delErr := h.Session.Query(
			`DELETE FROM promise_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?`,
			h.BucketFor(timeoutAt), h.shardFor(id), timeoutAt, origin, id,
		).Exec(); delErr != nil {
			log.Printf("onPromiseTimeout: delete promise_timeouts(%s): %v", id, delErr)
		}
		yield(LabelPromiseTimeoutCleanupPromiseTimeouts)
		return nil
	}
	if promiseTimeoutAt != timeoutAt {
		// Orphan — delete promise_timeouts entry.
		if delErr := h.Session.Query(
			`DELETE FROM promise_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?`,
			h.BucketFor(timeoutAt), h.shardFor(id), timeoutAt, origin, id,
		).Exec(); delErr != nil {
			log.Printf("onPromiseTimeout: delete promise_timeouts(%s): %v", id, delErr)
		}
		yield(LabelPromiseTimeoutCleanupPromiseTimeouts)
		return nil
	}

	if tags == nil {
		tags = map[string]string{}
	}

	in := promiseTimeoutInput{
		Origin:     origin,
		ID:         id,
		Tags:       tags,
		Target:     target,
		TimeoutAt:  timeoutAt,
		CreatedAt:  createdAt,
		TaskState:  taskState,
		TaskTRetry: taskTimeoutRetry,
		TaskTLease: taskTimeoutLease,
		Listeners:  listeners,
		Callbacks:  callbacks,
	}
	_, err = h.tryTimeout(in, timeoutAt, yield)
	return err
}

// ─────────────────────────────────────────────────────────────────────────────
// enqueueResume
// ─────────────────────────────────────────────────────────────────────────────

// enqueueResume atomically settles a promise and enqueues resume signals for
// all suspended awaiters. It does NOT send execute or unblock messages — the
// caller is responsible for those after a successful return.
//
// Returns (awaiters, intended, nil) on success — awaiters is non-nil.
// Returns (nil, concurrentSD, nil) when the promise was already settled by a concurrent writer — awaiters is nil.
// Returns (nil, settledData{}, err) on CQL error or concurrent task modification.
func (h *Handler) enqueueResume(
	settledID string,
	origin string,
	callbacks []string,
	now int64,
	settleStmt string,
	settleArgs []interface{},
	intended settledData,
	yield func(string),
) ([]resumeAwaiter, settledData, error) {
	type awaiterInfo struct {
		id          string
		state       string
		timeoutAt   int64
		taskState   string
		taskVersion int
		target      string
	}
	type preinsertEntry struct {
		awaiterID string
		retryAt   int64
	}

	// 1. Read awaiter state for each callback ID.
	awaiters := make([]awaiterInfo, 0, len(callbacks))
	for _, cbID := range callbacks {
		var (
			aState       string
			aTimeoutAt   int64
			aTaskState   string
			aTaskVersion int
			aTarget      string
		)
		err := h.Session.Query(
			`SELECT state, timeout_at, task_state, task_version, target
			 FROM promises WHERE origin = ? AND id = ?`,
			origin, cbID,
		).Scan(&aState, &aTimeoutAt, &aTaskState, &aTaskVersion, &aTarget)
		yield(LabelEnqueueResumeReadAwaiters)
		if err == gocql.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, settledData{}, err
		}
		awaiters = append(awaiters, awaiterInfo{
			id:          cbID,
			state:       aState,
			timeoutAt:   aTimeoutAt,
			taskState:   aTaskState,
			taskVersion: aTaskVersion,
			target:      aTarget,
		})
	}

	// 2. Pre-insert retry timeouts for suspended awaiters.
	var preinserts []preinsertEntry

	// rollback deletes successfully pre-inserted task_timeouts entries on failure.
	rollback := func() {
		for _, p := range preinserts {
			if err := h.Session.Query(
				`DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
				h.BucketFor(p.retryAt), h.shardFor(p.awaiterID), p.retryAt, origin, p.awaiterID,
			).Exec(); err != nil {
				log.Printf("enqueueResume: rollback task_timeouts(%s): %v", p.awaiterID, err)
			}
			yield(LabelEnqueueResumeRollbackTaskTimeoutsRetry)
		}
	}

	for _, a := range awaiters {
		if a.taskState != "suspended" {
			continue
		}
		retryAt := now + RetryTimeout
		if err := h.Session.Query(
			`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)`,
			h.BucketFor(retryAt), h.shardFor(a.id), retryAt, a.id, origin, a.timeoutAt,
		).Exec(); err != nil {
			rollback()
			return nil, settledData{}, err
		}
		yield(LabelEnqueueResumePreinsertTaskTimeoutsRetry)
		preinserts = append(preinserts, preinsertEntry{awaiterID: a.id, retryAt: retryAt})
	}

	// 3. Build LoggedBatch.
	batch := h.Session.NewBatch(gocql.LoggedBatch)
	// First statement: the caller-supplied settle.
	batch.Query(settleStmt, settleArgs...)

	// One UPDATE per awaiter.
	for _, a := range awaiters {
		retryAt := now + RetryTimeout

		switch a.taskState {
		case "fulfilled":
			// no statement

		case "suspended":
			batch.Query(
				`UPDATE promises SET task_state = 'pending', task_resumes = ?, task_timeout_retry = ?
				 WHERE origin = ? AND id = ?
				 IF task_state = 'suspended'`,
				[]string{settledID}, retryAt, origin, a.id,
			)

		case "halted":
			batch.Query(
				`UPDATE promises SET task_resumes = task_resumes + ?
				 WHERE origin = ? AND id = ?
				 IF task_state = 'halted'`,
				[]string{settledID}, origin, a.id,
			)

		case "pending":
			batch.Query(
				`UPDATE promises SET task_resumes = task_resumes + ?
				 WHERE origin = ? AND id = ?
				 IF task_state = 'pending'`,
				[]string{settledID}, origin, a.id,
			)

		case "acquired":
			batch.Query(
				`UPDATE promises SET task_resumes = task_resumes + ?
				 WHERE origin = ? AND id = ?
				 IF task_state = 'acquired'`,
				[]string{settledID}, origin, a.id,
			)
		}
	}

	// 4. Execute the batch.
	batchRow := make(map[string]interface{})
	applied, batchIter, err := h.Session.MapExecuteBatchCAS(batch, batchRow)
	if batchIter != nil {
		batchIter.Close()
	}
	yield(LabelEnqueueResumeCommit)

	// 5. On CQL error: rollback and return error.
	if err != nil {
		rollback()
		return nil, settledData{}, err
	}

	// 6. On !applied: rollback and distinguish concurrent-settle vs concurrent-modify.
	if !applied {
		rollback()
		promState, _ := batchRow["state"].(string)
		if promState != "pending" {
			// Extract settled state from batchRow.
			valHdrs, _ := batchRow["value_headers"].(map[string]string)
			valData, _ := batchRow["value_data"].(string)
			settledAtVal, _ := batchRow["settled_at"].(int64)
			return nil, settledData{State: promState, ValHdrs: valHdrs, ValData: valData, SettledAt: settledAtVal}, nil
		}
		return nil, settledData{}, errors.New("concurrent modification; please retry")
	}

	// 7. On success: collect suspended awaiters for the caller to send execute messages.
	resumeAwaiters := make([]resumeAwaiter, 0)
	for _, a := range awaiters {
		if a.taskState == "suspended" {
			resumeAwaiters = append(resumeAwaiters, resumeAwaiter{
				target:      a.target,
				id:          a.id,
				taskVersion: a.taskVersion,
			})
		}
	}

	return resumeAwaiters, intended, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// tryTimeout
// ─────────────────────────────────────────────────────────────────────────────

// tryTimeout eagerly settles a pending promise that has logically expired.
// It builds the settle statement and calls enqueueResume, then on a win sends
// execute/unblock messages and performs auxiliary timeout-entry cleanup.
//
// Returns the settledData (either from this call's win or from a concurrent settle).
func (h *Handler) tryTimeout(in promiseTimeoutInput, now int64, yield func(string)) (settledData, error) {
	newState := "rejected_timedout"
	if in.Tags["resonate:timer"] == "true" {
		newState = "resolved"
	}

	var settleStmt string
	var settleArgs []interface{}
	if in.Target != "" {
		settleStmt = `UPDATE promises SET
		     state = ?, settled_at = ?,
		     value_headers = null, value_data = null,
		     callbacks = {}, listeners = {},
		     task_state = 'fulfilled', task_pid = null, task_ttl = null,
		     task_timeout_retry = null, task_timeout_lease = null,
		     task_resumes = {}
		 WHERE origin = ? AND id = ?
		 IF state = 'pending' AND callbacks = ?
		 AND settled_at = null AND value_headers = null AND value_data = null`
		settleArgs = []interface{}{newState, in.TimeoutAt, in.Origin, in.ID, in.Callbacks}
	} else {
		settleStmt = `UPDATE promises SET
		     state = ?, settled_at = ?,
		     value_headers = null, value_data = null,
		     callbacks = {}, listeners = {}
		 WHERE origin = ? AND id = ?
		 IF state = 'pending' AND callbacks = ?
		 AND settled_at = null AND value_headers = null AND value_data = null`
		settleArgs = []interface{}{newState, in.TimeoutAt, in.Origin, in.ID, in.Callbacks}
	}

	settledAtCopy := in.TimeoutAt
	unblockRec := PromiseRecord{
		ID:        in.ID,
		State:     newState,
		Tags:      in.Tags,
		TimeoutAt: in.TimeoutAt,
		CreatedAt: in.CreatedAt,
		SettledAt: &settledAtCopy,
	}

	intended := settledData{State: newState, SettledAt: in.TimeoutAt}
	awaiters, sd, err := h.enqueueResume(
		in.ID, in.Origin, in.Callbacks, now,
		settleStmt, settleArgs, intended, yield)
	if err != nil {
		return settledData{}, err
	}

	for _, a := range awaiters {
		h.sendExecute(a.target, a.id, a.taskVersion)
	}
	if awaiters != nil {
		h.sendUnblock(in.Listeners, unblockRec)
		// Async cleanup on win.
		if delErr := h.Session.Query(
			`DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?`,
			h.BucketFor(in.TimeoutAt), h.shardFor(in.ID), in.TimeoutAt, in.Origin, in.ID,
		).Exec(); delErr != nil {
			log.Printf("tryTimeout: delete promise_timeouts(%s): %v", in.ID, delErr)
		}
		yield(LabelPromiseTimeoutCleanupPromiseTimeouts)
		if in.Target != "" {
			if in.TaskTRetry != nil {
				if delErr := h.Session.Query(
					`DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
					h.BucketFor(*in.TaskTRetry), h.shardFor(in.ID), *in.TaskTRetry, in.Origin, in.ID,
				).Exec(); delErr != nil {
					log.Printf("tryTimeout: delete retry timeout(%s): %v", in.ID, delErr)
				}
				yield(LabelPromiseTimeoutCleanupTaskTimeoutsRetry)
			}
			if in.TaskTLease != nil {
				if delErr := h.Session.Query(
					`DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
					h.BucketFor(*in.TaskTLease), h.shardFor(in.ID), *in.TaskTLease, in.Origin, in.ID,
				).Exec(); delErr != nil {
					log.Printf("tryTimeout: delete lease timeout(%s): %v", in.ID, delErr)
				}
				yield(LabelPromiseTimeoutCleanupTaskTimeoutsLease)
			}
		}
	}
	return sd, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// onTaskRetryTimeout
// ─────────────────────────────────────────────────────────────────────────────

// onTaskRetryTimeout re-enqueues a pending task for execution.
func (h *Handler) onTaskRetryTimeout(origin string, id string, timeoutAt int64, promiseTimeoutAt int64, now int64, yield func(string)) (err error) {
	var taskState string
	var taskVersion int
	var target string
	var promiseTAt int64
	var taskTimeoutRetry *int64
	var taskTimeoutLease *int64

	defer func() {
		if err == nil {
			if delErr := h.Session.Query(
				`DELETE FROM task_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
				h.BucketFor(timeoutAt), h.shardFor(id), timeoutAt, origin, id,
			).Exec(); delErr != nil {
				log.Printf("onTaskRetryTimeout: delete old timeout(%s): %v", id, delErr)
			}
			yield(LabelTaskRetryTimeoutCleanupTaskTimeoutsRetry)
		}
	}()

	err = h.Session.Query(
		`SELECT task_state, task_version, target, timeout_at, task_timeout_retry, task_timeout_lease
		 FROM promises WHERE origin = ? AND id = ?`,
		origin, id,
	).Scan(&taskState, &taskVersion, &target, &promiseTAt, &taskTimeoutRetry, &taskTimeoutLease)
	yield(LabelTaskRetryTimeoutRead)
	if err == gocql.ErrNotFound {
		if promiseTimeoutAt <= now {
			return nil // logically timed out — defer deletes entry
		}
		return errors.New("skip cleanup")
	}
	if err != nil {
		return err
	}
	// onTaskLeaseTimeout pre-inserts a retry entry before its LWT commits. If
	// that entry fires during the window between the pre-insert and the LWT, the
	// task row still shows task_state='acquired'. We must not delete the entry in
	// that case or the task will be stuck in pending with no retry timeout once
	// the LWT commits. We use task_timeout_lease to distinguish this from a
	// genuine orphan: if the lease is newer than this entry the task was
	// re-acquired after the entry was created, so the entry is definitely stale;
	// if the lease expired before this entry's timestamp the entry was likely
	// pre-inserted by onTaskLeaseTimeout after the lease fired.
	if taskState == "acquired" {
		if taskTimeoutLease != nil && *taskTimeoutLease > timeoutAt {
			return nil // lease is newer than this entry: orphan from a previous pending cycle
		}
		return errors.New("skip cleanup") // lease expired before this entry: may be onTaskLeaseTimeout pre-insert
	}
	if taskState != "pending" {
		return nil
	}
	if taskTimeoutRetry == nil || *taskTimeoutRetry > timeoutAt {
		return nil
	}
	if *taskTimeoutRetry < timeoutAt {
		return errors.New("skip cleanup")
	}

	retryAt := now + RetryTimeout
	if err = h.Session.Query(
		`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)`,
		h.BucketFor(retryAt), h.shardFor(id), retryAt, id, origin, promiseTAt,
	).Exec(); err != nil {
		return err
	}
	yield(LabelTaskRetryTimeoutPreinsertTaskTimeoutsRetry)

	row := make(map[string]interface{})
	var applied bool
	applied, err = h.Session.Query(
		`UPDATE promises SET task_timeout_retry = ?
		 WHERE origin = ? AND id = ?
		 IF task_state = 'pending' AND task_timeout_retry = ?`,
		retryAt, origin, id, timeoutAt,
	).MapScanCAS(row)
	yield(LabelTaskRetryTimeoutCommit)
	if err != nil {
		return err
	}
	if !applied {
		if delErr := h.Session.Query(
			`DELETE FROM task_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
			h.BucketFor(retryAt), h.shardFor(id), retryAt, origin, id,
		).Exec(); delErr != nil {
			log.Printf("onTaskRetryTimeout: rollback new timeout(%s): %v", id, delErr)
		}
		yield(LabelTaskRetryTimeoutRollbackTaskTimeoutsRetry)
		return nil
	}

	h.sendExecute(target, id, taskVersion)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// onTaskLeaseTimeout
// ─────────────────────────────────────────────────────────────────────────────

// onTaskLeaseTimeout releases an acquired task back to pending when its lease expires.
func (h *Handler) onTaskLeaseTimeout(origin string, id string, timeoutAt int64, promiseTimeoutAt int64, now int64, yield func(string)) (err error) {
	var taskState string
	var taskVersion int
	var target string
	var promiseTAt int64
	var taskTimeoutLease *int64

	defer func() {
		if err == nil {
			if delErr := h.Session.Query(
				`DELETE FROM task_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
				h.BucketFor(timeoutAt), h.shardFor(id), timeoutAt, origin, id,
			).Exec(); delErr != nil {
				log.Printf("onTaskLeaseTimeout: delete lease timeout(%s): %v", id, delErr)
			}
			yield(LabelTaskLeaseTimeoutCleanupTaskTimeoutsLease)
		}
	}()

	err = h.Session.Query(
		`SELECT task_state, task_version, target, timeout_at, task_timeout_lease
		 FROM promises WHERE origin = ? AND id = ?`,
		origin, id,
	).Scan(&taskState, &taskVersion, &target, &promiseTAt, &taskTimeoutLease)
	yield(LabelTaskLeaseTimeoutRead)
	if err == gocql.ErrNotFound {
		if promiseTimeoutAt <= now {
			return nil // logically timed out — defer deletes entry
		}
		return errors.New("skip cleanup")
	}
	if err != nil {
		return err
	}
	if taskState != "acquired" {
		return nil
	}
	if taskTimeoutLease == nil || *taskTimeoutLease != timeoutAt {
		return nil
	}

	// Release back to pending, version unchanged (only acquire bumps version).
	retryAt := now + RetryTimeout

	// Pre-insert retry timeout before the LWT so a kill between the two leaves an
	// orphan entry rather than a pending task with no retry timeout.
	if err = h.Session.Query(
		`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)`,
		h.BucketFor(retryAt), h.shardFor(id), retryAt, id, origin, promiseTAt,
	).Exec(); err != nil {
		return err
	}
	yield(LabelTaskLeaseTimeoutPreinsertTaskTimeoutsRetry)

	// LWT: release back to pending. Pin task_timeout_lease to guard against
	// a release-then-reacquire ABA race between the SELECT and this statement.
	row := make(map[string]interface{})
	var applied bool
	applied, err = h.Session.Query(
		`UPDATE promises SET
		     task_state = 'pending',
		     task_pid = null, task_ttl = null,
		     task_timeout_retry = ?,
		     task_timeout_lease = null
		 WHERE origin = ? AND id = ?
		 IF task_state = 'acquired' AND task_timeout_lease = ?`,
		retryAt, origin, id, timeoutAt,
	).MapScanCAS(row)
	yield(LabelTaskLeaseTimeoutCommit)
	if err != nil {
		return err
	}
	if !applied {
		if delErr := h.Session.Query(
			`DELETE FROM task_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
			h.BucketFor(retryAt), h.shardFor(id), retryAt, origin, id,
		).Exec(); delErr != nil {
			log.Printf("onTaskLeaseTimeout: rollback retry timeout(%s): %v", id, delErr)
		}
		yield(LabelTaskLeaseTimeoutRollbackTaskTimeoutsRetry)
		return nil
	}

	// Lease entry cleaned up by defer. Retry timeout was pre-inserted above.
	h.sendExecute(target, id, taskVersion)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

// sendUnblock builds an unblock message for rec and dispatches it to each
// listener address.
func (h *Handler) sendUnblock(listeners []string, rec PromiseRecord) {
	if len(listeners) == 0 {
		return
	}
	msg := UnblockMsg{
		Kind: "unblock",
		Data: struct {
			Promise PromiseRecord `json:"promise"`
		}{Promise: rec},
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return
	}
	for _, addr := range listeners {
		h.Dispatcher.Send(addr, b)
	}
}

// sendExecute dispatches an execute message for the given task. The Recorder
// (when installed via the debug Dispatcher) coalesces duplicate executes for
// the same task_id at snap time.
func (h *Handler) sendExecute(target, taskID string, version int) {
	if target == "" {
		return
	}
	msg := ExecuteMsg{
		Kind: "execute",
		Data: struct {
			Task TaskRef `json:"task"`
		}{Task: TaskRef{ID: taskID, Version: version}},
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return
	}
	h.Dispatcher.Send(target, b)
}
