package core

import (
	"encoding/json"
	"log"

	"github.com/gocql/gocql"
)

// ─────────────────────────────────────────────────────────────────────────────
// task.get
// ─────────────────────────────────────────────────────────────────────────────

// TaskGet implements the task.get action.
//
// Semantics (mirrors spec T-01):
//   - Row not found or target IS NULL → 404 (no task on this promise).
//   - If promise is pending and now >= timeout_at → project task as fulfilled.
//   - Otherwise return task columns as-is.
func (h *Handler) TaskGet(head RequestHead, req TaskGetData, now int64, yield func(string)) any {
	id := req.ID
	origin, _ := resolveOrigin(head.Origin, "", id)

	row, err := h.readAndTryTimeout(id, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "task.get",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}
	if err != nil {
		log.Printf("task.get read(%s): %v", id, err)
		return Res[string]{
			Kind: "task.get",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if row.Task == nil {
		return Res[string]{
			Kind: "task.get",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}

	return Res[TaskGetResData]{
		Kind: "task.get",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: TaskGetResData{Task: *row.Task},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// task.create
// ─────────────────────────────────────────────────────────────────────────────

// TaskCreate implements the task.create action.
//
// Semantics (mirrors spec T-02):
//   - If promise does not exist: INSERT with task_state=acquired (pending case)
//     or task_state=fulfilled (born-expired case).
//   - If promise already exists:
//     1. No target → 422
//     2. Pending + logically expired → 200 (projected timeout, task fulfilled)
//     3. Settled → 200 with current state
//     4. Pending + task pending → re-acquire via LWT → 200
//     5. Pending + task acquired/suspended/halted → 409
//
// Version semantics: new acquired tasks start at version 1 (conceptual: the
// task.create atomically does promise-create at v0 + acquire → v1). Born-expired
// tasks start at version 0. Re-acquire bumps the existing version by 1.
func (h *Handler) TaskCreate(head RequestHead, req TaskCreateData, now int64, yield func(string)) any {
	inner := req.Action.Data
	id := *inner.ID
	origin, err := resolveOrigin(req.Action.Head.Origin, inner.Tags["resonate:origin"], id)
	if err != nil {
		return Res[string]{
			Kind: "task.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
			Data: err.Error(),
		}
	}
	target := inner.Tags["resonate:target"]

	tags := inner.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	// ── 1. Compute initial state ──────────────────────────────────────────────

	var (
		state       string
		createdAt   int64
		settledAt   *int64
		taskState   string
		taskVersion int
		taskPID     *string
		taskTTL     *int64
		taskLease   *int64
	)

	if now >= *inner.TimeoutAt {
		// born expired — task is immediately fulfilled
		if tags["resonate:timer"] == "true" {
			state = "resolved"
		} else {
			state = "rejected_timedout"
		}
		createdAt = *inner.TimeoutAt
		sa := *inner.TimeoutAt
		settledAt = &sa
		taskState = "fulfilled"
		taskVersion = 0
		// taskPID, taskTTL, taskLease remain nil
	} else {
		// pending — task is immediately acquired; version starts at 1
		// (task.create is atomic promise-create-at-v0 + acquire-to-v1)
		state = "pending"
		createdAt = now
		taskState = "acquired"
		taskVersion = 1
		pid := *req.PID
		taskPID = &pid
		ttl := int64(*req.TTL)
		taskTTL = &ttl
		lease := now + int64(*req.TTL)
		taskLease = &lease
	}

	// ── 2a. Pre-insert promise_timeouts and task_timeouts before the LWT so kills
	// between here and the LWT leave orphan entries rather than a committed promise
	// or acquired task with no corresponding timeout entries.
	if state == "pending" {
		if err := h.Session.Query(
			`INSERT INTO promise_timeouts (bucket, shard, timeout_at, promise_id, origin) VALUES (?, ?, ?, ?, ?)`,
			h.BucketFor(*inner.TimeoutAt), h.shardFor(id), *inner.TimeoutAt, id, origin,
		).Exec(); err != nil {
			log.Printf("task.create: pre-insert promise_timeouts(%s): %v", id, err)
			return Res[string]{
				Kind: "task.create",
				Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
				Data: err.Error(),
			}
		}
		yield(LabelTaskCreatePreinsertPromiseTimeouts)
		if taskLease != nil {
			if err := h.Session.Query(
				`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 1, ?, ?, ?)`,
				h.BucketFor(*taskLease), h.shardFor(id), *taskLease, id, origin, *inner.TimeoutAt,
			).Exec(); err != nil {
				log.Printf("task.create: pre-insert task_timeouts lease(%s): %v", id, err)
				return Res[string]{
					Kind: "task.create",
					Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
					Data: err.Error(),
				}
			}
			yield(LabelTaskCreatePreinsertTaskTimeoutsLease)
		}
	}

	// ── 2b. LWT INSERT ────────────────────────────────────────────────────────

	row := make(map[string]interface{})
	applied, err := h.Session.Query(
		`INSERT INTO promises (
		    id, origin, branch, parent, target,
		    state, param_headers, param_data,
		    value_headers, value_data,
		    tags, timeout_at, created_at, settled_at,
		    callbacks, listeners,
		    task_state, task_version, task_ttl, task_pid, task_resumes,
		    task_timeout_retry, task_timeout_lease
		) VALUES (
		    ?, ?, ?, ?, ?,
		    ?, ?, ?,
		    null, null,
		    ?, ?, ?, ?,
		    {}, {},
		    ?, ?, ?, ?, {},
		    null, ?
		) IF NOT EXISTS`,
		id, origin,
		tags["resonate:branch"],
		tags["resonate:parent"],
		target,
		state, inner.Param.Headers, inner.Param.Data,
		tags, *inner.TimeoutAt, createdAt, settledAt,
		taskState, taskVersion, taskTTL, taskPID,
		taskLease,
	).MapScanCAS(row)
	yield(LabelTaskCreateCommit)
	if err != nil {
		log.Printf("task.create LWT(%s): %v", id, err)
		return Res[string]{
			Kind: "task.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	if !applied {
		// Roll back the pre-inserted timeout entries unless the existing row
		// legitimately owns an entry at that PK — i.e. the existing promise is
		// still pending with the same timeout (promise_timeouts), or the
		// existing task is acquired with the same lease (task_timeouts).
		// Settled promises and non-acquired tasks have no legitimate hint row,
		// so any entry at the matching PK is the one we just pre-inserted.
		if state == "pending" {
			existingState, _ := row["state"].(string)
			existingTimeoutAt, _ := row["timeout_at"].(int64)
			if !(existingState == "pending" && existingTimeoutAt == *inner.TimeoutAt) {
				h.Session.Query(
					`DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?`,
					h.BucketFor(*inner.TimeoutAt), h.shardFor(id), *inner.TimeoutAt, origin, id,
				).Exec()
				yield(LabelTaskCreateRollbackPromiseTimeouts)
			}
			if taskLease != nil {
				existingTaskState, _ := row["task_state"].(string)
				existingLeaseAt, _ := row["task_timeout_lease"].(int64)
				if !(existingTaskState == "acquired" && existingLeaseAt == *taskLease) {
					h.Session.Query(
						`DELETE FROM task_timeouts
						 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
						h.BucketFor(*taskLease), h.shardFor(id), *taskLease, origin, id,
					).Exec()
					yield(LabelTaskCreateRollbackTaskTimeoutsLease)
				}
			}
		}
		return h.taskCreateConflict(head, req, id, origin, now, yield, row)
	}

	// ── 3. Auxiliary writes (pending only) — timeout entries were pre-inserted above.

	// ── 4. Build and return response ──────────────────────────────────────────

	pr := PromiseRecord{
		ID:        id,
		State:     state,
		Param:     *inner.Param,
		Tags:      tags,
		TimeoutAt: *inner.TimeoutAt,
		CreatedAt: createdAt,
		SettledAt: settledAt,
	}

	taskRec := TaskRecord{
		ID:      id,
		State:   taskState,
		Version: taskVersion,
		Resumes: json.RawMessage("[]"),
	}
	if taskPID != nil {
		taskRec.PID = *taskPID
	}
	if taskTTL != nil {
		ttlInt := int(*taskTTL)
		taskRec.TTL = &ttlInt
	}

	return Res[TaskCreateResData]{
		Kind: "task.create",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: TaskCreateResData{
			Task:    taskRec,
			Promise: pr,
			Preload: []PromiseRecord{},
		},
	}
}

// taskCreateConflict handles the case where the LWT INSERT found an existing row.
func (h *Handler) taskCreateConflict(
	head RequestHead,
	req TaskCreateData,
	id, origin string,
	now int64,
	yield func(string),
	row map[string]interface{},
) any {
	// Extract existing row fields.
	existingTarget, _ := row["target"].(string)
	existingState, _ := row["state"].(string)
	existingTimeoutAt, _ := row["timeout_at"].(int64)
	existingTaskState, _ := row["task_state"].(string)
	existingTaskVersion, _ := row["task_version"].(int)
	existingCreatedAt, _ := row["created_at"].(int64)
	existingParamHdrs, _ := row["param_headers"].(map[string]string)
	existingParamData, _ := row["param_data"].(string)
	existingValueHdrs, _ := row["value_headers"].(map[string]string)
	existingValueData, _ := row["value_data"].(string)
	existingTags, _ := row["tags"].(map[string]string)
	if existingTags == nil {
		existingTags = map[string]string{}
	}
	var existingSettledAt *int64
	if sa, ok := row["settled_at"].(int64); ok && sa > 0 {
		existingSettledAt = &sa
	}
	existingTaskPID, _ := row["task_pid"].(string)
	existingTaskTTLRaw := row["task_ttl"]
	existingTaskResumes, _ := row["task_resumes"].([]string)
	existingTRetryVal, _ := row["task_timeout_retry"].(int64)
	var existingTaskTimeoutRetry *int64
	if existingTRetryVal > 0 {
		existingTaskTimeoutRetry = &existingTRetryVal
	}
	existingTLeaseVal, _ := row["task_timeout_lease"].(int64)
	var existingTaskTimeoutLease *int64
	if existingTLeaseVal > 0 {
		existingTaskTimeoutLease = &existingTLeaseVal
	}
	existingListeners, _ := row["listeners"].([]string)
	existingCallbacks, _ := row["callbacks"].([]string)
	if len(existingCallbacks) == 0 {
		existingCallbacks = nil
	}

	buildExistingPromise := func(state string, sa *int64) PromiseRecord {
		return PromiseRecord{
			ID:        id,
			State:     state,
			Param:     Value{Headers: existingParamHdrs, Data: existingParamData},
			Value:     Value{Headers: existingValueHdrs, Data: existingValueData},
			Tags:      existingTags,
			TimeoutAt: existingTimeoutAt,
			CreatedAt: existingCreatedAt,
			SettledAt: sa,
		}
	}

	buildExistingTask := func(state string, version int) TaskRecord {
		resumesJSON := json.RawMessage("[]")
		if len(existingTaskResumes) > 0 {
			if b, err := json.Marshal(existingTaskResumes); err == nil {
				resumesJSON = b
			}
		}
		tr := TaskRecord{
			ID:      id,
			State:   state,
			Version: version,
			Resumes: resumesJSON,
			PID:     existingTaskPID,
		}
		if existingTaskTTLRaw != nil {
			// gocql may return null int columns as int64(0) rather than nil.
			if ttlVal, ok := existingTaskTTLRaw.(int64); ok && ttlVal > 0 {
				ttlInt := int(ttlVal)
				tr.TTL = &ttlInt
			}
		}
		return tr
	}

	ok200 := func(task TaskRecord, promise PromiseRecord) any {
		return Res[TaskCreateResData]{
			Kind: "task.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
			Data: TaskCreateResData{
				Task:    task,
				Promise: promise,
				Preload: []PromiseRecord{},
			},
		}
	}

	// Case 1: no target → 422
	if existingTarget == "" {
		return Res[string]{
			Kind: "task.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 422, Version: head.Version},
			Data: "Promise has no address",
		}
	}

	// Case 3: settled → 200 with current state
	if existingState != "pending" {
		return ok200(
			buildExistingTask(existingTaskState, existingTaskVersion),
			buildExistingPromise(existingState, existingSettledAt),
		)
	}

	// Case 3b: promise pending but logically expired → eagerly settle, task fulfilled.
	if now >= existingTimeoutAt {
		in := promiseTimeoutInput{
			Origin:     origin,
			ID:         id,
			Tags:       existingTags,
			Target:     existingTarget,
			TimeoutAt:  existingTimeoutAt,
			CreatedAt:  existingCreatedAt,
			TaskState:  existingTaskState,
			TaskTRetry: existingTaskTimeoutRetry,
			TaskTLease: existingTaskTimeoutLease,
			Listeners:  existingListeners,
			Callbacks:  existingCallbacks,
		}
		sd, tryErr := h.tryTimeout(in, now, yield)
		if tryErr != nil {
			log.Printf("task.create tryTimeout(%s): %v", id, tryErr)
			return Res[string]{Kind: "task.create", Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version}, Data: tryErr.Error()}
		}
		settledAtFinal := sd.SettledAt
		return ok200(
			TaskRecord{ID: id, State: "fulfilled", Version: existingTaskVersion, Resumes: json.RawMessage("[]")},
			PromiseRecord{
				ID:        id,
				State:     sd.State,
				Param:     Value{Headers: existingParamHdrs, Data: existingParamData},
				Value:     Value{Headers: sd.ValHdrs, Data: sd.ValData},
				Tags:      existingTags,
				TimeoutAt: existingTimeoutAt,
				CreatedAt: existingCreatedAt,
				SettledAt: &settledAtFinal,
			},
		)
	}

	// Cases 4 & 5: promise is pending and not yet expired.

	// Case 4: task pending → re-acquire
	if existingTaskState == "pending" {
		return h.taskCreateReacquire(head, req, id, origin, now, yield, row,
			existingTaskVersion, existingParamHdrs, existingParamData, existingTags, existingTimeoutAt, existingCreatedAt,
		)
	}

	// Case 5: task acquired / suspended / halted → 409
	return Res[string]{
		Kind: "task.create",
		Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
		Data: "Task already exists",
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// task.acquire
// ─────────────────────────────────────────────────────────────────────────────

// TaskAcquire implements the task.acquire action.
//
// Semantics (mirrors spec T-03):
//   - Read the full row; 404 if not found or promise has no target.
//   - Project lazy timeout (no write): if promise pending and now >= timeout_at → 409.
//   - If task_state != "pending" → 409 "Task not in pending state".
//   - LWT: IF task_state='pending' AND task_version=req.version → acquired, version+1.
//   - On failure: distinguish version mismatch vs state mismatch → 409.
//   - On success: swap retry timeout → lease timeout; return task + promise.
func (h *Handler) TaskAcquire(head RequestHead, req TaskAcquireData, now int64, yield func(string)) any {
	id := req.ID
	origin, _ := resolveOrigin(head.Origin, "", id)

	// 1. Read full row (may eagerly timeout).
	row, err := h.readAndTryTimeout(id, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "task.acquire",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}
	if err != nil {
		log.Printf("task.acquire read(%s): %v", id, err)
		return Res[string]{
			Kind: "task.acquire",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if row.Task == nil {
		return Res[string]{
			Kind: "task.acquire",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}

	// 2. Timeout applied or already settled → task logically fulfilled.
	if row.Promise.State != "pending" {
		return Res[string]{
			Kind: "task.acquire",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Task not in pending state",
		}
	}

	// 3. State precheck (avoids LWT round-trip for obvious cases).
	if row.Task.State != "pending" {
		return Res[string]{
			Kind: "task.acquire",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Task not in pending state",
		}
	}

	// 4. LWT: pending → acquired, version bumped.
	leaseAt := now + int64(req.TTL)
	newVersion := *req.Version + 1

	// Pre-insert lease timeout before the LWT so a kill between the two leaves an
	// orphan type=1 entry rather than an acquired task with no lease timeout.
	if err := h.Session.Query(
		`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 1, ?, ?, ?)`,
		h.BucketFor(leaseAt), h.shardFor(id), leaseAt, id, origin, row.Promise.TimeoutAt,
	).Exec(); err != nil {
		log.Printf("task.acquire: pre-insert lease timeout(%s): %v", id, err)
		return Res[string]{
			Kind: "task.acquire",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	yield(LabelTaskAcquirePreinsertTaskTimeoutsLease)

	lwtRow := make(map[string]interface{})
	applied, err := h.Session.Query(
		`UPDATE promises
		 SET task_state = 'acquired',
		     task_version = ?,
		     task_pid = ?,
		     task_ttl = ?,
		     task_resumes = {},
		     task_timeout_retry = null,
		     task_timeout_lease = ?
		 WHERE origin = ? AND id = ?
		 IF task_state = 'pending' AND task_version = ?`,
		newVersion, req.PID, int64(req.TTL), leaseAt,
		origin, id,
		*req.Version,
	).MapScanCAS(lwtRow)
	yield(LabelTaskAcquireCommit)
	if err != nil {
		log.Printf("task.acquire LWT(%s): %v", id, err)
		return Res[string]{
			Kind: "task.acquire",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	if !applied {
		// Roll back the pre-inserted lease entry unless the existing task
		// legitimately owns one at that PK.
		existingTaskState, _ := lwtRow["task_state"].(string)
		existingLeaseAt, _ := lwtRow["task_timeout_lease"].(int64)
		if !(existingTaskState == "acquired" && existingLeaseAt == leaseAt) {
			h.Session.Query(
				`DELETE FROM task_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
				h.BucketFor(leaseAt), h.shardFor(id), leaseAt, origin, id,
			).Exec()
			yield(LabelTaskAcquireRollbackTaskTimeoutsLease)
		}
		// Distinguish version mismatch vs state mismatch from the LWT response row.
		curTaskState, _ := lwtRow["task_state"].(string)
		curTaskVersion, _ := lwtRow["task_version"].(int)
		msg := "Task not in pending state"
		if curTaskState == "pending" && curTaskVersion != *req.Version {
			msg = "Version mismatch"
		}
		return Res[string]{
			Kind: "task.acquire",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: msg,
		}
	}

	// 5. Auxiliary: delete old retry timeout. Lease timeout was pre-inserted above.
	if row.TaskTRetry != nil {
		h.Session.Query(
			`DELETE FROM task_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
			h.BucketFor(*row.TaskTRetry), h.shardFor(id), *row.TaskTRetry, origin, id,
		).Exec()
		yield(LabelTaskAcquireCleanupTaskTimeoutsRetry)
	}

	// 6. Build and return response.
	ttlInt := req.TTL
	taskRec := TaskRecord{
		ID:      id,
		State:   "acquired",
		Version: newVersion,
		Resumes: json.RawMessage("[]"),
		PID:     req.PID,
		TTL:     &ttlInt,
	}

	return Res[TaskAcquireResData]{
		Kind: "task.acquire",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: TaskAcquireResData{
			Task:    taskRec,
			Promise: row.Promise,
			Preload: []PromiseRecord{},
		},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// task.release
// ─────────────────────────────────────────────────────────────────────────────

// TaskRelease implements the task.release action.
//
// Semantics (mirrors spec T-08):
//   - Read row; 404 if not found or no target.
//   - Lazy timeout projection: if promise pending and now >= timeout_at → 409 "Task not acquired".
//   - LWT: IF task_state='acquired' AND task_version=req.version → pending, version+1.
//   - On failure: distinguish state mismatch ("Task not acquired") vs version mismatch.
//   - On success: swap lease timeout → retry timeout; send execute message.
func (h *Handler) TaskRelease(head RequestHead, req TaskReleaseData, now int64, yield func(string)) any {
	id := req.ID
	origin, _ := resolveOrigin(head.Origin, "", id)

	// 1. Read row (may eagerly timeout).
	row, err := h.readAndTryTimeout(id, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "task.release",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}
	if err != nil {
		log.Printf("task.release read(%s): %v", id, err)
		return Res[string]{
			Kind: "task.release",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if row.Task == nil {
		return Res[string]{
			Kind: "task.release",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}

	// 2. Timeout applied or already settled → task logically fulfilled (not acquired).
	if row.Promise.State != "pending" {
		return Res[string]{
			Kind: "task.release",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Task not acquired",
		}
	}

	// 3. LWT: acquired → pending, version unchanged (only acquire bumps version).
	retryAt := now + RetryTimeout

	// Pre-insert retry timeout before the LWT so a kill between the two leaves an
	// orphan type=0 entry rather than a pending task with no retry timeout.
	if err := h.Session.Query(
		`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)`,
		h.BucketFor(retryAt), h.shardFor(id), retryAt, id, origin, row.Promise.TimeoutAt,
	).Exec(); err != nil {
		log.Printf("task.release: pre-insert retry timeout(%s): %v", id, err)
		return Res[string]{
			Kind: "task.release",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	yield(LabelTaskReleasePreinsertTaskTimeoutsRetry)

	lwtRow := make(map[string]interface{})
	applied, err := h.Session.Query(
		`UPDATE promises
		 SET task_state = 'pending',
		     task_pid = null,
		     task_ttl = null,
		     task_timeout_retry = ?,
		     task_timeout_lease = null
		 WHERE origin = ? AND id = ?
		 IF task_state = 'acquired' AND task_version = ?`,
		retryAt,
		origin, id,
		*req.Version,
	).MapScanCAS(lwtRow)
	yield(LabelTaskReleaseCommit)
	if err != nil {
		log.Printf("task.release LWT(%s): %v", id, err)
		return Res[string]{
			Kind: "task.release",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	if !applied {
		// Only delete the pre-inserted retry entry if the existing task does not
		// already have the same retry timeout — matching means our pre-insert was
		// idempotent on an existing entry that must not be removed.
		// Use TaskTRetry from the initial read: the LWT failure response for
		// UPDATE...IF only returns IF-clause columns, not task_timeout_retry.
		existingRetryAt := int64(0)
		if row.TaskTRetry != nil {
			existingRetryAt = *row.TaskTRetry
		}
		if existingRetryAt != retryAt {
			h.Session.Query(
				`DELETE FROM task_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
				h.BucketFor(retryAt), h.shardFor(id), retryAt, origin, id,
			).Exec()
			yield(LabelTaskReleaseRollbackTaskTimeoutsRetry)
		}
		// Distinguish version mismatch from state mismatch.
		curTaskState, _ := lwtRow["task_state"].(string)
		curTaskVersion, _ := lwtRow["task_version"].(int)
		msg := "Task not acquired"
		if curTaskState == "acquired" && curTaskVersion != *req.Version {
			msg = "Version mismatch"
		}
		return Res[string]{
			Kind: "task.release",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: msg,
		}
	}

	// 4. Auxiliary: delete old lease timeout. Retry timeout was pre-inserted above.
	if row.TaskTLease != nil {
		if delErr := h.Session.Query(
			`DELETE FROM task_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
			h.BucketFor(*row.TaskTLease), h.shardFor(id), *row.TaskTLease, origin, id,
		).Exec(); delErr != nil {
			log.Printf("task.release: delete lease timeout(%s): %v", id, delErr)
		}
		yield(LabelTaskReleaseCleanupTaskTimeoutsLease)
	}

	// Send execute message so another worker can pick up the task.
	h.sendExecute(row.Target, id, *req.Version)

	return Res[struct{}]{
		Kind: "task.release",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// task.fence
// ─────────────────────────────────────────────────────────────────────────────

// TaskFence implements the task.fence action.
//
// Semantics (mirrors spec T-04):
//   - Read the row; 404 if not found or no target.
//   - Lazy timeout projection (no write): if promise pending and now >= timeout_at → 409.
//   - If task_state != "acquired" → 409 "Fence check failed".
//   - If task_version != req.version → 409 "Fence check failed".
//   - Dispatch inner action (promise.create or promise.settle) and return wrapped result.
func (h *Handler) TaskFence(head RequestHead, req TaskFenceData, now int64, yield func(string)) any {
	id := req.ID
	origin, _ := resolveOrigin(head.Origin, "", id)

	// 1. Decode and validate inner action (input-only, no DB needed).
	var innerEnv struct {
		Kind string          `json:"kind"`
		Head RequestHead     `json:"head"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(req.Action, &innerEnv); err != nil {
		return Res[string]{
			Kind: "task.fence",
			Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
			Data: "Invalid action envelope: " + err.Error(),
		}
	}
	var (
		createData *PromiseCreateData
		settleData *PromiseSettleData
	)
	switch innerEnv.Kind {
	case "promise.create":
		var d PromiseCreateData
		if err := json.Unmarshal(innerEnv.Data, &d); err != nil {
			return Res[string]{
				Kind: "task.fence",
				Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
				Data: "Invalid promise.create data: " + err.Error(),
			}
		}
		if d.ID != nil && *d.ID == id {
			return Res[string]{
				Kind: "task.fence",
				Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
				Data: "Action ID must not equal task ID",
			}
		}
		createData = &d
	case "promise.settle":
		var d PromiseSettleData
		if err := json.Unmarshal(innerEnv.Data, &d); err != nil {
			return Res[string]{
				Kind: "task.fence",
				Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
				Data: "Invalid promise.settle data: " + err.Error(),
			}
		}
		if d.ID == id {
			return Res[string]{
				Kind: "task.fence",
				Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
				Data: "Action ID must not equal task ID",
			}
		}
		settleData = &d
	default:
		return Res[string]{
			Kind: "task.fence",
			Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
			Data: "Unknown action kind: " + innerEnv.Kind,
		}
	}

	// 2. Read row (may eagerly timeout).
	row, err := h.readAndTryTimeout(id, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "task.fence",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}
	if err != nil {
		log.Printf("task.fence read(%s): %v", id, err)
		return Res[string]{
			Kind: "task.fence",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if row.Task == nil {
		return Res[string]{
			Kind: "task.fence",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}

	// 3. Timeout applied or already settled → fence check failed.
	if row.Promise.State != "pending" {
		return Res[string]{
			Kind: "task.fence",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Fence check failed",
		}
	}

	// 4. Fence checks.
	if row.Task.State != "acquired" {
		return Res[string]{
			Kind: "task.fence",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Fence check failed",
		}
	}
	if row.Task.Version != *req.Version {
		return Res[string]{
			Kind: "task.fence",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Fence check failed",
		}
	}

	// 5. Dispatch inner action.
	var actionResult any
	switch {
	case createData != nil:
		actionResult = h.PromiseCreate(innerEnv.Head, *createData, now, yield)
	case settleData != nil:
		actionResult = h.PromiseSettle(innerEnv.Head, *settleData, now, yield)
	}

	// 6. Marshal inner action result and return wrapped response.
	actionBytes, err := json.Marshal(actionResult)
	if err != nil {
		log.Printf("task.fence marshal action result: %v", err)
		return Res[string]{
			Kind: "task.fence",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	return Res[TaskFenceResData]{
		Kind: "task.fence",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: TaskFenceResData{
			Action:  json.RawMessage(actionBytes),
			Preload: []PromiseRecord{},
		},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// task.heartbeat
// ─────────────────────────────────────────────────────────────────────────────

// TaskHeartbeat implements the task.heartbeat action.
//
// Semantics (mirrors spec T-05):
//   - For each TaskRef in req.Tasks:
//     1. Read the promise row (origin = ref.ID, root assumption).
//     2. Skip if not found, task_state != 'acquired', or task_version != ref.Version.
//     3. LWT: SET task_timeout_lease = now + task_ttl
//     IF task_state = 'acquired' AND task_version = ref.Version.
//     4. On LWT success: DELETE old lease entry, INSERT new lease entry.
//   - Return 200 {} on success; 500 on any DB error (stops processing remaining tasks).
func (h *Handler) TaskHeartbeat(head RequestHead, req TaskHeartbeatData, now int64, yield func(string)) any {
	for _, ref := range req.Tasks {
		id := ref.ID
		origin, _ := resolveOrigin(head.Origin, "", id)

		// 1. Read row (may eagerly timeout).
		row, err := h.readAndTryTimeout(id, origin, now, yield)
		if err != nil {
			if err != gocql.ErrNotFound {
				log.Printf("task.heartbeat read(%s): %v", id, err)
				return Res[string]{
					Kind: "task.heartbeat",
					Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
					Data: err.Error(),
				}
			}
			continue
		}
		if row.Task == nil {
			continue
		}

		// 2. Skip if state, version, or pid doesn't match.
		// If timeout was applied by helper, Task.State == "fulfilled" → skipped here.
		if row.Task.State != "acquired" || row.Task.Version != ref.Version || row.Task.PID != *req.PID {
			continue
		}
		if row.Task.TTL == nil {
			continue // shouldn't happen for an acquired task, but guard anyway
		}

		// 3. LWT: refresh lease.
		newLease := now + int64(*row.Task.TTL)

		// Pre-insert new lease timeout before the LWT so a kill between the two
		// leaves an orphan entry rather than an acquired task with no lease timeout.
		if err := h.Session.Query(
			`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at)
			 VALUES (?, ?, ?, 1, ?, ?, ?)`,
			h.BucketFor(newLease), h.shardFor(id), newLease, id, origin, row.Promise.TimeoutAt,
		).Exec(); err != nil {
			log.Printf("task.heartbeat: pre-insert lease timeout(%s): %v", id, err)
			return Res[string]{
				Kind: "task.heartbeat",
				Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
				Data: err.Error(),
			}
		}
		yield(LabelTaskHeartbeatPreinsertTaskTimeoutsLease)

		lwtRow := make(map[string]interface{})
		applied, err := h.Session.Query(
			`UPDATE promises
			 SET task_timeout_lease = ?
			 WHERE origin = ? AND id = ?
			 IF task_state = 'acquired' AND task_version = ?`,
			newLease, origin, id, ref.Version,
		).MapScanCAS(lwtRow)
		yield(LabelTaskHeartbeatCommit)
		if err != nil {
			log.Printf("task.heartbeat LWT(%s): %v", id, err)
			return Res[string]{
				Kind: "task.heartbeat",
				Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
				Data: err.Error(),
			}
		}

		if applied {
			// 4. Swap lease timeout: delete old entry. New one was pre-inserted above.
			// Skip when old == new: the pre-insert was idempotent and there is nothing to remove.
			if row.TaskTLease != nil && *row.TaskTLease != newLease {
				h.Session.Query(
					`DELETE FROM task_timeouts
					 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
					h.BucketFor(*row.TaskTLease), h.shardFor(id), *row.TaskTLease, origin, id,
				).Exec()
				yield(LabelTaskHeartbeatCleanupTaskTimeoutsLease)
			}
		} else {
			// Cleanup pre-inserted entry if the task no longer owns this lease time.
			existingLeaseAt, _ := lwtRow["task_timeout_lease"].(int64)
			if existingLeaseAt != newLease {
				h.Session.Query(
					`DELETE FROM task_timeouts
					 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
					h.BucketFor(newLease), h.shardFor(id), newLease, origin, id,
				).Exec()
				yield(LabelTaskHeartbeatRollbackTaskTimeoutsLease)
			}
		}
	}

	return Res[struct{}]{
		Kind: "task.heartbeat",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// task.suspend
// ─────────────────────────────────────────────────────────────────────────────

// TaskSuspend implements the task.suspend action.
//
// Semantics (mirrors spec T-06):
//   - Read task row; 404 if not found or no target.
//   - task_state != 'acquired' → 409 "Task not acquired".
//   - task_version != req.version → 409 "Version mismatch".
//   - Read each awaited promise (SELECT only); promise not found → 422.
//   - If any awaited promise is non-pending or logically expired → 300 {preload: []}.
//   - Otherwise: single batch LWT atomically suspends the task and registers
//     callbacks on all awaited promises; delete lease timeout; return 200 {}.
func (h *Handler) TaskSuspend(head RequestHead, req TaskSuspendData, now int64, yield func(string)) any {
	id := req.ID
	origin, _ := resolveOrigin(head.Origin, "", id)

	// Validate request before any DB work (spec schema refinements).
	if len(req.Actions) == 0 {
		return Res[string]{
			Kind: "task.suspend",
			Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
			Data: "data.actions: Actions array cannot be empty",
		}
	}
	for _, action := range req.Actions {
		if action.Data.Awaiter != id {
			return Res[string]{
				Kind: "task.suspend",
				Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
				Data: "All action awaiter IDs must match the task ID",
			}
		}
		if action.Data.Awaited == id {
			return Res[string]{
				Kind: "task.suspend",
				Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
				Data: "Action awaited promise must not equal the task ID",
			}
		}
	}

	// 1. Read row (may eagerly timeout).
	row, err := h.readAndTryTimeout(id, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "task.suspend",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}
	if err != nil {
		log.Printf("task.suspend read(%s): %v", id, err)
		return Res[string]{
			Kind: "task.suspend",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if row.Task == nil {
		return Res[string]{
			Kind: "task.suspend",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}

	// 2. State and version prechecks (helper applied timeout if expired).
	if row.Promise.State != "pending" || row.Task.State != "acquired" {
		return Res[string]{
			Kind: "task.suspend",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Task not acquired",
		}
	}
	if row.Task.Version != *req.Version {
		return Res[string]{
			Kind: "task.suspend",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Version mismatch",
		}
	}

	// 4a. Read each awaited promise (SELECT only — no writes yet).
	// Awaited promises live in the task's partition (origin = id).
	type awaitedRead struct {
		state      string
		timeoutAt  int64
		tags       map[string]string
		target     string
		createdAt  int64
		taskState  string
		taskTRetry *int64
		taskTLease *int64
		listeners  []string
		callbacks  []string
	}
	awaitedReads := make([]awaitedRead, len(req.Actions))
	for i, action := range req.Actions {
		awaitedID := action.Data.Awaited
		var ar awaitedRead
		err := h.Session.Query(
			`SELECT state, timeout_at, tags, target, created_at,
			        task_state, task_timeout_retry, task_timeout_lease,
			        listeners, callbacks
			 FROM promises WHERE origin = ? AND id = ?`,
			origin, awaitedID,
		).Scan(&ar.state, &ar.timeoutAt, &ar.tags, &ar.target, &ar.createdAt,
			&ar.taskState, &ar.taskTRetry, &ar.taskTLease,
			&ar.listeners, &ar.callbacks)
		yield(LabelTaskSuspendReadAwaiters)
		if err == gocql.ErrNotFound {
			return Res[string]{
				Kind: "task.suspend",
				Head: ResponseHead{CorrID: head.CorrID, Status: 422, Version: head.Version},
				Data: "Awaited promise not found",
			}
		}
		if err != nil {
			log.Printf("task.suspend: read awaited(%s → %s): %v", id, awaitedID, err)
			return Res[string]{
				Kind: "task.suspend",
				Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
				Data: err.Error(),
			}
		}
		awaitedReads[i] = ar
	}

	// 4b. If any awaited promise is non-pending (or expired), clear resumes and redirect.
	for i := range awaitedReads {
		ar := &awaitedReads[i]
		if ar.state == "pending" && ar.timeoutAt <= now {
			if ar.tags == nil {
				ar.tags = map[string]string{}
			}
			in := promiseTimeoutInput{
				Origin:     origin,
				ID:         req.Actions[i].Data.Awaited,
				Tags:       ar.tags,
				Target:     ar.target,
				TimeoutAt:  ar.timeoutAt,
				CreatedAt:  ar.createdAt,
				TaskState:  ar.taskState,
				TaskTRetry: ar.taskTRetry,
				TaskTLease: ar.taskTLease,
				Listeners:  ar.listeners,
				Callbacks:  ar.callbacks,
			}
			if _, tryErr := h.tryTimeout(in, now, yield); tryErr != nil {
				log.Printf("task.suspend tryTimeout awaited(%s): %v", in.ID, tryErr)
				return Res[string]{Kind: "task.suspend", Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version}, Data: tryErr.Error()}
			}
			ar.state = "settled"
		}
		if ar.state != "pending" {
			clearRow := make(map[string]interface{})
			clearApplied, clearErr := h.Session.Query(
				`UPDATE promises SET task_resumes = {}
				 WHERE origin = ? AND id = ?
				 IF task_state = 'acquired' AND task_version = ?`,
				origin, id, row.Task.Version,
			).MapScanCAS(clearRow)
			yield(LabelTaskSuspendClearResumes)
			if clearErr != nil {
				log.Printf("task.suspend clear resumes LWT(%s): %v", id, clearErr)
				return Res[string]{
					Kind: "task.suspend",
					Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
					Data: clearErr.Error(),
				}
			}
			if !clearApplied {
				if clearRow["task_state"] == "acquired" {
					return Res[string]{
						Kind: "task.suspend",
						Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
						Data: "Version mismatch",
					}
				}
				return Res[string]{
					Kind: "task.suspend",
					Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
					Data: "Task not acquired",
				}
			}
			return Res[TaskSuspend300ResData]{
				Kind: "task.suspend",
				Head: ResponseHead{CorrID: head.CorrID, Status: 300, Version: head.Version},
				Data: TaskSuspend300ResData{Preload: []PromiseRecord{}},
			}
		}
	}

	// 4c. All awaited promises are pending: single conditional batch that
	// atomically suspends the task and appends the callback entry to every
	// awaited promise. All rows share origin = id (the task's partition),
	// satisfying ScyllaDB's same-partition requirement for batch LWTs.
	batch := h.Session.NewBatch(gocql.LoggedBatch)
	batch.Query(
		`UPDATE promises
		 SET task_state = 'suspended',
		     task_pid = null,
		     task_ttl = null,
		     task_resumes = {},
		     task_timeout_lease = null
		 WHERE origin = ? AND id = ?
		 IF task_state = 'acquired' AND task_version = ?`,
		origin, id, *req.Version)
	for _, action := range req.Actions {
		batch.Query(
			`UPDATE promises SET callbacks = callbacks + ?
			 WHERE origin = ? AND id = ?
			 IF state = 'pending'`,
			[]string{id}, origin, action.Data.Awaited)
	}
	batchRow := make(map[string]interface{})
	applied, batchIter, err := h.Session.MapExecuteBatchCAS(batch, batchRow) // TODO: investigate MapExecuteBatchCAS vs ExecuteBatchCAS with manual batch row parsing for perf
	if batchIter != nil {
		batchIter.Close()
	}
	yield(LabelTaskSuspendCommit)
	if err != nil {
		log.Printf("task.suspend batch LWT(%s): %v", id, err)
		return Res[string]{
			Kind: "task.suspend",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if !applied {
		return Res[string]{
			Kind: "task.suspend",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: "Concurrent modification; please retry",
		}
	}

	// 6. Auxiliary: delete lease timeout.
	if row.TaskTLease != nil {
		if err := h.Session.Query(
			`DELETE FROM task_timeouts
			 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
			h.BucketFor(*row.TaskTLease), h.shardFor(id), *row.TaskTLease, origin, id,
		).Exec(); err != nil {
			log.Printf("task.suspend: delete lease timeout(%s): %v", id, err)
		}
		yield(LabelTaskSuspendCleanupTaskTimeoutsLease)
	}

	return Res[TaskSuspend200ResData]{
		Kind: "task.suspend",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// task.fulfill
// ─────────────────────────────────────────────────────────────────────────────

// TaskFulfill implements the task.fulfill action.
//
// Semantics (mirrors spec T-07):
//   - Read full row; 404 if not found or no target.
//   - Lazy timeout projection: if promise pending and now >= timeout_at → 409 "Task not acquired".
//   - If task_state != "acquired" → 409 "Task not acquired".
//   - If task_version != req.version → 409 "Version mismatch".
//   - LWT: SET state=settleState, task_state='fulfilled' ... IF task_state='acquired' AND task_version=req.version.
//   - Auxiliary: delete promise_timeouts entry, delete lease task_timeouts entry.
//   - For each callback awaiter: if suspended → transition to pending + send execute; else → append id to task_resumes.
//   - Send unblock messages to each listener.
//   - Return 200 {promise}.
func (h *Handler) TaskFulfill(head RequestHead, req TaskFulfillData, now int64, yield func(string)) any {
	id := req.ID
	origin, _ := resolveOrigin(head.Origin, "", id)

	// 1. Read full row (may eagerly timeout).
	row, err := h.readAndTryTimeout(id, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "task.fulfill",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}
	if err != nil {
		log.Printf("task.fulfill read(%s): %v", id, err)
		return Res[string]{
			Kind: "task.fulfill",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if row.Task == nil {
		return Res[string]{
			Kind: "task.fulfill",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}

	// 2. State and version checks (helper applied timeout if expired).
	if row.Promise.State != "pending" || row.Task.State != "acquired" {
		return Res[string]{
			Kind: "task.fulfill",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Task not acquired",
		}
	}
	if row.Task.Version != *req.Version {
		return Res[string]{
			Kind: "task.fulfill",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Version mismatch",
		}
	}

	// 4. Determine settle parameters from the inner action.
	settleState := req.Action.Data.State
	settledAtVal := now
	if settledAtVal < row.Promise.CreatedAt {
		settledAtVal = row.Promise.CreatedAt
	}
	settleValueHeaders := req.Action.Data.Value.Headers
	settleValueData := req.Action.Data.Value.Data

	// 5. Build settle statement. Add state='pending' to IF condition so that
	// batchRow["state"] is populated on batch failure, allowing enqueueResume
	// to distinguish concurrent-settle from concurrent-modify.
	settleStmt := `UPDATE promises SET
		     state = ?, value_headers = ?, value_data = ?, settled_at = ?,
		     callbacks = {}, listeners = {},
		     task_state = 'fulfilled', task_pid = null, task_ttl = null,
		     task_timeout_retry = null, task_timeout_lease = null,
		     task_resumes = {}
		 WHERE origin = ? AND id = ?
		 IF state = 'pending' AND task_state = 'acquired' AND task_version = ? AND callbacks = ?
		 AND settled_at = null AND value_headers = null AND value_data = null`
	settleArgs := []interface{}{settleState, settleValueHeaders, settleValueData, settledAtVal, origin, id, *req.Version, row.Callbacks}

	settledAtCopy := settledAtVal
	unblockRec := PromiseRecord{
		ID:        id,
		State:     settleState,
		Param:     row.Promise.Param,
		Value:     Value{Headers: settleValueHeaders, Data: settleValueData},
		Tags:      row.Promise.Tags,
		TimeoutAt: row.Promise.TimeoutAt,
		CreatedAt: row.Promise.CreatedAt,
		SettledAt: &settledAtCopy,
	}

	// 6. Call enqueueResume.
	intended := settledData{State: settleState, ValHdrs: settleValueHeaders, ValData: settleValueData, SettledAt: settledAtVal}
	awaiters, _, enqErr := h.enqueueResume(id, origin, row.Callbacks, settledAtVal, settleStmt, settleArgs, intended, yield)

	// 7. Handle result.
	if awaiters == nil && enqErr == nil {
		// Promise already settled — task is no longer acquired.
		return Res[string]{
			Kind: "task.fulfill",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Task not acquired",
		}
	}
	if enqErr != nil {
		log.Printf("task.fulfill enqueueResume(%s): %v", id, enqErr)
		return Res[string]{
			Kind: "task.fulfill",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: enqErr.Error(),
		}
	}
	for _, a := range awaiters {
		h.sendExecute(a.target, a.id, a.taskVersion)
	}
	h.sendUnblock(row.Listeners, unblockRec)

	// 8. Cleanup (best-effort, only on success).
	h.Session.Query(
		`DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?`,
		h.BucketFor(row.Promise.TimeoutAt), h.shardFor(id), row.Promise.TimeoutAt, origin, id,
	).Exec()
	yield(LabelTaskFulfillCleanupPromiseTimeouts)
	if row.TaskTLease != nil {
		h.Session.Query(
			`DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
			h.BucketFor(*row.TaskTLease), h.shardFor(id), *row.TaskTLease, origin, id,
		).Exec()
		yield(LabelTaskFulfillCleanupTaskTimeoutsLease)
	}

	// 9. Return settled promise.
	settledAtFinal := settledAtVal
	return Res[TaskFulfillResData]{
		Kind: "task.fulfill",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: TaskFulfillResData{
			Promise: PromiseRecord{
				ID:        id,
				State:     settleState,
				Param:     row.Promise.Param,
				Value:     Value{Headers: settleValueHeaders, Data: settleValueData},
				Tags:      row.Promise.Tags,
				TimeoutAt: row.Promise.TimeoutAt,
				CreatedAt: row.Promise.CreatedAt,
				SettledAt: &settledAtFinal,
			},
		},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// task.halt
// ─────────────────────────────────────────────────────────────────────────────

// TaskHalt implements the task.halt action.
//
// Semantics (mirrors spec T-09):
//   - Read row; 404 if not found or no target.
//   - If task_state == 'fulfilled' → 409 "Task is fulfilled".
//   - If task_state == 'halted' → 200 (idempotent).
//   - Otherwise (pending / acquired / suspended): LWT transition to 'halted'.
//   - On success: delete the relevant timeout entry (retry for pending, lease for acquired).
//   - On LWT failure (concurrent modification): 409 "Concurrent modification; please retry".
func (h *Handler) TaskHalt(head RequestHead, req TaskHaltData, now int64, yield func(string)) any {
	id := req.ID
	origin, _ := resolveOrigin(head.Origin, "", id)

	// 1. Read row (may eagerly timeout).
	row, err := h.readAndTryTimeout(id, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "task.halt",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}
	if err != nil {
		log.Printf("task.halt read(%s): %v", id, err)
		return Res[string]{
			Kind: "task.halt",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if row.Task == nil {
		return Res[string]{
			Kind: "task.halt",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}

	// 2. Terminal state checks (helper applied timeout if expired).
	if row.Task.State == "fulfilled" {
		return Res[string]{
			Kind: "task.halt",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Task is fulfilled",
		}
	}
	if row.Task.State == "halted" {
		return Res[struct{}]{
			Kind: "task.halt",
			Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		}
	}

	// 3. LWT: transition to halted.
	lwtRow := make(map[string]interface{})
	applied, err := h.Session.Query(
		`UPDATE promises
		 SET task_state = 'halted',
		     task_pid = null,
		     task_ttl = null,
		     task_timeout_retry = null,
		     task_timeout_lease = null
		 WHERE origin = ? AND id = ?
		 IF task_state IN ('pending', 'acquired', 'suspended')`,
		origin, id,
	).MapScanCAS(lwtRow)
	yield(LabelTaskHaltCommit)

	if err != nil {
		log.Printf("task.halt LWT(%s): %v", id, err)
		return Res[string]{
			Kind: "task.halt",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	if !applied {
		switch lwtRow["task_state"] {
		case "halted":
			return Res[struct{}]{
				Kind: "task.halt",
				Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
			}
		case "fulfilled":
			return Res[string]{
				Kind: "task.halt",
				Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
				Data: "Task is already fulfilled",
			}
		default:
			log.Printf("task.halt LWT(%s): unexpected task_state=%v", id, lwtRow["task_state"])
			return Res[string]{
				Kind: "task.halt",
				Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
				Data: "Unexpected task state after LWT failure",
			}
		}
	}

	// 4. Auxiliary: delete the relevant timeout entry.
	switch row.Task.State {
	case "acquired":
		if row.TaskTLease != nil {
			h.Session.Query(
				`DELETE FROM task_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
				h.BucketFor(*row.TaskTLease), h.shardFor(id), *row.TaskTLease, origin, id,
			).Exec()
			yield(LabelTaskHaltCleanupTaskTimeoutsLease)
		}
	case "pending":
		if row.TaskTRetry != nil {
			h.Session.Query(
				`DELETE FROM task_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
				h.BucketFor(*row.TaskTRetry), h.shardFor(id), *row.TaskTRetry, origin, id,
			).Exec()
			yield(LabelTaskHaltCleanupTaskTimeoutsRetry)
		}
	}

	return Res[struct{}]{
		Kind: "task.halt",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// task.continue
// ─────────────────────────────────────────────────────────────────────────────

// TaskContinue implements the task.continue action.
//
// Semantics (mirrors spec T-10):
//   - Read row; 404 if not found or no target.
//   - LWT: IF task_state = 'halted' → pending, version+1, set retry timeout.
//   - On LWT failure: 409 "Task is not halted".
//   - Auxiliary: INSERT retry task_timeouts entry; send execute message.
//   - Return 200 {} or 404/409.
func (h *Handler) TaskContinue(head RequestHead, req TaskContinueData, now int64, yield func(string)) any {
	id := req.ID
	origin, _ := resolveOrigin(head.Origin, "", id)

	// 1. Read row (may eagerly timeout).
	row, err := h.readAndTryTimeout(id, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "task.continue",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}
	if err != nil {
		log.Printf("task.continue read(%s): %v", id, err)
		return Res[string]{
			Kind: "task.continue",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if row.Task == nil {
		return Res[string]{
			Kind: "task.continue",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Task not found",
		}
	}

	// 2. LWT: halted → pending (version unchanged; bumps only on pending→acquired).
	retryAt := now + RetryTimeout

	// Pre-insert retry timeout before the LWT so a kill between the two leaves an
	// orphan type=0 entry rather than a pending task with no retry timeout.
	if err := h.Session.Query(
		`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)`,
		h.BucketFor(retryAt), h.shardFor(id), retryAt, id, origin, row.Promise.TimeoutAt,
	).Exec(); err != nil {
		log.Printf("task.continue: pre-insert retry timeout(%s): %v", id, err)
		return Res[string]{
			Kind: "task.continue",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	yield(LabelTaskContinuePreinsertTaskTimeoutsRetry)

	lwtRow := make(map[string]interface{})
	applied, err := h.Session.Query(
		`UPDATE promises
		 SET task_state = 'pending',
		     task_timeout_retry = ?,
		     task_timeout_lease = null
		 WHERE origin = ? AND id = ?
		 IF task_state = 'halted'`,
		retryAt,
		origin, id,
	).MapScanCAS(lwtRow)
	yield(LabelTaskContinueCommit)
	if err != nil {
		log.Printf("task.continue LWT(%s): %v", id, err)
		return Res[string]{
			Kind: "task.continue",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	if !applied {
		// Only keep the pre-inserted entry if the task already owned a retry timeout
		// at exactly retryAt (our INSERT was idempotent on an existing entry). The LWT
		// failure response only returns IF-clause columns, so use the value from the
		// initial read instead.
		if row.TaskTRetry == nil || *row.TaskTRetry != retryAt {
			h.Session.Query(
				`DELETE FROM task_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
				h.BucketFor(retryAt), h.shardFor(id), retryAt, origin, id,
			).Exec()
			yield(LabelTaskContinueRollbackTaskTimeoutsRetry)
		}
		return Res[string]{
			Kind: "task.continue",
			Head: ResponseHead{CorrID: head.CorrID, Status: 409, Version: head.Version},
			Data: "Task is not halted",
		}
	}

	// 3. Auxiliary: send execute message. Retry timeout was pre-inserted above.
	h.sendExecute(row.Target, id, row.Task.Version)

	return Res[struct{}]{
		Kind: "task.continue",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
	}
}

// taskCreateReacquire handles the re-acquire path (existing promise pending, task pending).
func (h *Handler) taskCreateReacquire(
	head RequestHead,
	req TaskCreateData,
	id, origin string,
	now int64,
	yield func(string),
	row map[string]interface{},
	existingTaskVersion int,
	existingParamHdrs map[string]string,
	existingParamData string,
	existingTags map[string]string,
	existingTimeoutAt, existingCreatedAt int64,
) any {
	newVersion := existingTaskVersion + 1
	leaseAt := now + int64(*req.TTL)

	// Pre-insert lease timeout before the LWT so a kill between the two leaves an
	// orphan type=1 entry rather than an acquired task with no lease timeout.
	if err := h.Session.Query(
		`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 1, ?, ?, ?)`,
		h.BucketFor(leaseAt), h.shardFor(id), leaseAt, id, origin, existingTimeoutAt,
	).Exec(); err != nil {
		log.Printf("task.create re-acquire: pre-insert lease timeout(%s): %v", id, err)
		return Res[string]{
			Kind: "task.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	yield(LabelTaskCreatePreinsertTaskTimeoutsLease)

	// LWT UPDATE: pending → acquired, bump version explicitly.
	reRow := make(map[string]interface{})
	reApplied, err := h.Session.Query(
		`UPDATE promises
		 SET task_state = 'acquired',
		     task_pid = ?,
		     task_ttl = ?,
		     task_version = ?,
		     task_resumes = {},
		     task_timeout_retry = null,
		     task_timeout_lease = ?
		 WHERE origin = ? AND id = ?
		 IF task_state = 'pending' AND task_version = ?`,
		*req.PID, int64(*req.TTL), newVersion, leaseAt,
		origin, id,
		existingTaskVersion,
	).MapScanCAS(reRow)
	yield(LabelTaskCreateCommit)
	if err != nil {
		log.Printf("task.create re-acquire LWT(%s): %v", id, err)
		return Res[string]{
			Kind: "task.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if !reApplied {
		// Cleanup pre-inserted lease entry on concurrent modification.
		existingLeaseAt, _ := reRow["task_timeout_lease"].(int64)
		if existingLeaseAt != leaseAt {
			h.Session.Query(
				`DELETE FROM task_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
				h.BucketFor(leaseAt), h.shardFor(id), leaseAt, origin, id,
			).Exec()
			yield(LabelTaskCreateRollbackTaskTimeoutsLease)
		}
		return Res[string]{
			Kind: "task.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: "Concurrent modification; please retry",
		}
	}

	// Auxiliary: delete old retry timeout. Lease timeout was pre-inserted above.
	if retryRaw := row["task_timeout_retry"]; retryRaw != nil {
		if retryAt, ok := retryRaw.(int64); ok {
			h.Session.Query(
				`DELETE FROM task_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
				h.BucketFor(retryAt), h.shardFor(id), retryAt, origin, id,
			).Exec()
			yield(LabelTaskCreateCleanupTaskTimeoutsRetry)
		}
	}

	ttlInt := *req.TTL
	taskRec := TaskRecord{
		ID:      id,
		State:   "acquired",
		Version: newVersion,
		Resumes: json.RawMessage("[]"),
		PID:     *req.PID,
		TTL:     &ttlInt,
	}
	promRec := PromiseRecord{
		ID:        id,
		State:     "pending",
		Param:     Value{Headers: existingParamHdrs, Data: existingParamData},
		Tags:      existingTags,
		TimeoutAt: existingTimeoutAt,
		CreatedAt: existingCreatedAt,
	}

	return Res[TaskCreateResData]{
		Kind: "task.create",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: TaskCreateResData{
			Task:    taskRec,
			Promise: promRec,
			Preload: []PromiseRecord{},
		},
	}
}
