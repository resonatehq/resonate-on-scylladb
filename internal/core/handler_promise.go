package core

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"

	"github.com/gocql/gocql"
)

// ─────────────────────────────────────────────────────────────────────────────
// promise.get
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handler) PromiseGet(head RequestHead, req PromiseGetData, now int64, yield func(string)) any {
	id := req.ID
	origin, _ := resolveOrigin(head.Origin, "", id)

	row, err := h.readAndTryTimeout(id, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "promise.get",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Promise not found",
		}
	}
	if err != nil {
		slog.Error("promise.get read", "id", id, "err", err)
		return Res[string]{
			Kind: "promise.get",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	return Res[PromiseGetResData]{
		Kind: "promise.get",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: PromiseGetResData{Promise: row.Promise},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Read helpers
// ─────────────────────────────────────────────────────────────────────────────

// readPromise reads the full PromiseRecord for (origin, id).
// Returns gocql.ErrNotFound if no row exists.
func (h *Handler) readPromise(id, origin string, yield func(string)) (PromiseRecord, error) {
	var (
		state                      string
		paramHeaders, valueHeaders map[string]string
		paramData, valueData       string
		tags                       map[string]string
		timeoutAt, createdAt       int64
		settledAt                  *int64
	)
	err := h.Session.Query(
		`SELECT state, param_headers, param_data, value_headers, value_data,
		        tags, timeout_at, created_at, settled_at
		 FROM promises WHERE origin = ? AND id = ?`,
		origin, id,
	).Scan(
		&state, &paramHeaders, &paramData, &valueHeaders, &valueData,
		&tags, &timeoutAt, &createdAt, &settledAt,
	)
	yield(LabelPromiseRead)
	if err != nil {
		return PromiseRecord{}, err
	}
	if tags == nil {
		tags = map[string]string{}
	}
	return PromiseRecord{
		ID:        id,
		State:     state,
		Param:     Value{Headers: paramHeaders, Data: paramData},
		Value:     Value{Headers: valueHeaders, Data: valueData},
		Tags:      tags,
		TimeoutAt: timeoutAt,
		CreatedAt: createdAt,
		SettledAt: settledAt,
	}, nil
}

// promiseRow is the internal result of readAndTryTimeout. It carries every
// column needed by any handler so callers avoid separate per-field SELECTs.
type promiseRow struct {
	Promise     PromiseRecord
	Task        *TaskRecord // nil when target == ""
	Target      string      // task delivery address; "" if no task
	Callbacks   []string    // promise callbacks at read time
	Listeners   []string    // promise listeners at read time
	TaskResumes []string    // task resumes at read time (raw, for LWT conditions)
	TaskTRetry  *int64      // task retry timeout; nil if none
	TaskTLease  *int64      // task lease timeout; nil if none
}

// readAndTryTimeout reads the full promise/task row for (origin, id) and
// eagerly applies a timeout if the promise is pending past its deadline.
// Returns gocql.ErrNotFound when no row exists. On success the returned
// Promise always reflects the post-timeout state.
func (h *Handler) readAndTryTimeout(
	id, origin string,
	now int64,
	yield func(string),
) (*promiseRow, error) {
	var (
		state                      string
		paramHeaders, valueHeaders map[string]string
		paramData, valueData       string
		tags                       map[string]string
		timeoutAt, createdAt       int64
		settledAt                  *int64
		target                     string
		taskState                  string
		taskVersion                int
		taskPID                    *string
		taskTTL                    *int64
		taskResumes                []string
		taskTimeoutRetry           *int64
		taskTimeoutLease           *int64
		listeners                  []string
		callbacks                  []string
	)
	err := h.Session.Query(
		`SELECT state,
		        param_headers, param_data, value_headers, value_data,
		        tags, timeout_at, created_at, settled_at,
		        target,
		        task_state, task_version, task_pid, task_ttl, task_resumes,
		        task_timeout_retry, task_timeout_lease,
		        listeners, callbacks
		 FROM promises WHERE origin = ? AND id = ?`,
		origin, id,
	).Scan(
		&state,
		&paramHeaders, &paramData, &valueHeaders, &valueData,
		&tags, &timeoutAt, &createdAt, &settledAt,
		&target,
		&taskState, &taskVersion, &taskPID, &taskTTL, &taskResumes,
		&taskTimeoutRetry, &taskTimeoutLease,
		&listeners, &callbacks,
	)
	yield(LabelPromiseRead)
	if err != nil {
		return nil, err
	}
	if tags == nil {
		tags = map[string]string{}
	}

	pid := ""
	if taskPID != nil {
		pid = *taskPID
	}

	promise := PromiseRecord{
		ID:        id,
		State:     state,
		Param:     Value{Headers: paramHeaders, Data: paramData},
		Value:     Value{Headers: valueHeaders, Data: valueData},
		Tags:      tags,
		TimeoutAt: timeoutAt,
		CreatedAt: createdAt,
		SettledAt: settledAt,
	}

	var task *TaskRecord
	if target != "" {
		sort.Strings(taskResumes)
		resumesJSON := json.RawMessage("[]")
		if len(taskResumes) > 0 {
			if b, marshalErr := json.Marshal(taskResumes); marshalErr == nil {
				resumesJSON = b
			}
		}
		tr := TaskRecord{
			ID:      id,
			State:   taskState,
			Version: taskVersion,
			Resumes: resumesJSON,
			PID:     pid,
		}
		if taskTTL != nil {
			ttlInt := int(*taskTTL)
			tr.TTL = &ttlInt
		}
		task = &tr
	}

	row := &promiseRow{
		Promise:     promise,
		Task:        task,
		Target:      target,
		Callbacks:   callbacks,
		Listeners:   listeners,
		TaskResumes: taskResumes,
		TaskTRetry:  taskTimeoutRetry,
		TaskTLease:  taskTimeoutLease,
	}

	if state == "pending" && now >= timeoutAt {
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
		sd, tryErr := h.tryTimeout(in, now, yield)
		if tryErr != nil {
			return nil, tryErr
		}
		settledAtVal := sd.SettledAt
		row.Promise.State = sd.State
		row.Promise.Value = Value{Headers: sd.ValHdrs, Data: sd.ValData}
		row.Promise.SettledAt = &settledAtVal
		if task != nil {
			row.Task.State = "fulfilled"
			row.Task.PID = ""
			row.Task.TTL = nil
			row.Task.Resumes = json.RawMessage("[]")
		}
	}

	return row, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// promise.create
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handler) PromiseCreate(head RequestHead, req PromiseCreateData, now int64, yield func(string)) Res[PromiseCreateResData] {
	id := *req.ID
	origin, err := resolveOrigin(head.Origin, req.Tags["resonate:origin"], id)
	if err != nil {
		return Res[PromiseCreateResData]{
			Kind: "promise.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
		}
	}
	target := req.Tags["resonate:target"]
	hasTask := target != ""

	// 1. Compute initial state
	var (
		state     string
		createdAt int64
		settledAt *int64
	)
	var taskStateArg *string
	var taskVersionArg *int

	// Pre-compute task retry timeout for pending tasks so it can be included
	// atomically in the LWT INSERT (avoids a two-phase write that would leave
	// task_timeout_retry null between the INSERT and a follow-up UPDATE).
	var taskTimeoutRetryArg *int64
	var taskRetryImmediate bool
	var taskRetryAt int64

	if now >= *req.TimeoutAt {
		// born expired
		if req.Tags["resonate:timer"] == "true" {
			state = "resolved"
		} else {
			state = "rejected_timedout"
		}
		createdAt = *req.TimeoutAt
		sa := *req.TimeoutAt
		settledAt = &sa
		if hasTask {
			ts := "fulfilled"
			tv := 0
			taskStateArg = &ts
			taskVersionArg = &tv
		}
	} else {
		state = "pending"
		createdAt = now
		if hasTask {
			ts := "pending"
			tv := 0
			taskStateArg = &ts
			taskVersionArg = &tv

			// Compute retry timeout: use delay tag if present, else now+RetryTimeout.
			taskRetryImmediate = true
			if ds := req.Tags["resonate:delay"]; ds != "" {
				if d, parseErr := strconv.ParseInt(ds, 10, 64); parseErr == nil && d > now {
					taskRetryAt = d
					taskRetryImmediate = false
				}
			}
			if taskRetryImmediate {
				taskRetryAt = now + RetryTimeout
			}
			taskTimeoutRetryArg = &taskRetryAt
		}
	}

	tags := req.Tags
	if tags == nil {
		tags = map[string]string{}
	}

	// 2a. Pre-insert promise_timeouts (and task_timeouts for tasks) before the LWT so
	// kills between here and the LWT leave orphan entries rather than a committed
	// promise/task row with no corresponding timeout entry.
	if state == "pending" {
		if err := h.Session.Query(
			`INSERT INTO promise_timeouts (bucket, shard, timeout_at, promise_id, origin) VALUES (?, ?, ?, ?, ?)`,
			h.BucketFor(*req.TimeoutAt), h.shardFor(id), *req.TimeoutAt, id, origin,
		).Exec(); err != nil {
			slog.Error("promise.create: pre-insert promise_timeouts", "id", id, "err", err)
			return Res[PromiseCreateResData]{
				Kind: "promise.create",
				Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			}
		}
		yield(LabelPromiseCreatePreinsertPromiseTimeouts)
		if hasTask {
			if err := h.Session.Query(
				`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)`,
				h.BucketFor(taskRetryAt), h.shardFor(id), taskRetryAt, id, origin, *req.TimeoutAt,
			).Exec(); err != nil {
				slog.Error("promise.create: pre-insert task_timeouts", "id", id, "err", err)
				return Res[PromiseCreateResData]{
					Kind: "promise.create",
					Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
				}
			}
			yield(LabelPromiseCreatePreinsertTaskTimeoutsRetry)
		}
	}

	// 2b. LWT INSERT — task_timeout_retry is included atomically so that
	// task.acquire always sees a non-null value when it reads the row.
	row := make(map[string]interface{})
	applied, err := h.Session.Query(
		`INSERT INTO promises (
		    id, origin, branch, parent, target,
		    state, param_headers, param_data,
		    value_headers, value_data,
		    tags, timeout_at, created_at, settled_at,
		    callbacks, listeners,
		    task_state, task_version, task_ttl, task_pid, task_resumes,
		    task_timeout_retry
		) VALUES (
		    ?, ?, ?, ?, ?,
		    ?, ?, ?,
		    null, null,
		    ?, ?, ?, ?,
		    {}, {},
		    ?, ?, null, null, null,
		    ?
		) IF NOT EXISTS`,
		id, origin,
		req.Tags["resonate:branch"],
		req.Tags["resonate:parent"],
		target,
		state, req.Param.Headers, req.Param.Data,
		tags, *req.TimeoutAt, createdAt, settledAt,
		taskStateArg, taskVersionArg,
		taskTimeoutRetryArg,
	).MapScanCAS(row)
	yield(LabelPromiseCreateCommit)
	if err != nil {
		slog.Error("promise.create LWT", "id", id, "err", err)
		return Res[PromiseCreateResData]{
			Kind: "promise.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
		}
	}

	if !applied {
		// The failed LWT returns the full existing row — extract it directly.
		existingState, _ := row["state"].(string)
		existingTimeoutAt, _ := row["timeout_at"].(int64)
		existingCreatedAt, _ := row["created_at"].(int64)
		existingParamHeaders, _ := row["param_headers"].(map[string]string)
		existingParamData, _ := row["param_data"].(string)
		existingValueHeaders, _ := row["value_headers"].(map[string]string)
		existingValueData, _ := row["value_data"].(string)
		existingTags, _ := row["tags"].(map[string]string)
		if existingTags == nil {
			existingTags = map[string]string{}
		}
		var existingSettledAt *int64
		if existingState != "pending" {
			sv, _ := row["settled_at"].(int64)
			existingSettledAt = &sv
		}

		// Roll back the pre-inserted timeout entries unless the existing row
		// legitimately owns an entry at that PK — i.e. the existing promise is
		// still pending with the same timeout (promise_timeouts), or the
		// existing task is pending with the same retry deadline (task_timeouts
		// type=0). Settled promises and non-pending tasks have no legitimate
		// hint row, so any entry at that PK is the one we just pre-inserted.
		if state == "pending" {
			if !(existingState == "pending" && existingTimeoutAt == *req.TimeoutAt) {
				h.Session.Query(
					`DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?`,
					h.BucketFor(*req.TimeoutAt), h.shardFor(id), *req.TimeoutAt, origin, id,
				).Exec()
				yield(LabelPromiseCreateRollbackPromiseTimeouts)
			}
			if hasTask {
				existingTaskState, _ := row["task_state"].(string)
				existingRetryAt, _ := row["task_timeout_retry"].(int64)
				if !(existingTaskState == "pending" && existingRetryAt == taskRetryAt) {
					h.Session.Query(
						`DELETE FROM task_timeouts
						 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
						h.BucketFor(taskRetryAt), h.shardFor(id), taskRetryAt, origin, id,
					).Exec()
					yield(LabelPromiseCreateRollbackTaskTimeoutsRetry)
				}
			}
		}

		// Eagerly settle if expired.
		if existingState == "pending" && now >= existingTimeoutAt {
			existingTarget, _ := row["target"].(string)
			existingTaskState, _ := row["task_state"].(string)
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
				existingCallbacks = nil // MapScanCAS returns []string{} for null SET; normalize for IF callbacks = ?
			}
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
				slog.Warn("promise.create tryTimeout", "id", id, "err", tryErr)
				return Res[PromiseCreateResData]{
					Kind: "promise.create",
					Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
				}
			}
			settledAtFinal := sd.SettledAt
			return Res[PromiseCreateResData]{
				Kind: "promise.create",
				Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
				Data: PromiseCreateResData{
					Promise: PromiseRecord{
						ID:        id,
						State:     sd.State,
						Param:     Value{Headers: existingParamHeaders, Data: existingParamData},
						Value:     Value{Headers: sd.ValHdrs, Data: sd.ValData},
						Tags:      existingTags,
						TimeoutAt: existingTimeoutAt,
						CreatedAt: existingCreatedAt,
						SettledAt: &settledAtFinal,
					},
				},
			}
		}

		return Res[PromiseCreateResData]{
			Kind: "promise.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
			Data: PromiseCreateResData{
				Promise: PromiseRecord{
					ID:        id,
					State:     existingState,
					Param:     Value{Headers: existingParamHeaders, Data: existingParamData},
					Value:     Value{Headers: existingValueHeaders, Data: existingValueData},
					Tags:      existingTags,
					TimeoutAt: existingTimeoutAt,
					CreatedAt: existingCreatedAt,
					SettledAt: existingSettledAt,
				},
			},
		}
	}

	// 3. Auxiliary writes — task_timeouts was pre-inserted above.
	if state == "pending" && hasTask && taskRetryImmediate {
		// Immediately dispatch execute so a worker can pick up the task without
		// waiting for the retry timeout to fire.
		h.sendExecute(target, id, 0)
	}

	return Res[PromiseCreateResData]{
		Kind: "promise.create",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: PromiseCreateResData{
			Promise: PromiseRecord{
				ID:        id,
				State:     state,
				Param:     *req.Param,
				Tags:      tags,
				TimeoutAt: *req.TimeoutAt,
				CreatedAt: createdAt,
				SettledAt: settledAt,
			},
		},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// promise.register_callback
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handler) PromiseRegisterCallback(head RequestHead, req PromiseRegisterCallbackData, now int64, yield func(string)) any {
	awaitedID := req.Awaited
	awaiterID := req.Awaiter
	origin, _ := resolveOrigin(head.Origin, "", awaitedID)

	// Reject self-callbacks: schema refinement, returns 400.
	if awaiterID == awaitedID {
		return Res[string]{
			Kind: "promise.register_callback",
			Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
			Data: "Awaited and awaiter must be different promises",
		}
	}

	// 1. Read awaited promise (may eagerly timeout).
	awaitedRow, err := h.readAndTryTimeout(awaitedID, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "promise.register_callback",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Awaited promise not found",
		}
	}
	if err != nil {
		slog.Error("promise.register_callback read awaited", "id", awaitedID, "err", err)
		return Res[string]{
			Kind: "promise.register_callback",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	// 2. Read awaiter promise (may eagerly timeout).
	awaiterRow, err := h.readAndTryTimeout(awaiterID, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "promise.register_callback",
			Head: ResponseHead{CorrID: head.CorrID, Status: 422, Version: head.Version},
			Data: "Awaiter promise not found",
		}
	}
	if err != nil {
		slog.Error("promise.register_callback read awaiter", "id", awaiterID, "err", err)
		return Res[string]{
			Kind: "promise.register_callback",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	// 3. Validate awaiter has a task (delivery address).
	if awaiterRow.Task == nil {
		return Res[string]{
			Kind: "promise.register_callback",
			Head: ResponseHead{CorrID: head.CorrID, Status: 422, Version: head.Version},
			Data: "Awaiter has no address",
		}
	}

	// 4-5. If either side is settled, no callback can be registered.
	if awaiterRow.Promise.State != "pending" || awaitedRow.Promise.State != "pending" {
		if awaitedRow.Promise.State != "pending" {
			if err := h.resumeCallbackAwaiter(
				awaitedID, origin,
				awaiterID,
				awaiterRow.Task.State,
				awaiterRow.Task.Version, awaiterRow.Target,
				awaiterRow.Promise.TimeoutAt,
				now, yield,
			); err != nil {
				slog.Error("promise.register_callback resumeCallbackAwaiter", "awaited_id", awaitedID, "awaiter_id", awaiterID, "err", err)
				return Res[string]{
					Kind: "promise.register_callback",
					Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
					Data: err.Error(),
				}
			}
		}
		return Res[PromiseRegisterCallbackResData]{
			Kind: "promise.register_callback",
			Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
			Data: PromiseRegisterCallbackResData{Promise: awaitedRow.Promise},
		}
	}

	// 6. Register callback via LWT (guards against concurrent settle).
	lwtRow := make(map[string]interface{})
	applied, err := h.Session.Query(
		`UPDATE promises SET callbacks = callbacks + ? WHERE origin = ? AND id = ? IF state = 'pending'`,
		[]string{awaiterID}, origin, awaitedID,
	).MapScanCAS(lwtRow)
	yield(LabelPromiseRegisterCallbackCommitAwaited)
	if err != nil {
		slog.Error("promise.register_callback LWT", "awaited_id", awaitedID, "awaiter_id", awaiterID, "err", err)
		return Res[string]{
			Kind: "promise.register_callback",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	if !applied {
		// Concurrent settle won — re-read and return current state.
		pr, readErr := h.readPromise(awaitedID, origin, yield)
		if readErr != nil {
			slog.Warn("promise.register_callback concurrent readback", "id", awaitedID, "err", readErr)
			return Res[string]{
				Kind: "promise.register_callback",
				Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
				Data: readErr.Error(),
			}
		}
		return Res[PromiseRegisterCallbackResData]{
			Kind: "promise.register_callback",
			Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
			Data: PromiseRegisterCallbackResData{Promise: pr},
		}
	}

	// 7. Return awaited promise (still pending, callback registered).
	return Res[PromiseRegisterCallbackResData]{
		Kind: "promise.register_callback",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: PromiseRegisterCallbackResData{Promise: awaitedRow.Promise},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// promise.register_listener
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handler) PromiseRegisterListener(head RequestHead, req PromiseRegisterListenerData, now int64, yield func(string)) any {
	awaitedID := req.Awaited
	awaitedOrigin, _ := resolveOrigin(head.Origin, "", awaitedID)
	address := req.Address

	// 1. Read awaited promise (may eagerly timeout).
	row, err := h.readAndTryTimeout(awaitedID, awaitedOrigin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "promise.register_listener",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Promise not found",
		}
	}
	if err != nil {
		slog.Error("promise.register_listener read", "id", awaitedID, "err", err)
		return Res[string]{
			Kind: "promise.register_listener",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	// Already settled (or eagerly timed out by helper).
	if row.Promise.State != "pending" {
		return Res[PromiseRegisterListenerResData]{
			Kind: "promise.register_listener",
			Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
			Data: PromiseRegisterListenerResData{Promise: row.Promise},
		}
	}

	// 2. LWT: add address to listeners set, guarded against concurrent settle.
	lwtRow := make(map[string]interface{})
	applied, err := h.Session.Query(
		`UPDATE promises SET listeners = listeners + ? WHERE origin = ? AND id = ? IF state = 'pending'`,
		[]string{address}, awaitedOrigin, awaitedID,
	).MapScanCAS(lwtRow)
	yield(LabelPromiseRegisterListenerCommit)
	if err != nil {
		slog.Error("promise.register_listener LWT", "id", awaitedID, "err", err)
		return Res[string]{
			Kind: "promise.register_listener",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	if !applied {
		// Concurrent settle won — re-read and return current state.
		pr, readErr := h.readPromise(awaitedID, awaitedOrigin, yield)
		if readErr != nil {
			slog.Warn("promise.register_listener concurrent readback", "id", awaitedID, "err", readErr)
			return Res[string]{
				Kind: "promise.register_listener",
				Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
				Data: readErr.Error(),
			}
		}
		return Res[PromiseRegisterListenerResData]{
			Kind: "promise.register_listener",
			Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
			Data: PromiseRegisterListenerResData{Promise: pr},
		}
	}

	// 3. Return the pending promise (state at time of read).
	return Res[PromiseRegisterListenerResData]{
		Kind: "promise.register_listener",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: PromiseRegisterListenerResData{Promise: row.Promise},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// promise.settle
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handler) PromiseSettle(head RequestHead, req PromiseSettleData, now int64, yield func(string)) any {
	id := req.ID
	origin, _ := resolveOrigin(head.Origin, "", id)

	// 1. Read current state (may eagerly timeout).
	row, err := h.readAndTryTimeout(id, origin, now, yield)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "promise.settle",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Promise not found",
		}
	}
	if err != nil {
		slog.Error("promise.settle read", "id", id, "err", err)
		return Res[string]{
			Kind: "promise.settle",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	// 2. Already settled (or eagerly timed out by helper) — return current state.
	if row.Promise.State != "pending" {
		return Res[PromiseSettleResData]{
			Kind: "promise.settle",
			Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
			Data: PromiseSettleResData{Promise: row.Promise},
		}
	}

	// 3. Promise is pending and not expired; proceed with explicit settle.
	settleState := req.State
	settledAtVal := now
	if settledAtVal < row.Promise.CreatedAt {
		settledAtVal = row.Promise.CreatedAt
	}
	settleValueHeaders := req.Value.Headers
	settleValueData := req.Value.Data

	// 4. Build settle statement (two variants: with task, without task).
	var settleStmt string
	var settleArgs []interface{}
	if row.Target != "" {
		settleStmt = `UPDATE promises SET
		     state = ?, value_headers = ?, value_data = ?, settled_at = ?,
		     callbacks = {}, listeners = {},
		     task_state = 'fulfilled', task_pid = null, task_ttl = null,
		     task_timeout_retry = null, task_timeout_lease = null,
		     task_resumes = {}
		 WHERE origin = ? AND id = ?
		 IF state = 'pending' AND callbacks = ?
		 AND settled_at = null AND value_headers = null AND value_data = null`
		settleArgs = []interface{}{settleState, settleValueHeaders, settleValueData, settledAtVal, origin, id, row.Callbacks}
	} else {
		settleStmt = `UPDATE promises SET
		     state = ?, value_headers = ?, value_data = ?, settled_at = ?,
		     callbacks = {}, listeners = {}
		 WHERE origin = ? AND id = ?
		 IF state = 'pending' AND callbacks = ?
		 AND settled_at = null AND value_headers = null AND value_data = null`
		settleArgs = []interface{}{settleState, settleValueHeaders, settleValueData, settledAtVal, origin, id, row.Callbacks}
	}

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

	// 5. Call enqueueResume.
	intended := settledData{State: settleState, ValHdrs: settleValueHeaders, ValData: settleValueData, SettledAt: settledAtVal}
	awaiters, sd, enqErr := h.enqueueResume(id, origin, row.Callbacks, settledAtVal, settleStmt, settleArgs, intended, yield)

	// 6. Handle result.
	if enqErr != nil {
		slog.Error("promise.settle enqueueResume", "id", id, "err", enqErr)
		return Res[string]{
			Kind: "promise.settle",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: enqErr.Error(),
		}
	}
	if awaiters != nil {
		// This call won the settle.
		for _, a := range awaiters {
			h.sendExecute(a.target, a.id, a.taskVersion)
		}
		h.sendUnblock(row.Listeners, unblockRec)

		// 7. Cleanup (best-effort, only on success).
		h.Session.Query(
			`DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?`,
			h.BucketFor(row.Promise.TimeoutAt), h.shardFor(id), row.Promise.TimeoutAt, origin, id,
		).Exec()
		yield(LabelPromiseSettleCleanupPromiseTimeouts)
		if row.Target != "" {
			if row.TaskTRetry != nil {
				h.Session.Query(
					`DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
					h.BucketFor(*row.TaskTRetry), h.shardFor(id), *row.TaskTRetry, origin, id,
				).Exec()
				yield(LabelPromiseSettleCleanupTaskTimeoutsRetry)
			}
			if row.TaskTLease != nil {
				h.Session.Query(
					`DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 1 AND origin = ? AND task_id = ?`,
					h.BucketFor(*row.TaskTLease), h.shardFor(id), *row.TaskTLease, origin, id,
				).Exec()
				yield(LabelPromiseSettleCleanupTaskTimeoutsLease)
			}
		}
	}

	// 8. Return settled promise (from sd, whether we won or a concurrent settle won).
	settledAtFinal := sd.SettledAt
	return Res[PromiseSettleResData]{
		Kind: "promise.settle",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: PromiseSettleResData{
			Promise: PromiseRecord{
				ID:        id,
				State:     sd.State,
				Param:     row.Promise.Param,
				Value:     Value{Headers: sd.ValHdrs, Data: sd.ValData},
				Tags:      row.Promise.Tags,
				TimeoutAt: row.Promise.TimeoutAt,
				CreatedAt: row.Promise.CreatedAt,
				SettledAt: &settledAtFinal,
			},
		},
	}
}

// resumeCallbackAwaiter signals a single awaiter to resume after its awaited
// promise was found already settled during PromiseRegisterCallback.
// It mirrors the per-awaiter logic inside enqueueResume but without the settle
// stmt, since the awaited promise is already settled.
func (h *Handler) resumeCallbackAwaiter(
	awaitedID string,
	origin string,
	awaiterID string,
	taskState string,
	taskVersion int,
	target string,
	timeoutAt int64,
	now int64,
	yield func(string),
) error {
	switch taskState {
	case "fulfilled":
		// no statement

	case "suspended":
		retryAt := now + RetryTimeout
		if err := h.Session.Query(
			`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)`,
			h.BucketFor(retryAt), h.shardFor(awaiterID), retryAt, awaiterID, origin, timeoutAt,
		).Exec(); err != nil {
			return err
		}
		yield(LabelPromiseRegisterCallbackResumePreinsert)

		lwtRow := make(map[string]interface{})
		applied, err := h.Session.Query(
			`UPDATE promises SET task_state = 'pending', task_resumes = ?, task_timeout_retry = ?
			 WHERE origin = ? AND id = ?
			 IF task_state = 'suspended'`,
			[]string{awaitedID}, retryAt, origin, awaiterID,
		).MapScanCAS(lwtRow)
		yield(LabelPromiseRegisterCallbackResumeCommit)

		if err != nil {
			h.Session.Query(
				`DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
				h.BucketFor(retryAt), h.shardFor(awaiterID), retryAt, origin, awaiterID,
			).Exec()
			yield(LabelPromiseRegisterCallbackResumeRollback)
			return err
		}
		if !applied {
			// Awaiter moved on concurrently — preinsert is stale, clean it up.
			h.Session.Query(
				`DELETE FROM task_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
				h.BucketFor(retryAt), h.shardFor(awaiterID), retryAt, origin, awaiterID,
			).Exec()
			yield(LabelPromiseRegisterCallbackResumeRollback)
			return fmt.Errorf("concurrent modification")
		}
		h.sendExecute(target, awaiterID, taskVersion)

	case "pending", "acquired", "halted":
		lwtRow := make(map[string]interface{})
		applied, err := h.Session.Query(
			`UPDATE promises SET task_resumes = task_resumes + ?
			 WHERE origin = ? AND id = ?
			 IF task_state = ?`,
			[]string{awaitedID}, origin, awaiterID, taskState,
		).MapScanCAS(lwtRow)
		yield(LabelPromiseRegisterCallbackResumeCommit)
		if err != nil {
			return err
		}
		if !applied {
			return fmt.Errorf("concurrent modification")
		}
	}

	return nil
}
