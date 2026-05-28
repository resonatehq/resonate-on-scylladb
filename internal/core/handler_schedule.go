package core

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"text/template"
	"time"

	"github.com/gocql/gocql"
	"github.com/robfig/cron/v3"
)

// ─────────────────────────────────────────────────────────────────────────────
// nextCron: compute next cron occurrence
// ─────────────────────────────────────────────────────────────────────────────

// nextCron computes the next occurrence of a cron expression after nowMs
// milliseconds. Accepts 5-field (minute hour dom month dow) or 6-field
// (second minute hour dom month dow) expressions. Returns milliseconds.
func nextCron(cronExpr string, nowMs int64) (int64, error) {
	fields := strings.Fields(cronExpr)
	var parser cron.Parser
	switch len(fields) {
	case 5:
		parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	case 6:
		parser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	default:
		return 0, fmt.Errorf("parse cron %q: expected 5 or 6 fields, got %d", cronExpr, len(fields))
	}
	sched, err := parser.Parse(cronExpr)
	if err != nil {
		return 0, fmt.Errorf("parse cron %q: %w", cronExpr, err)
	}
	now := time.UnixMilli(nowMs).UTC()
	next := sched.Next(now)
	return next.UnixMilli(), nil
}

// ─────────────────────────────────────────────────────────────────────────────
// readSchedule: helper to read a single schedule row
// ─────────────────────────────────────────────────────────────────────────────

func readScheduleRow(session *gocql.Session, origin, id string, yield func(string)) (*ScheduleRecord, error) {
	var (
		sCron          string
		promiseID      string
		promiseTimeout int64
		paramHeaders   map[string]string
		paramData      string
		promiseTags    map[string]string
		nextRunAt      int64
		lastRunAt      *int64
		createdAt      int64
		token          gocql.UUID
	)
	err := session.Query(
		`SELECT cron, promise_id, promise_timeout,
		        promise_param_headers, promise_param_data, promise_tags,
		        next_run_at, last_run_at, created_at, create_token
		 FROM schedules WHERE origin = ? AND id = ?`,
		origin, id,
	).Scan(
		&sCron, &promiseID, &promiseTimeout,
		&paramHeaders, &paramData, &promiseTags,
		&nextRunAt, &lastRunAt, &createdAt, &token,
	)
	yield(LabelScheduleRead)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if promiseTags == nil {
		promiseTags = map[string]string{}
	}
	rec := ScheduleRecord{
		ID:             id,
		Origin:         &origin,
		Cron:           sCron,
		PromiseID:      promiseID,
		PromiseTimeout: promiseTimeout,
		PromiseParam:   Value{Headers: paramHeaders, Data: paramData},
		PromiseTags:    promiseTags,
		NextRunAt:      nextRunAt,
		LastRunAt:      lastRunAt,
		CreatedAt:      createdAt,
		Token:          token.String(),
	}
	return &rec, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// schedule.create (S-02)
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handler) ScheduleCreate(head RequestHead, data ScheduleCreateData, now int64, yield func(string)) any {
	nextRunAt, err := nextCron(data.Cron, now)
	if err != nil {
		return Res[string]{
			Kind: "schedule.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 400, Version: head.Version},
			Data: fmt.Sprintf("invalid cron expression: %v", err),
		}
	}

	origin, _ := resolveOrigin(head.Origin, "", *data.ID)

	promiseTags := data.PromiseTags
	if promiseTags == nil {
		promiseTags = map[string]string{}
	}

	token, _ := gocql.RandomUUID()

	// Pre-insert schedule_timeouts before the LWT (mirrors PromiseCreate pattern).
	// If killed after this but before the LWT, the orphan entry fires, finds no
	// schedule row, and skips cleanup — safe. token=gocql.RandomUUID() makes this
	// row distinct from any concurrent ScheduleCreate's pre-insert.
	if err := h.Session.Query(
		`INSERT INTO schedule_timeouts (bucket, shard, timeout_at, schedule_id, origin, create_token) VALUES (?, ?, ?, ?, ?, ?)`,
		h.BucketFor(nextRunAt), h.shardFor(*data.ID), nextRunAt, *data.ID, origin, token,
	).Exec(); err != nil {
		slog.Error("schedule.create: pre-insert schedule_timeouts", "id", *data.ID, "err", err)
		return Res[string]{
			Kind: "schedule.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	yield(LabelScheduleCreatePreinsertScheduleTimeouts)

	row := make(map[string]interface{})
	applied, err := h.Session.Query(
		`INSERT INTO schedules (
			id, origin, cron, promise_id, promise_timeout,
			promise_param_headers, promise_param_data, promise_tags,
			next_run_at, last_run_at, created_at, create_token
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, null, ?, ?)
		IF NOT EXISTS`,
		*data.ID, origin, data.Cron, *data.PromiseID, *data.PromiseTimeout,
		data.PromiseParam.Headers, data.PromiseParam.Data, promiseTags,
		nextRunAt, now, token,
	).MapScanCAS(row)
	yield(LabelScheduleCreateCommit)
	if err != nil {
		slog.Error("schedule.create LWT", "id", *data.ID, "err", err)
		return Res[string]{
			Kind: "schedule.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	if !applied {
		existingCron, _ := row["cron"].(string)
		existingPromiseID, _ := row["promise_id"].(string)
		existingPromiseTimeout, _ := row["promise_timeout"].(int64)
		existingParamHeaders, _ := row["promise_param_headers"].(map[string]string)
		existingParamData, _ := row["promise_param_data"].(string)
		existingPromiseTags, _ := row["promise_tags"].(map[string]string)
		if existingPromiseTags == nil {
			existingPromiseTags = map[string]string{}
		}
		existingNextRunAt, _ := row["next_run_at"].(int64)
		existingCreatedAt, _ := row["created_at"].(int64)
		existingToken, _ := row["create_token"].(gocql.UUID)
		// gocql MapScanCAS returns int64(0) for null bigint; 0 is never a valid run time.
		var existingLastRunAt *int64
		if v, _ := row["last_run_at"].(int64); v != 0 {
			existingLastRunAt = &v
		}

		// Rollback the pre-inserted timeout only when it is a distinct row from the
		// existing schedule's entry. If token == existingToken the pre-insert was
		// idempotent (same PK) and rolling it back would delete the live entry.
		if token != existingToken {
			h.Session.Query(
				`DELETE FROM schedule_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND schedule_id = ? AND create_token = ?`,
				h.BucketFor(nextRunAt), h.shardFor(*data.ID), nextRunAt, origin, *data.ID, token,
			).Exec()
			yield(LabelScheduleCreateRollbackScheduleTimeouts)
		}
		return Res[ScheduleCreateResData]{
			Kind: "schedule.create",
			Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
			Data: ScheduleCreateResData{Schedule: ScheduleRecord{
				ID:             *data.ID,
				Cron:           existingCron,
				PromiseID:      existingPromiseID,
				PromiseTimeout: existingPromiseTimeout,
				PromiseParam:   Value{Headers: existingParamHeaders, Data: existingParamData},
				PromiseTags:    existingPromiseTags,
				NextRunAt:      existingNextRunAt,
				LastRunAt:      existingLastRunAt,
				CreatedAt:      existingCreatedAt,
			}},
		}
	}

	return Res[ScheduleCreateResData]{
		Kind: "schedule.create",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: ScheduleCreateResData{
			Schedule: ScheduleRecord{
				ID:             *data.ID,
				Cron:           data.Cron,
				PromiseID:      *data.PromiseID,
				PromiseTimeout: *data.PromiseTimeout,
				PromiseParam:   data.PromiseParam,
				PromiseTags:    promiseTags,
				NextRunAt:      nextRunAt,
				CreatedAt:      now,
			},
		},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// schedule.get (S-01)
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handler) ScheduleGet(head RequestHead, data ScheduleGetData, now int64, yield func(string)) any {
	origin, _ := resolveOrigin(head.Origin, "", data.ID)
	rec, err := readScheduleRow(h.Session, origin, data.ID, yield)
	if err != nil {
		slog.Error("schedule.get", "id", data.ID, "err", err)
		return Res[string]{
			Kind: "schedule.get",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if rec == nil {
		return Res[string]{
			Kind: "schedule.get",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Schedule not found",
		}
	}
	rec.Origin = nil
	return Res[ScheduleGetResData]{
		Kind: "schedule.get",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: ScheduleGetResData{Schedule: *rec},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// schedule.delete (S-03)
// ─────────────────────────────────────────────────────────────────────────────

func (h *Handler) ScheduleDelete(head RequestHead, data ScheduleDeleteData, now int64, yield func(string)) any {
	origin, _ := resolveOrigin(head.Origin, "", data.ID)
	var nextRunAt int64
	var token gocql.UUID
	err := h.Session.Query(
		`SELECT next_run_at, create_token FROM schedules WHERE origin = ? AND id = ?`,
		origin, data.ID,
	).Scan(&nextRunAt, &token)
	yield(LabelScheduleDeleteRead)
	if err == gocql.ErrNotFound {
		return Res[string]{
			Kind: "schedule.delete",
			Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
			Data: "Schedule not found",
		}
	} else if err != nil {
		slog.Error("schedule.delete read", "id", data.ID, "err", err)
		return Res[string]{
			Kind: "schedule.delete",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}

	row := make(map[string]interface{})
	applied, err := h.Session.Query(
		`DELETE FROM schedules WHERE origin = ? AND id = ? IF next_run_at = ?`,
		origin, data.ID, nextRunAt,
	).MapScanCAS(row)
	yield(LabelScheduleDeleteCommit)
	if err != nil {
		slog.Error("schedule.delete LWT", "id", data.ID, "err", err)
		return Res[string]{
			Kind: "schedule.delete",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: err.Error(),
		}
	}
	if !applied {
		if _, ok := row["next_run_at"].(int64); !ok {
			return Res[string]{
				Kind: "schedule.delete",
				Head: ResponseHead{CorrID: head.CorrID, Status: 404, Version: head.Version},
				Data: "Schedule not found",
			}
		}
		return Res[string]{
			Kind: "schedule.delete",
			Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
			Data: "Schedule modified concurrently, retry",
		}
	}

	if err := h.Session.Query(
		`DELETE FROM schedule_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND schedule_id = ? AND create_token = ?`,
		h.BucketFor(nextRunAt), h.shardFor(data.ID), nextRunAt, origin, data.ID, token,
	).Exec(); err != nil {
		slog.Warn("schedule.delete cleanup schedule_timeouts", "id", data.ID, "err", err)
	}
	yield(LabelScheduleDeleteCleanupScheduleTimeouts)

	return Res[struct{}]{
		Kind: "schedule.delete",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// onScheduleTimeout
// ─────────────────────────────────────────────────────────────────────────────

// onScheduleTimeout fires all due schedule occurrences from timeoutAt up to now,
// creates the resulting promises, and advances the schedule row to the first
// future occurrence in a single LWT.
func (h *Handler) onScheduleTimeout(origin, id string, timeoutAt int64, token gocql.UUID, now int64, yield func(string)) (err error) {
	defer func() {
		if err == nil {
			if delErr := h.Session.Query(
				`DELETE FROM schedule_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND schedule_id = ? AND create_token = ?`,
				h.BucketFor(timeoutAt), h.shardFor(id), timeoutAt, origin, id, token,
			).Exec(); delErr != nil {
				slog.Warn("onScheduleTimeout: delete schedule_timeouts", "id", id, "timeout_at", timeoutAt, "err", delErr)
			}
			yield(LabelScheduleTimeoutCleanupScheduleTimeouts)
		}
	}()

	rec, err := readScheduleRow(h.Session, origin, id, yield)
	if err != nil {
		return err
	}
	if rec == nil {
		return errors.New("skip cleanup")
	}

	// Stale entry: the schedule was already advanced past this occurrence by a
	// previous execution. Defer cleans up the entry.
	if timeoutAt < rec.NextRunAt {
		return nil
	}

	// Pre-insert orphan: this entry was written as the next-occurrence anchor
	// before the schedule row advanced to it. Leave it in place so it fires
	// correctly once next_run_at catches up.
	if timeoutAt > rec.NextRunAt {
		return errors.New("skip cleanup")
	}

	// a. Walk the cron forward from timeoutAt, collecting every occurrence that
	// is due (fireTime <= now). The first occurrence past now becomes the anchor
	// for the next schedule_timeouts entry. All promise creates use IF NOT EXISTS,
	// so the whole loop is safe to retry from the original timeoutAt entry.
	type occurrence struct {
		fireTime  int64
		promiseID string
	}
	var occurrences []occurrence
	currentT := timeoutAt
	for currentT <= now {
		promiseID, tmplErr := expandScheduleTemplate(rec.PromiseID, id, currentT)
		if tmplErr != nil {
			slog.Error("onScheduleTimeout: template error", "id", id, "time", currentT, "err", tmplErr)
			return tmplErr
		}
		occurrences = append(occurrences, occurrence{currentT, promiseID})
		next, cronErr := nextCron(rec.Cron, currentT)
		if cronErr != nil {
			return fmt.Errorf("onScheduleTimeout: advance cron(%s): %w", id, cronErr)
		}
		currentT = next
	}
	finalNextRunAt := currentT
	lastRunAt := occurrences[len(occurrences)-1].fireTime

	// b. Create promises for all due occurrences. Return an error on any failure
	// to leave the schedule_timeouts(timeoutAt) entry in place as the retry anchor.
	for _, occ := range occurrences {
		if !h.createSchedulePromise(occ.promiseID, rec, occ.fireTime, now, yield) {
			return fmt.Errorf("onScheduleTimeout(%s): createSchedulePromise failed at %d", id, occ.fireTime)
		}
	}

	// c. Insert the next schedule_timeouts entry before modifying anything else.
	// Return an error to leave the current entry as the retry anchor.
	recToken, _ := gocql.ParseUUID(rec.Token)
	if err = h.Session.Query(
		`INSERT INTO schedule_timeouts (bucket, shard, timeout_at, schedule_id, origin, create_token) VALUES (?, ?, ?, ?, ?, ?)`,
		h.BucketFor(finalNextRunAt), h.shardFor(id), finalNextRunAt, id, origin, recToken,
	).Exec(); err != nil {
		return err
	}
	yield(LabelScheduleTimeoutPreinsertScheduleTimeouts)

	// d. Advance the schedule row with a single LWT that jumps directly from
	// timeoutAt to finalNextRunAt, setting last_run_at to the final occurrence.
	//
	// Three outcomes, all handled by returning err directly:
	//
	//   applied=true,  err=nil  — normal: schedule advanced, defer cleans up.
	//
	//   applied=false, err=nil  — either a concurrent execution already advanced
	//     next_run_at (our promise creates were idempotent) or the schedule was
	//     deleted between step (b) and here. In both cases the promises exist
	//     and there is nothing left to retry, so returning nil lets defer clean
	//     up the current entry.
	//
	//   err!=nil — CQL failure: return the error so defer is blocked and the
	//     current entry stays as a retry anchor.
	schedRow := make(map[string]interface{})
	_, err = h.Session.Query(
		`UPDATE schedules SET next_run_at = ?, last_run_at = ? WHERE origin = ? AND id = ? IF next_run_at = ?`,
		finalNextRunAt, lastRunAt, origin, id, timeoutAt,
	).MapScanCAS(schedRow)
	yield(LabelScheduleTimeoutCommitSchedules)
	return err
}

// expandScheduleTemplate substitutes {{.id}} and {{.timestamp}} in the template string.
func expandScheduleTemplate(tmplStr, scheduleID string, timestamp int64) (string, error) {
	tmpl, err := template.New("").Parse(tmplStr)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, map[string]interface{}{
		"id":        scheduleID,
		"timestamp": timestamp,
	}); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// createSchedulePromise creates a promise for one schedule occurrence.
// fireTime is the cron interval that triggered this occurrence; createdAt and
// timeoutAt are both derived from it. Returns false if any step fails so the
// caller can leave the schedule_timeouts entry in place for retry.
func (h *Handler) createSchedulePromise(promiseID string, s *ScheduleRecord, fireTime int64, now int64, yield func(string)) bool {
	tags := make(map[string]string, len(s.PromiseTags)+1)
	for k, v := range s.PromiseTags {
		tags[k] = v
	}
	tags["resonate:schedule"] = s.ID
	tags["resonate:origin"] = promiseID
	tags["resonate:branch"] = promiseID
	tags["resonate:parent"] = promiseID

	target := tags["resonate:target"]
	timeoutAt := fireTime + s.PromiseTimeout

	// Born-expired: the tick advanced past this promise's deadline before it was
	// created. Insert as already-settled with no promise_timeouts entry.
	if now >= timeoutAt {
		var settledState string
		if s.PromiseTags["resonate:timer"] == "true" {
			settledState = "resolved"
		} else {
			settledState = "rejected_timedout"
		}
		var taskStateVal interface{}
		var taskVersionVal interface{}
		if target != "" {
			taskStateVal = "fulfilled"
			taskVersionVal = 0
		}
		row := make(map[string]interface{})
		_, err := h.Session.Query(
			`INSERT INTO promises (
				id, origin, branch, parent, target,
				state, param_headers, param_data,
				value_headers, value_data,
				tags, timeout_at, created_at, settled_at,
				callbacks, listeners,
				task_state, task_version, task_ttl, task_pid, task_resumes
			) VALUES (
				?, ?, null, null, ?,
				?, ?, ?,
				null, null,
				?, ?, ?, ?,
				{}, {},
				?, ?, null, null, null
			) IF NOT EXISTS`,
			promiseID, promiseID, target,
			settledState, s.PromiseParam.Headers, s.PromiseParam.Data,
			tags, timeoutAt, fireTime, timeoutAt,
			taskStateVal, taskVersionVal,
		).MapScanCAS(row)
		yield(LabelScheduleTimeoutCommitPromises)
		if err != nil {
			slog.Error("createSchedulePromise born-expired", "id", promiseID, "err", err)
			return false
		}
		return true
	}

	// Pre-insert promise_timeouts before the LWT so a kill between here and the
	// LWT leaves an orphan entry (cleaned up by the timeout loop) rather than a
	// pending promise with no timeout entry.
	if err := h.Session.Query(
		`INSERT INTO promise_timeouts (bucket, shard, timeout_at, promise_id, origin) VALUES (?, ?, ?, ?, ?)`,
		h.BucketFor(timeoutAt), h.shardFor(promiseID), timeoutAt, promiseID, promiseID,
	).Exec(); err != nil {
		slog.Error("createSchedulePromise: pre-insert promise_timeouts", "id", promiseID, "err", err)
		return false
	}
	yield(LabelScheduleTimeoutPreinsertPromiseTimeouts)

	// Pre-insert task_timeouts before the LWT when target is set.
	var retryAt int64
	if target != "" {
		retryAt = now + RetryTimeout
		if err := h.Session.Query(
			`INSERT INTO task_timeouts (bucket, shard, timeout_at, timeout_type, task_id, origin, promise_timeout_at) VALUES (?, ?, ?, 0, ?, ?, ?)`,
			h.BucketFor(retryAt), h.shardFor(promiseID), retryAt, promiseID, promiseID, timeoutAt,
		).Exec(); err != nil {
			slog.Error("createSchedulePromise: pre-insert task_timeouts", "id", promiseID, "err", err)
			return false
		}
		yield(LabelScheduleTimeoutPreinsertTaskTimeoutsRetry)
	}

	row := make(map[string]interface{})
	var applied bool
	var err error

	if target != "" {
		taskVersion := 0
		applied, err = h.Session.Query(
			`INSERT INTO promises (
				id, origin, branch, parent, target,
				state, param_headers, param_data,
				value_headers, value_data,
				tags, timeout_at, created_at, settled_at,
				callbacks, listeners,
				task_state, task_version, task_ttl, task_pid, task_resumes,
				task_timeout_retry, task_timeout_lease
			) VALUES (
				?, ?, null, null, ?,
				'pending', ?, ?,
				null, null,
				?, ?, ?, null,
				{}, {},
				'pending', ?, null, null, null,
				?, null
			) IF NOT EXISTS`,
			promiseID, promiseID, target,
			s.PromiseParam.Headers, s.PromiseParam.Data,
			tags, timeoutAt, fireTime,
			taskVersion,
			retryAt,
		).MapScanCAS(row)
	} else {
		applied, err = h.Session.Query(
			`INSERT INTO promises (
				id, origin, branch, parent, target,
				state, param_headers, param_data,
				value_headers, value_data,
				tags, timeout_at, created_at, settled_at,
				callbacks, listeners,
				task_state, task_version, task_ttl, task_pid, task_resumes
			) VALUES (
				?, ?, null, null, null,
				'pending', ?, ?,
				null, null,
				?, ?, ?, null,
				{}, {},
				null, null, null, null, null
			) IF NOT EXISTS`,
			promiseID, promiseID,
			s.PromiseParam.Headers, s.PromiseParam.Data,
			tags, timeoutAt, fireTime,
		).MapScanCAS(row)
	}
	yield(LabelScheduleTimeoutCommitPromises)
	if err != nil {
		slog.Error("createSchedulePromise", "id", promiseID, "err", err)
		h.Session.Query(
			`DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?`,
			h.BucketFor(timeoutAt), h.shardFor(promiseID), timeoutAt, promiseID, promiseID,
		).Exec()
		yield(LabelScheduleTimeoutRollbackPromiseTimeouts)
		if target != "" {
			h.Session.Query(
				`DELETE FROM task_timeouts
				 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
				h.BucketFor(retryAt), h.shardFor(promiseID), retryAt, promiseID, promiseID,
			).Exec()
			yield(LabelScheduleTimeoutRollbackTaskTimeoutsRetry)
		}
		return false
	}
	if !applied {
		// Rollback promise_timeouts unless the existing promise owns the same entry.
		existingTimeoutAt, _ := row["timeout_at"].(int64)
		if existingTimeoutAt != timeoutAt {
			h.Session.Query(
				`DELETE FROM promise_timeouts WHERE bucket = ? AND shard = ? AND timeout_at = ? AND origin = ? AND promise_id = ?`,
				h.BucketFor(timeoutAt), h.shardFor(promiseID), timeoutAt, promiseID, promiseID,
			).Exec()
			yield(LabelScheduleTimeoutRollbackPromiseTimeouts)
		}
		// Rollback task_timeouts unless the existing task owns the same retry entry.
		if target != "" {
			existingTaskState, _ := row["task_state"].(string)
			existingRetryAt, _ := row["task_timeout_retry"].(int64)
			if !(existingTaskState == "pending" && existingRetryAt == retryAt) {
				h.Session.Query(
					`DELETE FROM task_timeouts
					 WHERE bucket = ? AND shard = ? AND timeout_at = ? AND timeout_type = 0 AND origin = ? AND task_id = ?`,
					h.BucketFor(retryAt), h.shardFor(promiseID), retryAt, promiseID, promiseID,
				).Exec()
				yield(LabelScheduleTimeoutRollbackTaskTimeoutsRetry)
			}
		}
		return true
	}

	// INSERT applied: send execute to the target if set.
	if target != "" {
		h.sendExecute(target, promiseID, 0)
	}
	return true
}
