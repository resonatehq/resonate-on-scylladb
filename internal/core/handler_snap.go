package core

import (
	"encoding/json"
	"log/slog"

	"github.com/gocql/gocql"
	"github.com/resonateio/resonate-on-scylladb/internal/base"
)

// DebugSnap returns a complete point-in-time snapshot of the server state.
// It full-scans all five tables and denormalizes callbacks and listeners
// out of the promises rows. Used by the test harness to verify invariants.
func (h *Handler) DebugSnap(head RequestHead, now int64, yield func(string)) Res[DebugSnapResData] {
	promises := make([]PromiseRecord, 0)
	callbacks := make([]CallbackEntry, 0)
	listeners := make([]ListenerEntry, 0)
	tasks := make([]TaskRecord, 0)
	promiseTimeouts := make([]TimeoutEntry, 0)
	taskTimeouts := make([]TaskTimeoutEntry, 0)
	schedules := make([]ScheduleRecord, 0)
	scheduleTimeouts := make([]TimeoutEntry, 0)

	// ── promises ──────────────────────────────────────────────────────────────
	// Scan all promise rows. Extract PromiseRecords, CallbackEntries,
	// ListenerEntries, and TaskRecords (for rows where target is set).
	{
		var (
			id, target, state                string
			paramHeaders, valueHeaders, tags map[string]string
			paramData, valueData             string
			timeoutAt, createdAt             int64
			settledAt                        *int64
			cbSet, lnSet                     []string
			taskState, taskPID               string
			taskVersion                      int
			taskTTL                          *int64
			taskResumes                      []string
		)
		iter := h.Session.Query(
			`SELECT id, target, state,
			        param_headers, param_data, value_headers, value_data,
			        tags, timeout_at, created_at, settled_at,
			        callbacks, listeners,
			        task_state, task_version, task_ttl, task_pid, task_resumes
			 FROM promises`,
		).Iter()
		yield(LabelDebugSnapScanPromises)
		for iter.Scan(
			&id, &target, &state,
			&paramHeaders, &paramData, &valueHeaders, &valueData,
			&tags, &timeoutAt, &createdAt, &settledAt,
			&cbSet, &lnSet,
			&taskState, &taskVersion, &taskTTL, &taskPID, &taskResumes,
		) {
			if tags == nil {
				tags = map[string]string{}
			}
			promises = append(promises, PromiseRecord{
				ID:        id,
				State:     state,
				Param:     Value{Headers: paramHeaders, Data: paramData},
				Value:     Value{Headers: valueHeaders, Data: valueData},
				Tags:      tags,
				TimeoutAt: timeoutAt,
				CreatedAt: createdAt,
				SettledAt: settledAt,
			})
			for _, awaiterID := range cbSet {
				callbacks = append(callbacks, CallbackEntry{Awaiter: awaiterID, Awaited: id})
			}
			for _, addr := range lnSet {
				listeners = append(listeners, ListenerEntry{ID: id, Address: addr})
			}
			if target != "" {
				resumesJSON := json.RawMessage("[]")
				if len(taskResumes) > 0 {
					if b, err := json.Marshal(taskResumes); err == nil {
						resumesJSON = b
					}
				}
				tr := TaskRecord{
					ID:      id,
					State:   taskState,
					Version: taskVersion,
					Resumes: resumesJSON,
					PID:     taskPID,
				}
				if taskTTL != nil {
					ttl := int(*taskTTL)
					tr.TTL = &ttl
				}
				tasks = append(tasks, tr)
			}
		}
		if err := iter.Close(); err != nil {
			slog.Error("debug.snap: promises scan", "err", err)
		}
	}

	// ── promise_timeouts ──────────────────────────────────────────────────────
	{
		var (
			ptTimeout int64
			ptID      string
		)
		iter := h.Session.Query(
			`SELECT timeout_at, promise_id FROM promise_timeouts`,
		).Iter()
		yield(LabelDebugSnapScanPromiseTimeouts)
		for iter.Scan(&ptTimeout, &ptID) {
			promiseTimeouts = append(promiseTimeouts, TimeoutEntry{
				ID:      ptID,
				Timeout: ptTimeout,
			})
		}
		if err := iter.Close(); err != nil {
			slog.Error("debug.snap: promise_timeouts scan", "err", err)
		}
	}

	// ── task_timeouts ─────────────────────────────────────────────────────────
	{
		var (
			ttTimeout int64
			ttType    int8
			ttID      string
		)
		iter := h.Session.Query(
			`SELECT timeout_at, timeout_type, task_id FROM task_timeouts`,
		).Iter()
		yield(LabelDebugSnapScanTaskTimeouts)
		for iter.Scan(&ttTimeout, &ttType, &ttID) {
			taskTimeouts = append(taskTimeouts, TaskTimeoutEntry{
				ID:      ttID,
				Type:    int(ttType),
				Timeout: ttTimeout,
			})
		}
		if err := iter.Close(); err != nil {
			slog.Error("debug.snap: task_timeouts scan", "err", err)
		}
	}

	// ── schedules ─────────────────────────────────────────────────────────────
	{
		var (
			sID, sCron, sPromiseID string
			sPromiseTimeout        int64
			sParamHeaders          map[string]string
			sParamData             string
			sPromiseTags           map[string]string
			sNextRunAt, sCreatedAt int64
			sLastRunAt             *int64
			sToken                 gocql.UUID
		)
		iter := h.Session.Query(
			`SELECT id, cron, promise_id, promise_timeout,
			        promise_param_headers, promise_param_data, promise_tags,
			        next_run_at, last_run_at, created_at, create_token
			 FROM schedules`,
		).Iter()
		yield(LabelDebugSnapScanSchedules)
		for iter.Scan(
			&sID, &sCron, &sPromiseID, &sPromiseTimeout,
			&sParamHeaders, &sParamData, &sPromiseTags,
			&sNextRunAt, &sLastRunAt, &sCreatedAt, &sToken,
		) {
			if sPromiseTags == nil {
				sPromiseTags = map[string]string{}
			}
			schedules = append(schedules, ScheduleRecord{
				ID:             sID,
				Cron:           sCron,
				PromiseID:      sPromiseID,
				PromiseTimeout: sPromiseTimeout,
				PromiseParam:   Value{Headers: sParamHeaders, Data: sParamData},
				PromiseTags:    sPromiseTags,
				NextRunAt:      sNextRunAt,
				LastRunAt:      sLastRunAt,
				CreatedAt:      sCreatedAt,
				Token:          sToken.String(),
			})
		}
		if err := iter.Close(); err != nil {
			slog.Error("debug.snap: schedules scan", "err", err)
		}
	}

	// ── schedule_timeouts ─────────────────────────────────────────────────────
	{
		var (
			stTimeout int64
			stID      string
		)
		iter := h.Session.Query(
			`SELECT timeout_at, schedule_id FROM schedule_timeouts`,
		).Iter()
		yield(LabelDebugSnapScanScheduleTimeouts)
		for iter.Scan(&stTimeout, &stID) {
			scheduleTimeouts = append(scheduleTimeouts, TimeoutEntry{
				ID:      stID,
				Timeout: stTimeout,
			})
		}
		if err := iter.Close(); err != nil {
			slog.Error("debug.snap: schedule_timeouts scan", "err", err)
		}
	}

	return Res[DebugSnapResData]{
		Kind: "debug.snap",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
		Data: DebugSnapResData{
			Promises:         promises,
			PromiseTimeouts:  promiseTimeouts,
			Callbacks:        callbacks,
			Listeners:        listeners,
			Tasks:            tasks,
			TaskTimeouts:     taskTimeouts,
			Schedules:        schedules,
			ScheduleTimeouts: scheduleTimeouts,
			Messages: func() []base.MessageEntry {
				if h.Recorder == nil {
					return nil
				}
				return h.Recorder.Snap()
			}(),
		},
	}
}
