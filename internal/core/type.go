package core

import (
	"encoding/json"

	"github.com/resonateio/resonate-on-scylladb/internal/base"
)

// =============================================================================
// SHARED TYPES
// =============================================================================

type Value struct {
	Headers map[string]string `json:"headers,omitempty"`
	Data    string            `json:"data,omitempty"`
}

// =============================================================================
// RECORD TYPES
// =============================================================================

type PromiseRecord struct {
	ID        string            `json:"id"`
	State     string            `json:"state"` // pending | resolved | rejected | rejected_canceled | rejected_timedout
	Param     Value             `json:"param"`
	Value     Value             `json:"value"`
	Tags      map[string]string `json:"tags"`
	TimeoutAt int64             `json:"timeoutAt"`
	CreatedAt int64             `json:"createdAt"`
	SettledAt *int64            `json:"settledAt,omitempty"`
	Origin    *string           `json:"origin,omitempty"`
}

type TaskRecord struct {
	ID      string          `json:"id"`
	State   string          `json:"state"` // pending | acquired | suspended | halted | fulfilled
	Version int             `json:"version"`
	Resumes json.RawMessage `json:"resumes"` // []string | int | bool
	TTL     *int            `json:"ttl,omitempty"`
	PID     string          `json:"pid,omitempty"`
	Origin  *string         `json:"origin,omitempty"`
}

type ScheduleRecord struct {
	ID             string            `json:"id"`
	Cron           string            `json:"cron"`
	PromiseID      string            `json:"promiseId"`
	PromiseTimeout int64             `json:"promiseTimeout"`
	PromiseParam   Value             `json:"promiseParam"`
	PromiseTags    map[string]string `json:"promiseTags"`
	CreatedAt      int64             `json:"createdAt"`
	NextRunAt      int64             `json:"nextRunAt"`
	LastRunAt      *int64            `json:"lastRunAt,omitempty"`
	Origin         *string           `json:"origin,omitempty"`
	Token          string            `json:"-"`
}

// =============================================================================
// MESSAGE TYPES
// =============================================================================

type MessageHead struct {
	ServerURL string `json:"serverUrl,omitempty"`
}

type ExecuteMsg struct {
	Kind string      `json:"kind"` // "execute"
	Head MessageHead `json:"head"`
	Data struct {
		Task TaskRef `json:"task"`
	} `json:"data"`
}

type UnblockMsg struct {
	Kind string      `json:"kind"` // "unblock"
	Head MessageHead `json:"head"`
	Data struct {
		Promise PromiseRecord `json:"promise"`
	} `json:"data"`
}

// =============================================================================
// ENVELOPE TYPES
// =============================================================================

type RequestHead struct {
	Auth      string `json:"auth,omitempty"`
	CorrID    string `json:"corrId"`
	Version   string `json:"version"`
	Origin    string `json:"resonate:origin,omitempty"`
	DebugTime *int64 `json:"resonate:debug_time,omitempty"`
}

type ResponseHead struct {
	CorrID  string `json:"corrId"`
	Status  int    `json:"status"`
	Version string `json:"version"`
}

// =============================================================================
// HELPER TYPES
// =============================================================================

// TaskRef is a (id, version) pair used in heartbeat and embedded task references.
type TaskRef struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
}

// TimeoutEntry appears in promise_timeouts and schedule_timeouts snap data.
type TimeoutEntry struct {
	ID      string `json:"id"`
	Origin  string `json:"origin,omitempty"`
	Timeout int64  `json:"timeout"`
}

// CallbackEntry appears in callbacks snap data.
type CallbackEntry struct {
	Awaiter string  `json:"awaiter"`
	Awaited string  `json:"awaited"`
	Origin  *string `json:"origin,omitempty"`
}

// ListenerEntry appears in listeners snap data.
type ListenerEntry struct {
	ID      string  `json:"id"`
	Address string  `json:"address"`
	Origin  *string `json:"origin,omitempty"`
}

// TaskTimeoutEntry appears in task_timeouts snap data.
type TaskTimeoutEntry struct {
	ID      string `json:"id"`
	Origin  string `json:"origin,omitempty"`
	Type    int    `json:"type"`
	Timeout int64  `json:"timeout"`
}

// =============================================================================
// EMBEDDED REQUEST ENVELOPES
// Full request envelopes nested inside other requests (task.create, task.fulfill,
// task.suspend, task.fence).
// =============================================================================

type PromiseCreateReq struct {
	Kind string            `json:"kind"` // "promise.create"
	Head RequestHead       `json:"head"`
	Data PromiseCreateData `json:"data"`
}

type PromiseSettleReq struct {
	Kind string            `json:"kind"` // "promise.settle"
	Head RequestHead       `json:"head"`
	Data PromiseSettleData `json:"data"`
}

type PromiseRegisterCallbackReq struct {
	Kind string                      `json:"kind"` // "promise.register_callback"
	Head RequestHead                 `json:"head"`
	Data PromiseRegisterCallbackData `json:"data"`
}

// =============================================================================
// REQUEST DATA TYPES — PROMISE
// =============================================================================

type PromiseGetData struct {
	ID string `json:"id"`
}

type PromiseCreateData struct {
	ID        *string           `json:"id"`
	TimeoutAt *int64            `json:"timeoutAt"`
	Param     *Value            `json:"param"`
	Tags      map[string]string `json:"tags"`
}

type PromiseSettleData struct {
	ID    string `json:"id"`
	State string `json:"state"` // resolved | rejected | rejected_canceled
	Value *Value `json:"value"`
}

type PromiseRegisterCallbackData struct {
	Awaited string `json:"awaited"`
	Awaiter string `json:"awaiter"`
}

type PromiseRegisterListenerData struct {
	Awaited string `json:"awaited"`
	Address string `json:"address"`
}

type PromiseSearchData struct {
	State  string            `json:"state,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
	Limit  *int              `json:"limit,omitempty"`
	Cursor string            `json:"cursor,omitempty"`
}

// =============================================================================
// REQUEST DATA TYPES — TASK
// =============================================================================

type TaskGetData struct {
	ID string `json:"id"`
}

// TaskCreateData.Action is a full promise.create request envelope.
type TaskCreateData struct {
	PID    *string          `json:"pid"`
	TTL    *int             `json:"ttl"`
	Action PromiseCreateReq `json:"action"`
}

type TaskAcquireData struct {
	ID      string `json:"id"`
	Version *int   `json:"version"`
	PID     string `json:"pid"`
	TTL     int    `json:"ttl"`
}

type TaskReleaseData struct {
	ID      string `json:"id"`
	Version *int   `json:"version"`
}

// TaskSuspendData.Actions are full promise.register_callback request envelopes.
type TaskSuspendData struct {
	ID      string                       `json:"id"`
	Version *int                         `json:"version"`
	Actions []PromiseRegisterCallbackReq `json:"actions"`
}

type TaskHaltData struct {
	ID string `json:"id"`
}

type TaskContinueData struct {
	ID string `json:"id"`
}

// TaskFulfillData.Action is a full promise.settle request envelope.
type TaskFulfillData struct {
	ID      string            `json:"id"`
	Version *int              `json:"version"`
	Action  *PromiseSettleReq `json:"action"`
}

// TaskFenceData.Action is either a promise.create or promise.settle request envelope.
type TaskFenceData struct {
	ID      string          `json:"id"`
	Version *int            `json:"version"`
	Action  json.RawMessage `json:"action"` // PromiseCreateReq | PromiseSettleReq
}

type TaskHeartbeatData struct {
	PID   *string   `json:"pid"`
	Tasks []TaskRef `json:"tasks"`
}

type TaskSearchData struct {
	State  string `json:"state,omitempty"`
	Limit  *int   `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

// =============================================================================
// REQUEST DATA TYPES — SCHEDULE
// =============================================================================

type ScheduleGetData struct {
	ID string `json:"id"`
}

type ScheduleCreateData struct {
	ID             *string           `json:"id"`
	Cron           string            `json:"cron"`
	PromiseID      *string           `json:"promiseId"`
	PromiseTimeout *int64            `json:"promiseTimeout"`
	PromiseParam   Value             `json:"promiseParam"`
	PromiseTags    map[string]string `json:"promiseTags"`
}

type ScheduleDeleteData struct {
	ID string `json:"id"`
}

type ScheduleSearchData struct {
	Tags   map[string]string `json:"tags,omitempty"`
	Limit  *int              `json:"limit,omitempty"`
	Cursor string            `json:"cursor,omitempty"`
}

// =============================================================================
// REQUEST DATA TYPES — DEBUG
// =============================================================================

type DebugTickData struct {
	Time int64 `json:"time"`
}

// DebugStartData, DebugResetData, DebugSnapData, DebugStopData have no fields.

// =============================================================================
// RESPONSE DATA TYPES — PROMISE
// =============================================================================

type PromiseGetResData struct {
	Promise PromiseRecord `json:"promise"`
}

type PromiseCreateResData struct {
	Promise PromiseRecord `json:"promise"`
}

type PromiseSettleResData struct {
	Promise PromiseRecord `json:"promise"`
}

type PromiseRegisterCallbackResData struct {
	Promise PromiseRecord `json:"promise"`
}

type PromiseRegisterListenerResData struct {
	Promise PromiseRecord `json:"promise"`
}

type PromiseSearchResData struct {
	Promises []PromiseRecord `json:"promises"`
	Cursor   string          `json:"cursor,omitempty"`
}

// =============================================================================
// RESPONSE DATA TYPES — TASK
// =============================================================================

type TaskGetResData struct {
	Task TaskRecord `json:"task"`
}

type TaskCreateResData struct {
	Task    TaskRecord      `json:"task"`
	Promise PromiseRecord   `json:"promise"`
	Preload []PromiseRecord `json:"preload"`
}

type TaskAcquireResData struct {
	Task    TaskRecord      `json:"task"`
	Promise PromiseRecord   `json:"promise"`
	Preload []PromiseRecord `json:"preload"`
}

// TaskSuspend200ResData is the data payload for a 200 task.suspend response.
// All callbacks were registered immediately; no preload is needed.
type TaskSuspend200ResData struct{}

// TaskSuspend300ResData is the data payload for a 300 task.suspend response.
// At least one awaited promise was already settled; preload carries its record.
type TaskSuspend300ResData struct {
	Preload []PromiseRecord `json:"preload"`
}

type TaskFulfillResData struct {
	Promise PromiseRecord `json:"promise"`
}

type TaskFenceResData struct {
	Action  json.RawMessage `json:"action"` // PromiseCreateResData | PromiseSettleResData
	Preload []PromiseRecord `json:"preload"`
}

type TaskSearchResData struct {
	Tasks  []TaskRecord `json:"tasks"`
	Cursor string       `json:"cursor,omitempty"`
}

// TaskReleaseResData, TaskHaltResData, TaskContinueResData, TaskHeartbeatResData have no fields.

// =============================================================================
// RESPONSE DATA TYPES — SCHEDULE
// =============================================================================

type ScheduleGetResData struct {
	Schedule ScheduleRecord `json:"schedule"`
}

type ScheduleCreateResData struct {
	Schedule ScheduleRecord `json:"schedule"`
}

type ScheduleSearchResData struct {
	Schedules []ScheduleRecord `json:"schedules"`
	Cursor    string           `json:"cursor,omitempty"`
}

// ScheduleDeleteResData has no fields.

// =============================================================================
// RESPONSE DATA TYPES — DEBUG
// =============================================================================

// DebugTickAction is one side-effect produced by a debug.tick.
// Kind is one of: promise.settle | task.release | task.retry
// Data shape varies by kind; decode with json.RawMessage.
type DebugTickAction struct {
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

type DebugSnapResData struct {
	Promises         []PromiseRecord     `json:"promises"`
	PromiseTimeouts  []TimeoutEntry      `json:"promiseTimeouts"`
	Callbacks        []CallbackEntry     `json:"callbacks"`
	Listeners        []ListenerEntry     `json:"listeners"`
	Tasks            []TaskRecord        `json:"tasks"`
	TaskTimeouts     []TaskTimeoutEntry  `json:"taskTimeouts"`
	Schedules        []ScheduleRecord    `json:"schedules"`
	ScheduleTimeouts []TimeoutEntry      `json:"scheduleTimeouts"`
	Messages         []base.MessageEntry `json:"messages"`
}

// DebugStartResData, DebugResetResData, DebugStopResData have no fields.
