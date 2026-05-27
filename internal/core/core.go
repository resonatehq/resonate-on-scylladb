package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/resonateio/resonate-on-scylladb/internal/auth"
	"github.com/resonateio/resonate-on-scylladb/internal/base"
)

var ErrUnauthorized = errors.New("unauthorized")

// errRes marshals a protocol-conformant error response. The returned bytes are
// always valid JSON; the error return value is reserved for server faults only.
func errRes(kind, corrID, version string, status int, msg string) []byte {
	out, _ := json.Marshal(Res[string]{
		Kind: kind,
		Head: ResponseHead{CorrID: corrID, Status: status, Version: version},
		Data: msg,
	})
	return out
}

type Handler struct {
	Session        *gocql.Session
	Host           string
	Auth           *auth.Auth
	Dispatcher     base.Transport
	Recorder       *base.Recorder    // non-nil while a debug session is active (between debug.start and debug.stop)
	Backgrounds    []base.Background // background loops; stopped by debug.start, restarted by debug.stop
	BucketWidth    int64             // ms per timeout bucket; must be > 0
	BucketLookback int               // past buckets scanned by TickAt in addition to current
	Shards         int               // number of timeout-table shards; 0 or 1 means single shard
	Debug          bool              // when true: debug.* requests honored, debug_time header honored

	realDispatcher base.Transport // saved by debug.start, restored by debug.stop
	maxDebugTick   int64          // maximum req.Time seen across debug.tick calls in this session
	monotonicFloor atomic.Int64   // ensures now never goes backwards across requests
}

// envelope is the top-level request wrapper, decoded before dispatch.
type envelope struct {
	Kind string          `json:"kind"`
	Head RequestHead     `json:"head"`
	Data json.RawMessage `json:"data"`
}

// Res is the generic response envelope shared by all action handlers.
type Res[D any] struct {
	Kind string       `json:"kind"`
	Head ResponseHead `json:"head"`
	Data D            `json:"data"`
}

// --- dispatcher ---

func (h *Handler) Handle(raw []byte, yield func(string)) ([]byte, error) {
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return errRes("", "", "", 400, fmt.Sprintf("decode envelope: %s", err)), nil
	}

	if h.Auth != nil {
		if err := h.Auth.Check(env.Head.Auth); err != nil {
			return nil, ErrUnauthorized
		}
	}

	if env.Head.CorrID == "" {
		return errRes(env.Kind, "", env.Head.Version, 400, "head.corrId is required"), nil
	}
	if env.Head.Version == "" {
		return errRes(env.Kind, env.Head.CorrID, "", 400, "head.version is required"), nil
	}

	var now int64
	if h.Debug && env.Head.DebugTime != nil {
		now = *env.Head.DebugTime
	} else {
		t := time.Now().UnixMilli()
		for {
			prev := h.monotonicFloor.Load()
			if t <= prev {
				now = prev
				break
			}
			if h.monotonicFloor.CompareAndSwap(prev, t) {
				now = t
				break
			}
		}
	}

	notImplemented := func() any {
		return Res[string]{
			Kind: env.Kind,
			Head: ResponseHead{CorrID: env.Head.CorrID, Status: 501, Version: env.Head.Version},
			Data: "Not implemented",
		}
	}

	decode := func(dst any) error {
		return json.Unmarshal(env.Data, dst)
	}

	var res any
	switch env.Kind {

	// --- Debug --- (gated on h.Debug)

	case "debug.start", "debug.reset", "debug.tick", "debug.snap", "debug.stop":
		if !h.Debug {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("unknown kind: %s", env.Kind)), nil
		}
		switch env.Kind {
		case "debug.start":
			res = h.DebugStart(env.Head, now, yield)
		case "debug.reset":
			res = h.DebugReset(env.Head, now, yield)
		case "debug.tick":
			var req DebugTickData
			if err := decode(&req); err != nil {
				return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
			}
			res = h.DebugTick(env.Head, req, now, yield)
		case "debug.snap":
			res = h.DebugSnap(env.Head, now, yield)
		case "debug.stop":
			res = h.DebugStop(env.Head, now, yield)
		}

	// --- Promise ---

	case "promise.create":
		var req PromiseCreateData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.PromiseCreate(env.Head, req, now, yield)

	case "promise.get":
		var req PromiseGetData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.PromiseGet(env.Head, req, now, yield)

	case "promise.settle":
		var req PromiseSettleData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.PromiseSettle(env.Head, req, now, yield)

	case "promise.register_callback":
		var req PromiseRegisterCallbackData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.PromiseRegisterCallback(env.Head, req, now, yield)

	case "promise.register_listener":
		var req PromiseRegisterListenerData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.PromiseRegisterListener(env.Head, req, now, yield)

	case "promise.search":
		var req PromiseSearchData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		res = notImplemented()

	// --- Task ---

	case "task.create":
		var req TaskCreateData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.TaskCreate(env.Head, req, now, yield)

	case "task.get":
		var req TaskGetData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.TaskGet(env.Head, req, now, yield)

	case "task.acquire":
		var req TaskAcquireData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.TaskAcquire(env.Head, req, now, yield)

	case "task.release":
		var req TaskReleaseData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.TaskRelease(env.Head, req, now, yield)

	case "task.suspend":
		var req TaskSuspendData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.TaskSuspend(env.Head, req, now, yield)

	case "task.fulfill":
		var req TaskFulfillData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.TaskFulfill(env.Head, req, now, yield)

	case "task.fence":
		var req TaskFenceData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.TaskFence(env.Head, req, now, yield)

	case "task.heartbeat":
		var req TaskHeartbeatData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.TaskHeartbeat(env.Head, req, now, yield)

	case "task.halt":
		var req TaskHaltData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.TaskHalt(env.Head, req, now, yield)

	case "task.continue":
		var req TaskContinueData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.TaskContinue(env.Head, req, now, yield)

	case "task.search":
		var req TaskSearchData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		res = notImplemented()

	// --- Schedule ---

	case "schedule.create":
		var req ScheduleCreateData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.ScheduleCreate(env.Head, req, now, yield)

	case "schedule.get":
		var req ScheduleGetData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.ScheduleGet(env.Head, req, now, yield)

	case "schedule.delete":
		var req ScheduleDeleteData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		if err := req.Validate(); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, err.Error()), nil
		}
		res = h.ScheduleDelete(env.Head, req, now, yield)

	case "schedule.search":
		var req ScheduleSearchData
		if err := decode(&req); err != nil {
			return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("decode %s: %s", env.Kind, err)), nil
		}
		res = notImplemented()

	default:
		return errRes(env.Kind, env.Head.CorrID, env.Head.Version, 400, fmt.Sprintf("unknown kind: %s", env.Kind)), nil
	}

	out, err := json.Marshal(res)
	if err != nil {
		return nil, fmt.Errorf("encode response: %w", err)
	}
	return out, nil
}

// --- action handlers ---

// DebugStart switches the server into recorder mode: installs the Recorder as
// the active Dispatcher and stops all background loops.
func (h *Handler) DebugStart(head RequestHead, now int64, yield func(string)) Res[struct{}] {
	if h.realDispatcher == nil {
		rec := base.NewRecorder()
		h.realDispatcher = h.Dispatcher
		h.Dispatcher = rec
		h.Recorder = rec
		h.maxDebugTick = 0
		for _, b := range h.Backgrounds {
			b.Stop()
		}
	}
	return Res[struct{}]{
		Kind: "debug.start",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
	}
}

func (h *Handler) DebugReset(head RequestHead, now int64, yield func(string)) Res[struct{}] {
	tables := []string{
		"promises",
		"promise_timeouts",
		"task_timeouts",
		"schedules",
		"schedule_timeouts",
		"workers",
	}
	errc := make(chan error, len(tables))
	for _, t := range tables {
		t := t
		go func() {
			errc <- h.Session.Query("TRUNCATE TABLE " + t).Exec()
		}()
	}
	for range tables {
		if err := <-errc; err != nil {
			return Res[struct{}]{
				Kind: "debug.reset",
				Head: ResponseHead{CorrID: head.CorrID, Status: 500, Version: head.Version},
				Data: struct{}{},
			}
		}
	}
	if h.Recorder != nil {
		h.Recorder.Clear()
	}
	return Res[struct{}]{
		Kind: "debug.reset",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
	}
}

// DebugStop restores the real Dispatcher and restarts all background loops.
// It first replays any messages captured by the Recorder so that sends that
// occurred during the debug session (execute from promise.create, unblock from
// promise.settle, etc.) reach their destination. After replay it runs a
// full-table scan tick bounded to maxDebugTick+RetryTimeout+1 so that task
// retry timeouts created during the session (which land in old buckets never
// visited by the production bucketed TickAt) are processed immediately, while
// schedule occurrences beyond the session window are not triggered.
func (h *Handler) DebugStop(head RequestHead, now int64, yield func(string)) Res[struct{}] {
	if h.realDispatcher != nil {
		h.Dispatcher = h.realDispatcher
		h.realDispatcher = nil
		if h.Recorder != nil {
			for _, entry := range h.Recorder.Snap() {
				h.Dispatcher.Send(entry.Address, entry.Message)
			}
			h.Recorder = nil
		}
		stopTick := h.maxDebugTick + RetryTimeout + 1
		h.debugTickAt(stopTick, func(string) {})
		for _, b := range h.Backgrounds {
			b.Init()
		}
	}
	return Res[struct{}]{
		Kind: "debug.stop",
		Head: ResponseHead{CorrID: head.CorrID, Status: 200, Version: head.Version},
	}
}
