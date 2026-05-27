package test

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

const pendingRetryTTL = int64(30_000)

// ─── Origin resolution ────────────────────────────────────────────────────────

func resolveOrigin(headOrigin, tagOrigin, id string) (string, error) {
	switch {
	case headOrigin != "" && tagOrigin != "" && headOrigin != tagOrigin:
		return "", fmt.Errorf("resonate:origin mismatch: head=%q tag=%q", headOrigin, tagOrigin)
	case headOrigin != "":
		return headOrigin, nil
	case tagOrigin != "":
		return tagOrigin, nil
	default:
		return id, nil
	}
}

// ─── Wire record types ────────────────────────────────────────────────────────

type Value struct {
	Headers map[string]string `json:"headers,omitempty"`
	Data    string            `json:"data,omitempty"`
}

type PromiseRecord struct {
	ID        string            `json:"id"`
	Origin    *string           `json:"origin,omitempty"`
	State     string            `json:"state"`
	Param     Value             `json:"param"`
	Value     Value             `json:"value"`
	Tags      map[string]string `json:"tags"`
	TimeoutAt int64             `json:"timeoutAt"`
	CreatedAt int64             `json:"createdAt"`
	SettledAt *int64            `json:"settledAt,omitempty"`
}

type TaskRecord struct {
	ID      string   `json:"id"`
	Origin  *string  `json:"origin,omitempty"`
	State   string   `json:"state"`
	Version int      `json:"version"`
	Resumes []string `json:"resumes"`
	PID     string   `json:"pid,omitempty"`
	TTL     *int64   `json:"ttl,omitempty"`
}

type ScheduleRecord struct {
	ID             string            `json:"id"`
	Origin         *string           `json:"origin,omitempty"`
	Cron           string            `json:"cron"`
	PromiseID      string            `json:"promiseId"`
	PromiseTimeout int64             `json:"promiseTimeout"`
	PromiseParam   Value             `json:"promiseParam"`
	PromiseTags    map[string]string `json:"promiseTags"`
	CreatedAt      int64             `json:"createdAt"`
	NextRunAt      int64             `json:"nextRunAt"`
	LastRunAt      *int64            `json:"lastRunAt,omitempty"`
}

// ─── Internal state types ─────────────────────────────────────────────────────

type Promise struct {
	Origin    string
	ID        string
	State     string
	Param     Value
	Value     Value
	Tags      map[string]string
	TimeoutAt int64
	CreatedAt int64
	SettledAt *int64
	Callbacks map[string]string   // awaiterID → awaiterOrigin
	Listeners map[string]struct{} // delivery addresses
}

type Task struct {
	Origin  string
	ID      string
	State   string
	Version int
	PID     string
	TTL     *int64
	Resumes map[string]struct{}
}

type Schedule struct {
	ID             string
	Origin         string
	Cron           string
	PromiseID      string
	PromiseTimeout int64
	PromiseParam   Value
	PromiseTags    map[string]string
	CreatedAt      int64
	LastRunAt      *int64
}

type PTimeout struct {
	Origin  string
	ID      string
	Timeout int64
}
type TTimeout struct {
	Origin  string
	ID      string
	Type    int
	Timeout int64
}
type STimeout struct {
	Origin    string
	ID        string
	Timeout   int64
	CreatedAt int64
}

// ─── Snap types ───────────────────────────────────────────────────────────────

type CallbackEntry struct {
	Awaiter string  `json:"awaiter"`
	Awaited string  `json:"awaited"`
	Origin  *string `json:"origin,omitempty"`
}

type ListenerEntry struct {
	ID      string  `json:"id"`
	Address string  `json:"address"`
	Origin  *string `json:"origin,omitempty"`
}

type TaskTimeoutEntry struct {
	ID      string `json:"id"`
	Origin  string `json:"origin,omitempty"`
	Type    int    `json:"type"`
	Timeout int64  `json:"timeout"`
}

type TimeoutEntry struct {
	ID      string `json:"id"`
	Origin  string `json:"origin,omitempty"`
	Timeout int64  `json:"timeout"`
}

type MessageEntry struct {
	Address string          `json:"address"`
	Message json.RawMessage `json:"message"`
}

type SnapData struct {
	Promises         []PromiseRecord    `json:"promises"`
	PromiseTimeouts  []TimeoutEntry     `json:"promiseTimeouts"`
	Callbacks        []CallbackEntry    `json:"callbacks"`
	Listeners        []ListenerEntry    `json:"listeners"`
	Tasks            []TaskRecord       `json:"tasks"`
	TaskTimeouts     []TaskTimeoutEntry `json:"taskTimeouts"`
	Schedules        []ScheduleRecord   `json:"schedules"`
	ScheduleTimeouts []TimeoutEntry     `json:"scheduleTimeouts"`
	Messages         []MessageEntry     `json:"messages"`
}

// ─── Server ───────────────────────────────────────────────────────────────────

type Server struct {
	promises  map[string]map[string]*Promise  // origin → id → Promise
	tasks     map[string]map[string]*Task     // origin → id → Task
	schedules map[string]map[string]*Schedule // origin → id → Schedule
	pTimeouts []PTimeout
	tTimeouts []TTimeout
	sTimeouts []STimeout
	outgoing  []MessageEntry
}

func New() *Server {
	return &Server{
		promises:  make(map[string]map[string]*Promise),
		tasks:     make(map[string]map[string]*Task),
		schedules: make(map[string]map[string]*Schedule),
	}
}

// ─── Two-level map helpers ────────────────────────────────────────────────────

func (s *Server) getPromise(origin, id string) *Promise {
	if inner := s.promises[origin]; inner != nil {
		return inner[id]
	}
	return nil
}

func (s *Server) setPromise(origin, id string, p *Promise) {
	if s.promises[origin] == nil {
		s.promises[origin] = make(map[string]*Promise)
	}
	s.promises[origin][id] = p
}

func (s *Server) getTask(origin, id string) *Task {
	if inner := s.tasks[origin]; inner != nil {
		return inner[id]
	}
	return nil
}

func (s *Server) setTask(origin, id string, t *Task) {
	if s.tasks[origin] == nil {
		s.tasks[origin] = make(map[string]*Task)
	}
	s.tasks[origin][id] = t
}

func (s *Server) getSchedule(origin, id string) *Schedule {
	if inner := s.schedules[origin]; inner != nil {
		return inner[id]
	}
	return nil
}

func (s *Server) setSchedule(origin, id string, sc *Schedule) {
	if s.schedules[origin] == nil {
		s.schedules[origin] = make(map[string]*Schedule)
	}
	s.schedules[origin][id] = sc
}

// Clone returns a deep copy of s.
func (s *Server) Clone() *Server {
	c := &Server{
		promises:  make(map[string]map[string]*Promise, len(s.promises)),
		tasks:     make(map[string]map[string]*Task, len(s.tasks)),
		schedules: make(map[string]map[string]*Schedule, len(s.schedules)),
		pTimeouts: make([]PTimeout, len(s.pTimeouts)),
		tTimeouts: make([]TTimeout, len(s.tTimeouts)),
		sTimeouts: make([]STimeout, len(s.sTimeouts)),
		outgoing:  make([]MessageEntry, len(s.outgoing)),
	}
	for origin, inner := range s.promises {
		c.promises[origin] = make(map[string]*Promise, len(inner))
		for id, p := range inner {
			c.promises[origin][id] = p.clone()
		}
	}
	for origin, inner := range s.tasks {
		c.tasks[origin] = make(map[string]*Task, len(inner))
		for id, t := range inner {
			c.tasks[origin][id] = t.clone()
		}
	}
	for origin, inner := range s.schedules {
		c.schedules[origin] = make(map[string]*Schedule, len(inner))
		for id, sc := range inner {
			c.schedules[origin][id] = sc.clone()
		}
	}
	copy(c.pTimeouts, s.pTimeouts)
	copy(c.tTimeouts, s.tTimeouts)
	copy(c.sTimeouts, s.sTimeouts)
	for i, m := range s.outgoing {
		msg := make(json.RawMessage, len(m.Message))
		copy(msg, m.Message)
		c.outgoing[i] = MessageEntry{Address: m.Address, Message: msg}
	}
	return c
}

func (p *Promise) clone() *Promise {
	c := *p
	c.Param = cloneValue(p.Param)
	c.Value = cloneValue(p.Value)
	c.Tags = cloneStrMap(p.Tags)
	if p.SettledAt != nil {
		sa := *p.SettledAt
		c.SettledAt = &sa
	}
	c.Callbacks = cloneStrMap(p.Callbacks)
	c.Listeners = cloneStrSet(p.Listeners)
	return &c
}

func (t *Task) clone() *Task {
	c := *t
	if t.TTL != nil {
		ttl := *t.TTL
		c.TTL = &ttl
	}
	c.Resumes = cloneStrSet(t.Resumes)
	return &c
}

func (sc *Schedule) clone() *Schedule {
	c := *sc
	c.PromiseParam = cloneValue(sc.PromiseParam)
	c.PromiseTags = cloneStrMap(sc.PromiseTags)
	if sc.LastRunAt != nil {
		t := *sc.LastRunAt
		c.LastRunAt = &t
	}
	return &c
}

func cloneValue(v Value) Value {
	return Value{Headers: cloneStrMap(v.Headers), Data: v.Data}
}

func cloneStrMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	c := make(map[string]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func cloneStrSet(s map[string]struct{}) map[string]struct{} {
	if s == nil {
		return nil
	}
	c := make(map[string]struct{}, len(s))
	for k := range s {
		c[k] = struct{}{}
	}
	return c
}

// ─── Apply ────────────────────────────────────────────────────────────────────

type envelope struct {
	Kind string          `json:"kind"`
	Head map[string]any  `json:"head"`
	Data json.RawMessage `json:"data"`
}

func (s *Server) Apply(now int64, raw []byte) ([]byte, error) {
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	kind := env.Kind
	headOrigin, _ := env.Head["resonate:origin"].(string)

	unmarshal := func(dst any) error {
		return json.Unmarshal(env.Data, dst)
	}

	type promiseGetData struct {
		ID string `json:"id"`
	}
	type promiseCreateData struct {
		ID        string            `json:"id"`
		TimeoutAt int64             `json:"timeoutAt"`
		Param     Value             `json:"param"`
		Tags      map[string]string `json:"tags"`
	}
	type promiseSettleData struct {
		ID    string `json:"id"`
		State string `json:"state"`
		Value Value  `json:"value"`
	}
	type promiseRegisterCallbackData struct {
		Awaited string `json:"awaited"`
		Awaiter string `json:"awaiter"`
	}
	type promiseRegisterListenerData struct {
		Awaited string `json:"awaited"`
		Address string `json:"address"`
	}
	type taskGetData struct {
		ID string `json:"id"`
	}
	type taskCreateData struct {
		PID    string `json:"pid"`
		TTL    int64  `json:"ttl"`
		Action struct {
			Head map[string]any    `json:"head"`
			Data promiseCreateData `json:"data"`
		} `json:"action"`
	}
	type taskAcquireData struct {
		ID      string `json:"id"`
		Version int    `json:"version"`
		PID     string `json:"pid"`
		TTL     int64  `json:"ttl"`
	}
	type taskReleaseData struct {
		ID      string `json:"id"`
		Version int    `json:"version"`
	}
	type taskSuspendData struct {
		ID      string `json:"id"`
		Version int    `json:"version"`
		Actions []struct {
			Data promiseRegisterCallbackData `json:"data"`
		} `json:"actions"`
	}
	type taskFulfillData struct {
		ID      string `json:"id"`
		Version int    `json:"version"`
		Action  struct {
			Data promiseSettleData `json:"data"`
		} `json:"action"`
	}
	type taskFenceData struct {
		ID      string          `json:"id"`
		Version int             `json:"version"`
		Action  json.RawMessage `json:"action"`
	}
	type taskHeartbeatData struct {
		PID   string `json:"pid"`
		Tasks []struct {
			ID      string `json:"id"`
			Version int    `json:"version"`
		} `json:"tasks"`
	}
	type taskHaltData struct {
		ID string `json:"id"`
	}
	type taskContinueData struct {
		ID string `json:"id"`
	}
	type scheduleGetData struct {
		ID string `json:"id"`
	}
	type scheduleCreateData struct {
		ID             string            `json:"id"`
		Cron           string            `json:"cron"`
		PromiseID      string            `json:"promiseId"`
		PromiseTimeout int64             `json:"promiseTimeout"`
		PromiseParam   Value             `json:"promiseParam"`
		PromiseTags    map[string]string `json:"promiseTags"`
	}
	type scheduleDeleteData struct {
		ID string `json:"id"`
	}
	type debugTickData struct {
		Time int64 `json:"time"`
	}

	switch kind {
	case "promise.get":
		var d promiseGetData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		origin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.promiseGet(now, origin, d.ID)

	case "promise.create":
		var d promiseCreateData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		tagOrigin := d.Tags["resonate:origin"]
		origin, err := resolveOrigin(headOrigin, tagOrigin, d.ID)
		if err != nil {
			return s.respond(kind, 400, err.Error())
		}
		return s.promiseCreate(now, origin, d.ID, d.TimeoutAt, d.Param, d.Tags)

	case "promise.settle":
		var d promiseSettleData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		origin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.promiseSettle(now, origin, d.ID, d.State, d.Value)

	case "promise.register_callback":
		var d promiseRegisterCallbackData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		if d.Awaited == d.Awaiter {
			return s.respond(kind, 400, "data: Awaited and awaiter must be different promises")
		}
		awaitedOrigin, _ := resolveOrigin(headOrigin, "", d.Awaited)
		awaiterOrigin, _ := resolveOrigin(headOrigin, "", d.Awaiter)
		return s.promiseRegisterCallback(now, awaitedOrigin, d.Awaited, awaiterOrigin, d.Awaiter)

	case "promise.register_listener":
		var d promiseRegisterListenerData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		awaitedOrigin, _ := resolveOrigin(headOrigin, "", d.Awaited)
		return s.promiseRegisterListener(now, awaitedOrigin, d.Awaited, d.Address)

	case "promise.search":
		return s.respond(kind, 501, "Not implemented")

	case "task.get":
		var d taskGetData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		origin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.taskGet(now, origin, d.ID)

	case "task.create":
		var d taskCreateData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		innerHeadOrigin, _ := d.Action.Head["resonate:origin"].(string)
		a := d.Action.Data
		innerTagOrigin := a.Tags["resonate:origin"]
		promiseOrigin, err := resolveOrigin(innerHeadOrigin, innerTagOrigin, a.ID)
		if err != nil {
			return s.respond(kind, 400, err.Error())
		}
		return s.taskCreate(now, d.PID, d.TTL, promiseOrigin, a.ID, a.TimeoutAt, a.Param, a.Tags)

	case "task.acquire":
		var d taskAcquireData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		origin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.taskAcquire(now, origin, d.ID, d.Version, d.PID, d.TTL)

	case "task.release":
		var d taskReleaseData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		origin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.taskRelease(now, origin, d.ID, d.Version)

	case "task.suspend":
		var d taskSuspendData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		taskOrigin, _ := resolveOrigin(headOrigin, "", d.ID)
		var validationErrs []string
		for i, a := range d.Actions {
			if a.Data.Awaited == a.Data.Awaiter {
				validationErrs = append(validationErrs, fmt.Sprintf("data.actions.%d.data: Awaited and awaiter must be different promises", i))
			}
		}
		for _, a := range d.Actions {
			if a.Data.Awaited == d.ID {
				validationErrs = append(validationErrs, "data: Action awaited promise must not equal the task ID")
				break
			}
		}
		if len(validationErrs) > 0 {
			return s.respond(kind, 400, strings.Join(validationErrs, "; "))
		}
		// Each action's awaited/awaiter origin is resolved from headOrigin.
		pairs := make([][4]string, len(d.Actions))
		for i, a := range d.Actions {
			awaitedOrigin, _ := resolveOrigin(headOrigin, "", a.Data.Awaited)
			awaiterOrigin, _ := resolveOrigin(headOrigin, "", a.Data.Awaiter)
			pairs[i] = [4]string{awaitedOrigin, a.Data.Awaited, awaiterOrigin, a.Data.Awaiter}
		}
		return s.taskSuspend(now, taskOrigin, d.ID, d.Version, pairs)

	case "task.fulfill":
		var d taskFulfillData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		taskOrigin, _ := resolveOrigin(headOrigin, "", d.ID)
		actionP := d.Action.Data
		promiseOrigin, _ := resolveOrigin(headOrigin, "", actionP.ID)
		return s.taskFulfill(now, taskOrigin, d.ID, d.Version, promiseOrigin, actionP.ID, actionP.State, actionP.Value)

	case "task.fence":
		var d taskFenceData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		taskOrigin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.taskFence(now, taskOrigin, d.ID, d.Version, d.Action)

	case "task.heartbeat":
		var d taskHeartbeatData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		refs := make([][3]any, len(d.Tasks))
		for i, t := range d.Tasks {
			taskOrigin, _ := resolveOrigin(headOrigin, "", t.ID)
			refs[i] = [3]any{taskOrigin, t.ID, t.Version}
		}
		return s.taskHeartbeat(now, d.PID, refs)

	case "task.halt":
		var d taskHaltData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		origin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.taskHalt(now, origin, d.ID)

	case "task.continue":
		var d taskContinueData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		origin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.taskContinue(now, origin, d.ID)

	case "task.search":
		return s.respond(kind, 501, "Not implemented")

	case "schedule.get":
		var d scheduleGetData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		origin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.scheduleGet(origin, d.ID)

	case "schedule.create":
		var d scheduleCreateData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		origin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.scheduleCreate(now, origin, d.ID, d.Cron, d.PromiseID, d.PromiseTimeout, d.PromiseParam, d.PromiseTags)

	case "schedule.delete":
		var d scheduleDeleteData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		origin, _ := resolveOrigin(headOrigin, "", d.ID)
		return s.scheduleDelete(origin, d.ID)

	case "schedule.search":
		return s.respond(kind, 501, "Not implemented")

	case "debug.start":
		return s.respond(kind, 200, struct{}{})

	case "debug.reset":
		s.promises = make(map[string]map[string]*Promise)
		s.tasks = make(map[string]map[string]*Task)
		s.schedules = make(map[string]map[string]*Schedule)
		s.pTimeouts = s.pTimeouts[:0]
		s.tTimeouts = s.tTimeouts[:0]
		s.sTimeouts = s.sTimeouts[:0]
		s.outgoing = s.outgoing[:0]
		return s.respond(kind, 200, struct{}{})

	case "debug.snap":
		return s.debugSnap()

	case "debug.tick":
		var d debugTickData
		if err := unmarshal(&d); err != nil {
			return s.respond(kind, 400, err.Error())
		}
		return s.debugTick(d.Time)

	case "debug.stop":
		return s.respond(kind, 200, struct{}{})

	default:
		return nil, fmt.Errorf("unknown kind: %s", kind)
	}
}

// ─── Promise handlers ─────────────────────────────────────────────────────────

func (s *Server) promiseGet(now int64, origin, id string) ([]byte, error) {
	p := s.getPromise(origin, id)
	if p == nil {
		return s.respond("promise.get", 404, "Promise not found")
	}
	s.settleIfExpired(now, p)
	return s.respond("promise.get", 200, map[string]any{
		"promise": s.toPromiseRecord(p),
	})
}

func (s *Server) promiseCreate(now int64, origin, id string, timeoutAt int64, param Value, tags map[string]string) ([]byte, error) {
	if existing := s.getPromise(origin, id); existing != nil {
		s.settleIfExpired(now, existing)
		return s.respond("promise.create", 200, map[string]any{
			"promise": s.toPromiseRecord(existing),
		})
	}
	if tags == nil {
		tags = map[string]string{}
	}

	if now >= timeoutAt {
		t := timeoutAt
		p := &Promise{
			Origin:    origin,
			ID:        id,
			State:     s.timeoutState(tags),
			Param:     param,
			Tags:      tags,
			TimeoutAt: timeoutAt,
			CreatedAt: timeoutAt,
			SettledAt: &t,
			Callbacks: make(map[string]string),
			Listeners: make(map[string]struct{}),
		}
		s.setPromise(origin, id, p)
		if tags["resonate:target"] != "" {
			s.setTask(origin, id, &Task{
				Origin:  origin,
				ID:      id,
				State:   "fulfilled",
				Version: 0,
				Resumes: make(map[string]struct{}),
			})
		}
		return s.respond("promise.create", 200, map[string]any{"promise": s.toPromiseRecord(p)})
	}

	p := &Promise{
		Origin:    origin,
		ID:        id,
		State:     "pending",
		Param:     param,
		Tags:      tags,
		TimeoutAt: timeoutAt,
		CreatedAt: now,
		Callbacks: make(map[string]string),
		Listeners: make(map[string]struct{}),
	}
	s.setPromise(origin, id, p)
	s.setPTimeout(PTimeout{origin, id, timeoutAt})

	if addr := tags["resonate:target"]; addr != "" {
		delayVal := tags["resonate:delay"]
		delay := int64(0)
		deferred := false
		if delayVal != "" {
			if _, err := fmt.Sscanf(delayVal, "%d", &delay); err == nil && delay > now {
				deferred = true
			}
		}
		s.setTask(origin, id, &Task{Origin: origin, ID: id, State: "pending", Version: 0, Resumes: make(map[string]struct{})})
		if deferred {
			s.setTTimeout(TTimeout{origin, id, 0, delay})
		} else {
			s.setTTimeout(TTimeout{origin, id, 0, now + pendingRetryTTL})
			s.sendExecute(addr, id, 0)
		}
	}
	return s.respond("promise.create", 200, map[string]any{"promise": s.toPromiseRecord(p)})
}

func (s *Server) promiseSettle(now int64, origin, id, state string, value Value) ([]byte, error) {
	p := s.getPromise(origin, id)
	if p == nil {
		return s.respond("promise.settle", 404, "Promise not found")
	}
	s.settleIfExpired(now, p)
	if p.State != "pending" {
		return s.respond("promise.settle", 200, map[string]any{"promise": s.toPromiseRecord(p)})
	}
	p.State = state
	p.Value = value
	t := now
	p.SettledAt = &t
	s.delPTimeout(origin, id)
	s.triggerSettlement(origin, id, now)
	return s.respond("promise.settle", 200, map[string]any{"promise": s.toPromiseRecord(p)})
}

func (s *Server) promiseRegisterCallback(now int64, awaitedOrigin, awaited, awaiterOrigin, awaiter string) ([]byte, error) {
	awaitedP := s.getPromise(awaitedOrigin, awaited)
	if awaitedP == nil {
		return s.respond("promise.register_callback", 404, "Awaited promise not found")
	}
	s.settleIfExpired(now, awaitedP)
	awaiterP := s.getPromise(awaiterOrigin, awaiter)
	if awaiterP == nil {
		return s.respond("promise.register_callback", 422, "Awaiter promise not found")
	}
	s.settleIfExpired(now, awaiterP)
	if awaiterP.Tags["resonate:target"] == "" {
		return s.respond("promise.register_callback", 422, "Awaiter has no address")
	}
	if awaitedP.State == "pending" {
		if awaiterP.State == "pending" {
			awaitedP.Callbacks[awaiter] = awaiterOrigin
		}
	} else {
		// Awaited promise is already settled: EnqueueResume on the awaiter task.
		t := s.getTask(awaiterOrigin, awaiter)
		if t != nil && awaiterP.State == "pending" {
			if awaiterP.TimeoutAt <= now {
				if t.State == "suspended" {
					t.State = "pending"
					t.Resumes = make(map[string]struct{})
					s.setTTimeout(TTimeout{awaiterOrigin, awaiter, 0, now + pendingRetryTTL})
				}
			} else {
				switch t.State {
				case "suspended":
					t.State = "pending"
					t.Resumes = map[string]struct{}{awaited: {}}
					s.setTTimeout(TTimeout{awaiterOrigin, awaiter, 0, now + pendingRetryTTL})
					s.sendExecute(awaiterP.Tags["resonate:target"], awaiter, t.Version)
				case "pending", "acquired", "halted":
					t.Resumes[awaited] = struct{}{}
				}
			}
		}
	}
	return s.respond("promise.register_callback", 200, map[string]any{
		"promise": s.toPromiseRecord(awaitedP),
	})
}

func (s *Server) promiseRegisterListener(now int64, awaitedOrigin, awaited, address string) ([]byte, error) {
	p := s.getPromise(awaitedOrigin, awaited)
	if p == nil {
		return s.respond("promise.register_listener", 404, "Promise not found")
	}
	s.settleIfExpired(now, p)
	if p.State == "pending" {
		p.Listeners[address] = struct{}{}
	}
	return s.respond("promise.register_listener", 200, map[string]any{
		"promise": s.toPromiseRecord(p),
	})
}

// ─── Task handlers ────────────────────────────────────────────────────────────

func (s *Server) taskGet(now int64, origin, id string) ([]byte, error) {
	p := s.getPromise(origin, id)
	s.settleIfExpired(now, p)
	t := s.getTask(origin, id)
	if t == nil {
		return s.respond("task.get", 404, "Task not found")
	}
	return s.respond("task.get", 200, map[string]any{"task": s.toTaskRecord(t)})
}

func (s *Server) taskCreate(now int64, pid string, ttl int64, promiseOrigin, promiseID string, timeoutAt int64, param Value, tags map[string]string) ([]byte, error) {
	if tags == nil {
		tags = map[string]string{}
	}
	if existingTask := s.getTask(promiseOrigin, promiseID); existingTask != nil {
		p := s.getPromise(promiseOrigin, promiseID)
		s.settleIfExpired(now, p)
		switch existingTask.State {
		case "pending":
			newVersion := existingTask.Version + 1
			existingTask.State = "acquired"
			existingTask.Version = newVersion
			existingTask.PID = pid
			ttlVal := ttl
			existingTask.TTL = &ttlVal
			existingTask.Resumes = make(map[string]struct{})
			s.setTTimeout(TTimeout{promiseOrigin, promiseID, 1, now + ttl})
			return s.respond("task.create", 200, map[string]any{
				"task":    s.toTaskRecord(existingTask),
				"promise": s.toPromiseRecord(p),
				"preload": s.preload(promiseOrigin, promiseID),
			})
		case "fulfilled":
			return s.respond("task.create", 200, map[string]any{
				"task":    s.toTaskRecord(existingTask),
				"promise": s.toPromiseRecord(p),
				"preload": s.preload(promiseOrigin, promiseID),
			})
		default:
			return s.respond("task.create", 409, "Task already exists")
		}
	}

	if existing := s.getPromise(promiseOrigin, promiseID); existing != nil {
		if existing.Tags["resonate:target"] == "" {
			return s.respond("task.create", 422, "Promise has no address")
		}
		return s.respond("task.create", 409, "Promise already exists")
	}

	if now >= timeoutAt {
		t := timeoutAt
		p := &Promise{
			Origin:    promiseOrigin,
			ID:        promiseID,
			State:     s.timeoutState(tags),
			Param:     param,
			Tags:      tags,
			TimeoutAt: timeoutAt,
			CreatedAt: timeoutAt,
			SettledAt: &t,
			Callbacks: make(map[string]string),
			Listeners: make(map[string]struct{}),
		}
		s.setPromise(promiseOrigin, promiseID, p)
		task := &Task{Origin: promiseOrigin, ID: promiseID, State: "fulfilled", Version: 0, Resumes: make(map[string]struct{})}
		s.setTask(promiseOrigin, promiseID, task)
		return s.respond("task.create", 200, map[string]any{
			"task":    s.toTaskRecord(task),
			"promise": s.toPromiseRecord(p),
			"preload": []PromiseRecord{},
		})
	}

	p := &Promise{
		Origin:    promiseOrigin,
		ID:        promiseID,
		State:     "pending",
		Param:     param,
		Tags:      tags,
		TimeoutAt: timeoutAt,
		CreatedAt: now,
		Callbacks: make(map[string]string),
		Listeners: make(map[string]struct{}),
	}
	s.setPromise(promiseOrigin, promiseID, p)
	s.setPTimeout(PTimeout{promiseOrigin, promiseID, timeoutAt})

	ttlVal := ttl
	task := &Task{Origin: promiseOrigin, ID: promiseID, State: "acquired", Version: 1, PID: pid, TTL: &ttlVal, Resumes: make(map[string]struct{})}
	s.setTask(promiseOrigin, promiseID, task)
	s.setTTimeout(TTimeout{promiseOrigin, promiseID, 1, now + ttl})

	return s.respond("task.create", 200, map[string]any{
		"task":    s.toTaskRecord(task),
		"promise": s.toPromiseRecord(p),
		"preload": s.preload(promiseOrigin, promiseID),
	})
}

func (s *Server) taskAcquire(now int64, origin, id string, version int, pid string, ttl int64) ([]byte, error) {
	p := s.getPromise(origin, id)
	s.settleIfExpired(now, p)
	t := s.getTask(origin, id)
	if t == nil {
		return s.respond("task.acquire", 404, "Task not found")
	}
	if t.State != "pending" {
		return s.respond("task.acquire", 409, "Task not in pending state")
	}
	if p.State != "pending" {
		return s.respond("task.acquire", 409, "Task logically fulfilled")
	}
	if t.Version != version {
		return s.respond("task.acquire", 409, "Version mismatch")
	}
	newVersion := t.Version + 1
	t.State = "acquired"
	t.Version = newVersion
	t.PID = pid
	ttlVal := ttl
	t.TTL = &ttlVal
	t.Resumes = make(map[string]struct{})
	s.setTTimeout(TTimeout{origin, id, 1, now + ttl})
	return s.respond("task.acquire", 200, map[string]any{
		"task":    s.toTaskRecord(t),
		"promise": s.toPromiseRecord(p),
		"preload": s.preload(origin, id),
	})
}

func (s *Server) taskRelease(now int64, origin, id string, version int) ([]byte, error) {
	p := s.getPromise(origin, id)
	s.settleIfExpired(now, p)
	t := s.getTask(origin, id)
	if t == nil {
		return s.respond("task.release", 404, "Task not found")
	}
	if t.State != "acquired" {
		return s.respond("task.release", 409, "Task not acquired")
	}
	if p.State != "pending" {
		return s.respond("task.release", 409, "Task not acquired")
	}
	if t.Version != version {
		return s.respond("task.release", 409, "Version mismatch")
	}
	t.State = "pending"
	t.PID = ""
	t.TTL = nil
	s.setTTimeout(TTimeout{origin, id, 0, now + pendingRetryTTL})
	s.sendExecute(p.Tags["resonate:target"], id, t.Version)
	return s.respond("task.release", 200, struct{}{})
}

func (s *Server) taskFulfill(now int64, taskOrigin, taskID string, version int, promiseOrigin, promiseID, state string, value Value) ([]byte, error) {
	p := s.getPromise(taskOrigin, taskID)
	s.settleIfExpired(now, p)
	t := s.getTask(taskOrigin, taskID)
	if t == nil {
		return s.respond("task.fulfill", 404, "Task not found")
	}
	if t.State != "acquired" {
		return s.respond("task.fulfill", 409, "Task not acquired")
	}
	if p.State != "pending" {
		return s.respond("task.fulfill", 409, "Task not acquired")
	}
	if t.Version != version {
		return s.respond("task.fulfill", 409, "Version mismatch")
	}
	actionP := s.getPromise(promiseOrigin, promiseID)
	s.settleIfExpired(now, actionP)
	if actionP.State != "pending" {
		s.triggerFulfilled(taskOrigin, taskID)
		return s.respond("task.fulfill", 200, map[string]any{"promise": s.toPromiseRecord(actionP)})
	}
	settled := now
	actionP.State = state
	actionP.Value = value
	actionP.SettledAt = &settled
	s.delPTimeout(promiseOrigin, promiseID)
	s.triggerSettlement(promiseOrigin, promiseID, now)
	return s.respond("task.fulfill", 200, map[string]any{"promise": s.toPromiseRecord(actionP)})
}

func (s *Server) taskSuspend(now int64, taskOrigin, id string, version int, actions [][4]string) ([]byte, error) {
	p := s.getPromise(taskOrigin, id)
	s.settleIfExpired(now, p)
	t := s.getTask(taskOrigin, id)
	if t == nil {
		return s.respond("task.suspend", 404, "Task not found")
	}
	if t.State != "acquired" {
		return s.respond("task.suspend", 409, "Task not acquired")
	}
	if p.State != "pending" {
		return s.respond("task.suspend", 409, "Task not acquired")
	}
	if t.Version != version {
		return s.respond("task.suspend", 409, "Version mismatch")
	}

	type awaitedRef struct {
		origin string
		p      *Promise
	}
	// Pass 1: nil-check all awaiteds before settling any, matching the handler's
	// read-all-then-settle structure. Settling inside the nil-check loop would
	// leave earlier awaiteds settled even when a later one returns 422.
	fetchedAwaiteds := make([]awaitedRef, 0, len(actions))
	for _, pair := range actions {
		awaitedOrigin, awaitedID := pair[0], pair[1]
		awaited := s.getPromise(awaitedOrigin, awaitedID)
		if awaited == nil {
			return s.respond("task.suspend", 422, struct{}{})
		}
		fetchedAwaiteds = append(fetchedAwaiteds, awaitedRef{awaitedOrigin, awaited})
	}
	// Pass 2: settle and categorize.
	var pendingAwaiteds []awaitedRef
	hasSettled := false
	for _, ref := range fetchedAwaiteds {
		s.settleIfExpired(now, ref.p)
		if ref.p.State == "pending" {
			pendingAwaiteds = append(pendingAwaiteds, ref)
		} else {
			hasSettled = true
		}
	}

	if hasSettled {
		t.Resumes = make(map[string]struct{})
		return s.respond("task.suspend", 300, map[string]any{"preload": s.preload(taskOrigin, id)})
	}

	for _, ref := range pendingAwaiteds {
		ref.p.Callbacks[id] = taskOrigin
	}
	t.State = "suspended"
	t.PID = ""
	t.TTL = nil
	t.Resumes = make(map[string]struct{})
	s.delTTimeout(taskOrigin, id)
	return s.respond("task.suspend", 200, struct{}{})
}

func (s *Server) taskHalt(now int64, origin, id string) ([]byte, error) {
	p := s.getPromise(origin, id)
	s.settleIfExpired(now, p)
	t := s.getTask(origin, id)
	if t == nil {
		return s.respond("task.halt", 404, "Task not found")
	}
	if p.State != "pending" || t.State == "fulfilled" {
		return s.respond("task.halt", 409, "Task is fulfilled")
	}
	if t.State != "halted" {
		t.State = "halted"
		t.PID = ""
		t.TTL = nil
		s.delTTimeout(origin, id)
	}
	return s.respond("task.halt", 200, struct{}{})
}

func (s *Server) taskContinue(now int64, origin, id string) ([]byte, error) {
	p := s.getPromise(origin, id)
	s.settleIfExpired(now, p)
	t := s.getTask(origin, id)
	if t == nil {
		return s.respond("task.continue", 404, "Task not found")
	}
	if p.State != "pending" {
		return s.respond("task.continue", 409, "Task is not halted")
	}
	if t.State != "halted" {
		return s.respond("task.continue", 409, "Task is not halted")
	}
	t.State = "pending"
	s.setTTimeout(TTimeout{origin, id, 0, now + pendingRetryTTL})
	s.sendExecute(p.Tags["resonate:target"], id, t.Version)
	return s.respond("task.continue", 200, struct{}{})
}

func (s *Server) taskFence(now int64, taskOrigin, id string, version int, actionRaw json.RawMessage) ([]byte, error) {
	// 1. Decode and validate inner action (input-only, no state lookup needed).
	var kindHolder struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(actionRaw, &kindHolder); err != nil {
		return s.respond("task.fence", 400, err.Error())
	}

	type createPayload struct {
		Head map[string]any `json:"head"`
		Data struct {
			ID        string            `json:"id"`
			TimeoutAt int64             `json:"timeoutAt"`
			Param     Value             `json:"param"`
			Tags      map[string]string `json:"tags"`
		} `json:"data"`
	}
	type settlePayload struct {
		Head map[string]any `json:"head"`
		Data struct {
			ID    string `json:"id"`
			State string `json:"state"`
			Value Value  `json:"value"`
		} `json:"data"`
	}

	var create *createPayload
	var settle *settlePayload
	switch kindHolder.Kind {
	case "promise.create":
		var d createPayload
		if err2 := json.Unmarshal(actionRaw, &d); err2 != nil {
			return s.respond("task.fence", 400, err2.Error())
		}
		if d.Data.ID == id {
			return s.respond("task.fence", 400, "Action ID must not equal task ID")
		}
		create = &d
	case "promise.settle":
		var d settlePayload
		if err2 := json.Unmarshal(actionRaw, &d); err2 != nil {
			return s.respond("task.fence", 400, err2.Error())
		}
		if d.Data.ID == id {
			return s.respond("task.fence", 400, "Action ID must not equal task ID")
		}
		settle = &d
	default:
		return s.respond("task.fence", 400, "Unknown action kind: "+kindHolder.Kind)
	}

	// 2. Task/promise state checks.
	t := s.getTask(taskOrigin, id)
	if t == nil {
		return s.respond("task.fence", 404, "Task not found")
	}
	p := s.getPromise(taskOrigin, id)
	s.settleIfExpired(now, p)
	if t.State != "acquired" {
		return s.respond("task.fence", 409, "Fence check failed")
	}
	if p.State != "pending" {
		return s.respond("task.fence", 409, "Fence check failed")
	}
	if t.Version != version {
		return s.respond("task.fence", 409, "Fence check failed")
	}

	// 3. Dispatch.
	var innerResp []byte
	var err error
	switch {
	case create != nil:
		innerHeadOrigin, _ := create.Head["resonate:origin"].(string)
		innerTagOrigin := create.Data.Tags["resonate:origin"]
		promiseOrigin, err2 := resolveOrigin(innerHeadOrigin, innerTagOrigin, create.Data.ID)
		if err2 != nil {
			return s.respond("task.fence", 400, err2.Error())
		}
		innerResp, err = s.promiseCreate(now, promiseOrigin, create.Data.ID, create.Data.TimeoutAt, create.Data.Param, create.Data.Tags)
	case settle != nil:
		innerHeadOrigin, _ := settle.Head["resonate:origin"].(string)
		promiseOrigin, _ := resolveOrigin(innerHeadOrigin, "", settle.Data.ID)
		innerResp, err = s.promiseSettle(now, promiseOrigin, settle.Data.ID, settle.Data.State, settle.Data.Value)
	}
	if err != nil {
		return nil, err
	}
	var innerParsed any
	json.Unmarshal(innerResp, &innerParsed)
	return s.respond("task.fence", 200, map[string]any{
		"action":  innerParsed,
		"preload": []PromiseRecord{},
	})
}

func (s *Server) taskHeartbeat(now int64, pid string, refs [][3]any) ([]byte, error) {
	for _, ref := range refs {
		origin := ref[0].(string)
		id := ref[1].(string)
		version := ref[2].(int)
		p := s.getPromise(origin, id)
		s.settleIfExpired(now, p)
		t := s.getTask(origin, id)
		if t == nil || t.State != "acquired" || t.Version != version || t.PID != pid {
			continue
		}
		if p.State != "pending" {
			continue
		}
		ttl := int64(30_000)
		if t.TTL != nil {
			ttl = *t.TTL
		}
		s.setTTimeout(TTimeout{origin, id, 1, now + ttl})
	}
	return s.respond("task.heartbeat", 200, struct{}{})
}

// ─── Schedule handlers ────────────────────────────────────────────────────────

func (s *Server) scheduleGet(origin, id string) ([]byte, error) {
	sc := s.getSchedule(origin, id)
	if sc == nil {
		return s.respond("schedule.get", 404, "Schedule not found")
	}
	return s.respond("schedule.get", 200, map[string]any{"schedule": s.toScheduleRecord(sc)})
}

func (s *Server) scheduleCreate(now int64, origin, id, cronExpr, promiseID string, promiseTimeout int64, promiseParam Value, promiseTags map[string]string) ([]byte, error) {
	if existing := s.getSchedule(origin, id); existing != nil {
		return s.respond("schedule.create", 200, map[string]any{"schedule": s.toScheduleRecord(existing)})
	}
	nextRunAt, err := nextCron(cronExpr, now)
	if err != nil {
		return s.respond("schedule.create", 400, "Invalid cron expression")
	}
	if promiseTags == nil {
		promiseTags = map[string]string{}
	}
	sc := &Schedule{
		ID:             id,
		Origin:         origin,
		Cron:           cronExpr,
		PromiseID:      promiseID,
		PromiseTimeout: promiseTimeout,
		PromiseParam:   promiseParam,
		PromiseTags:    promiseTags,
		CreatedAt:      now,
	}
	s.setSchedule(origin, id, sc)
	s.setSTimeout(STimeout{origin, id, nextRunAt, now})
	return s.respond("schedule.create", 200, map[string]any{"schedule": s.toScheduleRecord(sc)})
}

func (s *Server) scheduleDelete(origin, id string) ([]byte, error) {
	if s.getSchedule(origin, id) == nil {
		return s.respond("schedule.delete", 404, "Schedule not found")
	}
	if inner := s.schedules[origin]; inner != nil {
		delete(inner, id)
	}
	s.delSTimeout(origin, id)
	return s.respond("schedule.delete", 200, struct{}{})
}

// ─── Debug handlers ───────────────────────────────────────────────────────────

func (s *Server) debugSnap() ([]byte, error) {
	type pKey struct{ origin, id string }
	var pKeys []pKey
	for origin, inner := range s.promises {
		for id := range inner {
			pKeys = append(pKeys, pKey{origin, id})
		}
	}
	sort.Slice(pKeys, func(i, j int) bool {
		if pKeys[i].origin != pKeys[j].origin {
			return pKeys[i].origin < pKeys[j].origin
		}
		return pKeys[i].id < pKeys[j].id
	})

	promises := make([]PromiseRecord, 0, len(pKeys))
	callbacks := make([]CallbackEntry, 0)
	listeners := make([]ListenerEntry, 0)
	for _, k := range pKeys {
		p := s.promises[k.origin][k.id]
		rec := s.toPromiseRecord(p)
		o := p.Origin
		rec.Origin = &o
		promises = append(promises, rec)
		awaiters := make([]string, 0, len(p.Callbacks))
		for awaiter := range p.Callbacks {
			awaiters = append(awaiters, awaiter)
		}
		sort.Strings(awaiters)
		for _, awaiter := range awaiters {
			o := p.Origin
			callbacks = append(callbacks, CallbackEntry{Awaiter: awaiter, Awaited: p.ID, Origin: &o})
		}
		addrs := make([]string, 0, len(p.Listeners))
		for addr := range p.Listeners {
			addrs = append(addrs, addr)
		}
		sort.Strings(addrs)
		for _, addr := range addrs {
			o := p.Origin
			listeners = append(listeners, ListenerEntry{ID: p.ID, Address: addr, Origin: &o})
		}
	}

	type tKey struct{ origin, id string }
	var tKeys []tKey
	for origin, inner := range s.tasks {
		for id := range inner {
			tKeys = append(tKeys, tKey{origin, id})
		}
	}
	sort.Slice(tKeys, func(i, j int) bool {
		if tKeys[i].origin != tKeys[j].origin {
			return tKeys[i].origin < tKeys[j].origin
		}
		return tKeys[i].id < tKeys[j].id
	})
	tasks := make([]TaskRecord, 0, len(tKeys))
	for _, k := range tKeys {
		rec := s.toTaskRecord(s.tasks[k.origin][k.id])
		o := k.origin
		rec.Origin = &o
		tasks = append(tasks, rec)
	}

	pTimeouts := make([]TimeoutEntry, len(s.pTimeouts))
	for i, pt := range s.pTimeouts {
		pTimeouts[i] = TimeoutEntry{ID: pt.ID, Origin: pt.Origin, Timeout: pt.Timeout}
	}
	tTimeouts := make([]TaskTimeoutEntry, len(s.tTimeouts))
	for i, tt := range s.tTimeouts {
		tTimeouts[i] = TaskTimeoutEntry{ID: tt.ID, Origin: tt.Origin, Type: tt.Type, Timeout: tt.Timeout}
	}

	type schedKey struct{ origin, id string }
	var sKeys []schedKey
	for origin, inner := range s.schedules {
		for id := range inner {
			sKeys = append(sKeys, schedKey{origin, id})
		}
	}
	sort.Slice(sKeys, func(i, j int) bool {
		if sKeys[i].origin != sKeys[j].origin {
			return sKeys[i].origin < sKeys[j].origin
		}
		return sKeys[i].id < sKeys[j].id
	})
	schedules := make([]ScheduleRecord, 0, len(sKeys))
	for _, k := range sKeys {
		rec := s.toScheduleRecord(s.schedules[k.origin][k.id])
		o := k.origin
		rec.Origin = &o
		schedules = append(schedules, rec)
	}

	schedTimeouts := make([]TimeoutEntry, len(s.sTimeouts))
	for i, st := range s.sTimeouts {
		schedTimeouts[i] = TimeoutEntry{ID: st.ID, Origin: st.Origin, Timeout: st.Timeout}
	}

	messages := s.outgoing
	if messages == nil {
		messages = []MessageEntry{}
	}
	data := SnapData{
		Promises:         promises,
		PromiseTimeouts:  pTimeouts,
		Callbacks:        callbacks,
		Listeners:        listeners,
		Tasks:            tasks,
		TaskTimeouts:     tTimeouts,
		Schedules:        schedules,
		ScheduleTimeouts: schedTimeouts,
		Messages:         messages,
	}
	return s.respond("debug.snap", 200, data)
}

func (s *Server) debugTick(now int64) ([]byte, error) {
	type settleRef struct{ origin, id, state string }
	type releaseRef struct {
		origin  string
		id      string
		version int
	}
	type retryRef struct {
		origin  string
		id      string
		version int
	}

	var settles []settleRef
	var releases []releaseRef
	var retries []retryRef

	for _, pt := range s.pTimeouts {
		if now >= pt.Timeout {
			p := s.getPromise(pt.Origin, pt.ID)
			if p != nil && p.State == "pending" {
				settles = append(settles, settleRef{pt.Origin, pt.ID, s.timeoutState(p.Tags)})
			}
		}
	}
	for _, tt := range s.tTimeouts {
		if now < tt.Timeout {
			continue
		}
		if tt.Type == 1 {
			t := s.getTask(tt.Origin, tt.ID)
			if t != nil && t.State == "acquired" {
				releases = append(releases, releaseRef{tt.Origin, tt.ID, t.Version})
			}
		} else {
			t := s.getTask(tt.Origin, tt.ID)
			if t != nil && t.State == "pending" {
				retries = append(retries, retryRef{tt.Origin, tt.ID, t.Version})
			}
		}
	}

	// Phase 1: settle promises
	type settledRef struct{ origin, id string }
	var settled []settledRef
	for _, sv := range settles {
		p := s.getPromise(sv.origin, sv.id)
		if p == nil || p.State != "pending" {
			continue
		}
		t := p.TimeoutAt
		p.State = sv.state
		p.SettledAt = &t
		s.delPTimeout(sv.origin, sv.id)
		settled = append(settled, settledRef{sv.origin, sv.id})
	}

	// Phase 2: fulfill tasks whose own promise settled
	for _, ref := range settled {
		s.triggerFulfilled(ref.origin, ref.id)
	}

	// Phase 3: resume callbacks and notify listeners
	for _, ref := range settled {
		s.triggerCallbacks(ref.origin, ref.id, now)
		s.triggerListeners(ref.origin, ref.id)
	}

	// Task releases
	for _, r := range releases {
		t := s.getTask(r.origin, r.id)
		if t == nil || t.State != "acquired" || t.Version != r.version {
			continue
		}
		t.State = "pending"
		t.PID = ""
		t.TTL = nil
		s.setTTimeout(TTimeout{r.origin, r.id, 0, now + pendingRetryTTL})
		p := s.getPromise(r.origin, r.id)
		s.sendExecute(p.Tags["resonate:target"], r.id, t.Version)
	}

	// Task retries
	for _, r := range retries {
		t := s.getTask(r.origin, r.id)
		if t == nil || t.State != "pending" {
			continue
		}
		s.setTTimeout(TTimeout{r.origin, r.id, 0, now + pendingRetryTTL})
		p := s.getPromise(r.origin, r.id)
		s.sendExecute(p.Tags["resonate:target"], r.id, t.Version)
	}

	// Schedule timeouts — sort by (timeout, id) for determinism
	type stEntry struct {
		origin  string
		id      string
		timeout int64
	}
	var due []stEntry
	for _, st := range s.sTimeouts {
		if now >= st.Timeout {
			due = append(due, stEntry{st.Origin, st.ID, st.Timeout})
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].timeout != due[j].timeout {
			return due[i].timeout < due[j].timeout
		}
		return due[i].id < due[j].id
	})

	for _, entry := range due {
		sc := s.getSchedule(entry.origin, entry.id)
		if sc == nil {
			continue
		}
		currentTimeout := entry.timeout
		for currentTimeout <= now {
			promiseID := strings.ReplaceAll(sc.PromiseID, "{{.id}}", sc.ID)
			promiseID = strings.ReplaceAll(promiseID, "{{.timestamp}}", fmt.Sprintf("%d", currentTimeout))

			tags := make(map[string]string, len(sc.PromiseTags)+1)
			for k, v := range sc.PromiseTags {
				tags[k] = v
			}
			tags["resonate:schedule"] = sc.ID
			tags["resonate:origin"] = promiseID
			tags["resonate:branch"] = promiseID
			tags["resonate:parent"] = promiseID

			timeoutAt := currentTimeout + sc.PromiseTimeout
			if s.getPromise(promiseID, promiseID) == nil && now >= timeoutAt {
				t := timeoutAt
				s.setPromise(promiseID, promiseID, &Promise{
					Origin:    promiseID,
					ID:        promiseID,
					State:     s.timeoutState(tags),
					Param:     sc.PromiseParam,
					Tags:      tags,
					TimeoutAt: timeoutAt,
					CreatedAt: currentTimeout,
					SettledAt: &t,
					Callbacks: make(map[string]string),
					Listeners: make(map[string]struct{}),
				})
			} else {
				s.promiseCreate(currentTimeout, promiseID, promiseID, timeoutAt, sc.PromiseParam, tags)
			}

			nextT, err := nextCron(sc.Cron, currentTimeout)
			if err != nil {
				break
			}
			lastRun := currentTimeout
			sc.LastRunAt = &lastRun
			currentTimeout = nextT
		}
		s.setSTimeout(STimeout{entry.origin, entry.id, currentTimeout, sc.CreatedAt})
	}

	return s.respond("debug.tick", 200, []any{})
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// settleIfExpired mutates p in place if it is a pending promise that has expired.
// Returns true if the promise was expired (and is now settled in the state).
func (s *Server) settleIfExpired(now int64, p *Promise) bool {
	if p == nil || p.State != "pending" || now < p.TimeoutAt {
		return false
	}
	p.State = s.timeoutState(p.Tags)
	t := p.TimeoutAt
	p.SettledAt = &t
	s.delPTimeout(p.Origin, p.ID)
	s.triggerSettlement(p.Origin, p.ID, now)
	return true
}

func (s *Server) triggerSettlement(origin, id string, now int64) {
	s.triggerFulfilled(origin, id)
	s.triggerCallbacks(origin, id, now)
	s.triggerListeners(origin, id)
}

func (s *Server) triggerFulfilled(origin, promiseID string) {
	t := s.getTask(origin, promiseID)
	if t == nil || t.State == "fulfilled" {
		return
	}
	t.State = "fulfilled"
	t.PID = ""
	t.TTL = nil
	t.Resumes = make(map[string]struct{})
	s.delTTimeout(origin, promiseID)
}

func (s *Server) triggerCallbacks(origin, promiseID string, now int64) {
	p := s.getPromise(origin, promiseID)
	if p == nil {
		return
	}
	for awaiterID, awaiterOrigin := range p.Callbacks {
		t := s.getTask(awaiterOrigin, awaiterID)
		if t == nil {
			continue
		}
		awaiterP := s.getPromise(awaiterOrigin, awaiterID)
		if awaiterP == nil || awaiterP.State != "pending" {
			continue
		}
		expired := awaiterP.TimeoutAt <= now
		if expired {
			if t.State == "suspended" {
				t.State = "pending"
				t.Resumes = make(map[string]struct{})
				s.setTTimeout(TTimeout{awaiterOrigin, awaiterID, 0, now + pendingRetryTTL})
			}
			continue
		}
		switch t.State {
		case "suspended":
			t.State = "pending"
			t.Resumes = map[string]struct{}{promiseID: {}}
			s.setTTimeout(TTimeout{awaiterOrigin, awaiterID, 0, now + pendingRetryTTL})
			s.sendExecute(awaiterP.Tags["resonate:target"], awaiterID, t.Version)
		case "pending", "acquired", "halted":
			t.Resumes[promiseID] = struct{}{}
		}
	}
	p.Callbacks = make(map[string]string)
}

func (s *Server) triggerListeners(origin, promiseID string) {
	p := s.getPromise(origin, promiseID)
	if p == nil || len(p.Listeners) == 0 {
		return
	}
	rec := s.toPromiseRecord(p)
	data, _ := json.Marshal(struct {
		Kind string   `json:"kind"`
		Head struct{} `json:"head"`
		Data struct {
			Promise PromiseRecord `json:"promise"`
		} `json:"data"`
	}{Kind: "unblock", Data: struct {
		Promise PromiseRecord `json:"promise"`
	}{Promise: rec}})
	for addr := range p.Listeners {
		s.outgoing = append(s.outgoing, MessageEntry{Address: addr, Message: data})
	}
	p.Listeners = make(map[string]struct{})
}

func (s *Server) sendExecute(address, taskID string, version int) {
	msg, _ := json.Marshal(struct {
		Kind string   `json:"kind"`
		Head struct{} `json:"head"`
		Data struct {
			Task struct {
				ID      string `json:"id"`
				Version int    `json:"version"`
			} `json:"task"`
		} `json:"data"`
	}{Kind: "execute", Data: struct {
		Task struct {
			ID      string `json:"id"`
			Version int    `json:"version"`
		} `json:"task"`
	}{Task: struct {
		ID      string `json:"id"`
		Version int    `json:"version"`
	}{ID: taskID, Version: version}}})

	for i, m := range s.outgoing {
		var k struct {
			Kind string `json:"kind"`
			Data struct {
				Task struct {
					ID string `json:"id"`
				} `json:"task"`
			} `json:"data"`
		}
		if json.Unmarshal(m.Message, &k) == nil && k.Kind == "execute" && k.Data.Task.ID == taskID {
			s.outgoing[i] = MessageEntry{Address: address, Message: msg}
			return
		}
	}
	s.outgoing = append(s.outgoing, MessageEntry{Address: address, Message: msg})
}

func (s *Server) preload(origin, promiseID string) []PromiseRecord {
	p := s.getPromise(origin, promiseID)
	if p == nil {
		return []PromiseRecord{}
	}
	branch := p.Tags["resonate:branch"]
	if branch == "" {
		return []PromiseRecord{}
	}
	var results []PromiseRecord
	for _, inner := range s.promises {
		for _, other := range inner {
			if other.ID != promiseID && other.Tags["resonate:branch"] == branch {
				results = append(results, s.toPromiseRecord(other))
			}
		}
	}
	if results == nil {
		return []PromiseRecord{}
	}
	return results
}

func (s *Server) timeoutState(tags map[string]string) string {
	if tags["resonate:timer"] == "true" {
		return "resolved"
	}
	return "rejected_timedout"
}

// ─── Converters ───────────────────────────────────────────────────────────────

func (s *Server) toPromiseRecord(p *Promise) PromiseRecord {
	return PromiseRecord{
		ID:        p.ID,
		State:     p.State,
		Param:     p.Param,
		Value:     p.Value,
		Tags:      p.Tags,
		TimeoutAt: p.TimeoutAt,
		CreatedAt: p.CreatedAt,
		SettledAt: p.SettledAt,
	}
}

func (s *Server) toTaskRecord(t *Task) TaskRecord {
	resumes := make([]string, 0, len(t.Resumes))
	for id := range t.Resumes {
		resumes = append(resumes, id)
	}
	sort.Strings(resumes)
	rec := TaskRecord{
		ID:      t.ID,
		State:   t.State,
		Version: t.Version,
		Resumes: resumes,
	}
	if t.PID != "" {
		rec.PID = t.PID
	}
	if t.TTL != nil {
		rec.TTL = t.TTL
	}
	return rec
}

func (s *Server) toScheduleRecord(sc *Schedule) ScheduleRecord {
	nextRunAt := int64(0)
	for _, st := range s.sTimeouts {
		if st.Origin == sc.Origin && st.ID == sc.ID {
			nextRunAt = st.Timeout
			break
		}
	}
	return ScheduleRecord{
		ID:             sc.ID,
		Cron:           sc.Cron,
		PromiseID:      sc.PromiseID,
		PromiseTimeout: sc.PromiseTimeout,
		PromiseParam:   sc.PromiseParam,
		PromiseTags:    sc.PromiseTags,
		CreatedAt:      sc.CreatedAt,
		NextRunAt:      nextRunAt,
		LastRunAt:      sc.LastRunAt,
	}
}

// ─── Timeout list helpers ─────────────────────────────────────────────────────

func (s *Server) setPTimeout(pt PTimeout) {
	for i, e := range s.pTimeouts {
		if e.Origin == pt.Origin && e.ID == pt.ID {
			s.pTimeouts[i] = pt
			return
		}
	}
	s.pTimeouts = append(s.pTimeouts, pt)
}

func (s *Server) delPTimeout(origin, id string) {
	for i, e := range s.pTimeouts {
		if e.Origin == origin && e.ID == id {
			s.pTimeouts = append(s.pTimeouts[:i], s.pTimeouts[i+1:]...)
			return
		}
	}
}

func (s *Server) setTTimeout(tt TTimeout) {
	for i, e := range s.tTimeouts {
		if e.Origin == tt.Origin && e.ID == tt.ID {
			s.tTimeouts[i] = tt
			return
		}
	}
	s.tTimeouts = append(s.tTimeouts, tt)
}

func (s *Server) delTTimeout(origin, id string) {
	for i, e := range s.tTimeouts {
		if e.Origin == origin && e.ID == id {
			s.tTimeouts = append(s.tTimeouts[:i], s.tTimeouts[i+1:]...)
			return
		}
	}
}

func (s *Server) setSTimeout(st STimeout) {
	for i, e := range s.sTimeouts {
		if e.Origin == st.Origin && e.ID == st.ID {
			s.sTimeouts[i] = st
			return
		}
	}
	s.sTimeouts = append(s.sTimeouts, st)
}

func (s *Server) delSTimeout(origin, id string) {
	for i, e := range s.sTimeouts {
		if e.Origin == origin && e.ID == id {
			s.sTimeouts = append(s.sTimeouts[:i], s.sTimeouts[i+1:]...)
			return
		}
	}
}

// ─── Response helper ──────────────────────────────────────────────────────────

func (s *Server) respond(kind string, status int, data any) ([]byte, error) {
	d, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"kind": kind,
		"head": map[string]any{"status": status},
		"data": json.RawMessage(d),
	})
}

// ─── Cron helper ──────────────────────────────────────────────────────────────

func nextCron(cronExpr string, nowMs int64) (int64, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(cronExpr)
	if err != nil {
		return 0, err
	}
	next := sched.Next(time.UnixMilli(nowMs).UTC())
	return next.UnixMilli(), nil
}
