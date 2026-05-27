package test

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
)

// traceEvent is one entry in a per-test trace ring. Emitted by the cooperative
// runner at every fiber-lifecycle checkpoint (spawn, advance, kill, done) and
// at every yield point inside a fiber. Fields use short JSON keys to keep the
// dump compact for agent consumption.
//
// Fields are populated by emit site:
//
//	spawn:    clock, fiber, kind="spawn"
//	advance:  clock, fiber, kind="advance"
//	yield:    clock, fiber, kind="yield", label
//	kill:     clock, fiber, kind="kill"
//	done:     clock, fiber, kind="done", status
type traceEvent struct {
	Clock   int64  `json:"c"`
	FiberID string `json:"f,omitempty"`
	Kind    string `json:"k"`
	Label   string `json:"l,omitempty"`
	Status  int    `json:"s,omitempty"`
}

// ring is a fixed-capacity FIFO of traceEvents.
//
// When cap > 0 and the ring is full, push overwrites the oldest event so we
// always keep the last `cap` events — the proximate cause of any failure.
//
// When cap == 0 the ring is unbounded: push appends without limit. Use only
// for short replay runs (iterations=1) where the full per-seed history fits
// comfortably in agent context.
type ring struct {
	events []traceEvent
	cap    int  // 0 = unbounded
	head   int  // index of next slot to write (only meaningful when cap > 0)
	full   bool // whether the ring has wrapped at least once
}

// newRing constructs a ring whose capacity comes from RESONATE_TEST_TRACE_TAIL.
// Defaults to 20. The value 0 is special-cased to mean "unbounded". Negative
// or malformed values fall back to the default.
func newRing() *ring {
	cap := 20
	if v := os.Getenv("RESONATE_TEST_TRACE_TAIL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cap = n
		}
	}
	return &ring{cap: cap}
}

// push adds an event. A nil receiver is a no-op so callers do not need to
// guard against unset rings.
func (r *ring) push(e traceEvent) {
	if r == nil {
		return
	}
	if r.cap == 0 {
		r.events = append(r.events, e)
		return
	}
	if len(r.events) < r.cap {
		r.events = append(r.events, e)
		return
	}
	r.events[r.head] = e
	r.head = (r.head + 1) % r.cap
	r.full = true
}

// dump returns the events in chronological order (oldest first).
func (r *ring) dump() []traceEvent {
	if r == nil {
		return nil
	}
	if r.cap == 0 || !r.full {
		return r.events
	}
	out := make([]traceEvent, r.cap)
	copy(out, r.events[r.head:])
	copy(out[r.cap-r.head:], r.events[:r.head])
	return out
}

// render produces a multi-line dump suitable for t.Logf. The first line is a
// short legend; subsequent lines are JSONL events. Returns an empty string
// when the ring is nil or has captured no events.
func (r *ring) render() string {
	if r == nil {
		return ""
	}
	events := r.dump()
	if len(events) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("trace: c=clock f=fiber k=kind l=yield-label s=status\n")
	for _, e := range events {
		line, _ := json.Marshal(e)
		b.Write(line)
		b.WriteByte('\n')
	}
	return b.String()
}
