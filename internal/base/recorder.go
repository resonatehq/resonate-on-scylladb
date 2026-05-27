package base

import (
	"encoding/json"
	"sync"
)

// MessageEntry is a single recorded outbound message.
type MessageEntry struct {
	Address string          `json:"address"`
	Message json.RawMessage `json:"message"` // ExecuteMsg | UnblockMsg
}

// Recorder is a Transport that records every Send into an in-memory buffer
// instead of delivering it. Used by the debug/test harness so debug.snap can
// report what the handler tried to send.
//
// Send is append-only and cheap. Coalescing happens at read time in Snap:
// for execute messages addressed to the same task_id, only the latest entry
// is returned. Unblock messages are returned verbatim.
type Recorder struct {
	mu      sync.Mutex
	entries []MessageEntry
}

func NewRecorder() *Recorder {
	return &Recorder{}
}

func (r *Recorder) Send(address string, payload []byte) {
	entry := MessageEntry{
		Address: address,
		Message: append(json.RawMessage(nil), payload...),
	}
	r.mu.Lock()
	r.entries = append(r.entries, entry)
	r.mu.Unlock()
}

// Snap returns a copy of the current buffer with execute messages coalesced
// by task_id (latest wins). The buffer itself is not modified.
func (r *Recorder) Snap() []MessageEntry {
	r.mu.Lock()
	src := make([]MessageEntry, len(r.entries))
	copy(src, r.entries)
	r.mu.Unlock()

	// Walk in order; for executes, remember the last index per task_id and
	// overwrite. Non-executes pass through untouched.
	type probe struct {
		Kind string `json:"kind"`
		Data struct {
			Task struct {
				ID string `json:"id"`
			} `json:"task"`
		} `json:"data"`
	}

	out := make([]MessageEntry, 0, len(src))
	idxByTask := make(map[string]int)
	for _, e := range src {
		var p probe
		if json.Unmarshal(e.Message, &p) == nil && p.Kind == "execute" && p.Data.Task.ID != "" {
			if i, ok := idxByTask[p.Data.Task.ID]; ok {
				out[i] = e
				continue
			}
			idxByTask[p.Data.Task.ID] = len(out)
		}
		out = append(out, e)
	}
	return out
}

// Clear empties the buffer.
func (r *Recorder) Clear() {
	r.mu.Lock()
	r.entries = r.entries[:0]
	r.mu.Unlock()
}
