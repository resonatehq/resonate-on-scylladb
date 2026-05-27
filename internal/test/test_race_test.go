package test

// TestRacePreinsertTickCreate verifies that the pre-insert/LWT kill-point pattern
// is safe against a debug.tick running between the pre-insert and the LWT.
//
// The scenario being tested:
//
//  1. Spawn promise.create "race-p" timeoutAt=T (now < T).
//     - Fiber advances ONE step: pre-inserts promise_timeouts, then yields.
//     - Fiber is killed — LWT never runs. DB has promise_timeouts but no promise.
//
//  2. Run debug.tick now=T (advances past the timeout).
//     - onPromiseTimeout reads "race-p" → ErrNotFound → returns nil (no delete).
//     - promise_timeouts entry for "race-p" is still present.
//
//  3. Spawn promise.create "race-p" timeoutAt=T again (same id, same timeout).
//     - Runs to completion. LWT commits — promise created as pending.
//
//  4. Snap and check AllInvariantsS.
//     - pendingPromiseHasTimeout must not fire.
//
// If onPromiseTimeout were to delete the pre-inserted orphan entry, step 3 would
// leave a pending promise with no promise_timeouts entry and the test would fail.

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
)

func TestRacePreinsertTickCreate(t *testing.T) {
	h := setupHandler(t)
	debugReset(t, h)
	defer debugReset(t, h)

	const (
		promiseID = "race-p"
		now0      = int64(1_000_000)         // initial time: promise is not yet expired
		timeoutAt = int64(9_999_999_999_999) // far future → state="pending" when created
		tickTime  = timeoutAt                // tick advances past the timeout
	)

	run := NewRunner(
		func(req []byte, yield func(string)) []byte {
			resp, _ := h.Handle(req, yield)
			return resp
		},
		42,
	)
	run.OnKill = killedResponse

	makeCreate := func(now int64) []byte {
		b, _ := json.Marshal(map[string]any{
			"kind": "promise.create",
			"head": map[string]any{
				"corrId":              fmt.Sprintf("corr-%d", now),
				"version":             "1.0.0",
				"resonate:debug_time": now,
			},
			"data": map[string]any{
				"id":        promiseID,
				"timeoutAt": timeoutAt,
				"param":     map[string]any{},
				"tags":      map[string]any{},
			},
		})
		return b
	}

	makeTick := func(now int64) []byte {
		b, _ := json.Marshal(map[string]any{
			"kind": "debug.tick",
			"head": map[string]any{
				"corrId":              fmt.Sprintf("corr-tick-%d", now),
				"version":             "1.0.0",
				"resonate:debug_time": now,
			},
			"data": map[string]any{"time": now},
		})
		return b
	}

	runToCompletion := func(id string) {
		for run.fibers[id] != nil {
			run.Tick(id)
		}
	}

	snap := func() core.DebugSnapResData {
		t.Helper()
		snapReq, _ := json.Marshal(map[string]any{
			"kind": "debug.snap",
			"head": makeHead(nil, now0, nil),
			"data": map[string]any{},
		})
		snapBytes, err := h.Handle(snapReq, func(string) {})
		if err != nil {
			t.Fatalf("snap error: %v", err)
		}
		var resp struct {
			Data core.DebugSnapResData `json:"data"`
		}
		if err := json.Unmarshal(snapBytes, &resp); err != nil {
			t.Fatalf("snap unmarshal: %v", err)
		}
		return resp.Data
	}

	// ── Step 1: spawn promise.create, advance one step (pre-insert done), kill ─
	//
	// After Spawn, the fiber is suspended at the first yield() — which for a
	// pending promise fires immediately after the promise_timeouts pre-insert.
	// Killing here leaves promise_timeouts in the DB but the promise row absent.
	createID, _ := run.Spawn(makeCreate(now0))
	if f, ok := run.fibers[createID]; ok {
		f.Kill()
		run.done[createID] = run.Clock
		delete(run.fibers, createID)
	} else {
		t.Fatal("promise.create completed without yielding — cannot exercise the race")
	}

	// Verify the orphan entry is present and the promise is absent (informational).
	afterKill := snap()
	for _, inv := range AllInvariantsS() {
		if err := inv.Check(afterKill); err != nil {
			t.Logf("after kill (expected orphan): %v", err)
		}
	}

	// ── Step 2: run debug.tick past the timeout ────────────────────────────────
	tickID, _ := run.Spawn(makeTick(tickTime))
	runToCompletion(tickID)

	// Verify the orphan entry survived the tick (informational — onPromiseTimeout
	// returns nil on ErrNotFound without deleting).
	afterTick := snap()
	for _, inv := range AllInvariantsS() {
		if err := inv.Check(afterTick); err != nil {
			t.Logf("after tick (orphan should still be present): %v", err)
		}
	}

	// ── Step 3: re-create the same promise — must succeed ─────────────────────
	create2ID, ch := run.Spawn(makeCreate(now0))
	runToCompletion(create2ID)
	<-ch // drain result

	// ── Step 4: snap and check — pendingPromiseHasTimeout must not fire ───────
	final := snap()
	for _, inv := range AllInvariantsS() {
		if err := inv.Check(final); err != nil {
			t.Errorf("after re-create: invariant violation: %v", err)
		}
	}
}
