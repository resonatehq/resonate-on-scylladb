package test

import (
	"encoding/json"
	"testing"
)

// TestDeterminism asserts that the cooperative-runner trace is byte-identical
// across two runs with the same seed.
//
// The seed-based reproducibility contract (TESTING.md §1) is load-bearing for
// every other diagnosis: when CI fails at seed N, an engineer or agent reruns
// with seed=N and expects to see the same failure. If anything in the test
// framework — RNG ordering, fiber scheduling, response status, yield labels —
// drifts between runs, that contract silently breaks. This test is the
// canary: same seed, same code → same trace, byte-for-byte. A divergence
// here is a real bug, and the right diagnosis order is "fix this flake first,
// then look at the substantive failure," because a flaky framework masks
// substantive bugs.
//
// Implementation: run a short kill-style scenario (5 spawns, fiber-runner
// drains to completion) twice against the same fresh-DB starting state.
// Compare the rendered traces.
func TestDeterminism(t *testing.T) {
	h := setupHandler(t)

	runOnce := func() string {
		debugReset(t, h)

		const seed = int64(424242)
		promiseIDs := pickPromisePool(seed)
		sched := NewRunner(
			func(req []byte, yield func(string)) []byte {
				resp, _ := h.Handle(req, yield)
				return resp
			},
			seed,
		)
		sched.OnKill = killedResponse
		// Unbounded ring so we compare the full trace, not just the tail.
		sched.Trace = &ring{cap: 0}

		reqRng := newRng(seed + 500)
		now := int64(1_000_000)
		for i := 0; i < 5; i++ {
			req := generateRequest(reqRng, now, nil, promiseIDs, []string{promiseIDs[0]})
			if req["kind"] == "debug.tick" {
				if data, ok := req["data"].(map[string]any); ok {
					if tickTime, ok := data["time"].(int64); ok && tickTime > now {
						now = tickTime
					}
					data["time"] = now
				}
			}
			if head, ok := req["head"].(map[string]any); ok {
				head["resonate:debug_time"] = now
			}
			reqBytes, _ := json.Marshal(req)
			sched.Spawn(reqBytes)
		}
		for sched.Active() {
			sched.Tick()
		}
		debugReset(t, h)
		return sched.Trace.render()
	}

	t1 := runOnce()
	t2 := runOnce()
	if t1 != t2 {
		t.Errorf("%s", failMsg("kind", "non_deterministic", "seed", 424242))
		t.Logf("--- run 1 trace ---\n%s", t1)
		t.Logf("--- run 2 trace ---\n%s", t2)
	}
}
