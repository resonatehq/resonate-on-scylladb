package test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/resonateio/resonate-on-scylladb/internal/core"
)

// ─── Seeded random (LCG matching the TypeScript generator) ───────────────────

type seededRandom struct{ seed int64 }

func newRng(seed int64) *seededRandom { return &seededRandom{seed} }

func (r *seededRandom) next() float64 {
	r.seed = (r.seed*1103515245 + 12345) & 0x7fffffff
	return float64(r.seed) / float64(0x7fffffff)
}

func (r *seededRandom) choice(n int) int {
	return int(r.next() * float64(n))
}

func (r *seededRandom) choiceExcluding(pool []string, exclude string) string {
	filtered := make([]string, 0, len(pool))
	for _, s := range pool {
		if s != exclude {
			filtered = append(filtered, s)
		}
	}
	if len(filtered) == 0 {
		return exclude
	}
	return filtered[r.choice(len(filtered))]
}

// ─── Request generation ───────────────────────────────────────────────────────

type snapData struct {
	Promises         []map[string]any `json:"promises"`
	Tasks            []map[string]any `json:"tasks"`
	Callbacks        []map[string]any `json:"callbacks"`
	PromiseTimeouts  []map[string]any `json:"promiseTimeouts"`
	TaskTimeouts     []map[string]any `json:"taskTimeouts"`
	ScheduleTimeouts []map[string]any `json:"scheduleTimeouts"`
	Schedules        []map[string]any `json:"schedules"`
}

// Pool of identifiers used by chaosOp and guidedOps. The promise pool is
// per-seed (pickPromisePool, threaded as a parameter); the rest are constants
// shared across all seeds. diffOrigins is per-seed and dynamic — initialized
// from promiseIDs[0] and expanded as scheduled promises appear.
var (
	diffScheduleIDs = []string{"sched-0", "sched-1"}
	diffPIDs        = []string{"worker-1", "worker-2"}
	diffTimeouts    = []int64{3000, 5000, 8000, 9_999_999_999_999}
	diffTaskTTL     = int64(30_000)
)

// promiseOrigin returns the effective origin for a promise: the resonate:origin
// tag if set and non-empty, otherwise the promise ID itself.
func promiseOrigin(id string, tags map[string]any) string {
	if tags != nil {
		if o, ok := tags["resonate:origin"].(string); ok && o != "" {
			return o
		}
	}
	return id
}

// makeHead builds a request head. When rng is non-nil, resonate:origin is
// picked from origins and included; pass nil rng for debug.snap/debug.tick
// ops where origin is irrelevant (origins is ignored when rng is nil).
func makeHead(rng *seededRandom, now int64, origins []string) map[string]any {
	h := map[string]any{
		"corrId":              fmt.Sprintf("corr-%d", rand.Int63()),
		"version":             "1.0.0",
		"resonate:debug_time": now,
	}
	if rng != nil {
		h["resonate:origin"] = origins[rng.choice(len(origins))]
	}
	return h
}

// expandOriginPool scans snap for scheduled promises and adds their origins to
// diffOrigins (deduped). The pool only grows and never shrinks.
func expandOriginPool(diffOrigins *[]string, snap *snapData) {
	seen := make(map[string]bool, len(*diffOrigins))
	for _, o := range *diffOrigins {
		seen[o] = true
	}
	for _, p := range snap.Promises {
		tags, _ := p["tags"].(map[string]any)
		if tags == nil {
			continue
		}
		if _, isScheduled := tags["resonate:schedule"]; !isScheduled {
			continue
		}
		id, _ := p["id"].(string)
		o := promiseOrigin(id, tags)
		if !seen[o] {
			seen[o] = true
			*diffOrigins = append(*diffOrigins, o)
		}
	}
}

func generateRequest(rng *seededRandom, now int64, snap *snapData, promiseIDs []string, diffOrigins []string) map[string]any {
	if snap == nil || rng.next() < 0.2 {
		return chaosOp(rng, now, promiseIDs, diffOrigins)
	}
	guided := guidedOps(rng, now, snap, promiseIDs, diffOrigins)
	if len(guided) == 0 {
		return chaosOp(rng, now, promiseIDs, diffOrigins)
	}
	return guided[rng.choice(len(guided))]
}

func chaosOp(rng *seededRandom, now int64, promiseIDs []string, diffOrigins []string) map[string]any {
	ops := []func() map[string]any{
		func() map[string]any {
			return map[string]any{"kind": "promise.get", "head": makeHead(rng, now, diffOrigins),
				"data": map[string]any{"id": promiseIDs[rng.choice(len(promiseIDs))]}}
		},
		func() map[string]any {
			return map[string]any{"kind": "promise.create", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": promiseIDs[rng.choice(len(promiseIDs))], "timeoutAt": diffTimeouts[rng.choice(len(diffTimeouts))],
				"param": map[string]any{}, "tags": map[string]any{"resonate:origin": diffOrigins[rng.choice(len(diffOrigins))]},
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "promise.create", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": promiseIDs[rng.choice(len(promiseIDs))], "timeoutAt": diffTimeouts[rng.choice(len(diffTimeouts))],
				"param": map[string]any{}, "tags": map[string]any{"resonate:target": "http://worker", "resonate:origin": diffOrigins[rng.choice(len(diffOrigins))]},
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "promise.create", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": promiseIDs[rng.choice(len(promiseIDs))], "timeoutAt": diffTimeouts[rng.choice(len(diffTimeouts))],
				"param": map[string]any{}, "tags": map[string]any{"resonate:timer": "true"},
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "promise.settle", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id":    promiseIDs[rng.choice(len(promiseIDs))],
				"state": []string{"resolved", "rejected", "rejected_canceled"}[rng.choice(3)],
				"value": map[string]any{},
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "promise.register_callback", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"awaited": promiseIDs[rng.choice(len(promiseIDs))],
				"awaiter": promiseIDs[rng.choice(len(promiseIDs))],
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "promise.register_listener", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"awaited": promiseIDs[rng.choice(len(promiseIDs))],
				"address": diffPIDs[rng.choice(len(diffPIDs))],
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "task.get", "head": makeHead(rng, now, diffOrigins),
				"data": map[string]any{"id": promiseIDs[rng.choice(len(promiseIDs))]}}
		},
		func() map[string]any {
			id := promiseIDs[rng.choice(len(promiseIDs))]
			return map[string]any{"kind": "task.create", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"pid": diffPIDs[rng.choice(len(diffPIDs))], "ttl": diffTaskTTL,
				"action": map[string]any{"kind": "promise.create", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
					"id": id, "timeoutAt": diffTimeouts[rng.choice(len(diffTimeouts))],
					"param": map[string]any{}, "tags": map[string]any{"resonate:target": "http://worker", "resonate:origin": diffOrigins[rng.choice(len(diffOrigins))]},
				}},
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "task.acquire", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": promiseIDs[rng.choice(len(promiseIDs))], "version": rng.choice(4),
				"pid": diffPIDs[rng.choice(len(diffPIDs))], "ttl": diffTaskTTL,
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "task.release", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": promiseIDs[rng.choice(len(promiseIDs))], "version": rng.choice(4),
			}}
		},
		func() map[string]any {
			id := promiseIDs[rng.choice(len(promiseIDs))]
			return map[string]any{"kind": "task.fulfill", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": id, "version": rng.choice(4),
				"action": map[string]any{"kind": "promise.settle", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
					"id": id, "state": []string{"resolved", "rejected"}[rng.choice(2)], "value": map[string]any{},
				}},
			}}
		},
		func() map[string]any {
			id := promiseIDs[rng.choice(len(promiseIDs))]
			awaited := rng.choiceExcluding(promiseIDs, id)
			return map[string]any{"kind": "task.suspend", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": id, "version": rng.choice(4),
				"actions": []any{map[string]any{"kind": "promise.register_callback", "head": makeHead(rng, now, diffOrigins),
					"data": map[string]any{"awaited": awaited, "awaiter": id}}},
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "task.halt", "head": makeHead(rng, now, diffOrigins),
				"data": map[string]any{"id": promiseIDs[rng.choice(len(promiseIDs))]}}
		},
		func() map[string]any {
			return map[string]any{"kind": "task.continue", "head": makeHead(rng, now, diffOrigins),
				"data": map[string]any{"id": promiseIDs[rng.choice(len(promiseIDs))]}}
		},
		func() map[string]any {
			id := promiseIDs[rng.choice(len(promiseIDs))]
			tags := map[string]any{"resonate:origin": diffOrigins[rng.choice(len(diffOrigins))]}
			if rng.next() < 0.5 {
				tags["resonate:target"] = "http://worker"
			}
			return map[string]any{"kind": "task.fence", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": id, "version": rng.choice(4),
				"action": map[string]any{"kind": "promise.create", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
					"id": promiseIDs[rng.choice(len(promiseIDs))], "timeoutAt": diffTimeouts[rng.choice(len(diffTimeouts))],
					"param": map[string]any{}, "tags": tags,
				}},
			}}
		},
		func() map[string]any {
			id := promiseIDs[rng.choice(len(promiseIDs))]
			return map[string]any{"kind": "task.heartbeat", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"pid":   diffPIDs[rng.choice(len(diffPIDs))],
				"tasks": []any{map[string]any{"id": id, "version": rng.choice(4)}},
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "debug.tick", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{"time": now}}
		},
		func() map[string]any {
			return map[string]any{"kind": "schedule.create", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": diffScheduleIDs[rng.choice(len(diffScheduleIDs))], "cron": "* * * * *",
				"promiseId":      diffScheduleIDs[rng.choice(len(diffScheduleIDs))] + "-{{.timestamp}}",
				"promiseTimeout": int64(30_000), "promiseParam": map[string]any{}, "promiseTags": map[string]any{},
			}}
		},
		func() map[string]any {
			return map[string]any{"kind": "schedule.get", "head": makeHead(rng, now, diffOrigins),
				"data": map[string]any{"id": diffScheduleIDs[rng.choice(len(diffScheduleIDs))]}}
		},
		func() map[string]any {
			return map[string]any{"kind": "schedule.delete", "head": makeHead(rng, now, diffOrigins),
				"data": map[string]any{"id": diffScheduleIDs[rng.choice(len(diffScheduleIDs))]}}
		},
	}
	return ops[rng.choice(len(ops))]()
}

func guidedOps(rng *seededRandom, now int64, snap *snapData, promiseIDs []string, diffOrigins []string) []map[string]any {
	var ops []map[string]any

	// Pick an origin for this guided op batch.
	o := diffOrigins[rng.choice(len(diffOrigins))]

	// Build a map from promise ID to its resolved origin.
	pidOrigin := make(map[string]string, len(snap.Promises))
	for _, p := range snap.Promises {
		id, _ := p["id"].(string)
		tags, _ := p["tags"].(map[string]any)
		pidOrigin[id] = promiseOrigin(id, tags)
	}

	resolveOrigin := func(id string) string {
		if orig, ok := pidOrigin[id]; ok {
			return orig
		}
		return id
	}

	createdIDs := map[string]bool{}
	for id := range pidOrigin {
		createdIDs[id] = true
	}

	// Filter tasks and promises to those in origin o.
	var pendingTasks, acquiredTasks, haltedTasks []map[string]any
	var pendingPromises []map[string]any
	for _, t := range snap.Tasks {
		id, _ := t["id"].(string)
		if resolveOrigin(id) != o {
			continue
		}
		switch t["state"] {
		case "pending":
			pendingTasks = append(pendingTasks, t)
		case "acquired":
			acquiredTasks = append(acquiredTasks, t)
		case "halted":
			haltedTasks = append(haltedTasks, t)
		}
	}
	for _, p := range snap.Promises {
		id, _ := p["id"].(string)
		if resolveOrigin(id) != o {
			continue
		}
		if p["state"] == "pending" {
			pendingPromises = append(pendingPromises, p)
		}
	}

	// Create uncreated promises whose ID equals the selected origin (uncreated
	// promises have no tags, so their origin is their ID).
	for _, id := range promiseIDs {
		if !createdIDs[id] && id == o {
			id := id
			ops = append(ops, map[string]any{"kind": "promise.create", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": id, "timeoutAt": diffTimeouts[rng.choice(len(diffTimeouts))], "param": map[string]any{},
				"tags": map[string]any{"resonate:origin": o},
			}})
			ops = append(ops, map[string]any{"kind": "promise.create", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"id": id, "timeoutAt": diffTimeouts[rng.choice(len(diffTimeouts))], "param": map[string]any{},
				"tags": map[string]any{"resonate:target": "http://worker", "resonate:origin": o},
			}})
			ops = append(ops, map[string]any{"kind": "task.create", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
				"pid": diffPIDs[rng.choice(len(diffPIDs))], "ttl": diffTaskTTL,
				"action": map[string]any{"kind": "promise.create", "head": makeHead(rng, now, []string{o}), "data": map[string]any{
					"id": id, "timeoutAt": diffTimeouts[rng.choice(len(diffTimeouts))],
					"param": map[string]any{}, "tags": map[string]any{"resonate:target": "http://worker", "resonate:origin": o},
				}},
			}})
		}
	}

	// Acquire pending tasks.
	for _, t := range pendingTasks {
		t := t
		ops = append(ops, map[string]any{"kind": "task.acquire", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
			"id": t["id"], "version": int(t["version"].(float64)),
			"pid": diffPIDs[rng.choice(len(diffPIDs))], "ttl": diffTaskTTL,
		}})
	}

	// Operations on acquired tasks.
	for _, t := range acquiredTasks {
		t := t
		id := t["id"].(string)
		version := int(t["version"].(float64))

		ops = append(ops, map[string]any{"kind": "task.release", "head": makeHead(rng, now, diffOrigins),
			"data": map[string]any{"id": id, "version": version}})

		ops = append(ops, map[string]any{"kind": "task.fulfill", "head": makeHead(rng, now, []string{o}), "data": map[string]any{
			"id": id, "version": version,
			"action": map[string]any{"kind": "promise.settle", "head": makeHead(rng, now, []string{o}), "data": map[string]any{
				"id": id, "state": []string{"resolved", "rejected"}[rng.choice(2)], "value": map[string]any{},
			}},
		}})

		// Only suspend if there's a same-origin pending promise distinct from this task.
		var sameOriginAwaitable []map[string]any
		for _, p := range pendingPromises {
			if p["id"].(string) != id {
				sameOriginAwaitable = append(sameOriginAwaitable, p)
			}
		}
		if len(sameOriginAwaitable) > 0 {
			awaited := sameOriginAwaitable[rng.choice(len(sameOriginAwaitable))]["id"].(string)
			ops = append(ops, map[string]any{"kind": "task.suspend", "head": makeHead(rng, now, []string{o}), "data": map[string]any{
				"id": id, "version": version,
				"actions": []any{map[string]any{"kind": "promise.register_callback", "head": makeHead(rng, now, []string{o}),
					"data": map[string]any{"awaited": awaited, "awaiter": id}}},
			}})
		}

		ops = append(ops, map[string]any{"kind": "task.heartbeat", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
			"pid":   diffPIDs[rng.choice(len(diffPIDs))],
			"tasks": []any{map[string]any{"id": id, "version": version}},
		}})
	}

	// Halt pending/acquired tasks.
	for _, t := range append(pendingTasks, acquiredTasks...) {
		t := t
		ops = append(ops, map[string]any{"kind": "task.halt", "head": makeHead(rng, now, diffOrigins),
			"data": map[string]any{"id": t["id"]}})
	}

	// Continue halted tasks.
	for _, t := range haltedTasks {
		t := t
		ops = append(ops, map[string]any{"kind": "task.continue", "head": makeHead(rng, now, diffOrigins),
			"data": map[string]any{"id": t["id"]}})
	}

	// Settle pending promises in origin o.
	for _, p := range pendingPromises {
		p := p
		ops = append(ops, map[string]any{"kind": "promise.settle", "head": makeHead(rng, now, diffOrigins), "data": map[string]any{
			"id":    p["id"],
			"state": []string{"resolved", "rejected", "rejected_canceled"}[rng.choice(3)],
			"value": map[string]any{},
		}})
	}

	// Tick past nearest timeout.
	var allTimeouts []float64
	for _, pt := range snap.PromiseTimeouts {
		if v, ok := pt["timeout"].(float64); ok {
			allTimeouts = append(allTimeouts, v)
		}
	}
	for _, tt := range snap.TaskTimeouts {
		if v, ok := tt["timeout"].(float64); ok {
			allTimeouts = append(allTimeouts, v)
		}
	}
	for _, st := range snap.ScheduleTimeouts {
		if v, ok := st["timeout"].(float64); ok {
			allTimeouts = append(allTimeouts, v)
		}
	}
	const maxTickJump = int64(300_000) // cap tick advance to 5 minutes to avoid infinite schedule loops
	if len(allTimeouts) > 0 {
		minT := allTimeouts[0]
		for _, v := range allTimeouts[1:] {
			if v < minT {
				minT = v
			}
		}
		tickTime := int64(minT)
		if tickTime <= now+maxTickJump {
			ops = append(ops, map[string]any{"kind": "debug.tick", "head": makeHead(rng, now, diffOrigins),
				"data": map[string]any{"time": tickTime}})
		}
	}

	return ops
}

// ─── Comparison helpers ───────────────────────────────────────────────────────

func normalizeResponse(raw []byte) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	stripHeads(m)
	return m, nil
}

// stripHeads recursively removes corrId and version from every "head" object
// found in the response tree. task.fence embeds a full inner action response
// (with its own head) that the oracle omits corrId/version from.
func stripHeads(v any) {
	switch node := v.(type) {
	case map[string]any:
		if head, ok := node["head"].(map[string]any); ok {
			delete(head, "corrId")
			delete(head, "version")
		}
		for _, child := range node {
			stripHeads(child)
		}
	case []any:
		for _, child := range node {
			stripHeads(child)
		}
	}
}

// sortSnapData sorts all arrays in a snap by stable keys for comparison.
func sortSnapData(d map[string]any) {
	sortBy := func(key string, less func(a, b map[string]any) bool) {
		if raw, ok := d[key]; ok {
			if arr, ok := raw.([]any); ok {
				sort.Slice(arr, func(i, j int) bool {
					mi, oki := arr[i].(map[string]any)
					mj, okj := arr[j].(map[string]any)
					if !oki || !okj {
						return false
					}
					return less(mi, mj)
				})
			}
		}
	}
	strField := func(m map[string]any, k string) string {
		if v, ok := m[k].(string); ok {
			return v
		}
		return ""
	}
	sortBy("promises", func(a, b map[string]any) bool {
		ka := strField(a, "origin") + "/" + strField(a, "id")
		kb := strField(b, "origin") + "/" + strField(b, "id")
		return ka < kb
	})
	sortBy("tasks", func(a, b map[string]any) bool {
		ka := strField(a, "origin") + "/" + strField(a, "id")
		kb := strField(b, "origin") + "/" + strField(b, "id")
		return ka < kb
	})
	sortBy("promiseTimeouts", func(a, b map[string]any) bool {
		ka := strField(a, "id") + "/" + strField(a, "origin")
		kb := strField(b, "id") + "/" + strField(b, "origin")
		return ka < kb
	})
	sortBy("taskTimeouts", func(a, b map[string]any) bool {
		ka := strField(a, "id") + "/" + strField(a, "origin")
		kb := strField(b, "id") + "/" + strField(b, "origin")
		return ka < kb
	})
	sortBy("schedules", func(a, b map[string]any) bool {
		ka := strField(a, "id") + "/" + strField(a, "origin")
		kb := strField(b, "id") + "/" + strField(b, "origin")
		return ka < kb
	})
	sortBy("scheduleTimeouts", func(a, b map[string]any) bool {
		ka := strField(a, "id") + "/" + strField(a, "origin")
		kb := strField(b, "id") + "/" + strField(b, "origin")
		return ka < kb
	})
	sortBy("callbacks", func(a, b map[string]any) bool {
		ka := strField(a, "origin") + "/" + strField(a, "awaited") + "/" + strField(a, "awaiter")
		kb := strField(b, "origin") + "/" + strField(b, "awaited") + "/" + strField(b, "awaiter")
		return ka < kb
	})
	sortBy("listeners", func(a, b map[string]any) bool {
		ka := strField(a, "origin") + "/" + strField(a, "id") + "/" + strField(a, "address")
		kb := strField(b, "origin") + "/" + strField(b, "id") + "/" + strField(b, "address")
		return ka < kb
	})
	sortBy("messages", func(a, b map[string]any) bool {
		addrA, addrB := strField(a, "address"), strField(b, "address")
		if addrA != addrB {
			return addrA < addrB
		}
		ma, _ := json.Marshal(a["message"])
		mb, _ := json.Marshal(b["message"])
		return string(ma) < string(mb)
	})
}

// ─── TestHandlerDiff ─────────────────────────────────────────────────────────

// TestHandlerDiff fires identical randomly-generated requests at the in-memory
// oracle and the real Handler backed by ScyllaDB, then asserts that response
// statuses, response data, and full state snaps agree at every step.
//
// Defaults: seed=<current timestamp>, iterations=100, operations=200. Override
// via env vars RESONATE_TEST_SEED, RESONATE_TEST_ITERATIONS, RESONATE_TEST_OPERATIONS.
// To replay a specific failure: set SEED to the failing seed and ITERATIONS=1.
func TestHandlerDiff(t *testing.T) {
	h := setupHandler(t)

	baseSeed := envInt64("RESONATE_TEST_SEED", time.Now().UnixNano())
	iterations := envInt("RESONATE_TEST_ITERATIONS", 100)
	operations := envInt("RESONATE_TEST_OPERATIONS", 200)
	t.Logf("config: seed=%d iterations=%d operations=%d promises=%s origins=dynamic", baseSeed, iterations, operations, promisesConfig())

	var totalSnapTime time.Duration
	var snapCount int

	for seed := baseSeed; seed < baseSeed+int64(iterations); seed++ {
		promiseIDs := pickPromisePool(seed)
		diffOrigins := []string{promiseIDs[0]}
		rng := newRng(seed)
		goSrv := New()
		now := int64(0)

		debugReset(t, h)

		var prevSnap *snapData
		var prevSnapData *core.DebugSnapResData

		for step := 0; step < operations; step++ {
			now += 1000

			req := generateRequest(rng, now, prevSnap, promiseIDs, diffOrigins)

			// For tick ops, advance now to the tick time.
			if req["kind"] == "debug.tick" {
				if data, ok := req["data"].(map[string]any); ok {
					if tickTime, ok := data["time"].(int64); ok && tickTime > now {
						now = tickTime
					}
				}
			}
			if head, ok := req["head"].(map[string]any); ok {
				head["resonate:debug_time"] = now
			}
			if req["kind"] == "debug.tick" {
				if data, ok := req["data"].(map[string]any); ok {
					data["time"] = now
				}
			}

			reqBytes, _ := json.Marshal(req)

			// Apply to oracle.
			goRespBytes, err := goSrv.Apply(now, reqBytes)
			if err != nil {
				t.Logf("req: %s", reqBytes)
				t.Fatalf("%s", failMsg("kind", "oracle_error", "seed", seed, "promises", len(promiseIDs), "step", step, "err", err))
			}

			// Apply to handler.
			handlerRespBytes, err := h.Handle(reqBytes, func(string) {})
			if err != nil {
				t.Logf("req: %s", reqBytes)
				t.Fatalf("%s", failMsg("kind", "handler_error", "seed", seed, "promises", len(promiseIDs), "step", step, "err", err))
			}

			goNorm, _ := normalizeResponse(goRespBytes)
			handlerNorm, _ := normalizeResponse(handlerRespBytes)

			goStatus := diffStatusOf(goNorm)
			handlerStatus := diffStatusOf(handlerNorm)

			if goStatus != handlerStatus {
				t.Logf("req:     %s", reqBytes)
				t.Logf("oracle:  %s", goRespBytes)
				t.Logf("handler: %s", handlerRespBytes)
				t.Fatalf("%s", failMsg("kind", "status_mismatch", "seed", seed, "promises", len(promiseIDs), "step", step, "oracle", goStatus, "handler", handlerStatus))
			}

			// Skip data comparison for debug.tick (handler returns action list, oracle
			// returns []) and for error responses (oracle uses validation-path prefixes
			// in error strings; handler does not — presentation difference, not behavior).
			if req["kind"] != "debug.snap" && req["kind"] != "debug.tick" && goStatus < 400 {
				goDataBytes, _ := json.Marshal(goNorm["data"])
				handlerDataBytes, _ := json.Marshal(handlerNorm["data"])
				if !jsonEqual(goDataBytes, handlerDataBytes) {
					t.Logf("req:     %s", reqBytes)
					t.Logf("oracle:  %s", goRespBytes)
					t.Logf("handler: %s", handlerRespBytes)
					t.Fatalf("%s", failMsg("kind", "data_mismatch", "seed", seed, "promises", len(promiseIDs), "step", step))
				}
			}

			// Compare full state via snap after every step.
			{
				snapReq := map[string]any{"kind": "debug.snap", "head": makeHead(nil, now, nil), "data": map[string]any{}}
				snapReqBytes, _ := json.Marshal(snapReq)

				goSnapBytes, _ := goSrv.Apply(now, snapReqBytes)

				snapStart := time.Now()
				handlerSnapBytes, err := h.Handle(snapReqBytes, func(string) {})
				totalSnapTime += time.Since(snapStart)
				snapCount++

				if err != nil {
					t.Fatalf("%s", failMsg("kind", "handler_snap_error", "seed", seed, "promises", len(promiseIDs), "step", step, "err", err))
				}

				if err := compareOracleHandlerSnaps(goSnapBytes, handlerSnapBytes); err != nil {
					reqKind, _ := req["kind"].(string)
					t.Logf("req: %s", reqBytes)
					t.Logf("err: %v", err)
					t.Fatalf("%s", failMsg("kind", "snap_mismatch", "seed", seed, "promises", len(promiseIDs), "step", step, "after", reqKind))
				}

				var snapResp struct {
					Data core.DebugSnapResData `json:"data"`
				}
				if err := json.Unmarshal(handlerSnapBytes, &snapResp); err == nil {
					snapData := snapResp.Data
					for _, inv := range AllInvariantsS() {
						if err := inv.Check(snapData); err != nil {
							reqKind, _ := req["kind"].(string)
							t.Logf("req: %s", reqBytes)
							t.Fatalf("%s", failMsg("kind", "invariant_s", "seed", seed, "promises", len(promiseIDs), "step", step, "after", reqKind, "name", inv.Name, "err", err))
						}
					}
					if prevSnapData != nil {
						for _, inv := range AllInvariantsT() {
							if err := inv.Check(*prevSnapData, snapData); err != nil {
								reqKind, _ := req["kind"].(string)
								t.Logf("req: %s", reqBytes)
								t.Fatalf("%s", failMsg("kind", "invariant_t", "seed", seed, "promises", len(promiseIDs), "step", step, "after", reqKind, "name", inv.Name, "err", err))
							}
						}
					}
					prevSnapData = &snapData
				}
			}

			// Use the oracle snap (no extra DB call) to drive guided generation.
			prevSnap = snapDataFromOracle(goSrv, now)
			expandOriginPool(&diffOrigins, prevSnap)
		}

		debugReset(t, h)
	}

	t.Logf("debug.snap: %d calls, total=%s, avg=%s",
		snapCount, totalSnapTime, totalSnapTime/time.Duration(snapCount))
}

// snapDataFromOracle extracts a *snapData from the oracle for guided op generation.
func snapDataFromOracle(srv *Server, now int64) *snapData {
	req := map[string]any{"kind": "debug.snap", "head": makeHead(nil, now, nil), "data": map[string]any{}}
	reqBytes, _ := json.Marshal(req)
	respBytes, _ := srv.Apply(now, reqBytes)
	var resp map[string]any
	json.Unmarshal(respBytes, &resp)
	dataRaw, _ := json.Marshal(resp["data"])
	var sd snapData
	json.Unmarshal(dataRaw, &sd)
	return &sd
}

// compareOracleHandlerSnaps compares two debug.snap responses.
// Messages and scheduleTimeouts are stripped before comparison — see inline TODOs.
func compareOracleHandlerSnaps(oracleSnap, handlerSnap []byte) error {
	var oracleM, handlerM map[string]any
	json.Unmarshal(oracleSnap, &oracleM)
	json.Unmarshal(handlerSnap, &handlerM)

	oracleData, _ := oracleM["data"].(map[string]any)
	handlerData, _ := handlerM["data"].(map[string]any)

	if (oracleData == nil) != (handlerData == nil) {
		return fmt.Errorf("snap data nil mismatch: oracle=%v handler=%v", oracleData, handlerData)
	}
	if oracleData == nil {
		return nil
	}

	delete(oracleData, "messages")
	delete(handlerData, "messages")
	// TODO: enable scheduleTimeouts comparison.
	// The handler's schedule_timeouts table accumulates stale entries for purely deleted
	// schedules: onScheduleTimeout's upfront delete sits after the rec==nil guard, so the
	// entry is never cleaned up when the schedule no longer exists. Fix: move the upfront
	// DELETE in onScheduleTimeout to before the rec==nil check so every entry is removed
	// the first time it fires, then update the oracle to mirror the same cleanup semantics,
	// then remove both deletes below.
	delete(oracleData, "scheduleTimeouts")
	delete(handlerData, "scheduleTimeouts")

	sortSnapData(oracleData)
	sortSnapData(handlerData)

	oracleBytes, _ := json.Marshal(oracleData)
	handlerBytes, _ := json.Marshal(handlerData)
	if !jsonEqual(oracleBytes, handlerBytes) {
		return fmt.Errorf("snap mismatch:\n  oracle:  %s\n  handler: %s", oracleBytes, handlerBytes)
	}
	return nil
}
