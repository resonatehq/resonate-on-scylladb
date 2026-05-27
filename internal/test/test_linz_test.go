package test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// ─── Input type ───────────────────────────────────────────────────────────────

type linInput struct {
	kind string
	now  int64
	req  []byte
}

// ─── Model ───────────────────────────────────────────────────────────────────

// resonateModel returns a porcupine.Model backed by the oracle.
//
// Because timers only fire when debug.tick is sent explicitly, there is no
// hidden nondeterminism: every state transition is deterministic. The model is
// therefore a plain Model (not NondeterministicModel): Step either confirms the
// observed output or rejects it.
func resonateModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return New()
		},

		Step: func(state, input, output interface{}) (bool, interface{}) {
			op := input.(linInput)
			outNorm, _ := normalizeResponse(output.([]byte))

			// 500 responses mean the op had no effect (LWT conflict between yields,
			// no state was modified). Accept at any position without changing oracle state.
			if diffStatusOf(outNorm) == 500 {
				return true, state
			}

			s := state.(*Server).Clone()

			got, err := s.Apply(op.now, op.req)
			if err != nil {
				return false, nil
			}

			gotNorm, _ := normalizeResponse(got)

			// Status must match.
			if diffStatusOf(gotNorm) != diffStatusOf(outNorm) {
				return false, nil
			}

			// For successful non-tick non-snap responses, data must also match.
			// (debug.tick: oracle returns [], handler returns action list;
			//  error responses: oracle includes validation path prefixes, handler doesn't)
			if op.kind != "debug.tick" && op.kind != "debug.snap" && diffStatusOf(gotNorm) < 400 {
				gotData, _ := json.Marshal(gotNorm["data"])
				outData, _ := json.Marshal(outNorm["data"])
				if !jsonEqual(gotData, outData) {
					return false, nil
				}
			}

			return true, s
		},

		Equal: func(s1, s2 interface{}) bool {
			return string(serverSnapBytes(s1.(*Server))) == string(serverSnapBytes(s2.(*Server)))
		},

		DescribeOperation: func(input, output interface{}) string {
			op := input.(linInput)
			return fmt.Sprintf("%s -> %s", op.req, output.([]byte))
		},
	}
}

// serverSnapBytes returns a canonical, sorted JSON representation of the
// server's observable state, with messages stripped (fire-and-forget).
func serverSnapBytes(s *Server) []byte {
	raw, _ := s.debugSnap()
	var m map[string]any
	json.Unmarshal(raw, &m)
	d, _ := m["data"].(map[string]any)
	delete(d, "messages")
	sortSnapData(d)
	b, _ := json.Marshal(d)
	return b
}

// ─── Test ─────────────────────────────────────────────────────────────────────

// TestLinearizabilitySequential runs a sequential oracle history through the
// Porcupine model. Validates Clone() correctness and model wiring.
//
// Defaults: seed=<current timestamp>, iterations=1000, operations=250. Override
// via env vars RESONATE_TEST_SEED, RESONATE_TEST_ITERATIONS, RESONATE_TEST_OPERATIONS.
// To replay a specific failure: set SEED to the failing seed and ITERATIONS=1.
func TestLinearizabilitySequential(t *testing.T) {
	model := resonateModel()

	baseSeed := envInt64("RESONATE_TEST_SEED", time.Now().UnixNano())
	iterations := envInt("RESONATE_TEST_ITERATIONS", 1000)
	operations := envInt("RESONATE_TEST_OPERATIONS", 250)
	t.Logf("config: seed=%d iterations=%d operations=%d promises=%s", baseSeed, iterations, operations, promisesConfig())

	for seed := baseSeed; seed < baseSeed+int64(iterations); seed++ {
		promiseIDs := pickPromisePool(seed)
		rng := newRng(seed)
		srv := New()
		now := int64(0)

		var ops []porcupine.Operation
		var prevSnap *snapData

		for step := 0; step < operations; step++ {
			now += 1000

			req := generateRequest(rng, now, prevSnap, promiseIDs, []string{promiseIDs[0]})

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
			kind := req["kind"].(string)

			respBytes, err := srv.Apply(now, reqBytes)
			if err != nil {
				t.Fatalf("%s", failMsg("kind", "oracle_error", "seed", seed, "promises", len(promiseIDs), "step", step, "err", err))
			}

			// Use non-overlapping timestamps so Porcupine sees a strict serial order.
			ops = append(ops, porcupine.Operation{
				ClientId: 0,
				Input:    linInput{kind: kind, now: now, req: reqBytes},
				Call:     now*2 - 1,
				Output:   respBytes,
				Return:   now * 2,
			})

			prevSnap = snapDataFromOracle(srv, now)
		}

		if !porcupine.CheckOperations(model, ops) {
			t.Fatalf("%s", failMsg("kind", "linearizability", "variant", "sequential", "seed", seed, "promises", len(promiseIDs)))
		}

		ops = ops[:0]
	}
}

// ─── Concurrent test ──────────────────────────────────────────────────────────

// TestLinearizabilityConcurrent fires randomly interleaved handler operations
// through the cooperative runner and checks the resulting history with
// Porcupine. Multiple fibers run concurrently, yielding at LWT points, so
// Porcupine must find a valid serial ordering — violations indicate real bugs.
//
// Defaults: seed=<current timestamp>, iterations=1000, operations=250. Override
// via env vars RESONATE_TEST_SEED, RESONATE_TEST_ITERATIONS, RESONATE_TEST_OPERATIONS.
// To replay a specific failure: set SEED to the failing seed and ITERATIONS=1.
func TestLinearizabilityConcurrent(t *testing.T) {
	h := setupHandler(t)
	model := resonateModel()

	baseSeed := envInt64("RESONATE_TEST_SEED", time.Now().UnixNano())
	iterations := envInt("RESONATE_TEST_ITERATIONS", 1000)
	operations := envInt("RESONATE_TEST_OPERATIONS", 250)
	maxWorkers := envInt("RESONATE_TEST_WORKERS", 10)
	traceAlways := os.Getenv("RESONATE_TEST_TRACE") == "1"
	t.Logf("config: seed=%d iterations=%d operations=%d promises=%s workers=%d trace=%v", baseSeed, iterations, operations, promisesConfig(), maxWorkers, traceAlways)

	type opRecord struct {
		fiberID   string
		input     linInput
		callClock int64
		resultCh  <-chan []byte
	}

	for seed := baseSeed; seed < baseSeed+int64(iterations); seed++ {
		promiseIDs := pickPromisePool(seed)
		debugReset(t, h)

		sched := NewRunner(
			func(req []byte, yield func(string)) []byte {
				resp, _ := h.Handle(req, yield)
				return resp
			},
			seed,
		)
		sched.Trace = newRing()

		// Separate RNGs: one for the spawn-vs-tick decision, one for requests.
		interleavRng := rand.New(rand.NewSource(seed))
		reqRng := newRng(seed + 1000)

		var records []opRecord
		// All concurrent ops share a single now so Porcupine cannot rely on
		// monotonically increasing time as a free serialization escape hatch.
		// Tick ops still carry their own target time; only non-tick ops see now.
		now := int64(1_000_000)
		n := operations

		for n > 0 || sched.Active() {
			if n > 0 && sched.ActiveCount() < maxWorkers && (!sched.Active() || interleavRng.Intn(2) == 0) {
				req := generateRequest(reqRng, now, nil, promiseIDs, []string{promiseIDs[0]})

				// Tick ops carry their own target time; that time becomes the op's now.
				opNow := now
				if req["kind"] == "debug.tick" {
					if data, ok := req["data"].(map[string]any); ok {
						if tickTime, ok := data["time"].(int64); ok && tickTime > opNow {
							opNow = tickTime
						}
						data["time"] = opNow
					}
				}
				if head, ok := req["head"].(map[string]any); ok {
					head["resonate:debug_time"] = opNow
				}

				reqBytes, _ := json.Marshal(req)
				kind := req["kind"].(string)

				id, ch := sched.Spawn(reqBytes)
				records = append(records, opRecord{
					fiberID:   id,
					input:     linInput{kind: kind, now: opNow, req: reqBytes},
					callClock: sched.Clock,
					resultCh:  ch,
				})
				n--
			} else {
				sched.Tick()
			}
		}

		// All fibers done — collect results and build Porcupine history.
		ops := make([]porcupine.Operation, len(records))
		for i, r := range records {
			output := <-r.resultCh // buffered, non-blocking
			ops[i] = porcupine.Operation{
				ClientId: i,
				Input:    r.input,
				Call:     r.callClock,
				Output:   output,
				Return:   sched.done[r.fiberID],
			}
		}

		if traceAlways && sched.Trace != nil {
			if s := sched.Trace.render(); s != "" {
				t.Logf("%s", s)
			}
		}

		result, info := porcupine.CheckOperationsVerbose(model, ops, 0)
		if result == porcupine.Illegal {
			base := fmt.Sprintf("/tmp/linearizability-seed%d", seed)

			if err := porcupine.VisualizePath(model, info, base+".html"); err != nil {
				t.Logf("visualization error: %v", err)
			} else {
				t.Logf("HTML: %s.html", base)
			}

			writeLinTrace(t, base+".trace", "Illegal", model, info, seed, ops)

			if !traceAlways {
				if s := sched.Trace.render(); s != "" {
					t.Logf("%s", s)
				}
			}
			t.Fatalf("%s", failMsg("kind", "linearizability", "variant", "concurrent", "seed", seed, "promises", len(promiseIDs), "trace", base+".trace"))
		} else if traceAlways {
			base := fmt.Sprintf("/tmp/linearizability-seed%d", seed)
			writeLinTrace(t, base+".trace", "Ok", model, info, seed, ops)
		}
	}
}

// ─── Trace output ─────────────────────────────────────────────────────────────

func writeLinTrace(t *testing.T, path string, result string, model porcupine.Model, info porcupine.LinearizationInfo, seed int64, ops []porcupine.Operation) {
	t.Helper()
	sep := strings.Repeat("═", 60)

	sorted := make([]porcupine.Operation, len(ops))
	copy(sorted, ops)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Call < sorted[j].Call })

	// Build set of linearized op keys from the maximal partial linearization.
	partials := info.PartialLinearizationsOperations()
	linearized := make(map[string]bool)
	maxLen := 0
	for _, partition := range partials {
		for _, lin := range partition {
			if len(lin) > maxLen {
				maxLen = len(lin)
			}
			for _, op := range lin {
				key := fmt.Sprintf("%d:%d:%d", op.Call, op.Return, op.ClientId)
				linearized[key] = true
			}
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Linearizability Trace\n")
	fmt.Fprintf(&sb, "  seed=%d  ops=%d  time=%s\n", seed, len(ops), time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&sb, "  Result: %s  linearized=%d/%d\n\n", result, maxLen, len(ops))

	// Section 1: full history with ✗ markers
	fmt.Fprintf(&sb, "%s\n", sep)
	fmt.Fprintf(&sb, "FULL HISTORY (sorted by call clock)\n")
	fmt.Fprintf(&sb, "%s\n\n", sep)
	for i, op := range sorted {
		in := op.Input.(linInput)
		out := op.Output.([]byte)
		key := fmt.Sprintf("%d:%d:%d", op.Call, op.Return, op.ClientId)
		marker := "  "
		if !linearized[key] {
			marker = "✗ "
		}
		fmt.Fprintf(&sb, "%s%4d. [%6d..%6d] %s\n              -> %s\n",
			marker, i+1, op.Call, op.Return, in.req, out)
	}

	// Section 2: unlinearizable ops only
	fmt.Fprintf(&sb, "\n%s\n", sep)
	fmt.Fprintf(&sb, "UNLINEARIZABLE OPERATIONS\n")
	fmt.Fprintf(&sb, "%s\n\n", sep)
	unlin := 0
	for i, op := range sorted {
		key := fmt.Sprintf("%d:%d:%d", op.Call, op.Return, op.ClientId)
		if !linearized[key] {
			unlin++
			in := op.Input.(linInput)
			out := op.Output.([]byte)
			fmt.Fprintf(&sb, "  %4d. [%6d..%6d] %s\n              -> %s\n",
				i+1, op.Call, op.Return, in.req, out)
		}
	}
	if unlin == 0 {
		fmt.Fprintf(&sb, "  (none)\n")
	}

	// Section 3: maximal partial linearization
	fmt.Fprintf(&sb, "\n%s\n", sep)
	fmt.Fprintf(&sb, "MAXIMAL PARTIAL LINEARIZATION\n")
	fmt.Fprintf(&sb, "%s\n", sep)
	for p, partition := range partials {
		if len(partials) > 1 {
			fmt.Fprintf(&sb, "\n── Partition %d ──\n", p)
		}
		for li, lin := range partition {
			if len(partition) > 1 {
				fmt.Fprintf(&sb, "\n  Linearization %d (%d ops):\n", li+1, len(lin))
			} else {
				fmt.Fprintf(&sb, "\nLinearization (%d ops):\n", len(lin))
			}
			for j, op := range lin {
				desc := model.DescribeOperation(op.Input, op.Output)
				fmt.Fprintf(&sb, "  %3d. [client %d] %s\n", j+1, op.ClientId, desc)
			}
		}
	}

	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Logf("trace write error: %v", err)
	} else {
		t.Logf("trace: %s", path)
	}
}
