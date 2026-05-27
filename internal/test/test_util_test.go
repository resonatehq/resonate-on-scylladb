package test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/resonateio/resonate-on-scylladb/internal/base"
	"github.com/resonateio/resonate-on-scylladb/internal/core"
	"github.com/resonateio/resonate-on-scylladb/internal/dbms"
)

func setupHandler(t *testing.T) *core.Handler {
	t.Helper()
	cfg := dbms.Config{
		Hosts:        testHosts(),
		Port:         testPort(),
		Username:     os.Getenv("SCYLLADB_USERNAME"),
		Password:     os.Getenv("SCYLLADB_PASSWORD"),
		TLS:          os.Getenv("SCYLLADB_TLS") == "1" || strings.EqualFold(os.Getenv("SCYLLADB_TLS"), "true"),
		TLSInsecure:  os.Getenv("SCYLLADB_TLS_INSECURE") == "1" || strings.EqualFold(os.Getenv("SCYLLADB_TLS_INSECURE"), "true"),
		Keyspace:     os.Getenv("SCYLLADB_KEYSPACE"),
		CreateSchema: true,
		Replication:  os.Getenv("SCYLLADB_REPLICATION"),
	}
	session, err := dbms.Connect(cfg)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	t.Cleanup(func() { session.Close() })
	rec := base.NewRecorder()
	return &core.Handler{
		Session:        session,
		Host:           strings.Join(cfg.Hosts, ","),
		BucketWidth:    3_600_000,
		BucketLookback: 1,
		Debug:          true,
		Dispatcher:     rec,
		Recorder:       rec,
	}
}

// testHosts resolves the seed list for tests. SCYLLADB_HOSTS (comma-separated,
// host or host:port) wins; otherwise SCYLLADB_HOST (single host) with default
// port; otherwise localhost.
func testHosts() []string {
	if v := os.Getenv("SCYLLADB_HOSTS"); v != "" {
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	}
	host := os.Getenv("SCYLLADB_HOST")
	if host == "" {
		host = "localhost"
	}
	return []string{host}
}

// testPort returns the SCYLLADB_PORT env var as int, or 0 (let dbms default
// to 9042).
func testPort() int {
	v := os.Getenv("SCYLLADB_PORT")
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

// envInt returns the env var parsed as a positive int, or fallback if unset,
// empty, malformed, or non-positive.
func envInt(name string, fallback int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// envInt64 returns the env var parsed as int64, or fallback if unset, empty,
// or malformed. Unlike envInt, zero and negative values are accepted (seeds
// can legitimately be any int64).
func envInt64(name string, fallback int64) int64 {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

// promisesConfig renders the promise-pool mode for the config log line.
// "N" if RESONATE_TEST_PROMISES is set, else "auto[1,10]" indicating
// per-seed pseudo-random sizing.
func promisesConfig() string {
	n := envInt("RESONATE_TEST_PROMISES", 0)
	if n == 0 {
		return "auto[1,10]"
	}
	return strconv.Itoa(n)
}

// pickPromisePool derives the promise-ID pool for the given seed. The pool
// size is the scope axis of the fuzz tests — smaller pools concentrate
// contention on fewer rows, larger pools spread it out.
//
// If RESONATE_TEST_PROMISES is set, that value fixes the pool size for every
// seed (useful for bug-hunting at a specific scope). Otherwise the size is
// pseudo-randomly drawn from [1, 10] using a seed-derived RNG, so every fuzz
// run naturally sweeps the contention spectrum and replay reproduces the
// same pool deterministically (same seed → same pool size).
//
// The pool is always p-0..p-(N-1).
func pickPromisePool(seed int64) []string {
	n := envInt("RESONATE_TEST_PROMISES", 0)
	if n == 0 {
		r := newRng(seed)
		n = 1 + r.choice(10)
	}
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("p-%d", i)
	}
	return out
}

// failMsg renders a structured key=value failure message with an automatic
// at=<file>:<line> prefix from the caller. String values are %q-quoted so
// embedded spaces and newlines do not break the format.
//
// kvs are key/value pairs: kvs[0]=key, kvs[1]=value, kvs[2]=key, ...
// An odd-length kvs ends with `<missing>` for the dangling key.
//
// Embed in t.Errorf / t.Fatalf rather than using directly so the test
// framework reports the assertion site:
//
//	t.Errorf("%s", failMsg("seed", seed, "kind", "linearizability"))
func failMsg(kvs ...any) string {
	_, file, line, _ := runtime.Caller(1)
	var b strings.Builder
	fmt.Fprintf(&b, "FAIL at=%s:%d", filepath.Base(file), line)
	for i := 0; i < len(kvs); i += 2 {
		if i+1 >= len(kvs) {
			fmt.Fprintf(&b, " %v=<missing>", kvs[i])
			break
		}
		switch v := kvs[i+1].(type) {
		case string:
			fmt.Fprintf(&b, " %v=%q", kvs[i], v)
		case error:
			fmt.Fprintf(&b, " %v=%q", kvs[i], v.Error())
		case []byte:
			fmt.Fprintf(&b, " %v=%q", kvs[i], string(v))
		default:
			fmt.Fprintf(&b, " %v=%v", kvs[i], v)
		}
	}
	return b.String()
}

func debugReset(t *testing.T, h *core.Handler) {
	t.Helper()
	start := time.Now()
	raw, _ := json.Marshal(struct {
		Kind string         `json:"kind"`
		Head map[string]any `json:"head"`
		Data struct{}       `json:"data"`
	}{Kind: "debug.reset", Head: map[string]any{
		"corrId":  fmt.Sprintf("corr-%d", rand.Int63()),
		"version": "1.0.0",
	}})
	resp, err := h.Handle(raw, func(string) {})
	if err != nil {
		t.Fatalf("debug.reset: %v", err)
	}
	var res struct {
		Head struct {
			Status int `json:"status"`
		} `json:"head"`
	}
	if jsonErr := json.Unmarshal(resp, &res); jsonErr != nil || res.Head.Status != 200 {
		t.Fatalf("debug.reset: non-200 status: %s", resp)
	}
	t.Logf("debug.reset: %s", time.Since(start))
}

// killedResponse builds a 500 JSON response for a request that was killed at a
// yield point. The envelope is parsed to echo back kind/corrId/version so the
// response is structurally identical to a real handler 500.
func killedResponse(req []byte) []byte {
	var env struct {
		Kind string `json:"kind"`
		Head struct {
			CorrID  string `json:"corrId,omitempty"`
			Version string `json:"version,omitempty"`
		} `json:"head"`
	}
	json.Unmarshal(req, &env)
	b, _ := json.Marshal(map[string]any{
		"kind": env.Kind,
		"head": map[string]any{
			"corrId":  env.Head.CorrID,
			"status":  500,
			"version": env.Head.Version,
		},
		"data": "killed",
	})
	return b
}

func diffStatusOf(m map[string]any) int {
	if head, ok := m["head"].(map[string]any); ok {
		if s, ok := head["status"].(float64); ok {
			return int(s)
		}
	}
	return 0
}

func jsonEqual(a, b []byte) bool {
	var av, bv any
	json.Unmarshal(a, &av)
	json.Unmarshal(b, &bv)
	ab, _ := json.Marshal(av)
	bb, _ := json.Marshal(bv)
	return string(ab) == string(bb)
}
