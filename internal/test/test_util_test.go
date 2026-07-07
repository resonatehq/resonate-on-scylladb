package test

import (
	"encoding/json"
	"flag"
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

var (
	testScyllaHosts       = stringArrayFlag{values: []string{"localhost"}}
	testScyllaPort        int
	testScyllaUsername    string
	testScyllaPassword    string
	testScyllaTLS         bool
	testScyllaTLSInsecure bool
	testScyllaKeyspace    string
	testScyllaReplication string
)

func init() {
	flag.Var(&testScyllaHosts, "scylladb-host", "ScyllaDB seed host (host or host:port); repeat for multiple seeds")
	flag.IntVar(&testScyllaPort, "scylladb-port", 0, "default CQL port for seeds without an explicit port and for gossip-discovered peers (0 = let gocql default to 9042)")
	flag.StringVar(&testScyllaUsername, "scylladb-username", "", "ScyllaDB username")
	flag.StringVar(&testScyllaPassword, "scylladb-password", "", "ScyllaDB password")
	flag.BoolVar(&testScyllaTLS, "scylladb-tls", false, "enable TLS")
	flag.BoolVar(&testScyllaTLSInsecure, "scylladb-tls-insecure", false, "skip certificate verification (only honored when -scylladb-tls is set)")
	flag.StringVar(&testScyllaKeyspace, "scylladb-keyspace", "", "keyspace name (defaults to \"resonate\")")
	flag.StringVar(&testScyllaReplication, "scylladb-replication", "", "replication fragment for CREATE KEYSPACE")
}

func setupHandler(t *testing.T) *core.Handler {
	t.Helper()
	if !testFlagChanged("scylladb-host") && os.Getenv("SCYLLADB_HOST") == "" && os.Getenv("SCYLLADB_HOSTS") == "" {
		t.Skip("skipping: no ScyllaDB host configured (set SCYLLADB_HOST or pass -scylladb-host)")
	}
	cfg := dbms.Config{
		Hosts:        testHosts(),
		Port:         testPort(),
		Username:     testStringFlag("scylladb-username", "SCYLLADB_USERNAME", testScyllaUsername),
		Password:     testStringFlag("scylladb-password", "SCYLLADB_PASSWORD", testScyllaPassword),
		TLS:          testBoolFlag("scylladb-tls", "SCYLLADB_TLS", testScyllaTLS),
		TLSInsecure:  testBoolFlag("scylladb-tls-insecure", "SCYLLADB_TLS_INSECURE", testScyllaTLSInsecure),
		Keyspace:     testStringFlag("scylladb-keyspace", "SCYLLADB_KEYSPACE", testScyllaKeyspace),
		CreateSchema: true,
		Replication:  testStringFlag("scylladb-replication", "SCYLLADB_REPLICATION", testScyllaReplication),
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

// testHosts resolves the seed list for tests. The repeatable -scylladb-host
// flag wins; otherwise SCYLLADB_HOSTS, SCYLLADB_HOST, then localhost.
func testHosts() []string {
	if testScyllaHosts.changed {
		return testScyllaHosts.values
	}
	if v := os.Getenv("SCYLLADB_HOSTS"); v != "" {
		return splitScyllaHosts(v)
	}
	host := os.Getenv("SCYLLADB_HOST")
	if host == "" {
		return testScyllaHosts.values
	}
	return []string{host}
}

// testPort returns -scylladb-port, SCYLLADB_PORT, or 0 (let dbms default to 9042).
func testPort() int {
	if testFlagChanged("scylladb-port") {
		return testScyllaPort
	}
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

func testStringFlag(flagName, envName, flagValue string) string {
	if testFlagChanged(flagName) {
		return flagValue
	}
	return os.Getenv(envName)
}

func testBoolFlag(flagName, envName string, flagValue bool) bool {
	if testFlagChanged(flagName) {
		return flagValue
	}
	return envBool(envName)
}

func testFlagChanged(name string) bool {
	changed := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			changed = true
		}
	})
	return changed
}

func envBool(name string) bool {
	v := os.Getenv(name)
	return v == "1" || strings.EqualFold(v, "true")
}

func splitScyllaHosts(v string) []string {
	parts := strings.Split(v, ",")
	hosts := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			hosts = append(hosts, p)
		}
	}
	return hosts
}

type stringArrayFlag struct {
	values  []string
	changed bool
}

func (f *stringArrayFlag) Set(v string) error {
	if !f.changed {
		f.values = nil
		f.changed = true
	}
	f.values = append(f.values, strings.TrimSpace(v))
	return nil
}

func (f *stringArrayFlag) String() string {
	return strings.Join(f.values, ",")
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

// pickPromisePool derives the promise-ID pool for the given seed. IDs use
// dot-structured naming (e.g. "a", "a.0", "b", "b.0", "b.1") so multiple IDs
// can share an origin, enabling cross-origin validation paths (callbacks,
// task.suspend) to be exercised by the diff tests.
//
// If RESONATE_TEST_PROMISES is set, that value fixes the total pool size for
// every seed. Otherwise the size is pseudo-randomly drawn from [1, 10]. In
// either case the number of origins (1-3) is drawn from the seeded RNG so the
// same seed → same pool.
func pickPromisePool(seed int64) []string {
	r := newRng(seed)
	n := envInt("RESONATE_TEST_PROMISES", 0)
	if n == 0 {
		n = 1 + r.choice(10)
	}

	maxOrigins := n
	if maxOrigins > 3 {
		maxOrigins = 3
	}
	numOrigins := 1 + r.choice(maxOrigins) // 1..min(n,3)

	roots := []string{"a", "b", "c"}
	counts := make([]int, numOrigins)
	for i := range counts {
		counts[i] = 1
	}
	for i := 0; i < n-numOrigins; i++ {
		counts[i%numOrigins]++
	}

	var out []string
	for i := 0; i < numOrigins; i++ {
		out = append(out, roots[i])
		for j := 0; j < counts[i]-1; j++ {
			out = append(out, fmt.Sprintf("%s.%d", roots[i], j))
		}
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
