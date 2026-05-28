package cmds

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func newTestCmd(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "serve"}
	registerServeFlags(cmd)
	return cmd
}

// Test 1: defaults load when no flags, env vars, or config file are provided.
func TestServeConfigDefaults(t *testing.T) {
	cmd := newTestCmd(t)
	cfg, warnings, err := loadServeConfig(cmd, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}
	if cfg.Server.Addr != ":8001" {
		t.Errorf("Server.Addr = %q, want :8001", cfg.Server.Addr)
	}
	if cfg.Server.Debug {
		t.Error("Server.Debug = true, want false")
	}
	if len(cfg.ScyllaDB.Hosts) != 1 || cfg.ScyllaDB.Hosts[0] != "localhost" {
		t.Errorf("ScyllaDB.Hosts = %v, want [localhost]", cfg.ScyllaDB.Hosts)
	}
	if cfg.ScyllaDB.Port != 0 {
		t.Errorf("ScyllaDB.Port = %d, want 0", cfg.ScyllaDB.Port)
	}
	if cfg.ScyllaDB.TLS.Enabled {
		t.Error("ScyllaDB.TLS.Enabled = true, want false")
	}
	if cfg.ScyllaDB.TLS.Insecure {
		t.Error("ScyllaDB.TLS.Insecure = true, want false")
	}
	if cfg.Timeouts.BucketWidth != time.Hour {
		t.Errorf("Timeouts.BucketWidth = %v, want 1h", cfg.Timeouts.BucketWidth)
	}
	if cfg.Timeouts.BucketLookback != 1 {
		t.Errorf("Timeouts.BucketLookback = %d, want 1", cfg.Timeouts.BucketLookback)
	}
	if cfg.Timeouts.Shards != 1 {
		t.Errorf("Timeouts.Shards = %d, want 1", cfg.Timeouts.Shards)
	}
	if cfg.Worker.TTL != 15*time.Second {
		t.Errorf("Worker.TTL = %v, want 15s", cfg.Worker.TTL)
	}
	if cfg.Worker.TickInterval != time.Second {
		t.Errorf("Worker.TickInterval = %v, want 1s", cfg.Worker.TickInterval)
	}
}

// Test 2: config file values populate every config group.
func TestServeConfigFromFile(t *testing.T) {
	yaml := `
server:
  addr: ":9000"
  debug: true

scylladb:
  hosts:
    - db-0
    - db-1
  port: 9142
  username: admin
  password: secret
  keyspace: testks
  replication: "{'class': 'SimpleStrategy', 'replication_factor': 1}"
  tls:
    enabled: true
    insecure: true

timeouts:
  bucket-width: 30m
  bucket-lookback: 2
  shards: 4

worker:
  ttl: 30s
  tick-interval: 5s
`
	path := writeConfigFile(t, yaml)
	cmd := newTestCmd(t)
	cfg, _, err := loadServeConfig(cmd, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.Addr != ":9000" {
		t.Errorf("Server.Addr = %q, want :9000", cfg.Server.Addr)
	}
	if !cfg.Server.Debug {
		t.Error("Server.Debug = false, want true")
	}
	if len(cfg.ScyllaDB.Hosts) != 2 || cfg.ScyllaDB.Hosts[0] != "db-0" || cfg.ScyllaDB.Hosts[1] != "db-1" {
		t.Errorf("ScyllaDB.Hosts = %v, want [db-0 db-1]", cfg.ScyllaDB.Hosts)
	}
	if cfg.ScyllaDB.Port != 9142 {
		t.Errorf("ScyllaDB.Port = %d, want 9142", cfg.ScyllaDB.Port)
	}
	if cfg.ScyllaDB.Username != "admin" {
		t.Errorf("ScyllaDB.Username = %q, want admin", cfg.ScyllaDB.Username)
	}
	if cfg.ScyllaDB.Password != "secret" {
		t.Errorf("ScyllaDB.Password = %q, want secret", cfg.ScyllaDB.Password)
	}
	if cfg.ScyllaDB.Keyspace != "testks" {
		t.Errorf("ScyllaDB.Keyspace = %q, want testks", cfg.ScyllaDB.Keyspace)
	}
	if !cfg.ScyllaDB.TLS.Enabled {
		t.Error("ScyllaDB.TLS.Enabled = false, want true")
	}
	if !cfg.ScyllaDB.TLS.Insecure {
		t.Error("ScyllaDB.TLS.Insecure = false, want true")
	}
	if cfg.Timeouts.BucketWidth != 30*time.Minute {
		t.Errorf("Timeouts.BucketWidth = %v, want 30m", cfg.Timeouts.BucketWidth)
	}
	if cfg.Timeouts.BucketLookback != 2 {
		t.Errorf("Timeouts.BucketLookback = %d, want 2", cfg.Timeouts.BucketLookback)
	}
	if cfg.Timeouts.Shards != 4 {
		t.Errorf("Timeouts.Shards = %d, want 4", cfg.Timeouts.Shards)
	}
	if cfg.Worker.TTL != 30*time.Second {
		t.Errorf("Worker.TTL = %v, want 30s", cfg.Worker.TTL)
	}
	if cfg.Worker.TickInterval != 5*time.Second {
		t.Errorf("Worker.TickInterval = %v, want 5s", cfg.Worker.TickInterval)
	}
}

// Test 3: environment variables override config file values.
func TestServeConfigEnvOverridesFile(t *testing.T) {
	yaml := `
scylladb:
  port: 9042
timeouts:
  shards: 2
`
	path := writeConfigFile(t, yaml)
	t.Setenv("SCYLLADB_PORT", "9999")
	t.Setenv("TIMEOUTS_SHARDS", "8")

	cmd := newTestCmd(t)
	cfg, _, err := loadServeConfig(cmd, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ScyllaDB.Port != 9999 {
		t.Errorf("ScyllaDB.Port = %d, want 9999 (env override)", cfg.ScyllaDB.Port)
	}
	if cfg.Timeouts.Shards != 8 {
		t.Errorf("Timeouts.Shards = %d, want 8 (env override)", cfg.Timeouts.Shards)
	}
}

// Test 4: CLI flags override environment variables.
func TestServeConfigFlagOverridesEnv(t *testing.T) {
	t.Setenv("SCYLLADB_PORT", "9999")
	t.Setenv("TIMEOUTS_SHARDS", "8")

	cmd := newTestCmd(t)
	must(t, cmd.Flags().Set("scylladb-port", "5555"))
	must(t, cmd.Flags().Set("shards", "3"))

	cfg, _, err := loadServeConfig(cmd, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ScyllaDB.Port != 5555 {
		t.Errorf("ScyllaDB.Port = %d, want 5555 (flag override)", cfg.ScyllaDB.Port)
	}
	if cfg.Timeouts.Shards != 3 {
		t.Errorf("Timeouts.Shards = %d, want 3 (flag override)", cfg.Timeouts.Shards)
	}
}

// Test 5: SCYLLADB_HOSTS=node-0,node-1 decodes into two hosts.
func TestServeConfigHostsEnvSplitOnComma(t *testing.T) {
	t.Setenv("SCYLLADB_HOSTS", "node-0,node-1")

	cmd := newTestCmd(t)
	cfg, _, err := loadServeConfig(cmd, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.ScyllaDB.Hosts) != 2 || cfg.ScyllaDB.Hosts[0] != "node-0" || cfg.ScyllaDB.Hosts[1] != "node-1" {
		t.Errorf("ScyllaDB.Hosts = %v, want [node-0 node-1]", cfg.ScyllaDB.Hosts)
	}
}

// Test 6: --config pointing to a missing file returns an error.
func TestServeConfigMissingExplicitFile(t *testing.T) {
	cmd := newTestCmd(t)
	_, _, err := loadServeConfig(cmd, "/nonexistent/path/resonate.yaml")
	if err == nil {
		t.Error("expected error for missing config file, got nil")
	}
}

// Test 7: omitting --config with no ./resonate.yaml present succeeds (uses defaults).
func TestServeConfigNoFileNoConfig(t *testing.T) {
	dir := t.TempDir()
	origDir := chdirTemp(t, dir)
	defer os.Chdir(origDir) //nolint:errcheck

	cmd := newTestCmd(t)
	cfg, _, err := loadServeConfig(cmd, "")
	if err != nil {
		t.Fatalf("unexpected error with no config file present: %v", err)
	}
	if cfg.Server.Addr != ":8001" {
		t.Errorf("Server.Addr = %q, want :8001", cfg.Server.Addr)
	}
}

// Test 8: omitting --config with a ./resonate.yaml present loads it.
func TestServeConfigAutoLoad(t *testing.T) {
	dir := t.TempDir()
	yaml := "server:\n  addr: \":7777\"\n"
	if err := os.WriteFile(filepath.Join(dir, "resonate.yaml"), []byte(yaml), 0o600); err != nil {
		t.Fatalf("write resonate.yaml: %v", err)
	}

	origDir := chdirTemp(t, dir)
	defer os.Chdir(origDir) //nolint:errcheck

	cmd := newTestCmd(t)
	cfg, _, err := loadServeConfig(cmd, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.Addr != ":7777" {
		t.Errorf("Server.Addr = %q, want :7777 (from auto-loaded resonate.yaml)", cfg.Server.Addr)
	}
}

// Test 9: validation rejects non-positive bucket-width.
func TestServeConfigValidationBucketWidth(t *testing.T) {
	yaml := "timeouts:\n  bucket-width: 0s\n"
	path := writeConfigFile(t, yaml)
	cmd := newTestCmd(t)
	_, _, err := loadServeConfig(cmd, path)
	if err == nil {
		t.Error("expected error for bucket-width=0, got nil")
	}
}

// Test 10: validation rejects negative bucket-lookback.
func TestServeConfigValidationBucketLookback(t *testing.T) {
	cmd := newTestCmd(t)
	must(t, cmd.Flags().Set("bucket-lookback", "-1"))
	_, _, err := loadServeConfig(cmd, "")
	if err == nil {
		t.Error("expected error for bucket-lookback=-1, got nil")
	}
}

// Test 11: validation rejects non-positive shards, worker.ttl, worker.tick-interval.
func TestServeConfigValidationNonPositive(t *testing.T) {
	cases := []struct {
		flag string
		val  string
	}{
		{"shards", "0"},
		{"worker-ttl", "0s"},
		{"worker-tick-interval", "0s"},
	}
	for _, tc := range cases {
		t.Run(tc.flag, func(t *testing.T) {
			cmd := newTestCmd(t)
			must(t, cmd.Flags().Set(tc.flag, tc.val))
			_, _, err := loadServeConfig(cmd, "")
			if err == nil {
				t.Errorf("expected error for %s=%s, got nil", tc.flag, tc.val)
			}
		})
	}
}

// Test 12: worker.tick-interval >= worker.ttl loads successfully and surfaces a warning.
func TestServeConfigTickIntervalWarning(t *testing.T) {
	cmd := newTestCmd(t)
	must(t, cmd.Flags().Set("worker-ttl", "10s"))
	must(t, cmd.Flags().Set("worker-tick-interval", "10s"))

	_, warnings, err := loadServeConfig(cmd, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) == 0 {
		t.Error("expected warning for tick-interval >= worker-ttl, got none")
	}
}

func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	return path
}

func chdirTemp(t *testing.T, dir string) string {
	t.Helper()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	return orig
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
