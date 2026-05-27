package dbms

import (
	"crypto/tls"
	_ "embed"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

//go:embed schema.cql
var cql string

// DefaultKeyspace is the keyspace name used when Config.Keyspace is empty.
const DefaultKeyspace = "resonate"

// DefaultReplication is used when Config.Replication is empty. Suitable for
// local single-node clusters; cloud/multi-node callers must override.
const DefaultReplication = "{'class': 'SimpleStrategy', 'replication_factor': 1}"

// Config controls how dbms connects to ScyllaDB and whether it manages schema.
type Config struct {
	// Hosts is the seed list. Required. Entries may be bare host or host:port.
	Hosts []string

	// Port is the default CQL port for hosts without an explicit port AND for
	// nodes discovered via gossip. Defaults to 9042 (plain) when zero. Set to
	// 9142 for TLS-enabled clusters.
	Port int

	// Auth (optional). Both must be set together.
	Username string
	Password string

	// TLS toggles TLS. Hostname verification is enabled unless TLSInsecure
	// is also true.
	TLS bool

	// TLSInsecure disables certificate verification. Only honored when TLS
	// is true. Use for dev clusters with self-signed/private-CA certs.
	TLSInsecure bool

	// Keyspace defaults to DefaultKeyspace.
	Keyspace string

	// CreateSchema, when true, drops and recreates the keyspace on connect
	// and applies the embedded table CQL. Intended for debug/test runs.
	// When false, Connect skips all DDL and assumes schema is already in place.
	CreateSchema bool

	// Replication is the CQL fragment used in CREATE KEYSPACE WITH replication = ...
	// Only honored when CreateSchema is true. Defaults to DefaultReplication.
	Replication string
}

// Connect opens a session to the configured cluster. If cfg.CreateSchema is
// true, it drops and recreates the keyspace and applies the table schema.
// Otherwise it just opens a keyspace-bound session — production assumes schema
// has been provisioned out of band.
func Connect(cfg Config) (*gocql.Session, error) {
	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("dbms.Connect: at least one host required")
	}
	keyspace := cfg.Keyspace
	if keyspace == "" {
		keyspace = DefaultKeyspace
	}

	if cfg.CreateSchema {
		repl := cfg.Replication
		if repl == "" {
			repl = DefaultReplication
		}

		// Phase 1: bootstrap session, no keyspace, recreate keyspace.
		bootstrap, err := newCluster(cfg, "").CreateSession()
		if err != nil {
			return nil, fmt.Errorf("bootstrap connect: %w", err)
		}
		if err := bootstrap.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec(); err != nil {
			bootstrap.Close()
			return nil, fmt.Errorf("drop keyspace: %w", err)
		}
		if err := bootstrap.Query(
			fmt.Sprintf("CREATE KEYSPACE %s WITH replication = %s", keyspace, repl),
		).Exec(); err != nil {
			bootstrap.Close()
			return nil, fmt.Errorf("create keyspace: %w", err)
		}
		bootstrap.Close()
	}

	// Phase 2: keyspace-bound session.
	session, err := newCluster(cfg, keyspace).CreateSession()
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	if cfg.CreateSchema {
		for _, stmt := range statements() {
			if err := session.Query(stmt).Exec(); err != nil {
				session.Close()
				return nil, fmt.Errorf("apply schema: %w", err)
			}
		}
	}

	return session, nil
}

// newCluster builds a *gocql.ClusterConfig with auth and TLS applied. If
// keyspace is empty, no keyspace is bound (used for the bootstrap session).
func newCluster(cfg Config, keyspace string) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.NumConns = 16
	if cfg.Port != 0 {
		cluster.Port = cfg.Port
	}
	if keyspace != "" {
		cluster.Keyspace = keyspace
	}
	if cfg.Username != "" || cfg.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}
	if cfg.TLS {
		cluster.SslOpts = &gocql.SslOptions{
			Config:                 &tls.Config{InsecureSkipVerify: cfg.TLSInsecure},
			EnableHostVerification: !cfg.TLSInsecure,
		}
	}
	return cluster
}

// statements splits schema.cql into individual CQL statements,
// skipping the CREATE KEYSPACE and USE lines (handled separately).
// Comment lines (-- ...) are stripped from each statement before
// classification so that leading comments do not confuse the prefix checks.
func statements() []string {
	var out []string
	for _, s := range strings.Split(cql, ";") {
		s = stripCQLComments(s)
		if s == "" {
			continue
		}
		upper := strings.ToUpper(s)
		if strings.HasPrefix(upper, "CREATE KEYSPACE") || strings.HasPrefix(upper, "USE ") {
			continue
		}
		out = append(out, s)
	}
	return out
}

// stripCQLComments removes CQL line comments (-- ...) and returns the
// remaining text trimmed of surrounding whitespace.
func stripCQLComments(s string) string {
	var lines []string
	for _, line := range strings.Split(s, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			lines = append(lines, line)
		}
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}
